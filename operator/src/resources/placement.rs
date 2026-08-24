//! Explicit provider traffic-placement calculations.
//!
//! Placement is deliberately separate from Grid scoring.  The inputs are the
//! already validated provider-level EPP signal and configured capacity; this
//! module never consumes the opaque scoring total.

use std::collections::{BTreeMap, HashMap};

use crate::crd::grid_network::PressureWeightedConfig;

/// One provider's typed pressure input for a single calculation.
#[derive(Clone, Debug)]
pub(crate) struct PressureInput<'a> {
    /// Stable provider identity used for state and deterministic ties.
    pub(crate) stable_id: &'a str,
    /// Existing routing selection group.
    pub(crate) selection_group: u32,
    /// Relative configured provider capacity.
    pub(crate) capacity_weight: u32,
    /// Normalized signal: queue utilization or KV-cache utilization.
    pub(crate) pressure: Option<f64>,
}

/// Cross-reconcile state for damping pressure-derived weights.
#[derive(Clone, Debug, Default)]
pub(crate) struct PlacementState {
    /// Smoothed availability by stable provider identity.
    availability: HashMap<String, f64>,
    /// Last published effective weight by stable provider identity.
    weights: HashMap<String, u32>,
}

/// Calculate deterministic, group-local pressure weights.
#[expect(
    clippy::too_many_lines,
    reason = "pressure calculation keeps validation, smoothing, normalization, and thresholding together"
)]
pub(crate) fn pressure_weights(
    inputs: &[PressureInput<'_>],
    config: &PressureWeightedConfig,
    state: &mut PlacementState,
) -> Result<HashMap<String, u32>, String> {
    validate_config(config)?;
    let floor = f64::from(config.availability_floor_percent) / 100.0;
    let mut next_availability = state.availability.clone();
    let mut by_group: BTreeMap<u32, Vec<(usize, f64)>> = BTreeMap::new();

    for (index, input) in inputs.iter().enumerate() {
        let pressure = input
            .pressure
            .filter(|value| value.is_finite() && (0.0..=1.0).contains(value))
            .ok_or_else(|| format!("provider {} has no fresh valid pressure sample", input.stable_id))?;
        let availability = (1.0 - pressure).max(floor);
        let previous = next_availability.get(input.stable_id).copied().unwrap_or(availability);
        let smoothed = previous + config.smoothing_factor * (availability - previous);
        next_availability.insert(input.stable_id.to_owned(), smoothed);
        let raw = f64::from(input.capacity_weight.max(1)) * smoothed;
        by_group.entry(input.selection_group).or_default().push((index, raw));
    }

    let mut proposed = HashMap::new();
    for members in by_group.values() {
        let total: f64 = members.iter().map(|(_, raw)| *raw).sum();
        if !total.is_finite() || total <= 0.0 {
            return Err("pressure weights have no positive finite capacity".to_owned());
        }
        let target = u64::from(config.maximum_weight);
        let mut floors = Vec::with_capacity(members.len());
        let mut remainder_order = Vec::with_capacity(members.len());
        let mut allocated = 0_u64;
        for (index, raw) in members {
            let exact = (*raw / total) * f64::from(config.maximum_weight);
            #[expect(
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss,
                reason = "exact is finite, non-negative, and bounded by maximum_weight <= 1000"
            )]
            let floor_value = exact.floor() as u64;
            allocated = allocated.saturating_add(floor_value);
            floors.push((*index, floor_value));
            #[expect(
                clippy::cast_precision_loss,
                reason = "floor_value is bounded by maximum_weight <= 1000"
            )]
            let remainder = exact - floor_value as f64;
            remainder_order.push((*index, remainder));
        }
        remainder_order.sort_by(|(left_index, left_rem), (right_index, right_rem)| {
            let left = inputs.get(*left_index).map_or("", |input| input.stable_id);
            let right = inputs.get(*right_index).map_or("", |input| input.stable_id);
            right_rem
                .total_cmp(left_rem)
                .then_with(|| left.cmp(right).then(left_index.cmp(right_index)))
        });
        let mut remaining = target.saturating_sub(allocated);
        let remainder_count = usize::try_from(remaining).unwrap_or(usize::MAX);
        let remainder_indexes: std::collections::HashSet<usize> = remainder_order
            .iter()
            .take(remainder_count)
            .map(|(index, _)| *index)
            .collect();
        for (index, value) in floors {
            let mut rounded = value;
            if remainder_indexes.contains(&index) {
                rounded = rounded.saturating_add(1);
                remaining = remaining.saturating_sub(1);
            }
            let provider = inputs
                .get(index)
                .ok_or_else(|| "pressure weight candidate index out of bounds".to_owned())?;
            let bounded = rounded.clamp(u64::from(config.minimum_weight), u64::from(config.maximum_weight));
            let bounded = u32::try_from(bounded).map_err(|error| format!("pressure weight exceeds u32: {error}"))?;
            proposed.insert(provider.stable_id.to_owned(), bounded);
        }
        // Distribute any remaining units after minimum/maximum clamping using
        // stable candidate order. This keeps output deterministic.
        for (index, _) in remainder_order {
            if remaining == 0 {
                break;
            }
            let provider = inputs
                .get(index)
                .ok_or_else(|| "pressure weight candidate index out of bounds".to_owned())?;
            let Some(current) = proposed.get(provider.stable_id).copied() else {
                continue;
            };
            if current < config.maximum_weight {
                proposed.insert(provider.stable_id.to_owned(), current + 1);
                remaining -= 1;
            }
        }
    }

    let threshold = f64::from(config.change_threshold_percent) / 100.0;
    let material = proposed.iter().any(|(id, weight)| {
        state.weights.get(id).is_none_or(|old| {
            let delta = (f64::from(*weight) - f64::from(*old)).abs() / f64::from((*old).max(1));
            delta >= threshold
        })
    }) || state.weights.keys().any(|id| !proposed.contains_key(id));
    if !material {
        state.availability = next_availability;
        return Ok(state.weights.clone());
    }
    state.availability = next_availability;
    state.weights.clone_from(&proposed);
    Ok(proposed)
}

/// Validate numeric pressure-placement controls before calculating weights.
fn validate_config(config: &PressureWeightedConfig) -> Result<(), String> {
    if config.minimum_weight == 0
        || config.maximum_weight == 0
        || config.minimum_weight > config.maximum_weight
        || config.maximum_weight > 1000
    {
        return Err("pressure weight bounds must satisfy 1 <= minimum <= maximum <= 1000".to_owned());
    }
    if !(0.0..=1.0).contains(&config.smoothing_factor) || config.smoothing_factor == 0.0 {
        return Err("smoothingFactor must be finite and in (0, 1]".to_owned());
    }
    if !(1..=100).contains(&config.availability_floor_percent) {
        return Err("availabilityFloorPercent must be between 1 and 100".to_owned());
    }
    Ok(())
}

#[cfg(test)]
#[expect(
    clippy::indexing_slicing,
    clippy::unwrap_used,
    reason = "placement unit tests use direct map assertions"
)]
mod tests {
    use super::*;
    use crate::crd::grid_network::{PressureSignal, PressureWeightedConfig};

    fn config() -> PressureWeightedConfig {
        PressureWeightedConfig {
            signal: PressureSignal::QueueDepth,
            minimum_weight: 1,
            maximum_weight: 1000,
            availability_floor_percent: 5,
            smoothing_factor: 1.0,
            change_threshold_percent: 0,
        }
    }

    #[test]
    fn pressure_and_capacity_produce_inverse_group_local_weights() {
        let inputs = [
            PressureInput {
                stable_id: "a",
                selection_group: 0,
                capacity_weight: 1,
                pressure: Some(0.0),
            },
            PressureInput {
                stable_id: "b",
                selection_group: 0,
                capacity_weight: 1,
                pressure: Some(0.5),
            },
        ];
        let weights = pressure_weights(&inputs, &config(), &mut PlacementState::default()).unwrap();
        assert_eq!(weights["a"], 667);
        assert_eq!(weights["b"], 333);
    }

    #[test]
    fn input_order_does_not_change_weights() {
        let a = [
            PressureInput {
                stable_id: "a",
                selection_group: 0,
                capacity_weight: 2,
                pressure: Some(0.2),
            },
            PressureInput {
                stable_id: "b",
                selection_group: 0,
                capacity_weight: 1,
                pressure: Some(0.4),
            },
        ];
        let b = [a[1].clone(), a[0].clone()];
        let mut one = PlacementState::default();
        let mut two = PlacementState::default();
        assert_eq!(
            pressure_weights(&a, &config(), &mut one).unwrap(),
            pressure_weights(&b, &config(), &mut two).unwrap()
        );
    }

    #[test]
    fn invalid_or_missing_pressure_fails_closed() {
        let input = [PressureInput {
            stable_id: "a",
            selection_group: 0,
            capacity_weight: 1,
            pressure: None,
        }];
        assert!(pressure_weights(&input, &config(), &mut PlacementState::default()).is_err());
    }

    #[test]
    fn failed_calculation_does_not_commit_partial_smoothing_state() {
        let mut state = PlacementState::default();
        let valid = [PressureInput {
            stable_id: "a",
            selection_group: 0,
            capacity_weight: 1,
            pressure: Some(0.2),
        }];
        let _unused = pressure_weights(&valid, &config(), &mut state).unwrap();
        let before = state.availability.clone();
        let invalid = [
            valid[0].clone(),
            PressureInput {
                stable_id: "b",
                selection_group: 0,
                capacity_weight: 1,
                pressure: None,
            },
        ];
        assert!(pressure_weights(&invalid, &config(), &mut state).is_err());
        assert_eq!(state.availability, before);
    }
}
