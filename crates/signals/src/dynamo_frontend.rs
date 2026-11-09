//! Strict normalization for the Dynamo Frontend v1 metrics contract.
//!
//! This parser is deliberately separate from the legacy Grid metrics parser.
//! Dynamo Frontend active requests and stage counts are not llm-d EPP queues,
//! so malformed, ambiguous, or unscoped samples are rejected instead of being
//! converted into neutral values.

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    exposition,
    provider_observation::{
        AdapterIdentity, AdapterKind, CountObservation, CountUnit, ModelReadiness, ObservationCapability,
        ObservationFreshness, ObservationScope, ObservationSource, ProviderModelObservation, StageRequestObservation,
    },
};

/// Dynamo Frontend v1 readiness family.
pub const MODEL_READY_METRIC: &str = "dynamo_frontend_model_ready";
/// Dynamo Frontend v1 active request family.
pub const ACTIVE_REQUESTS_METRIC: &str = "dynamo_frontend_active_requests";
/// Dynamo Frontend v1 pipeline stage family.
pub const STAGE_REQUESTS_METRIC: &str = "dynamo_frontend_stage_requests";

/// Maximum pipeline-stage series retained from one scrape.
const MAX_STAGE_SERIES: usize = 64;

/// Provider identity needed to attribute an observation to a source revision.
#[derive(Clone, Debug)]
pub struct ProviderRevision {
    /// Stable Grid site identity that scraped the provider.
    pub site: String,
    /// Kubernetes UID of the `InferenceProvider` resource.
    pub provider_uid: String,
    /// Kubernetes generation of the `InferenceProvider` resource.
    pub provider_generation: i64,
}

/// Adapter configuration required to interpret Dynamo Frontend v1.
#[derive(Clone, Copy, Debug)]
pub struct DynamoFrontendConfig<'config> {
    /// Primary model name used by Grid routing.
    pub primary_model: &'config str,
    /// Explicit metric-label aliases for the primary model.
    pub model_aliases: &'config [String],
    /// Shared profile name for comparable concurrency declarations.
    pub concurrency_profile: Option<&'config str>,
    /// Independently declared effective concurrent request capacity.
    pub concurrency_capacity: Option<u32>,
}

/// Parsed values for the three Dynamo Frontend v1 metric families.
#[derive(Default)]
struct ParsedDynamoSignals {
    /// The parsed `model_ready` state, if the family was present.
    readiness: Option<ModelReadiness>,
    /// The parsed Frontend active-request count, if the family was present.
    active_requests: Option<CountObservation>,
    /// Stage and phase counts keyed by their reported labels.
    stage_requests: BTreeMap<(String, String), u64>,
}

/// Parse a Dynamo Frontend v1 exposition into a canonical model observation.
///
/// The parser requires exact model scope for model-labeled families and an
/// explicit one-model Frontend configuration. A missing readiness family
/// produces Unknown readiness. Missing active or stage gauges remain absent.
///
/// # Errors
///
/// Returns an error for malformed target series, duplicate or ambiguous
/// samples, a model outside the configured primary-name mapping, invalid
/// counts, invalid source identity, or incomplete concurrency configuration.
pub fn parse_dynamo_frontend_v1(
    text: &str,
    config: &DynamoFrontendConfig<'_>,
    revision: ProviderRevision,
    observed_at_ms: i64,
) -> Result<ProviderModelObservation, String> {
    validate_config(config)?;
    validate_revision(&revision)?;
    let ParsedDynamoSignals {
        readiness,
        active_requests,
        stage_requests,
    } = parse_series(text, config)?;

    let stages = (!stage_requests.is_empty()).then(|| {
        stage_requests
            .into_iter()
            .map(|((stage, phase), value)| StageRequestObservation {
                stage,
                phase,
                count: CountObservation {
                    value,
                    unit: CountUnit::Requests,
                },
            })
            .collect()
    });
    let concurrency_capacity = config.concurrency_capacity.map(|capacity| CountObservation {
        value: u64::from(capacity),
        unit: CountUnit::ConcurrentRequests,
    });
    let concurrency_profile = config.concurrency_profile.map(str::to_owned);
    let mut capabilities = BTreeSet::from([
        ObservationCapability::ModelReadiness,
        ObservationCapability::ActiveRequests,
        ObservationCapability::StageRequests,
    ]);
    if concurrency_capacity.is_some() && concurrency_profile.is_some() {
        capabilities.insert(ObservationCapability::ComparableRequestPressure);
    }

    Ok(ProviderModelObservation {
        source: ObservationSource {
            site: revision.site,
            provider_uid: revision.provider_uid,
            provider_generation: revision.provider_generation,
            adapter: AdapterIdentity {
                kind: AdapterKind::DynamoFrontend,
                version: 1,
            },
        },
        model: config.primary_model.to_owned(),
        scope: ObservationScope::DedicatedSingleModel,
        observed_at_ms,
        freshness: ObservationFreshness::Fresh,
        readiness: readiness.unwrap_or(ModelReadiness::Unknown),
        active_requests,
        stage_requests: stages,
        concurrency_capacity,
        concurrency_profile,
        capabilities,
    })
}

/// Parse the pinned metric families, rejecting duplicates and out-of-scope models.
fn parse_series(text: &str, config: &DynamoFrontendConfig<'_>) -> Result<ParsedDynamoSignals, String> {
    let mut parsed = ParsedDynamoSignals::default();
    for line in text.lines() {
        let Some(family) = metric_family(line) else {
            continue;
        };
        if !is_target_family(family) {
            continue;
        }
        let metric =
            exposition::parse(line).ok_or_else(|| format!("malformed Dynamo Frontend v1 sample for {family}"))?;
        let labels = collect_labels(metric.labels(), family)?;
        match family {
            MODEL_READY_METRIC => {
                require_labels(&labels, &["model"], family)?;
                let model = required_label(&labels, "model", family)?;
                validate_model(model, config)?;
                if parsed.readiness.is_some() {
                    return Err(format!("duplicate {MODEL_READY_METRIC} series for model {model:?}"));
                }
                parsed.readiness = Some(match parse_count(metric.value_text(), family)? {
                    0 => ModelReadiness::NotReady,
                    1 => ModelReadiness::Ready,
                    _ => return Err(format!("{MODEL_READY_METRIC} must be 0 or 1")),
                });
            },
            ACTIVE_REQUESTS_METRIC => {
                require_labels(&labels, &["model"], family)?;
                let model = required_label(&labels, "model", family)?;
                validate_model(model, config)?;
                if parsed.active_requests.is_some() {
                    return Err(format!("duplicate {ACTIVE_REQUESTS_METRIC} series for model {model:?}"));
                }
                parsed.active_requests = Some(CountObservation {
                    value: parse_count(metric.value_text(), family)?,
                    unit: CountUnit::Requests,
                });
            },
            STAGE_REQUESTS_METRIC => {
                require_labels(&labels, &["phase", "stage"], family)?;
                let phase = required_label(&labels, "phase", family)?;
                let stage = required_label(&labels, "stage", family)?;
                if stage.is_empty() || stage.len() > 128 || phase.len() > 128 {
                    return Err(format!("invalid {STAGE_REQUESTS_METRIC} stage or phase label"));
                }
                if parsed.stage_requests.len() >= MAX_STAGE_SERIES {
                    return Err(format!(
                        "{STAGE_REQUESTS_METRIC} exceeds the {MAX_STAGE_SERIES}-series limit"
                    ));
                }
                let key = (stage.to_owned(), phase.to_owned());
                let value = parse_count(metric.value_text(), family)?;
                if parsed.stage_requests.insert(key, value).is_some() {
                    return Err(format!("duplicate {STAGE_REQUESTS_METRIC} stage and phase series"));
                }
            },
            _ => return Err(format!("unsupported Dynamo Frontend v1 metric family {family}")),
        }
    }
    Ok(parsed)
}

/// Evaluate freshness at a caller's current UTC timestamp.
///
/// Returns [`ObservationFreshness::Invalid`] when the sample timestamp is in the
/// future or timestamp subtraction overflows.
pub fn freshness_at(observed_at_ms: i64, now_ms: i64, stale_after_ms: u64) -> ObservationFreshness {
    let Some(age_ms) = now_ms.checked_sub(observed_at_ms) else {
        return ObservationFreshness::Invalid;
    };
    let Ok(age_ms) = u64::try_from(age_ms) else {
        return ObservationFreshness::Invalid;
    };
    if age_ms > stale_after_ms {
        ObservationFreshness::Stale
    } else {
        ObservationFreshness::Fresh
    }
}

/// The metric-family token before labels or a sample value.
fn metric_family(line: &str) -> Option<&str> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return None;
    }
    let end = line
        .find(|character: char| character == '{' || character.is_ascii_whitespace())
        .unwrap_or(line.len());
    line.get(..end)
}

/// Whether a family belongs to the pinned Dynamo Frontend v1 contract.
fn is_target_family(family: &str) -> bool {
    matches!(
        family,
        MODEL_READY_METRIC | ACTIVE_REQUESTS_METRIC | STAGE_REQUESTS_METRIC
    )
}

/// Validate non-empty model mapping and paired concurrency settings.
fn validate_config(config: &DynamoFrontendConfig<'_>) -> Result<(), String> {
    if config.primary_model.trim().is_empty() {
        return Err("primary model must not be blank".to_owned());
    }
    let mut aliases = BTreeSet::new();
    for alias in config.model_aliases {
        if alias.trim().is_empty() || alias == config.primary_model || !aliases.insert(alias) {
            return Err("model aliases must be non-blank, unique, and differ from the primary model".to_owned());
        }
    }
    match (config.concurrency_profile, config.concurrency_capacity) {
        (Some(profile), Some(capacity)) if !profile.trim().is_empty() && capacity > 0 => Ok(()),
        (None, None) => Ok(()),
        (Some(_), Some(_)) => {
            Err("concurrency profile and capacity must be non-blank and greater than zero".to_owned())
        },
        (Some(_), None) | (None, Some(_)) => {
            Err("concurrency profile and capacity must be configured together".to_owned())
        },
    }
}

/// Validate the source identity needed for a revision-bound observation.
fn validate_revision(revision: &ProviderRevision) -> Result<(), String> {
    if revision.site.trim().is_empty() || revision.provider_uid.trim().is_empty() {
        return Err("site and provider UID must not be blank".to_owned());
    }
    if revision.provider_generation < 0 {
        return Err("provider generation must not be negative".to_owned());
    }
    Ok(())
}

/// Ensure a metric label maps unambiguously to the configured primary model.
fn validate_model(model: &str, config: &DynamoFrontendConfig<'_>) -> Result<(), String> {
    if model.trim().is_empty() {
        return Err("Dynamo Frontend model label must not be blank".to_owned());
    }
    if model != config.primary_model && !config.model_aliases.iter().any(|alias| alias == model) {
        return Err(format!(
            "Frontend model label {model:?} is outside the configured one-model mapping"
        ));
    }
    Ok(())
}

/// Collect validated labels and reject duplicate label names.
fn collect_labels(labels: exposition::Labels<'_>, family: &str) -> Result<BTreeMap<String, String>, String> {
    let mut collected = BTreeMap::new();
    for (name, value) in labels {
        if collected.insert(name.to_owned(), value.into_owned()).is_some() {
            return Err(format!("duplicate label in {family} series"));
        }
        if collected.len() > 8 {
            return Err(format!("{family} exceeds the label-count limit"));
        }
    }
    Ok(collected)
}

/// Require exactly the labels defined by the pinned metric contract.
fn require_labels(labels: &BTreeMap<String, String>, expected: &[&str], family: &str) -> Result<(), String> {
    if labels.len() == expected.len() && expected.iter().all(|name| labels.contains_key(*name)) {
        Ok(())
    } else {
        Err(format!("{family} has missing or unexpected labels"))
    }
}

/// Return a required label value.
fn required_label<'labels>(
    labels: &'labels BTreeMap<String, String>,
    name: &str,
    family: &str,
) -> Result<&'labels str, String> {
    labels
        .get(name)
        .map(String::as_str)
        .ok_or_else(|| format!("{family} is missing its {name:?} label"))
}

/// Parse a Prometheus number as an exact, non-negative integer count.
fn parse_count(value: &str, family: &str) -> Result<u64, String> {
    let value = value.strip_prefix('+').unwrap_or(value);
    if value.is_empty() || value.starts_with('-') {
        return Err(format!("{family} count must be non-negative"));
    }
    let (mantissa, exponent) = value
        .split_once('e')
        .or_else(|| value.split_once('E'))
        .map_or((value, 0_i64), |(mantissa, exponent)| {
            (mantissa, exponent.parse::<i64>().unwrap_or(i64::MAX))
        });
    if exponent == i64::MAX {
        return Err(format!("{family} count has an invalid exponent"));
    }
    let (whole, fractional) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if whole.is_empty()
        || !whole.chars().all(|character| character.is_ascii_digit())
        || !fractional.chars().all(|character| character.is_ascii_digit())
    {
        return Err(format!("{family} count is not a valid integer value"));
    }
    let mut digits = String::with_capacity(whole.len().saturating_add(fractional.len()));
    digits.push_str(whole);
    digits.push_str(fractional);
    let fractional_len =
        i64::try_from(fractional.len()).map_err(|_error| format!("{family} count has too many decimal places"))?;
    let scale = fractional_len
        .checked_sub(exponent)
        .ok_or_else(|| format!("{family} count has an invalid exponent"))?;
    let integer = if scale > 0 {
        let digit_len = i64::try_from(digits.len()).map_err(|_error| format!("{family} count has too many digits"))?;
        if scale > digit_len {
            if digits.chars().all(|character| character == '0') {
                return Ok(0);
            }
            return Err(format!("{family} count has a fractional value"));
        }
        let places = usize::try_from(scale).map_err(|_error| format!("{family} count has too many decimal places"))?;
        let split_at = digits
            .len()
            .checked_sub(places)
            .ok_or_else(|| format!("{family} count has too many decimal places"))?;
        let (integer, decimal) = digits.split_at(split_at);
        if !decimal.chars().all(|character| character == '0') {
            return Err(format!("{family} count has a fractional value"));
        }
        if integer.is_empty() {
            "0".to_owned()
        } else {
            integer.to_owned()
        }
    } else if scale < 0 {
        if digits.chars().all(|character| character == '0') {
            return Ok(0);
        }
        let zeroes = scale
            .checked_neg()
            .ok_or_else(|| format!("{family} count is too large"))?;
        let digit_len = i64::try_from(digits.len()).map_err(|_error| format!("{family} count has too many digits"))?;
        if digit_len.checked_add(zeroes).is_none_or(|length| length > 20) {
            return Err(format!("{family} count is too large"));
        }
        let zeroes = usize::try_from(zeroes).map_err(|_error| format!("{family} count is too large"))?;
        digits.extend(std::iter::repeat_n('0', zeroes));
        digits
    } else {
        digits
    };
    let normalized = integer.trim_start_matches('0');
    let normalized = if normalized.is_empty() { "0" } else { normalized };
    normalized
        .parse::<u64>()
        .map_err(|_error| format!("{family} count is outside the supported integer range"))
}

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(clippy::expect_used, clippy::indexing_slicing, reason = "tests")]
mod tests {
    use super::*;

    const MODEL: &str = "Qwen/Qwen3-0.6B";

    fn config<'config>(
        primary_model: &'config str,
        aliases: &'config [String],
        profile: Option<&'config str>,
        capacity: Option<u32>,
    ) -> DynamoFrontendConfig<'config> {
        DynamoFrontendConfig {
            primary_model,
            model_aliases: aliases,
            concurrency_profile: profile,
            concurrency_capacity: capacity,
        }
    }

    fn revision() -> ProviderRevision {
        ProviderRevision {
            site: "east".to_owned(),
            provider_uid: "provider-uid".to_owned(),
            provider_generation: 3,
        }
    }

    fn parse(text: &str) -> Result<ProviderModelObservation, String> {
        parse_dynamo_frontend_v1(text, &config(MODEL, &[], None, None), revision(), 1_000)
    }

    #[test]
    fn absent_readiness_remains_unknown_and_absent_load_is_not_zero() {
        let observation = parse("").expect("empty scrape is an unknown observation");
        assert_eq!(
            observation.readiness,
            ModelReadiness::Unknown,
            "missing readiness is unknown"
        );
        assert_eq!(observation.active_requests, None, "missing active gauge stays absent");
        assert_eq!(observation.stage_requests, None, "missing stage family stays absent");
        assert_eq!(observation.concurrency_capacity, None, "missing capacity stays absent");
        assert!(
            !observation
                .capabilities
                .contains(&ObservationCapability::ComparableRequestPressure),
            "missing capacity cannot enable pressure comparison"
        );
    }

    #[test]
    fn readiness_zero_and_one_are_explicit_states() {
        let not_ready =
            parse("dynamo_frontend_model_ready{model=\"Qwen/Qwen3-0.6B\"} 0\n").expect("zero readiness parses");
        let ready = parse("dynamo_frontend_model_ready{model=\"Qwen/Qwen3-0.6B\"} 1\n").expect("one readiness parses");
        assert_eq!(not_ready.readiness, ModelReadiness::NotReady, "zero means not ready");
        assert_eq!(ready.readiness, ModelReadiness::Ready, "one means ready");
    }

    #[test]
    fn configured_model_alias_maps_to_primary_model() {
        let aliases = vec!["qwen3".to_owned()];
        let observation = parse_dynamo_frontend_v1(
            "dynamo_frontend_model_ready{model=\"qwen3\"} 1\n",
            &config(MODEL, &aliases, None, None),
            revision(),
            1_000,
        )
        .expect("configured alias parses");
        assert_eq!(observation.model, MODEL, "alias resolves to primary model");
        assert_eq!(
            observation.readiness,
            ModelReadiness::Ready,
            "alias readiness is retained"
        );
    }

    #[test]
    fn primary_and_configured_alias_labels_both_resolve_to_the_primary_model() {
        let aliases = vec!["qwen3".to_owned()];
        let observation = parse_dynamo_frontend_v1(
            "dynamo_frontend_model_ready{model=\"Qwen/Qwen3-0.6B\"} 1\n\
             dynamo_frontend_active_requests{model=\"qwen3\"} 2\n",
            &config(MODEL, &aliases, None, None),
            revision(),
            1_000,
        )
        .expect("configured aliases and the primary label map to one provider model");
        assert_eq!(observation.model, MODEL, "aliases never replace the canonical model");
        assert_eq!(
            observation.readiness,
            ModelReadiness::Ready,
            "primary readiness is retained"
        );
        assert_eq!(
            observation.active_requests.map(|count| count.value),
            Some(2),
            "the explicitly mapped alias is retained as active request data"
        );
    }

    #[test]
    fn duplicate_model_series_and_malformed_target_series_are_rejected() {
        let duplicate = parse(
            "dynamo_frontend_model_ready{model=\"Qwen/Qwen3-0.6B\"} 1\n\
             dynamo_frontend_model_ready{model=\"Qwen/Qwen3-0.6B\"} 1\n",
        );
        let malformed = parse("dynamo_frontend_model_ready{model=\"Qwen/Qwen3-0.6B\" 1\n");
        assert!(duplicate.is_err(), "duplicate model series are ambiguous");
        assert!(malformed.is_err(), "malformed target series fail the adapter");
    }

    #[test]
    fn unconfigured_second_model_is_rejected() {
        let result = parse(
            "dynamo_frontend_model_ready{model=\"Qwen/Qwen3-0.6B\"} 1\n\
             dynamo_frontend_model_ready{model=\"other-model\"} 1\n",
        );
        assert!(result.is_err(), "a multi-model Frontend is outside adapter scope");
    }

    #[test]
    fn negative_non_finite_and_fractional_counts_are_rejected() {
        let negative = parse("dynamo_frontend_active_requests{model=\"Qwen/Qwen3-0.6B\"} -1\n");
        let not_a_number = parse("dynamo_frontend_active_requests{model=\"Qwen/Qwen3-0.6B\"} NaN\n");
        let fractional = parse("dynamo_frontend_active_requests{model=\"Qwen/Qwen3-0.6B\"} 1.5\n");
        assert!(negative.is_err(), "negative request counts are invalid");
        assert!(not_a_number.is_err(), "non-finite request counts are invalid");
        assert!(fractional.is_err(), "request counts must be integral");
    }

    #[test]
    fn duplicate_labels_and_unexpected_labels_are_rejected() {
        let duplicate_label =
            parse("dynamo_frontend_model_ready{model=\"Qwen/Qwen3-0.6B\",model=\"Qwen/Qwen3-0.6B\"} 1\n");
        let unexpected_label =
            parse("dynamo_frontend_active_requests{model=\"Qwen/Qwen3-0.6B\",stage=\"dispatch\"} 1\n");
        assert!(duplicate_label.is_err(), "duplicate labels are invalid");
        assert!(
            unexpected_label.is_err(),
            "unrecognized model metric labels are invalid"
        );
    }

    #[test]
    fn stage_gauges_keep_stage_and_phase_without_becoming_queue_depth() {
        let observation = parse("dynamo_frontend_stage_requests{phase=\"aggregated\",stage=\"dispatch\"} 2\n")
            .expect("stage gauge parses");
        let stages = observation.stage_requests.expect("stage family is present");
        assert_eq!(stages.len(), 1, "one stage is preserved");
        assert_eq!(stages[0].stage, "dispatch", "stage label remains explicit");
        assert_eq!(stages[0].phase, "aggregated", "phase label remains explicit");
        assert_eq!(stages[0].count.value, 2, "stage count is retained");
    }

    #[test]
    fn concurrency_capacity_requires_profile_and_marks_comparability() {
        let observation = parse_dynamo_frontend_v1(
            "dynamo_frontend_model_ready{model=\"Qwen/Qwen3-0.6B\"} 1\n",
            &config(MODEL, &[], Some("mock-concurrency-v1"), Some(4)),
            revision(),
            1_000,
        )
        .expect("declared concurrency profile parses");
        assert_eq!(
            observation.concurrency_capacity.map(|count| count.value),
            Some(4),
            "capacity is declared"
        );
        assert_eq!(
            observation.concurrency_profile.as_deref(),
            Some("mock-concurrency-v1"),
            "profile is retained"
        );
        assert!(
            observation
                .capabilities
                .contains(&ObservationCapability::ComparableRequestPressure),
            "profile and capacity enable the comparison capability"
        );
        let missing_profile = config(MODEL, &[], None, Some(4));
        assert!(
            parse_dynamo_frontend_v1("", &missing_profile, revision(), 1_000).is_err(),
            "capacity without its profile is invalid"
        );
    }

    #[test]
    fn stale_timestamp_is_not_fresh_and_future_timestamp_is_invalid() {
        assert_eq!(
            freshness_at(1_000, 1_100, 100),
            ObservationFreshness::Fresh,
            "window boundary is fresh"
        );
        assert_eq!(
            freshness_at(1_000, 1_101, 100),
            ObservationFreshness::Stale,
            "old sample is stale"
        );
        assert_eq!(
            freshness_at(1_101, 1_100, 100),
            ObservationFreshness::Invalid,
            "future sample is invalid"
        );
    }

    #[test]
    fn pinned_frontend_v1_runtime_series_parse_without_semantic_aliases() {
        let observation = parse(include_str!("../tests/fixtures/dynamo-frontend-v1.prom"))
            .expect("series captured from the pinned Dynamo Frontend image parse");
        assert_eq!(observation.model, MODEL, "the primary model label is retained");
        assert_eq!(
            observation.readiness,
            ModelReadiness::Ready,
            "the runtime reports ready"
        );
        assert_eq!(
            observation.active_requests.map(|count| (count.value, count.unit)),
            Some((0, CountUnit::Requests)),
            "an observed zero remains a measured zero request count"
        );
        let stages = observation.stage_requests.expect("the runtime emits stage gauges");
        assert_eq!(stages.len(), 3, "the runtime stage and phase series remain separate");
        assert!(
            stages
                .iter()
                .any(|stage| stage.stage == "dispatch" && stage.phase == "aggregated"),
            "the stage sample retains its actual Dynamo labels"
        );
    }

    #[test]
    fn source_identity_and_adapter_version_are_attached() {
        let observation = parse("").expect("unknown observation parses");
        assert_eq!(observation.source.site, "east", "site identifies the source");
        assert_eq!(
            observation.source.provider_uid, "provider-uid",
            "UID fences resource recreation"
        );
        assert_eq!(
            observation.source.provider_generation, 3,
            "generation fences config revisions"
        );
        assert_eq!(observation.source.adapter.version, 1, "adapter version is explicit");
    }
}
