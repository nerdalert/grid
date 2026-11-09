//! Dynamo Frontend v1 provider observations, kept separate from llm-d EPP signals.

use grid_signals::{LoadStore, provider_observation as observation};

use super::{Over, SiteReading};

/// Read a provider only when its newest samples match the candidate's exact source revision.
#[expect(
    clippy::float_cmp,
    reason = "the parser admits only exact 0.0/1.0 Dynamo readiness values"
)]
pub(super) fn read(
    store: &LoadStore,
    site: &str,
    routing_cluster: &str,
    contract: &observation::ProviderAdapterContract,
    over: Over,
) -> SiteReading {
    if !contract.is_valid()
        || contract.adapter_kind != "dynamo/frontend"
        || contract.adapter_version != 1
        || contract.model_scope != "dedicatedSingleModel"
    {
        return SiteReading::default();
    }
    let identity = observation::ProviderSignalIdentity {
        adapter_kind: contract.adapter_kind.clone(),
        adapter_version: contract.adapter_version,
        provider_uid: contract.provider_uid.clone(),
        provider_generation: contract.provider_generation,
        model: contract.primary_model.clone(),
        model_scope: contract.model_scope.clone(),
    };
    let key = LoadStore::key(site, routing_cluster);
    let publication = store.learning_state(&key, over.now_ms, over.window_ms);
    let Some(publication_serial) = publication.epoch.map(|_| publication.serial) else {
        return SiteReading::default();
    };
    let ready = store
        .latest_for_adapter(
            &key,
            observation::DYNAMO_FRONTEND_V1_MODEL_READY_METRIC,
            &identity,
            over.now_ms,
            over.window_ms,
        )
        .filter(|sample| sample.publication_serial == Some(publication_serial));
    let Some(ready) = ready else {
        return SiteReading::default();
    };
    if ready.value == 0.0 {
        return SiteReading {
            unready: true,
            sampled_at: Some(ready.at_ms),
            ..SiteReading::default()
        };
    }
    if ready.value != 1.0 {
        return SiteReading::default();
    }

    let active = store
        .latest_for_adapter(
            &key,
            observation::DYNAMO_FRONTEND_V1_ACTIVE_REQUESTS_METRIC,
            &identity,
            over.now_ms,
            over.window_ms,
        )
        .filter(|sample| sample.publication_serial == Some(publication_serial));
    let Some(active) = active.filter(|sample| valid_count(sample.value)) else {
        return SiteReading {
            sampled_at: Some(ready.at_ms),
            ..SiteReading::default()
        };
    };
    let newest = active.at_ms.min(over.now_ms);
    let lookback_ms = over
        .now_ms
        .saturating_sub(newest)
        .saturating_add(over.horizon_ms.max(0));
    let active_requests = store.window_worst_for_adapter(
        &key,
        observation::DYNAMO_FRONTEND_V1_ACTIVE_REQUESTS_METRIC,
        &identity,
        over.now_ms,
        lookback_ms,
        true,
    );
    let Some(active_requests) = active_requests.filter(|value| valid_count(*value)) else {
        return SiteReading {
            sampled_at: Some(ready.at_ms),
            ..SiteReading::default()
        };
    };
    let request_pressure = if contract.is_comparable_dynamo_frontend(&contract.primary_model) {
        contract
            .concurrency_capacity
            .map(|capacity| active_requests / f64::from(capacity))
    } else {
        None
    };
    SiteReading {
        in_flight: Some(active_requests),
        sampled_at: Some(active.at_ms),
        request_pressure,
        ..SiteReading::default()
    }
}

/// Whether a request count is finite, non-negative, integral, and exactly representable.
fn valid_count(value: f64) -> bool {
    value.is_finite() && value >= 0.0 && value.fract() == 0.0 && value <= 9_007_199_254_740_991.0
}
