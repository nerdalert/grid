//! Canonical, model-scoped provider observations.
//!
//! These types describe observed state without assigning llm-d queue or KV-cache
//! meanings to signals from other serving systems. A consumer must inspect the
//! adapter identity, readiness, units, freshness, and capabilities before using
//! a value for routing.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

/// Canonical Dynamo Frontend v1 model-readiness series carried by the Grid peer protocol.
pub const DYNAMO_FRONTEND_V1_MODEL_READY_METRIC: &str = "grid_dynamo_frontend_v1_model_ready";
/// Canonical Dynamo Frontend v1 active-request series carried by the Grid peer protocol.
pub const DYNAMO_FRONTEND_V1_ACTIVE_REQUESTS_METRIC: &str = "grid_dynamo_frontend_v1_active_requests";
/// Canonical Dynamo Frontend v1 stage series carried by the Grid peer protocol.
pub const DYNAMO_FRONTEND_V1_STAGE_REQUESTS_METRIC: &str = "grid_dynamo_frontend_v1_stage_requests";

/// Canonical label naming the adapter family on a provider signal.
pub const ADAPTER_KIND_LABEL: &str = "grid_adapter_kind";
/// Canonical label naming the adapter contract version on a provider signal.
pub const ADAPTER_VERSION_LABEL: &str = "grid_adapter_version";
/// Canonical label carrying the source `InferenceProvider` UID on a provider signal.
pub const PROVIDER_UID_LABEL: &str = "grid_provider_uid";
/// Canonical label carrying the source `InferenceProvider` generation on a provider signal.
pub const PROVIDER_GENERATION_LABEL: &str = "grid_provider_generation";
/// Canonical label carrying the primary model represented by a provider signal.
pub const MODEL_LABEL: &str = "grid_model";
/// Canonical label carrying the model scope represented by a provider signal.
pub const MODEL_SCOPE_LABEL: &str = "grid_model_scope";

/// Provider identity and profile attached to one serving-inventory candidate.
///
/// The signal batch is separately fenced by `grid_provider_config_generation`.
/// The UID and generation here let a Gateway reject a batch when its candidate
/// inventory has not caught up to that signal revision. Unknown adapter names
/// and versions remain representable but are not eligible for dynamic scoring.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderAdapterContract {
    /// Kubernetes name identifying the source `InferenceProvider` resource.
    ///
    /// Signal samples are indexed by the candidate's routing cluster label; this
    /// field identifies the source alongside its UID and generation.
    pub provider_id: String,
    /// Adapter family name, for example `dynamo/frontend`.
    pub adapter_kind: String,
    /// Version of the adapter contract.
    pub adapter_version: u16,
    /// Kubernetes UID of the source `InferenceProvider`.
    pub provider_uid: String,
    /// Kubernetes generation of the source `InferenceProvider`.
    pub provider_generation: u64,
    /// Primary model represented by the adapter.
    pub primary_model: String,
    /// Scope asserted by the adapter, for example `dedicatedSingleModel`.
    pub model_scope: String,
    /// Shared request-accounting profile for comparable providers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub concurrency_profile: Option<String>,
    /// Independently declared effective concurrent-request capacity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub concurrency_capacity: Option<u32>,
}

impl ProviderAdapterContract {
    /// Whether the metadata is bounded and internally consistent.
    #[must_use]
    pub fn is_valid(&self) -> bool {
        crate::revision::valid_id(&self.provider_uid)
            && crate::revision::valid_target(&self.provider_id)
            && self.provider_generation > 0
            && self.provider_generation <= crate::revision::MAX_GENERATION
            && !self.primary_model.trim().is_empty()
            && self.primary_model.len() <= 256
            && !self.primary_model.chars().any(char::is_control)
            && match (&self.concurrency_profile, self.concurrency_capacity) {
                (None, None) => true,
                (Some(profile), Some(capacity)) => {
                    !profile.trim().is_empty()
                        && profile.len() <= 128
                        && !profile.chars().any(char::is_control)
                        && capacity > 0
                },
                _ => false,
            }
    }

    /// Whether this contract is eligible for the pinned Dynamo Frontend v1 pressure reader.
    #[must_use]
    pub fn is_comparable_dynamo_frontend(&self, model: &str) -> bool {
        self.is_valid()
            && self.adapter_kind == "dynamo/frontend"
            && self.adapter_version == 1
            && self.model_scope == "dedicatedSingleModel"
            && self.primary_model == model
            && self.concurrency_profile.is_some()
            && self.concurrency_capacity.is_some()
    }
}

/// Exact source identity attached to each canonical adapter sample.
///
/// A Gateway compares this with the serving candidate before it uses the
/// sample. Keeping it on every retained sample prevents an old provider
/// generation from being treated as current after a spec update.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderSignalIdentity {
    /// Adapter family name.
    pub adapter_kind: String,
    /// Adapter contract version.
    pub adapter_version: u16,
    /// Kubernetes UID of the source `InferenceProvider`.
    pub provider_uid: String,
    /// Kubernetes generation of the source `InferenceProvider`.
    pub provider_generation: u64,
    /// Primary model represented by this signal.
    pub model: String,
    /// Model scope asserted by the adapter.
    pub model_scope: String,
}

impl ProviderSignalIdentity {
    /// Whether this sample is from the exact adapter revision in `contract`.
    #[must_use]
    pub fn matches(&self, contract: &ProviderAdapterContract) -> bool {
        self.adapter_kind == contract.adapter_kind
            && self.adapter_version == contract.adapter_version
            && self.provider_uid == contract.provider_uid
            && self.provider_generation == contract.provider_generation
            && self.model == contract.primary_model
            && self.model_scope == contract.model_scope
    }
}

/// Versioned adapter identity attached to a provider observation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdapterIdentity {
    /// Adapter implementation family.
    pub kind: AdapterKind,
    /// Adapter contract version.
    pub version: u16,
}

/// A supported metrics adapter family.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub enum AdapterKind {
    /// Dynamo Frontend's version 1 metrics contract.
    #[serde(rename = "dynamo/frontend")]
    DynamoFrontend,
}

/// Source identity and provider revision for one observation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ObservationSource {
    /// Stable Grid site identity that scraped the provider.
    pub site: String,
    /// Kubernetes UID of the `InferenceProvider` resource.
    pub provider_uid: String,
    /// Kubernetes generation of the `InferenceProvider` resource.
    pub provider_generation: i64,
    /// Metrics adapter identity used to normalize the observation.
    pub adapter: AdapterIdentity,
}

/// Observation scope guaranteed by the adapter contract.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ObservationScope {
    /// One Frontend serves one primary model; configured label aliases resolve to it.
    DedicatedSingleModel,
}

/// Freshness of the scrape that produced the observation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ObservationFreshness {
    /// The scrape completed within the configured freshness window.
    Fresh,
    /// A recent cached observation is being used after a scrape failure.
    Stale,
    /// No usable sample was observed.
    Missing,
    /// A sample was present but violated the adapter contract.
    Invalid,
}

/// Readiness of the observed model from the serving system's perspective.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ModelReadiness {
    /// The provider reports that it can route requests for this model.
    Ready,
    /// The provider reports that it cannot route requests for this model.
    NotReady,
    /// The readiness sample is absent or cannot be established.
    Unknown,
}

/// Unit for a measured request count.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum CountUnit {
    /// Number of requests currently observed.
    Requests,
    /// Independently declared effective concurrent request capacity.
    ConcurrentRequests,
}

/// An integer count with an explicit unit.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CountObservation {
    /// Non-negative count value.
    pub value: u64,
    /// Meaning of the count.
    pub unit: CountUnit,
}

/// One Dynamo Frontend pipeline stage count.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StageRequestObservation {
    /// Stage label reported by Dynamo Frontend.
    pub stage: String,
    /// Phase label reported by Dynamo Frontend.
    pub phase: String,
    /// Number of requests in this stage and phase.
    pub count: CountObservation,
}

/// Signal the adapter can expose, independently of whether a scrape had a value.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ObservationCapability {
    /// Per-model readiness is represented explicitly.
    ModelReadiness,
    /// Frontend-wide active request count is available for a dedicated one-model Frontend.
    ActiveRequests,
    /// Pre-first-token pipeline stage counts are available.
    StageRequests,
    /// A matching model and declared concurrency profile permit request-pressure comparison.
    ComparableRequestPressure,
}

/// Normalized observation for one provider and one primary model.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderModelObservation {
    /// Provider and adapter source identity.
    pub source: ObservationSource,
    /// Primary model name used by Grid routing.
    pub model: String,
    /// Scope asserted by the adapter configuration.
    pub scope: ObservationScope,
    /// UTC Unix timestamp in milliseconds when this sample was collected.
    pub observed_at_ms: i64,
    /// Freshness of the scrape.
    pub freshness: ObservationFreshness,
    /// Readiness of the primary model.
    pub readiness: ModelReadiness,
    /// Frontend-wide active request count, when present.
    pub active_requests: Option<CountObservation>,
    /// Per-stage request counts, when the family is present.
    pub stage_requests: Option<Vec<StageRequestObservation>>,
    /// Effective concurrency denominator, only when independently declared.
    pub concurrency_capacity: Option<CountObservation>,
    /// Profile identifier shared only by comparable capacity declarations.
    pub concurrency_profile: Option<String>,
    /// Signals supported by the adapter contract.
    pub capabilities: BTreeSet<ObservationCapability>,
}
