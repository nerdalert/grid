//! Provider metrics collection for the [`GridNetwork`] overlay renderer.
//!
//! Scrapes Prometheus `/metrics` endpoints from `InferenceProvider` resources that have
//! `spec.metricsConfig` configured, parses the text with the configured signal
//! names, and returns a map keyed by provider routing identity for use with
//! `render_routing_overlay`.
//!
//! Legacy scrape failures are non-fatal: when a valid cached sample exists within
//! the `staleMetricsSeconds` grace period, it is reused. When no cache entry is
//! available (or the grace period has expired), the provider is marked unobservable
//! for legacy scoring. Versioned adapters retain explicit unknown/stale states; only
//! a fresh, valid model-readiness `0` excludes by adapter readiness. Unknown adapter
//! telemetry cannot become a zero-load sample and falls back to provider health and
//! the configured non-comparative routing order.
//!
//! [`GridNetwork`]: crate::crd::grid_network::GridNetwork

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use futures::StreamExt as _;
use tokio::sync::Mutex;

use crate::{
    crd::inference_provider::{EndpointTlsConfig, InferenceProvider, MetricSignalNames, MetricsAdapterKind},
    metrics_parser::{MetricNames, PartialMetrics, parse_prometheus_text},
    metrics_scraper::{self, scrape_metrics},
    resources::routing_overlay::routing_identity,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Scrape timeout used when the provider's `metricsConfig.timeout` cannot be parsed.
const DEFAULT_SCRAPE_TIMEOUT: Duration = Duration::from_secs(2);

/// Maximum provider scrapes started at once.
///
/// A bounded fan-out keeps one slow endpoint from delaying its siblings while
/// also keeping concurrent response bodies and TLS handshakes bounded.
const PROVIDER_SCRAPE_CONCURRENCY: usize = 16;

/// Metrics inserted for a provider whose configured metrics endpoint cannot be
/// observed (scrape failed and no valid cache entry).
///
/// `healthy: false` causes the scoring engine's `is_healthy` filter to exclude
/// the provider from active routing.  This prevents an unobservable secure
/// metrics endpoint from receiving favorable neutral default scores.
///
/// Providers without `metricsConfig` are unaffected; they have no metrics
/// entry in the map and receive neutral scoring as before.
const UNOBSERVABLE_METRICS: scoring::BackendMetrics = scoring::BackendMetrics {
    error_rate: 1.0,
    healthy: false,
    kv_cache_utilization: 1.0,
    latency_p99_ms: 5000.0,
    prefix_cache_hit_ratio: 0.0,
    queue_depth: 1.0,
};

// ---------------------------------------------------------------------------
// Timestamped metrics and cache
// ---------------------------------------------------------------------------

/// A [`scoring::BackendMetrics`] value paired with the [`Instant`] it was scraped.
///
/// Stored in [`MetricsCache`] so that a recent successful scrape result can be
/// reused during a brief endpoint outage if `metricsConfig.stale_metrics_seconds`
/// is configured.
#[derive(Clone, Debug)]
pub(crate) struct TimestampedMetrics {
    /// The scraped and parsed metrics.
    pub(crate) metrics: scoring::BackendMetrics,
    /// When the scrape completed successfully.
    pub(crate) scraped_at: Instant,
    /// Monotonic scrape generation assigned when this sample was collected.
    pub(crate) generation: u64,
    /// Canonical adapter observation, when this provider uses a versioned adapter.
    pub(crate) observation: Option<grid_signals::provider_observation::ProviderModelObservation>,
}

/// Cross-reconcile cache for recently-scraped provider metrics.
///
/// Keyed by `(network_name, provider_routing_identity)`.  Entries are updated on
/// every successful scrape and consulted when a subsequent scrape fails and the
/// provider has `metricsConfig.stale_metrics_seconds` set.
pub(crate) struct MetricsCache {
    /// Per-provider cached metrics keyed by `(network_name, routing_identity)`.
    entries: HashMap<(String, String), TimestampedMetrics>,
    /// Monotonic counter incremented on each scrape call.
    next_generation: u64,
}

impl MetricsCache {
    /// Create an empty cache with the generation counter starting at 1.
    pub(crate) fn new() -> Self {
        Self {
            entries: HashMap::new(),
            next_generation: 1,
        }
    }
}

/// Metrics collected from a single scrape call, with scrape-level identity.
#[derive(Default)]
pub(crate) struct CollectedMetrics {
    /// Parsed metrics keyed by provider routing identity.
    pub(crate) metrics: HashMap<String, scoring::BackendMetrics>,
    /// Scrape generation per provider, monotonically increasing per successful scrape.
    pub(crate) generations: HashMap<String, u64>,
    /// Canonical per-provider/model observations produced by versioned adapters.
    pub(crate) observations: HashMap<String, grid_signals::provider_observation::ProviderModelObservation>,
}

// ---------------------------------------------------------------------------
// Signals collection (poll mode)
// ---------------------------------------------------------------------------

/// Scrape each provider's endpoint, recording its serving gauges and readiness.
///
/// Sibling of [`collect_provider_metrics_with_refresh_interval`], which parses
/// the same text into [`scoring::BackendMetrics`] for local scoring. This keeps
/// the provider's own exposition as published, minus the families every process
/// exports about itself, so a reader concludes from the provider's raw series
/// and `signalNames` stays a scoring concern. Scrapes run concurrently, up to
/// [`PROVIDER_SCRAPE_CONCURRENCY`] at once. Fails closed on TLS: a provider
/// whose TLS will not resolve is skipped, never scraped in plaintext, and
/// counts as a failed scrape.
pub(crate) async fn collect_provider_signals(
    network_name: &str,
    providers: &[InferenceProvider],
    client: Option<&kube::Client>,
    readiness: &crate::readiness::ReadinessStore,
    local_site: Option<&str>,
) {
    futures::stream::iter(
        providers
            .iter()
            .filter(|provider| provider.spec.grid_network_ref == network_name),
    )
    .for_each_concurrent(PROVIDER_SCRAPE_CONCURRENCY, |provider| async move {
        let Some(name) = provider.metadata.name.as_deref() else {
            return;
        };
        let key = crate::readiness::key(network_name, name);
        readiness.bind_revision(&key, provider.metadata.uid.as_deref(), provider.metadata.generation);
        let Some(plan) = signal_scrape_plan(provider) else {
            return;
        };
        match scrape_provider_signals(provider, &plan, client).await {
            Ok(text) => record_scrape_for_site(readiness, &key, provider, &plan, &text, local_site),
            Err(class) => {
                crate::metrics::record_provider_scrape(crate::readiness::provider_of(&key), class.as_str());
                readiness.record_failure(&key, class, Instant::now());
            },
        }
    })
    .await;
}

/// Record one successful scrape: its ready-endpoint count and its declared signals.
#[cfg(test)]
fn record_scrape(
    readiness: &crate::readiness::ReadinessStore,
    key: &str,
    provider: &InferenceProvider,
    plan: &SignalScrapePlan<'_>,
    text: &str,
) {
    record_scrape_for_site(readiness, key, provider, plan, text, None);
}

/// Record one successful scrape, projecting an opted-in versioned adapter into
/// bounded canonical series before the existing peer publication fence.
#[expect(
    clippy::too_many_arguments,
    reason = "scrape projection needs the revision store, identity, provider plan, raw response, and local site"
)]
fn record_scrape_for_site(
    readiness: &crate::readiness::ReadinessStore,
    key: &str,
    provider: &InferenceProvider,
    plan: &SignalScrapePlan<'_>,
    text: &str,
    local_site: Option<&str>,
) {
    // Every sample feeds this site's own windows; only gauges are republished.
    let (parsed, republishable): (Vec<_>, Vec<_>) = crate::signals::parse_scrape(text).into_iter().unzip();
    let pool = provider
        .spec
        .metrics_config
        .as_ref()
        .and_then(|mc| mc.pool_name.as_deref());
    let ready = readiness.thawed(
        key,
        crate::readiness::ready_endpoints(&parsed, plan.ready_names(), pool),
        units_reporting(&parsed, pool),
        Instant::now(),
    );
    let missing = count_scrape(crate::readiness::provider_of(key), plan, pool, ready);
    let in_flight = in_flight_observation(&parsed, ready, pool, plan.identity);
    let latency = readiness.record_latency(key, &parsed, Instant::now());
    let mut observations: Vec<_> = parsed
        .into_iter()
        .zip(republishable)
        .filter_map(|(o, republishable)| republishable.then_some(o))
        .filter(|o| relayed(&o.metric))
        .filter(|o| o.metric != grid_signals::revision::METRIC)
        .filter(|o| pool.is_none_or(|pool| o.labels.get("name").or_else(|| o.labels.get(crate::signals::POOL_LABEL)).is_some_and(|name| name == pool)))
        // A local sample's freshness is its collection time, so drop any trailing
        // timestamp. Only relayed peer samples carry a per-sample stamp.
        .map(|mut o| {
            o.timestamp_ms = None;
            o
        })
        .chain(in_flight)
        .chain(latency)
        .collect();
    if provider
        .spec
        .metrics_config
        .as_ref()
        .is_some_and(|config| config.adapter.is_some())
    {
        match parse_adapter_observation(provider, local_site, text) {
            Ok(observation) => observations.extend(canonical_adapter_signals(&observation)),
            Err(error) => {
                tracing::warn!(provider = plan.identity, %error, "versioned adapter observation is unknown for this scrape");
            },
        }
    }
    readiness.record_success(key, ready, missing, observations, Instant::now());
}

/// Project one typed observation into the fixed, bounded Grid peer vocabulary.
/// Absence of the readiness series is its explicit `Unknown` wire state.
fn canonical_adapter_signals(
    observation: &grid_signals::provider_observation::ProviderModelObservation,
) -> Vec<crate::signals::Observation> {
    use grid_signals::provider_observation::{ModelReadiness, ObservationFreshness};

    if observation.freshness != ObservationFreshness::Fresh {
        return Vec::new();
    }
    let mut projected = Vec::new();
    // The validated serving candidate binds this provider series to its sole
    // configured primary model. Keeping the model name out of Prometheus labels
    // avoids losing it when the bounded shared LoadStore folds a provider batch.
    let mut labels = std::collections::BTreeMap::from([
        (
            grid_signals::provider_observation::ADAPTER_KIND_LABEL.to_owned(),
            "dynamo/frontend".to_owned(),
        ),
        (
            grid_signals::provider_observation::ADAPTER_VERSION_LABEL.to_owned(),
            observation.source.adapter.version.to_string(),
        ),
        (
            grid_signals::provider_observation::PROVIDER_UID_LABEL.to_owned(),
            observation.source.provider_uid.clone(),
        ),
        (
            grid_signals::provider_observation::PROVIDER_GENERATION_LABEL.to_owned(),
            observation.source.provider_generation.to_string(),
        ),
        (
            grid_signals::provider_observation::MODEL_LABEL.to_owned(),
            observation.model.clone(),
        ),
        (
            grid_signals::provider_observation::MODEL_SCOPE_LABEL.to_owned(),
            "dedicatedSingleModel".to_owned(),
        ),
    ]);
    if let Some(value) = match observation.readiness {
        ModelReadiness::Ready => Some(1.0),
        ModelReadiness::NotReady => Some(0.0),
        ModelReadiness::Unknown => None,
    } {
        projected.push(canonical_observation(
            grid_signals::provider_observation::DYNAMO_FRONTEND_V1_MODEL_READY_METRIC,
            labels.clone(),
            value,
        ));
    }
    if let Some(active) = observation.active_requests
        && let Some(value) = count_as_signal(active.value)
    {
        projected.push(canonical_observation(
            grid_signals::provider_observation::DYNAMO_FRONTEND_V1_ACTIVE_REQUESTS_METRIC,
            labels.clone(),
            value,
        ));
    }
    if let Some(stages) = &observation.stage_requests {
        for stage in stages {
            labels.insert("stage".to_owned(), stage.stage.clone());
            labels.insert("phase".to_owned(), stage.phase.clone());
            if let Some(value) = count_as_signal(stage.count.value) {
                projected.push(canonical_observation(
                    grid_signals::provider_observation::DYNAMO_FRONTEND_V1_STAGE_REQUESTS_METRIC,
                    labels.clone(),
                    value,
                ));
            }
            labels.remove("stage");
            labels.remove("phase");
        }
    }
    projected
}

/// Counts cross the Prometheus float wire as integers. Values above 2^53 cannot
/// be represented exactly, so refuse them instead of silently rounding pressure.
fn count_as_signal(value: u64) -> Option<f64> {
    const MAX_EXACT_INTEGER: u64 = 9_007_199_254_740_991;
    if value > MAX_EXACT_INTEGER {
        return None;
    }
    #[expect(
        clippy::cast_precision_loss,
        reason = "bounded to the exact integer range of binary64"
    )]
    {
        Some(value as f64)
    }
}

/// Build one un-attributed canonical sample. The publisher adds Grid ownership
/// labels and the complete batch's revision in the existing signal path.
fn canonical_observation(
    metric: &str,
    labels: std::collections::BTreeMap<String, String>,
    value: f64,
) -> crate::signals::Observation {
    crate::signals::Observation {
        metric: metric.to_owned(),
        labels,
        value,
        timestamp_ms: None,
        age: Duration::ZERO,
    }
}

/// Whether the scrape carried a per-unit queue series for the pool: the EPP's collector for
/// those stops reporting when the pool has no units, while its pool gauges freeze.
fn units_reporting(parsed: &[crate::signals::Observation], pool: Option<&str>) -> bool {
    parsed.iter().any(|o| {
        crate::readiness::PER_UNIT_QUEUE.contains(&o.metric.as_str())
            && pool.is_none_or(|pool| o.labels.get("name").is_some_and(|n| n == pool))
    })
}

/// Count a scrape that answered: `success` with the pool's ready-endpoint series, else
/// `no_series`, returned as the message naming what was missing.
fn count_scrape(provider: &str, plan: &SignalScrapePlan<'_>, pool: Option<&str>, ready: Option<f64>) -> Option<String> {
    if ready.is_some() {
        crate::metrics::record_provider_scrape(provider, "success");
        crate::metrics::set_provider_last_scrape_success(provider, std::time::SystemTime::now());
        return None;
    }
    crate::metrics::record_provider_scrape(provider, "no_series");
    let series = plan.ready_names().join(" or ");
    Some(match pool {
        Some(pool) => format!("metrics reachable, but no {series} series for poolName {pool}"),
        None => format!("metrics reachable, but no {series} series"),
    })
}

/// The resolved `grid_provider_in_flight_requests` sample, logging which source it came from.
fn in_flight_observation(
    parsed: &[crate::signals::Observation],
    ready: Option<f64>,
    pool: Option<&str>,
    identity: &str,
) -> Option<crate::signals::Observation> {
    let (value, source) = crate::readiness::in_flight(parsed, ready, pool)?;
    tracing::debug!(
        provider = identity,
        in_flight = value,
        ?source,
        "signals: provider in-flight resolved"
    );
    Some(crate::signals::Observation {
        metric: crate::readiness::IN_FLIGHT_SIGNAL.to_owned(),
        labels: std::collections::BTreeMap::new(),
        value,
        timestamp_ms: None,
        age: Duration::ZERO,
    })
}

/// Scrape one provider's exposition, or the bounded class of the TLS or scrape failure.
async fn scrape_provider_signals(
    provider: &InferenceProvider,
    plan: &SignalScrapePlan<'_>,
    client: Option<&kube::Client>,
) -> Result<String, crate::readiness::ScrapeClass> {
    let mc = provider
        .spec
        .metrics_config
        .as_ref()
        .ok_or(crate::readiness::ScrapeClass::Config)?;
    let identity = plan.identity;
    let tls_settings = mc.tls.as_ref().or(provider.spec.tls.as_ref());
    let tls_config = match resolve_tls_config(tls_settings, client, identity).await {
        Ok(cfg) => cfg,
        Err((_reason, e)) => {
            if tls_settings.is_some() {
                tracing::debug!(provider = identity, error = %e, "signals: provider metrics TLS unavailable; not scraping in plaintext");
            }
            return Err(crate::readiness::ScrapeClass::Tls);
        },
    };
    let timeout = parse_metrics_timeout(&mc.timeout);
    scrape_metrics(&plan.url, timeout, tls_config, mc.auth.as_ref().zip(client))
        .await
        .map_err(|e| {
            tracing::debug!(provider = identity, error = %e, "signals: provider scrape failed");
            e.class()
        })
}

/// What to scrape for one provider and which of its series to republish.
pub(crate) struct SignalScrapePlan<'provider> {
    /// The provider's routing identity.
    pub(crate) identity: &'provider str,
    /// The metrics URL.
    pub(crate) url: String,
    /// A declared ready-endpoints metric, replacing the defaults.
    ready_override: Option<&'provider str>,
}

impl SignalScrapePlan<'_> {
    /// The ready-endpoints names to read, preferred first.
    fn ready_names(&self) -> &[&str] {
        self.ready_override
            .as_ref()
            .map_or(&crate::readiness::DEFAULT_READY_ENDPOINTS[..], std::slice::from_ref)
    }
}

/// The scrape plan for a provider, if it is eligible.
///
/// Pure and synchronous: eligibility is decided here so the scrape path stays
/// the I/O alone. `None` for a provider with no metrics config, no routing
/// identity, or a blank endpoint. A provider with no signal names is still
/// scraped, for readiness.
pub(crate) fn signal_scrape_plan(provider: &InferenceProvider) -> Option<SignalScrapePlan<'_>> {
    let mc = provider.spec.metrics_config.as_ref()?;
    let identity = routing_identity(provider)?;
    let endpoint = provider.spec.endpoint.trim();
    if endpoint.is_empty() || mc.endpoint.as_deref().is_some_and(|ep| ep.trim().is_empty()) {
        return None;
    }
    Some(SignalScrapePlan {
        identity,
        url: metrics_url(mc.endpoint.as_deref().unwrap_or(endpoint), &mc.path),
        ready_override: mc.signal_names.ready_endpoints.as_deref(),
    })
}

/// Metric families every Go and controller-runtime process exports about itself.
const RUNTIME_FAMILIES: [&str; 4] = ["go_", "process_", "controller_runtime_", "workqueue_"];

/// Whether a gauge the provider publishes is relayed: everything it says about serving,
/// nothing about its own process, so the wire stays well under the store's series cap.
fn relayed(metric: &str) -> bool {
    !RUNTIME_FAMILIES.iter().any(|family| metric.starts_with(family))
}

// ---------------------------------------------------------------------------
// URL construction
// ---------------------------------------------------------------------------

/// Construct the metrics scrape URL from a provider endpoint and configured path.
///
/// Trims a trailing `/` from `endpoint` before appending `path`.  If `path`
/// does not start with `/`, one is prepended.
///
/// ```text
/// metrics_url("http://backend:8080",  "/metrics") -> "http://backend:8080/metrics"
/// metrics_url("http://backend:8080/", "/metrics") -> "http://backend:8080/metrics"
/// metrics_url("http://backend:8080",  "metrics")  -> "http://backend:8080/metrics"
/// ```
pub(crate) fn metrics_url(endpoint: &str, path: &str) -> String {
    let base = endpoint.trim_end_matches('/');
    if path.starts_with('/') {
        format!("{base}{path}")
    } else {
        format!("{base}/{path}")
    }
}

// ---------------------------------------------------------------------------
// Config conversion
// ---------------------------------------------------------------------------

/// Convert CRD metrics configuration to a [`MetricNames`] parser config.
///
/// Signal fields that are `None` in the CRD remain `None` in the parser config
/// and are not extracted from the Prometheus text.  Pool-name selection and
/// queue-capacity normalisation pass through when configured.
pub(crate) fn metric_names_from_config(
    cfg: &MetricSignalNames,
    pool_name: Option<&str>,
    queue_capacity: Option<u32>,
) -> MetricNames {
    MetricNames {
        queue_depth: cfg.queue_depth.clone(),
        kv_cache_utilization: cfg.kv_cache_utilization.clone(),
        latency_p99_ms: cfg.latency_p99_ms.clone(),
        prefix_cache_hit_ratio: cfg.prefix_cache_hit_ratio.clone(),
        error_rate: cfg.error_rate.clone(),
        healthy: cfg.healthy.clone(),
        pool_name: pool_name.map(str::to_owned),
        queue_capacity: queue_capacity.map(f64::from),
    }
}

// ---------------------------------------------------------------------------
// Timeout parsing
// ---------------------------------------------------------------------------

/// Parse a scrape, scoring neutrally when the provider's pool matched no configured signal.
///
/// A missing pool series is a configuration gap, not a failed scrape, so the provider
/// stays routable, as it would with no `poolName`.
fn parse_or_neutral(text: &str, names: &MetricNames, provider: &str) -> PartialMetrics {
    parse_prometheus_text(text, names).unwrap_or_else(|error| {
        tracing::warn!(provider, %error, "no configured signal for the provider's pool; scoring neutrally");
        PartialMetrics::default()
    })
}

/// Result of parsing a successful provider scrape.
enum ParsedProviderMetrics {
    /// Legacy configurable signal mapping.
    Legacy(PartialMetrics),
    /// Versioned, model-scoped adapter observation.
    Adapter(grid_signals::provider_observation::ProviderModelObservation),
}

/// Parse the explicitly configured Dynamo Frontend v1 adapter.
fn parse_adapter_observation(
    provider: &InferenceProvider,
    local_site: Option<&str>,
    text: &str,
) -> Result<grid_signals::provider_observation::ProviderModelObservation, String> {
    let adapter = provider
        .spec
        .metrics_config
        .as_ref()
        .and_then(|metrics| metrics.adapter.as_ref())
        .ok_or_else(|| "adapter configuration is absent".to_owned())?;
    let site = local_site
        .filter(|site| !site.trim().is_empty())
        .ok_or_else(|| "local Grid site identity is unavailable".to_owned())?;
    let provider_uid = provider
        .metadata
        .uid
        .clone()
        .filter(|uid| !uid.trim().is_empty())
        .ok_or_else(|| "InferenceProvider UID is unavailable".to_owned())?;
    let provider_generation = provider
        .metadata
        .generation
        .ok_or_else(|| "InferenceProvider generation is unavailable".to_owned())?;
    let observed_at_ms = i64::try_from(
        time::OffsetDateTime::now_utc()
            .unix_timestamp_nanos()
            .div_euclid(1_000_000),
    )
    .map_err(|_error| "current timestamp is outside the supported millisecond range".to_owned())?;
    match adapter.kind {
        MetricsAdapterKind::DynamoFrontendV1 => {
            let concurrency = adapter.concurrency.as_ref();
            let config = grid_signals::dynamo_frontend::DynamoFrontendConfig {
                primary_model: &adapter.primary_model,
                model_aliases: &adapter.model_aliases,
                concurrency_profile: concurrency.map(|value| value.profile.as_str()),
                concurrency_capacity: concurrency.map(|value| value.capacity),
            };
            grid_signals::dynamo_frontend::parse_dynamo_frontend_v1(
                text,
                &config,
                grid_signals::dynamo_frontend::ProviderRevision {
                    site: site.to_owned(),
                    provider_uid,
                    provider_generation,
                },
                observed_at_ms,
            )
        },
    }
}

/// Record an adapter observation when the scrape failed or violated its metric contract.
fn unavailable_adapter_observation(
    provider: &InferenceProvider,
    local_site: Option<&str>,
    freshness: grid_signals::provider_observation::ObservationFreshness,
) -> Option<grid_signals::provider_observation::ProviderModelObservation> {
    let mut observation = parse_adapter_observation(provider, local_site, "").ok()?;
    observation.freshness = freshness;
    Some(observation)
}

/// Whether a current, contract-valid observation explicitly says its model cannot route.
///
/// Missing, stale, or invalid telemetry carries no load score and falls back to provider
/// health and the configured non-comparative routing order. It must not be converted into an
/// idle sample or a readiness failure. This also lets signal-poll mode use its Gateway path
/// while the local operator intentionally skips overlay scrapes.
pub(crate) fn freshly_reports_model_not_ready(
    observation: &grid_signals::provider_observation::ProviderModelObservation,
) -> bool {
    observation.freshness == grid_signals::provider_observation::ObservationFreshness::Fresh
        && observation.readiness == grid_signals::provider_observation::ModelReadiness::NotReady
}

/// Whether a cached adapter observation still belongs to this provider revision and site.
fn observation_matches_provider(
    observation: &grid_signals::provider_observation::ProviderModelObservation,
    provider: &InferenceProvider,
    local_site: Option<&str>,
) -> bool {
    let Some(adapter) = provider
        .spec
        .metrics_config
        .as_ref()
        .and_then(|metrics| metrics.adapter.as_ref())
    else {
        return false;
    };
    let source = &observation.source;
    local_site.is_some_and(|site| source.site == site)
        && provider
            .metadata
            .uid
            .as_deref()
            .is_some_and(|uid| source.provider_uid == uid)
        && provider
            .metadata
            .generation
            .is_some_and(|generation| source.provider_generation == generation)
        && observation.model == adapter.primary_model
        && source.adapter.kind == grid_signals::provider_observation::AdapterKind::DynamoFrontend
        && source.adapter.version == 1
}

/// Copy a recent cached adapter observation as explicitly Stale without making it routable.
#[expect(
    clippy::too_many_arguments,
    reason = "cache age, provider revision, site, and output map are independent freshness inputs"
)]
fn try_cached_adapter_observation(
    identity: &str,
    stale_metrics_seconds: Option<u32>,
    cache_snapshot: &HashMap<String, TimestampedMetrics>,
    now: Instant,
    provider: &InferenceProvider,
    local_site: Option<&str>,
    observations: &mut HashMap<String, grid_signals::provider_observation::ProviderModelObservation>,
) -> bool {
    let Some(ttl_seconds) = stale_metrics_seconds.filter(|seconds| *seconds > 0) else {
        return false;
    };
    let Some(cached) = cache_snapshot.get(identity) else {
        return false;
    };
    if now.saturating_duration_since(cached.scraped_at) > Duration::from_secs(u64::from(ttl_seconds)) {
        return false;
    }
    let Some(mut observation) = cached.observation.clone() else {
        return false;
    };
    if !observation_matches_provider(&observation, provider, local_site) {
        return false;
    }
    observation.freshness = grid_signals::provider_observation::ObservationFreshness::Stale;
    observations.insert(identity.to_owned(), observation);
    true
}

/// Parse a timeout string (`"2s"`, `"500ms"`) to a [`Duration`].
///
/// Supports `s` and `ms` suffixes only; minutes and bare numbers are not
/// recognised.  Returns [`DEFAULT_SCRAPE_TIMEOUT`] for unrecognised formats,
/// empty strings, or zero values.
pub(crate) fn parse_metrics_timeout(s: &str) -> Duration {
    let s = s.trim();
    if let Some(ms_str) = s.strip_suffix("ms")
        && let Ok(n) = ms_str.trim().parse::<u64>()
        && n > 0
    {
        return Duration::from_millis(n);
    }
    if let Some(s_str) = s.strip_suffix('s')
        && let Ok(n) = s_str.trim().parse::<u64>()
        && n > 0
    {
        return Duration::from_secs(n);
    }
    DEFAULT_SCRAPE_TIMEOUT
}

// ---------------------------------------------------------------------------
// Collection
// ---------------------------------------------------------------------------

/// Scrape and parse live metrics for providers in `network_name` that have `spec.metricsConfig`.
///
/// This compatibility wrapper always performs a fresh collection. Production
/// reconciliation should use [`collect_provider_metrics_with_refresh_interval`]
/// so watch-triggered reconciles can reuse the current scrape generation.
///
/// Returns a map from provider routing identity (the value of
/// `spec.clusterName`, or `metadata.name` when absent) to
/// [`scoring::BackendMetrics`].
///
/// Providers without `metricsConfig` or with a blank endpoint are skipped and
/// not present in the returned map.  Scrape failures are logged at `warn`
/// level unless a cached sample is used.
///
/// TLS material is resolved from `metricsConfig.tls` when set, otherwise from
/// `spec.tls` when set. It is read from Kubernetes Secrets via `client` and
/// used for server verification (and optional mTLS client authentication).
/// TLS resolution failures are fail-closed: the scrape is skipped and the
/// provider falls back to stale cache or neutral scoring.
///
/// # Stale metrics grace period
///
/// When `metricsConfig.stale_metrics_seconds` is set and a scrape fails, the
/// function consults `cache` for a previously-scraped value.  If the cached
/// entry is no older than `stale_metrics_seconds`, the cached
/// [`scoring::BackendMetrics`] is used instead of neutral scoring.  After the
/// grace period the provider falls back to absent metrics (neutral scoring).
///
/// When `stale_metrics_seconds` is absent (default), scrape failures always
/// produce neutral scoring, the same backward-compatible behaviour as before
/// this field was added.
///
/// `now` is passed in (rather than read from `Instant::now()`) so tests can
/// control the clock without sleeping.
#[cfg(test)]
pub(crate) async fn collect_provider_metrics(
    network_name: &str,
    providers: &[InferenceProvider],
    cache: &Mutex<MetricsCache>,
    now: Instant,
    client: Option<&kube::Client>,
) -> CollectedMetrics {
    collect_provider_metrics_with_refresh_interval(network_name, providers, cache, now, Duration::ZERO, client).await
}

/// Collect provider metrics and attribute adapter observations to this local site.
#[expect(
    clippy::too_many_arguments,
    reason = "refresh, site attribution, client trust, provider set, and cache are independent collection inputs"
)]
pub(crate) async fn collect_provider_metrics_with_refresh_interval_for_site(
    network_name: &str,
    providers: &[InferenceProvider],
    cache: &Mutex<MetricsCache>,
    now: Instant,
    refresh_interval: Duration,
    local_site: Option<&str>,
    client: Option<&kube::Client>,
) -> CollectedMetrics {
    collect_provider_metrics_inner(
        network_name,
        providers,
        cache,
        now,
        refresh_interval,
        local_site,
        client,
    )
    .await
}

/// Collect provider metrics, reusing samples collected within `refresh_interval`.
///
/// Reconciliation can be triggered by an overlay write or another watch event
/// before the next metrics refresh is due. Reusing the cached sample in that
/// window is important: it preserves the sample generation and prevents one
/// underlying EPP observation from advancing admission hysteresis twice.
#[expect(
    clippy::too_many_arguments,
    reason = "sequential per-provider scrape loop with cache reuse, TLS resolution, and error handling"
)]
#[cfg(test)]
pub(crate) async fn collect_provider_metrics_with_refresh_interval(
    network_name: &str,
    providers: &[InferenceProvider],
    cache: &Mutex<MetricsCache>,
    now: Instant,
    refresh_interval: Duration,
    client: Option<&kube::Client>,
) -> CollectedMetrics {
    collect_provider_metrics_inner(network_name, providers, cache, now, refresh_interval, None, client).await
}

/// Shared scrape loop; `local_site` is required to construct a revision-bound adapter sample.
#[expect(
    clippy::cognitive_complexity,
    clippy::too_many_arguments,
    clippy::too_many_lines,
    clippy::large_stack_frames,
    reason = "sequential per-provider scrape loop with cache reuse, TLS resolution, and error handling"
)]
async fn collect_provider_metrics_inner(
    network_name: &str,
    providers: &[InferenceProvider],
    cache: &Mutex<MetricsCache>,
    now: Instant,
    refresh_interval: Duration,
    local_site: Option<&str>,
    client: Option<&kube::Client>,
) -> CollectedMetrics {
    // Snapshot the relevant cache entries and allocate a generation for this
    // refresh cycle. Cached samples reused below retain their own generation.
    // The lock is held for microseconds rather than across network I/O.
    let (cache_snapshot, scrape_generation) = {
        let mut guard = cache.lock().await;
        let generation = guard.next_generation;
        guard.next_generation = generation.wrapping_add(1);
        let snapshot: HashMap<String, TimestampedMetrics> = guard
            .entries
            .iter()
            .filter(|((net, _), _)| net == network_name)
            .map(|((_, id), tm)| (id.clone(), tm.clone()))
            .collect();
        drop(guard);
        (snapshot, generation)
    };

    let mut result = HashMap::new();
    let mut generations: HashMap<String, u64> = HashMap::new();
    let mut observations = HashMap::new();
    let mut cache_updates: Vec<((String, String), TimestampedMetrics)> = Vec::new();

    for provider in providers {
        if provider.spec.grid_network_ref != network_name {
            continue;
        }
        let Some(mc) = &provider.spec.metrics_config else {
            continue;
        };
        let Some(identity) = routing_identity(provider) else {
            continue;
        };
        let endpoint = provider.spec.endpoint.trim();
        if endpoint.is_empty() {
            continue;
        }
        if let Some(ep) = mc.endpoint.as_deref()
            && ep.trim().is_empty()
        {
            tracing::warn!(
                provider = identity,
                "metricsConfig.endpoint is present but blank; skipping metrics collection"
            );
            continue;
        }
        if let Some(pn) = mc.pool_name.as_deref()
            && pn.trim().is_empty()
        {
            tracing::warn!(
                provider = identity,
                "poolName is present but blank; skipping metrics collection"
            );
            continue;
        }
        let base = mc.endpoint.as_deref().unwrap_or(endpoint);
        let url = metrics_url(base, &mc.path);
        let timeout = parse_metrics_timeout(&mc.timeout);
        let names = metric_names_from_config(&mc.signal_names, mc.pool_name.as_deref(), mc.queue_capacity);
        let tls_settings = mc.tls.as_ref().or(provider.spec.tls.as_ref());

        if refresh_interval > Duration::ZERO
            && let Some(cached) = cache_snapshot.get(identity)
            && now.saturating_duration_since(cached.scraped_at) < refresh_interval
            && mc.adapter.is_none() == cached.observation.is_none()
            && cached
                .observation
                .as_ref()
                .is_none_or(|observation| observation_matches_provider(observation, provider, local_site))
        {
            result.insert(identity.to_owned(), cached.metrics);
            generations.insert(identity.to_owned(), cached.generation);
            if let Some(observation) = cached.observation.as_ref() {
                observations.insert(identity.to_owned(), observation.clone());
            }
            continue;
        }

        let tls_config = match resolve_tls_config(tls_settings, client, identity).await {
            Ok(cfg) => cfg,
            Err((_reason, e)) => {
                if mc.adapter.is_some() {
                    let reused = try_cached_adapter_observation(
                        identity,
                        mc.stale_metrics_seconds,
                        &cache_snapshot,
                        now,
                        provider,
                        local_site,
                        &mut observations,
                    );
                    if !reused
                        && let Some(observation) = unavailable_adapter_observation(
                            provider,
                            local_site,
                            grid_signals::provider_observation::ObservationFreshness::Missing,
                        )
                    {
                        observations.insert(identity.to_owned(), observation);
                    }
                    result.insert(identity.to_owned(), UNOBSERVABLE_METRICS);
                    generations.insert(identity.to_owned(), scrape_generation);
                    tracing::warn!(
                        provider = identity,
                        error = %e,
                        "adapter metrics TLS resolution failed; provider excluded from routing"
                    );
                    continue;
                }
                let used_cache = try_cached_metrics(
                    identity,
                    mc.stale_metrics_seconds,
                    &cache_snapshot,
                    now,
                    &mut result,
                    &mut generations,
                );
                if used_cache {
                    tracing::debug!(
                        provider = identity,
                        error = %e,
                        "metrics TLS resolution failed; using cached sample within stale_metrics_seconds grace period"
                    );
                } else {
                    if tls_settings.is_some() {
                        tracing::warn!(
                            provider = identity,
                            error = %e,
                            "metrics TLS resolution failed; provider excluded from routing"
                        );
                        result.insert(identity.to_owned(), UNOBSERVABLE_METRICS);
                        generations.insert(identity.to_owned(), scrape_generation);
                    } else {
                        tracing::warn!(
                            provider = identity,
                            error = %e,
                            "metrics scrape setup failed; provider metrics absent (neutral scoring)"
                        );
                    }
                }
                continue;
            },
        };

        let scrape_result = scrape_metrics(&url, timeout, tls_config, mc.auth.as_ref().zip(client)).await;
        let parse_result = match &scrape_result {
            Ok(text) => match mc.adapter.as_ref() {
                Some(_) => parse_adapter_observation(provider, local_site, text).map(ParsedProviderMetrics::Adapter),
                None => Ok(ParsedProviderMetrics::Legacy(parse_or_neutral(text, &names, identity))),
            },
            Err(e) => Err(e.to_string()),
        };
        match parse_result {
            Ok(ParsedProviderMetrics::Legacy(parsed)) => {
                let bm = parsed.into_backend_metrics();
                cache_updates.push((
                    (network_name.to_owned(), identity.to_owned()),
                    TimestampedMetrics {
                        metrics: bm,
                        scraped_at: now,
                        generation: scrape_generation,
                        observation: None,
                    },
                ));
                result.insert(identity.to_owned(), bm);
                generations.insert(identity.to_owned(), scrape_generation);
            },
            Ok(ParsedProviderMetrics::Adapter(observation)) => {
                let bm = if observation.freshness == grid_signals::provider_observation::ObservationFreshness::Fresh
                    && observation.readiness == grid_signals::provider_observation::ModelReadiness::Ready
                {
                    scoring::BackendMetrics::healthy_default()
                } else {
                    UNOBSERVABLE_METRICS
                };
                cache_updates.push((
                    (network_name.to_owned(), identity.to_owned()),
                    TimestampedMetrics {
                        metrics: bm,
                        scraped_at: now,
                        generation: scrape_generation,
                        observation: Some(observation.clone()),
                    },
                ));
                result.insert(identity.to_owned(), bm);
                generations.insert(identity.to_owned(), scrape_generation);
                observations.insert(identity.to_owned(), observation);
            },
            Err(e) => {
                if mc.adapter.is_some() {
                    if scrape_result.is_err() {
                        let reused = try_cached_adapter_observation(
                            identity,
                            mc.stale_metrics_seconds,
                            &cache_snapshot,
                            now,
                            provider,
                            local_site,
                            &mut observations,
                        );
                        if !reused
                            && let Some(observation) = unavailable_adapter_observation(
                                provider,
                                local_site,
                                grid_signals::provider_observation::ObservationFreshness::Missing,
                            )
                        {
                            observations.insert(identity.to_owned(), observation);
                        }
                    } else if let Some(observation) = unavailable_adapter_observation(
                        provider,
                        local_site,
                        grid_signals::provider_observation::ObservationFreshness::Invalid,
                    ) {
                        observations.insert(identity.to_owned(), observation);
                    }
                    result.insert(identity.to_owned(), UNOBSERVABLE_METRICS);
                    generations.insert(identity.to_owned(), scrape_generation);
                    tracing::warn!(
                        provider = identity,
                        url = %url,
                        error = %e,
                        "Dynamo Frontend adapter sample unavailable; dynamic pressure is disabled and provider readiness is handled separately"
                    );
                    continue;
                }
                let reason_str = scrape_result
                    .as_ref()
                    .err()
                    .map_or("MetricsScrapeError", |error| classify_scrape_error(error));
                let used_cache = try_cached_metrics(
                    identity,
                    mc.stale_metrics_seconds,
                    &cache_snapshot,
                    now,
                    &mut result,
                    &mut generations,
                );
                if used_cache {
                    tracing::debug!(
                        provider = identity,
                        url = %url,
                        reason = reason_str,
                        error = %e,
                        "metrics scrape failed; using cached sample within stale_metrics_seconds grace period"
                    );
                } else {
                    if tls_settings.is_some() {
                        tracing::warn!(
                            provider = identity,
                            url = %url,
                            reason = reason_str,
                            error = %e,
                            "metrics scrape failed; provider excluded from routing"
                        );
                        result.insert(identity.to_owned(), UNOBSERVABLE_METRICS);
                        generations.insert(identity.to_owned(), scrape_generation);
                    } else {
                        tracing::warn!(
                            provider = identity,
                            url = %url,
                            reason = reason_str,
                            error = %e,
                            "metrics scrape failed; provider metrics absent (neutral scoring)"
                        );
                    }
                }
            },
        }
    }

    // Write back successful scrapes to the shared cache.
    if !cache_updates.is_empty() {
        let mut guard = cache.lock().await;
        for (key, val) in cache_updates {
            guard.entries.insert(key, val);
        }
    }

    CollectedMetrics {
        metrics: result,
        generations,
        observations,
    }
}

/// Attempt to populate `result` with a cached metric sample for `identity`.
///
/// Returns `true` if a valid cached sample was found and inserted; `false` if
/// no grace period is configured, the cache has no entry, or the entry is
/// too old.
#[expect(
    clippy::too_many_arguments,
    reason = "generations mirror added alongside existing result out-param"
)]
fn try_cached_metrics(
    identity: &str,
    stale_metrics_seconds: Option<u32>,
    cache_snapshot: &HashMap<String, TimestampedMetrics>,
    now: Instant,
    result: &mut HashMap<String, scoring::BackendMetrics>,
    generations: &mut HashMap<String, u64>,
) -> bool {
    let Some(ttl_secs) = stale_metrics_seconds.filter(|&s| s > 0) else {
        return false;
    };
    let ttl = Duration::from_secs(u64::from(ttl_secs));
    let Some(cached) = cache_snapshot.get(identity) else {
        return false;
    };
    let age = now.saturating_duration_since(cached.scraped_at);
    if age > ttl {
        return false;
    }
    result.insert(identity.to_owned(), cached.metrics);
    generations.insert(identity.to_owned(), cached.generation);
    true
}

// ---------------------------------------------------------------------------
// TLS material resolution (delegates to shared endpoint_tls module)
// ---------------------------------------------------------------------------

/// Resolve a [`rustls::ClientConfig`] from a provider's metrics TLS configuration.
///
/// Thin delegation to [`endpoint_tls::resolve_tls_config`](super::endpoint_tls::resolve_tls_config).
async fn resolve_tls_config(
    tls_config: Option<&EndpointTlsConfig>,
    client: Option<&kube::Client>,
    provider_identity: &str,
) -> Result<Option<super::tls_backend::ClientTlsConfig>, (super::endpoint_tls::TlsFailureReason, String)> {
    super::endpoint_tls::resolve_tls_config(tls_config, client, provider_identity).await
}

// ---------------------------------------------------------------------------
// Metrics TLS validation (delegates to shared endpoint_tls module)
// ---------------------------------------------------------------------------

/// Verify that the metrics TLS Secrets are accessible and valid.
///
/// Delegates to [`endpoint_tls::verify_tls_accessible`](super::endpoint_tls::verify_tls_accessible)
/// and maps the generic [`TlsFailureReason`](super::endpoint_tls::TlsFailureReason) to
/// a `"Metrics"`-prefixed status reason string.
///
/// # Returns
///
/// - `Ok(None)`: TLS material is accessible and valid (or no TLS configured).
/// - `Ok(Some(reason_string))`: failure; the provider should be marked [`Degraded`] with the returned reason in
///   `status.reason`.
///
/// [`Degraded`]: crate::crd::inference_provider::ProviderPhase::Degraded
///
/// # Errors
///
/// Returns [`OperatorError`] on Kubernetes API failures.
///
/// [`OperatorError`]: crate::error::OperatorError
pub(crate) async fn verify_metrics_tls_accessible(
    client: &kube::Client,
    tls_config: Option<&EndpointTlsConfig>,
) -> Result<Option<String>, crate::error::OperatorError> {
    match super::endpoint_tls::verify_tls_accessible(client, tls_config).await? {
        Some(reason) => Ok(Some(reason.as_status_reason("Metrics"))),
        None => Ok(None),
    }
}

/// Classify a [`MetricsScrapeError`](crate::metrics_scraper::MetricsScrapeError) into a
/// bounded log-level reason string.
///
/// These categories are used for structured logging only; they do not
/// appear in `InferenceProvider.status.reason`.  Status reasons are
/// reserved for material/configuration failures that the controller
/// can observe during reconciliation (see [`endpoint_tls::TlsFailureReason`](super::endpoint_tls::TlsFailureReason)).
pub(crate) fn classify_scrape_error(err: &metrics_scraper::MetricsScrapeError) -> &'static str {
    match err {
        metrics_scraper::MetricsScrapeError::Timeout(_) => "MetricsScrapeTimeout",
        metrics_scraper::MetricsScrapeError::NonOkStatus { status, .. } if *status == 401 || *status == 403 => {
            "MetricsUnauthorized"
        },
        metrics_scraper::MetricsScrapeError::Transport(e) => {
            let msg = e.to_string().to_lowercase();
            if msg.contains("tls") || msg.contains("certificate") || msg.contains("handshake") || msg.contains("ssl") {
                "MetricsTlsHandshakeFailed"
            } else {
                "MetricsScrapeError"
            }
        },
        metrics_scraper::MetricsScrapeError::TlsMaterial(_) | metrics_scraper::MetricsScrapeError::HttpWithTls(_) => {
            "MetricsTlsMaterialInvalid"
        },
        metrics_scraper::MetricsScrapeError::Credential(_)
        | metrics_scraper::MetricsScrapeError::PlaintextCredential(_) => "MetricsCredentialUnavailable",
        metrics_scraper::MetricsScrapeError::BodyTooLarge(_) => "MetricsBodyTooLarge",
        metrics_scraper::MetricsScrapeError::InvalidUrl(_)
        | metrics_scraper::MetricsScrapeError::NonOkStatus { .. }
        | metrics_scraper::MetricsScrapeError::Encoding(_) => "MetricsScrapeError",
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    #[test]
    fn a_pool_with_no_configured_signal_scores_neutrally() {
        let names = MetricNames {
            queue_depth: Some("llm_d_epp_average_queue_size".to_owned()),
            pool_name: Some("pool-a".to_owned()),
            ..Default::default()
        };
        let other_pool = "llm_d_epp_average_queue_size{name=\"pool-b\"} 7\n";
        assert_eq!(
            parse_or_neutral(other_pool, &names, "p"),
            PartialMetrics::default(),
            "a pool miss keeps the provider, with neutral metrics"
        );
        let own_pool = "llm_d_epp_average_queue_size{name=\"pool-a\"} 3\n";
        assert_eq!(
            parse_or_neutral(own_pool, &names, "p").queue_depth,
            Some(3.0),
            "a pool hit keeps its signal"
        );
    }

    use super::*;
    use crate::crd::inference_provider::MetricsConfig;

    // -----------------------------------------------------------------------
    // Test utilities
    // -----------------------------------------------------------------------

    /// Start a one-shot HTTP server that returns the given raw response bytes.
    ///
    /// Returns the bound `http://127.0.0.1:{port}` base URL.
    async fn start_test_server(response: Vec<u8>) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap_or_else(|_| std::process::abort());
        let port = listener.local_addr().unwrap_or_else(|_| std::process::abort()).port();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = [0_u8; 4096];
                drop(stream.read(&mut buf).await);
                drop(stream.write_all(&response).await);
            }
        });
        format!("http://127.0.0.1:{port}")
    }

    /// Start an HTTP server that counts concurrent requests and gates the first batch on a barrier.
    async fn start_bounded_test_server(
        response: Vec<u8>,
        expected_requests: usize,
        barrier_requests: usize,
    ) -> (String, std::sync::Arc<AtomicUsize>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap_or_else(|_| std::process::abort());
        let port = listener.local_addr().unwrap_or_else(|_| std::process::abort()).port();
        let state = CountingServerState {
            current: std::sync::Arc::new(AtomicUsize::new(0)),
            peak: std::sync::Arc::new(AtomicUsize::new(0)),
            barrier: std::sync::Arc::new(tokio::sync::Barrier::new(barrier_requests)),
        };
        let server_state = state.clone();
        tokio::spawn(async move {
            let mut accepted = 0;
            while accepted < expected_requests {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                accepted += 1;
                tokio::spawn(serve_counted_request(
                    stream,
                    response.clone(),
                    server_state.clone(),
                    accepted <= barrier_requests,
                ));
            }
        });
        (format!("http://127.0.0.1:{port}"), state.peak)
    }

    /// Shared counters and barrier for bounded-scrape test requests.
    #[derive(Clone)]
    struct CountingServerState {
        /// Current connection count.
        current: std::sync::Arc<AtomicUsize>,
        /// Peak connection count.
        peak: std::sync::Arc<AtomicUsize>,
        /// Barrier that releases the first request batch.
        barrier: std::sync::Arc<tokio::sync::Barrier>,
    }

    /// Serve one request while updating the server's current and peak connection counts.
    async fn serve_counted_request(
        mut stream: tokio::net::TcpStream,
        response: Vec<u8>,
        state: CountingServerState,
        waits_at_barrier: bool,
    ) {
        let CountingServerState { current, peak, barrier } = state;
        let active = current.fetch_add(1, Ordering::SeqCst) + 1;
        peak.fetch_max(active, Ordering::SeqCst);
        let mut buf = [0_u8; 4096];
        drop(stream.read(&mut buf).await);
        if waits_at_barrier {
            barrier.wait().await;
        }
        drop(stream.write_all(&response).await);
        current.fetch_sub(1, Ordering::SeqCst);
    }

    /// Build a raw HTTP 200 response with a text/plain body.
    fn ok_response(body: &str) -> Vec<u8> {
        format!(
            "HTTP/1.0 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
        .into_bytes()
    }

    /// Build a raw HTTP error response with an empty body.
    fn err_response(status: u16) -> Vec<u8> {
        format!("HTTP/1.0 {status} Error\r\nContent-Length: 0\r\n\r\n").into_bytes()
    }

    fn provider_fixture(name: &str, endpoint: &str, mc: Option<MetricsConfig>) -> InferenceProvider {
        let mut spec = serde_json::json!({
            "gridNetworkRef": "net",
            "providerKind": "self_hosted",
            "backendKind": "local",
            "endpoint": endpoint,
            "models": [{"name": "model-a"}]
        });
        if let Some(m) = mc
            && let Some(s) = spec.as_object_mut()
        {
            s.insert(
                "metricsConfig".to_owned(),
                serde_json::to_value(m).unwrap_or_else(|_| std::process::abort()),
            );
        }
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis.fast/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": {"name": name, "uid": format!("uid-{name}"), "generation": 1},
            "spec": spec
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    /// A `KServe` EPP exposition for pool `qwen3-kserve`, in the label sets the lab EPP emits:
    /// `served` streaming requests answered, about 104 usage reports each.
    fn kserve_epp_exposition(served: f64, ready: f64) -> String {
        let l = r#"fairness_id="default-flow",model_name="qwen3",priority="0",target_model_name="qwen3""#;
        let (b01, b05, b1) = (served * 0.2, served * 0.6, served * 0.9);
        let (ttft_sum, tpot_sum) = (served * 0.4, served * 0.02);
        let (reports, tokens) = (served * 104.0, served * 104.0 * 300.0);
        let (requests, errors) = (served + 14.0, served / 100.0);
        format!(
            r#"# HELP llm_d_epp_ready_endpoints Ready endpoints.
# TYPE llm_d_epp_ready_endpoints gauge
llm_d_epp_ready_endpoints{{name="qwen3-kserve"}} {ready}
# TYPE llm_d_epp_average_running_requests gauge
llm_d_epp_average_running_requests{{name="qwen3-kserve"}} 128
# TYPE llm_d_epp_request_ttft_seconds histogram
llm_d_epp_request_ttft_seconds_bucket{{{l},streaming="false",le="0.1"}} 14
llm_d_epp_request_ttft_seconds_bucket{{{l},streaming="false",le="+Inf"}} 14
llm_d_epp_request_ttft_seconds_sum{{{l},streaming="false"}} 1.9
llm_d_epp_request_ttft_seconds_count{{{l},streaming="false"}} 14
llm_d_epp_request_ttft_seconds_bucket{{{l},streaming="true",le="0.1"}} {b01}
llm_d_epp_request_ttft_seconds_bucket{{{l},streaming="true",le="0.5"}} {b05}
llm_d_epp_request_ttft_seconds_bucket{{{l},streaming="true",le="1"}} {b1}
llm_d_epp_request_ttft_seconds_bucket{{{l},streaming="true",le="2.5"}} {served}
llm_d_epp_request_ttft_seconds_bucket{{{l},streaming="true",le="+Inf"}} {served}
llm_d_epp_request_ttft_seconds_sum{{{l},streaming="true"}} {ttft_sum}
llm_d_epp_request_ttft_seconds_count{{{l},streaming="true"}} {served}
# TYPE llm_d_epp_request_streaming_tpot_seconds histogram
llm_d_epp_request_streaming_tpot_seconds_sum{{{l}}} {tpot_sum}
llm_d_epp_request_streaming_tpot_seconds_count{{{l}}} {served}
# TYPE llm_d_epp_request_input_tokens histogram
llm_d_epp_request_input_tokens_sum{{{l}}} {tokens}
llm_d_epp_request_input_tokens_count{{{l}}} {reports}
# TYPE llm_d_epp_request_total counter
llm_d_epp_request_total{{{l}}} {requests}
# TYPE llm_d_epp_request_error_total counter
llm_d_epp_request_error_total{{{l},error_code="503"}} {errors}
"#
        )
    }

    #[test]
    fn a_kserve_epp_scrape_publishes_latency_and_republishes_gauges_only() {
        let readiness = crate::readiness::ReadinessStore::default();
        let mut mc = mc_with_queue("llm_d_epp_average_running_requests");
        mc.pool_name = Some("qwen3-kserve".to_owned());
        // A counter declared as a signal is still never republished.
        mc.signal_names.error_rate = Some("llm_d_epp_request_total".to_owned());
        let provider = provider_fixture("qwen3-hq-east", "http://epp:9090", Some(mc));
        let plan = signal_scrape_plan(&provider).unwrap_or_else(|| std::process::abort());
        record_scrape(
            &readiness,
            "net/p",
            &provider,
            &plan,
            &kserve_epp_exposition(11_000.0, 2.0),
        );
        assert!(readiness.take_fresh("net/p").is_some(), "first scrape recorded");
        record_scrape(
            &readiness,
            "net/p",
            &provider,
            &plan,
            &kserve_epp_exposition(11_581.0, 0.0),
        );
        let published = readiness.take_fresh("net/p").unwrap_or_default();
        let value = |name: &str| published.iter().find(|o| o.metric == name).map(|o| o.value);
        for name in [
            crate::latency::TTFT_P50_SIGNAL,
            crate::latency::TTFT_P90_SIGNAL,
            crate::latency::TPOT_SIGNAL,
            crate::latency::ERROR_RATIO_SIGNAL,
        ] {
            assert!(value(name).is_some(), "{name} missing from {published:?}");
        }
        assert!((value(crate::latency::TPOT_SIGNAL).unwrap_or_default() - 0.02).abs() < 1e-9);
        assert!(
            published.iter().all(|o| !o.metric.ends_with("_total")
                && !o.metric.ends_with("_count")
                && !o.metric.ends_with("_sum")
                && !o.metric.ends_with("_bucket")),
            "counters and histogram parts stay local: {published:?}"
        );
        assert!(
            value("llm_d_epp_average_running_requests").is_some(),
            "declared gauge republished"
        );
        let verdict = readiness
            .verdict("net/p", false, Duration::from_secs(15), Instant::now())
            .unwrap_or_else(|| std::process::abort());
        assert!(
            verdict.message.contains("answered"),
            "progress seen through the parser: {verdict:?}"
        );
    }

    #[test]
    fn raw_pool_samples_cannot_cross_attribution_or_spoof_revision() {
        let readiness = crate::readiness::ReadinessStore::default();
        let mut metrics = mc_with_queue("llm_d_epp_average_queue_size");
        metrics.pool_name = Some("a".to_owned());
        let provider = provider_fixture("p", "http://epp:9090", Some(metrics));
        let plan = signal_scrape_plan(&provider).unwrap_or_else(|| std::process::abort());
        record_scrape(
            &readiness,
            "net/p",
            &provider,
            &plan,
            "llm_d_epp_average_queue_size{name=\"a\"} 2\nllm_d_epp_average_queue_size{name=\"b\"} 99\nllm_d_epp_average_queue_size 88\ngrid_provider_config_generation{name=\"a\"} 42\n",
        );
        let observations = readiness.take_fresh("net/p").unwrap_or_default();
        assert_eq!(
            observations.len(),
            1,
            "only the selected pool's genuine raw gauge survives"
        );
        assert_eq!(observations.first().map(|sample| sample.value), Some(2.0));
    }

    fn mc_with_queue(metric_name: &str) -> MetricsConfig {
        MetricsConfig {
            path: "/metrics".to_owned(),
            timeout: "2s".to_owned(),
            signal_names: MetricSignalNames {
                queue_depth: Some(metric_name.to_owned()),
                ..Default::default()
            },
            stale_metrics_seconds: None,
            adapter: None,
            endpoint: None,
            pool_name: None,
            queue_capacity: None,
            tls: None,
            auth: None,
        }
    }

    fn mc_with_queue_and_ttl(metric_name: &str, ttl: u32) -> MetricsConfig {
        MetricsConfig {
            path: "/metrics".to_owned(),
            timeout: "2s".to_owned(),
            signal_names: MetricSignalNames {
                queue_depth: Some(metric_name.to_owned()),
                ..Default::default()
            },
            stale_metrics_seconds: Some(ttl),
            adapter: None,
            endpoint: None,
            pool_name: None,
            queue_capacity: None,
            tls: None,
            auth: None,
        }
    }

    fn dynamo_adapter_config(ttl: Option<u32>) -> MetricsConfig {
        MetricsConfig {
            path: "/metrics".to_owned(),
            stale_metrics_seconds: ttl,
            adapter: Some(crate::crd::inference_provider::MetricsAdapterConfig {
                kind: MetricsAdapterKind::DynamoFrontendV1,
                scope: crate::crd::inference_provider::MetricsAdapterScope::DedicatedSingleModel,
                primary_model: "model-a".to_owned(),
                model_aliases: Vec::new(),
                concurrency: None,
            }),
            ..MetricsConfig::default()
        }
    }

    #[test]
    fn dynamo_adapter_projects_only_versioned_canonical_signals() {
        use grid_signals::provider_observation::{
            DYNAMO_FRONTEND_V1_ACTIVE_REQUESTS_METRIC, DYNAMO_FRONTEND_V1_MODEL_READY_METRIC,
            DYNAMO_FRONTEND_V1_STAGE_REQUESTS_METRIC,
        };

        let provider = provider_fixture("dynamo", "http://frontend:8000", Some(dynamo_adapter_config(None)));
        let plan = signal_scrape_plan(&provider).unwrap_or_else(|| std::process::abort());
        let readiness = crate::readiness::ReadinessStore::default();
        readiness.bind_revision("net/dynamo", Some("uid-dynamo"), Some(1));
        record_scrape_for_site(
            &readiness,
            "net/dynamo",
            &provider,
            &plan,
            "dynamo_frontend_model_ready{model=\"model-a\"} 1\n\
             dynamo_frontend_active_requests{model=\"model-a\"} 3\n\
             dynamo_frontend_stage_requests{phase=\"aggregated\",stage=\"dispatch\"} 2\n",
            Some("site-a"),
        );
        let observations = readiness.take_fresh("net/dynamo").unwrap_or_default();
        let has_identity = |labels: &std::collections::BTreeMap<String, String>| {
            labels
                .get(grid_signals::provider_observation::ADAPTER_KIND_LABEL)
                .is_some_and(|value| value == "dynamo/frontend")
                && labels
                    .get(grid_signals::provider_observation::ADAPTER_VERSION_LABEL)
                    .is_some_and(|value| value == "1")
                && labels
                    .get(grid_signals::provider_observation::PROVIDER_UID_LABEL)
                    .is_some_and(|value| value == "uid-dynamo")
                && labels
                    .get(grid_signals::provider_observation::PROVIDER_GENERATION_LABEL)
                    .is_some_and(|value| value == "1")
                && labels
                    .get(grid_signals::provider_observation::MODEL_LABEL)
                    .is_some_and(|value| value == "model-a")
                && labels
                    .get(grid_signals::provider_observation::MODEL_SCOPE_LABEL)
                    .is_some_and(|value| value == "dedicatedSingleModel")
        };

        assert!(
            observations
                .iter()
                .any(|sample| sample.metric == DYNAMO_FRONTEND_V1_MODEL_READY_METRIC
                    && sample.value.to_bits() == 1.0_f64.to_bits()
                    && has_identity(&sample.labels))
        );
        assert!(
            observations
                .iter()
                .any(|sample| sample.metric == DYNAMO_FRONTEND_V1_ACTIVE_REQUESTS_METRIC
                    && sample.value.to_bits() == 3.0_f64.to_bits()
                    && has_identity(&sample.labels))
        );
        assert!(
            observations
                .iter()
                .any(|sample| sample.metric == DYNAMO_FRONTEND_V1_STAGE_REQUESTS_METRIC
                    && sample.value.to_bits() == 2.0_f64.to_bits()
                    && has_identity(&sample.labels)
                    && sample.labels.get("stage").is_some_and(|value| value == "dispatch")
                    && sample.labels.get("phase").is_some_and(|value| value == "aggregated"))
        );

        let absent_readiness = parse_adapter_observation(
            &provider,
            Some("site-a"),
            "dynamo_frontend_active_requests{model=\"model-a\"} 0\n",
        )
        .unwrap_or_else(|_| std::process::abort());
        let projected = canonical_adapter_signals(&absent_readiness);
        assert!(
            projected
                .iter()
                .all(|sample| sample.metric != DYNAMO_FRONTEND_V1_MODEL_READY_METRIC)
        );
        assert!(
            projected
                .iter()
                .any(|sample| sample.metric == DYNAMO_FRONTEND_V1_ACTIVE_REQUESTS_METRIC),
            "missing readiness remains unknown even when active request count is zero"
        );
    }

    #[test]
    fn canonical_adapter_counts_above_exact_prometheus_integer_range_are_omitted() {
        assert_eq!(count_as_signal(9_007_199_254_740_991), Some(9_007_199_254_740_991.0));
        assert_eq!(count_as_signal(9_007_199_254_740_992), None);
    }

    #[test]
    fn only_fresh_explicit_model_not_ready_excludes_by_adapter_readiness() {
        let mut observation = grid_signals::dynamo_frontend::parse_dynamo_frontend_v1(
            "dynamo_frontend_model_ready{model=\"model-a\"} 0\n",
            &grid_signals::dynamo_frontend::DynamoFrontendConfig {
                primary_model: "model-a",
                model_aliases: &[],
                concurrency_profile: None,
                concurrency_capacity: None,
            },
            grid_signals::dynamo_frontend::ProviderRevision {
                site: "site-a".to_owned(),
                provider_uid: "uid-a".to_owned(),
                provider_generation: 1,
            },
            1_799_996_400_000,
        )
        .unwrap_or_else(|_| std::process::abort());
        assert!(freshly_reports_model_not_ready(&observation));

        observation.readiness = grid_signals::provider_observation::ModelReadiness::Unknown;
        assert!(
            !freshly_reports_model_not_ready(&observation),
            "an absent readiness series remains unknown"
        );
        observation.readiness = grid_signals::provider_observation::ModelReadiness::NotReady;
        for freshness in [
            grid_signals::provider_observation::ObservationFreshness::Stale,
            grid_signals::provider_observation::ObservationFreshness::Missing,
            grid_signals::provider_observation::ObservationFreshness::Invalid,
        ] {
            observation.freshness = freshness;
            assert!(
                !freshly_reports_model_not_ready(&observation),
                "{freshness:?} readiness is not authoritative"
            );
        }
    }

    /// Return a fresh empty metrics cache wrapped in a Mutex.
    fn empty_cache() -> Mutex<MetricsCache> {
        Mutex::new(MetricsCache::new())
    }

    // -----------------------------------------------------------------------
    // URL construction
    // -----------------------------------------------------------------------

    #[test]
    fn metrics_url_appends_path_to_endpoint() {
        assert_eq!(
            metrics_url("http://backend:8080", "/metrics"),
            "http://backend:8080/metrics"
        );
    }

    #[test]
    fn metrics_url_trims_trailing_slash_from_endpoint() {
        assert_eq!(
            metrics_url("http://backend:8080/", "/metrics"),
            "http://backend:8080/metrics"
        );
    }

    #[test]
    fn metrics_url_prepends_slash_when_path_lacks_one() {
        assert_eq!(
            metrics_url("http://backend:8080", "metrics"),
            "http://backend:8080/metrics"
        );
    }

    #[test]
    fn metrics_url_with_custom_path() {
        assert_eq!(
            metrics_url("http://backend:8080", "/custom/prometheus"),
            "http://backend:8080/custom/prometheus"
        );
    }

    // -----------------------------------------------------------------------
    // Timeout parsing
    // -----------------------------------------------------------------------

    #[test]
    fn parse_metrics_timeout_seconds() {
        assert_eq!(parse_metrics_timeout("2s"), Duration::from_secs(2));
        assert_eq!(parse_metrics_timeout("10s"), Duration::from_secs(10));
    }

    #[test]
    fn parse_metrics_timeout_milliseconds() {
        assert_eq!(parse_metrics_timeout("500ms"), Duration::from_millis(500));
        assert_eq!(parse_metrics_timeout("100ms"), Duration::from_millis(100));
    }

    #[test]
    fn parse_metrics_timeout_invalid_returns_default() {
        assert_eq!(
            parse_metrics_timeout("5m"),
            DEFAULT_SCRAPE_TIMEOUT,
            "minutes not supported"
        );
        assert_eq!(
            parse_metrics_timeout("5"),
            DEFAULT_SCRAPE_TIMEOUT,
            "bare number not supported"
        );
        assert_eq!(parse_metrics_timeout(""), DEFAULT_SCRAPE_TIMEOUT, "empty string");
        assert_eq!(parse_metrics_timeout("abc"), DEFAULT_SCRAPE_TIMEOUT, "non-numeric");
        assert_eq!(parse_metrics_timeout("0s"), DEFAULT_SCRAPE_TIMEOUT, "zero seconds");
    }

    // -----------------------------------------------------------------------
    // Config conversion
    // -----------------------------------------------------------------------

    #[test]
    fn metric_names_from_config_maps_all_signal_names() {
        let cfg = MetricSignalNames {
            queue_depth: Some("my_queue".to_owned()),
            kv_cache_utilization: Some("my_kv".to_owned()),
            latency_p99_ms: Some("my_latency".to_owned()),
            prefix_cache_hit_ratio: Some("my_prefix".to_owned()),
            error_rate: Some("my_errors".to_owned()),
            healthy: Some("my_health".to_owned()),
            ready_endpoints: None,
        };
        let names = metric_names_from_config(&cfg, None, None);
        assert_eq!(names.queue_depth.as_deref(), Some("my_queue"));
        assert_eq!(names.kv_cache_utilization.as_deref(), Some("my_kv"));
        assert_eq!(names.latency_p99_ms.as_deref(), Some("my_latency"));
        assert_eq!(names.prefix_cache_hit_ratio.as_deref(), Some("my_prefix"));
        assert_eq!(names.error_rate.as_deref(), Some("my_errors"));
        assert_eq!(names.healthy.as_deref(), Some("my_health"));
    }

    #[test]
    fn serving_gauges_are_relayed_and_runtime_families_are_not() {
        // The relay carries what the provider says about serving, declared or not, and
        // nothing a process says about itself.
        for metric in [
            "llm_d_epp_average_running_requests",
            "inference_pool_ready_pods",
            "vllm:num_requests_waiting",
            "queue_depth",
        ] {
            assert!(relayed(metric), "{metric} is relayed");
        }
        for metric in [
            "go_goroutines",
            "process_resident_memory_bytes",
            "controller_runtime_active_workers",
            "workqueue_depth",
        ] {
            assert!(!relayed(metric), "{metric} is runtime noise");
        }
    }

    #[test]
    fn metric_names_from_config_maps_none_for_absent_signals() {
        let names = metric_names_from_config(&MetricSignalNames::default(), None, None);
        assert!(names.queue_depth.is_none());
        assert!(names.kv_cache_utilization.is_none());
        assert!(names.latency_p99_ms.is_none());
        assert!(names.prefix_cache_hit_ratio.is_none());
        assert!(names.error_rate.is_none());
        assert!(names.healthy.is_none());
    }

    #[test]
    fn metric_names_from_config_passes_pool_name_and_queue_capacity() {
        let cfg = MetricSignalNames {
            queue_depth: Some("q".to_owned()),
            ..Default::default()
        };
        let names = metric_names_from_config(&cfg, Some("my-pool"), Some(100));
        assert_eq!(names.pool_name.as_deref(), Some("my-pool"));
        assert_eq!(names.queue_capacity, Some(100.0));
    }

    // -----------------------------------------------------------------------
    // collect_provider_metrics
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn collect_metrics_no_config_returns_empty_map() {
        let provider = provider_fixture("prov-a", "http://127.0.0.1:9999", None);
        let result = collect_provider_metrics("net", &[provider], &empty_cache(), Instant::now(), None).await;
        assert!(
            result.metrics.is_empty(),
            "provider without metricsConfig must not appear in metrics map"
        );
    }

    /// Start a TLS metrics endpoint whose server identity is signed by `ca`.
    async fn tls_metrics_endpoint(ca: &certs::CaCert, metrics: &str) -> String {
        let server_cert =
            certs::generate_dns_cert(ca, "metrics-server", "localhost").unwrap_or_else(|_| std::process::abort());
        let response = ok_response(metrics);
        crate::resources::test_doubles::start_tls_http_server(&server_cert.cert_pem, &server_cert.key_pem, response)
            .await
    }

    #[tokio::test]
    async fn metrics_scrape_inherits_shared_tls() {
        let ca = certs::generate_ca("shared-metrics-ca").unwrap_or_else(|_| std::process::abort());
        let endpoint = tls_metrics_endpoint(&ca, "queue_depth 0.25\n").await;
        let client = crate::resources::test_doubles::mock_kube_client_with_secrets(HashMap::from([(
            "shared-ca",
            crate::resources::test_doubles::secret_with_key("ca.crt", ca.cert_pem.as_bytes()),
        )]));
        let mut provider = provider_fixture("prov-a", &endpoint, Some(mc_with_queue("queue_depth")));
        provider.spec.tls = Some(crate::resources::test_doubles::endpoint_tls_for_ca("shared-ca"));

        let result = collect_provider_metrics("net", &[provider], &empty_cache(), Instant::now(), Some(&client)).await;
        let metrics = result.metrics.get("prov-a").unwrap_or_else(|| std::process::abort());

        assert!(
            (metrics.queue_depth - 0.25).abs() < f64::EPSILON,
            "shared CA should enable the TLS metrics scrape"
        );
    }

    #[tokio::test]
    async fn metrics_override_takes_precedence_over_shared_tls() {
        let ca = certs::generate_ca("override-metrics-ca").unwrap_or_else(|_| std::process::abort());
        let endpoint = tls_metrics_endpoint(&ca, "queue_depth 0.75\n").await;
        let client = crate::resources::test_doubles::mock_kube_client_with_secrets(HashMap::from([(
            "override-ca",
            crate::resources::test_doubles::secret_with_key("ca.crt", ca.cert_pem.as_bytes()),
        )]));
        let mut metrics_config = mc_with_queue("queue_depth");
        metrics_config.tls = Some(crate::resources::test_doubles::endpoint_tls_for_ca("override-ca"));
        let mut provider = provider_fixture("prov-a", &endpoint, Some(metrics_config));
        provider.spec.tls = Some(crate::resources::test_doubles::endpoint_tls_for_ca("missing-shared-ca"));

        let result = collect_provider_metrics("net", &[provider], &empty_cache(), Instant::now(), Some(&client)).await;
        let metrics = result.metrics.get("prov-a").unwrap_or_else(|| std::process::abort());

        assert!(
            (metrics.queue_depth - 0.75).abs() < f64::EPSILON,
            "metricsConfig.tls should override shared TLS"
        );
    }

    #[tokio::test]
    async fn metrics_shared_tls_resolution_failure_marks_provider_unhealthy() {
        let client = crate::resources::test_doubles::mock_kube_client_with_secrets(HashMap::new());
        let mut provider = provider_fixture("prov-a", "https://localhost:1", Some(mc_with_queue("queue_depth")));
        provider.spec.tls = Some(crate::resources::test_doubles::endpoint_tls_for_ca("missing-shared-ca"));

        let result = collect_provider_metrics("net", &[provider], &empty_cache(), Instant::now(), Some(&client)).await;
        let metrics = result.metrics.get("prov-a").unwrap_or_else(|| std::process::abort());

        assert!(!metrics.healthy, "invalid shared TLS material must fail closed");
    }

    #[tokio::test]
    async fn signal_scrape_inherits_shared_tls() {
        let ca = certs::generate_ca("shared-signals-ca").unwrap_or_else(|_| std::process::abort());
        let endpoint = tls_metrics_endpoint(&ca, "queue_depth 0.25\n").await;
        let client = crate::resources::test_doubles::mock_kube_client_with_secrets(HashMap::from([(
            "shared-ca",
            crate::resources::test_doubles::secret_with_key("ca.crt", ca.cert_pem.as_bytes()),
        )]));
        let mut provider = provider_fixture("prov-a", &endpoint, Some(mc_with_queue("queue_depth")));
        provider.spec.tls = Some(crate::resources::test_doubles::endpoint_tls_for_ca("shared-ca"));

        let readiness = crate::readiness::ReadinessStore::default();
        collect_provider_signals("net", &[provider], Some(&client), &readiness, Some("site-a")).await;
        let observations = readiness.take_fresh("net/prov-a").unwrap_or_default();

        assert!(
            observations
                .iter()
                .any(|observation| observation.metric == "queue_depth"),
            "shared CA should enable the TLS signal scrape"
        );
    }

    #[tokio::test]
    async fn signal_scrape_override_takes_precedence_over_shared_tls() {
        let ca = certs::generate_ca("override-signals-ca").unwrap_or_else(|_| std::process::abort());
        let endpoint = tls_metrics_endpoint(&ca, "queue_depth 0.75\n").await;
        let client = crate::resources::test_doubles::mock_kube_client_with_secrets(HashMap::from([(
            "override-ca",
            crate::resources::test_doubles::secret_with_key("ca.crt", ca.cert_pem.as_bytes()),
        )]));
        let mut metrics_config = mc_with_queue("queue_depth");
        metrics_config.tls = Some(crate::resources::test_doubles::endpoint_tls_for_ca("override-ca"));
        let mut provider = provider_fixture("prov-a", &endpoint, Some(metrics_config));
        provider.spec.tls = Some(crate::resources::test_doubles::endpoint_tls_for_ca("missing-shared-ca"));

        let readiness = crate::readiness::ReadinessStore::default();
        collect_provider_signals("net", &[provider], Some(&client), &readiness, Some("site-a")).await;
        let observations = readiness.take_fresh("net/prov-a").unwrap_or_default();

        assert!(
            observations
                .iter()
                .any(|observation| observation.metric == "queue_depth"),
            "metricsConfig.tls should override shared TLS"
        );
    }

    #[tokio::test]
    async fn signal_scrape_shared_tls_resolution_failure_skips_provider() {
        let client = crate::resources::test_doubles::mock_kube_client_with_secrets(HashMap::new());
        let mut provider = provider_fixture("prov-a", "https://localhost:1", Some(mc_with_queue("queue_depth")));
        provider.spec.tls = Some(crate::resources::test_doubles::endpoint_tls_for_ca("missing-shared-ca"));

        let readiness = crate::readiness::ReadinessStore::default();
        collect_provider_signals("net", &[provider], Some(&client), &readiness, Some("site-a")).await;

        assert!(
            readiness.take_fresh("net/prov-a").is_none(),
            "invalid shared TLS material must skip the scrape"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn one_slow_signal_scrape_does_not_delay_its_siblings() {
        let slow_url = crate::resources::test_doubles::start_delayed_http_server(
            ok_response("slow_queue 0.1\n"),
            Duration::from_millis(300),
        )
        .await;
        let fast_url = start_test_server(ok_response("fast_queue 0.2\n")).await;
        let mut slow_metrics = mc_with_queue("slow_queue");
        slow_metrics.timeout = "100ms".to_owned();
        let slow = provider_fixture("slow", &slow_url, Some(slow_metrics));
        let fast = provider_fixture("fast", &fast_url, Some(mc_with_queue("fast_queue")));
        let readiness = crate::readiness::ReadinessStore::default();
        let started = tokio::time::Instant::now();

        collect_provider_signals("net", &[slow, fast], None, &readiness, Some("site-a")).await;

        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_millis(200),
            "a fast sibling must not wait for a slow provider's timeout: {elapsed:?}"
        );
        assert!(
            readiness.take_fresh("net/fast").is_some(),
            "the fast sibling still records its scrape"
        );
        assert!(
            readiness.take_fresh("net/slow").is_none(),
            "the slow provider records a failure, not a fresh sample"
        );
    }

    #[tokio::test]
    async fn provider_signal_scrapes_are_bounded_at_sixteen() {
        let (endpoint, peak_connections) = start_bounded_test_server(ok_response("bounded_queue 0.1\n"), 17, 16).await;
        let mut metrics = mc_with_queue("bounded_queue");
        metrics.timeout = "10s".to_owned();
        let providers: Vec<_> = (0..17)
            .map(|index| provider_fixture(&format!("provider-{index}"), &endpoint, Some(metrics.clone())))
            .collect();
        let readiness = crate::readiness::ReadinessStore::default();

        let collected = tokio::time::timeout(
            Duration::from_secs(15),
            collect_provider_signals("net", &providers, None, &readiness, Some("site-a")),
        )
        .await;
        assert!(collected.is_ok(), "the bounded scrape pass should finish");

        for provider in &providers {
            let name = provider.metadata.name.as_deref().unwrap_or_default();
            assert!(
                readiness.take_fresh(&format!("net/{name}")).is_some(),
                "every bounded provider should complete its scrape"
            );
        }
        assert_eq!(
            peak_connections.load(Ordering::SeqCst),
            16,
            "seventeen providers must use at most sixteen concurrent scrapes"
        );
    }

    #[tokio::test]
    async fn collect_metrics_blank_endpoint_is_skipped() {
        let provider = provider_fixture("prov-a", "", Some(mc_with_queue("my_queue")));
        let result = collect_provider_metrics("net", &[provider], &empty_cache(), Instant::now(), None).await;
        assert!(
            result.metrics.is_empty(),
            "provider with blank endpoint must not be scraped"
        );
    }

    #[tokio::test]
    async fn collect_metrics_valid_scrape_inserts_backend_metrics() {
        let body = "my_queue 0.2\n";
        let base_url = start_test_server(ok_response(body)).await;
        let provider = provider_fixture("prov-a", &base_url, Some(mc_with_queue("my_queue")));

        let result = collect_provider_metrics("net", &[provider], &empty_cache(), Instant::now(), None).await;
        assert!(
            result.metrics.contains_key("prov-a"),
            "provider must appear in metrics map after successful scrape"
        );
        let bm = result
            .metrics
            .get("prov-a")
            .copied()
            .unwrap_or_else(|| std::process::abort());
        assert!(bm.queue_depth.is_finite(), "queue_depth must be finite");
        assert!(
            bm.queue_depth >= 0.0 && bm.queue_depth <= 1.0,
            "queue_depth must be in [0,1]"
        );
    }

    #[tokio::test]
    async fn collect_metrics_uses_routing_identity_as_key() {
        let body = "my_queue 0.3\n";
        let base_url = start_test_server(ok_response(body)).await;
        let provider: InferenceProvider = serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis.fast/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": {"name": "prov-a"},
            "spec": {
                "gridNetworkRef": "net",
                "providerKind": "self_hosted",
                "backendKind": "local",
                "endpoint": base_url,
                "models": [{"name": "model-a"}],
                "clusterName": "site-x",
                "metricsConfig": {
                    "path": "/metrics",
                    "timeout": "2s",
                    "signalNames": {"queueDepth": "my_queue"}
                }
            }
        }))
        .unwrap_or_else(|_| std::process::abort());

        let result = collect_provider_metrics("net", &[provider], &empty_cache(), Instant::now(), None).await;
        assert!(
            result.metrics.contains_key("site-x"),
            "metrics must be keyed by clusterName, not metadata.name"
        );
        assert!(
            !result.metrics.contains_key("prov-a"),
            "metadata.name must not be used as key when clusterName is set"
        );
    }

    #[tokio::test]
    async fn collect_metrics_scrape_failure_plaintext_omits_entry() {
        let provider = provider_fixture("prov-a", "http://127.0.0.1:1", Some(mc_with_queue("my_queue")));
        let result = collect_provider_metrics("net", &[provider], &empty_cache(), Instant::now(), None).await;
        assert!(
            !result.metrics.contains_key("prov-a"),
            "plaintext provider scrape failure must not insert unobservable metrics"
        );
    }

    #[tokio::test]
    async fn collect_metrics_non_2xx_plaintext_omits_entry() {
        let base_url = start_test_server(err_response(503)).await;
        let provider = provider_fixture("prov-a", &base_url, Some(mc_with_queue("my_queue")));
        let result = collect_provider_metrics("net", &[provider], &empty_cache(), Instant::now(), None).await;
        assert!(
            !result.metrics.contains_key("prov-a"),
            "plaintext provider non-2xx response must not insert unobservable metrics"
        );
    }

    #[tokio::test]
    async fn collect_metrics_malformed_body_produces_finite_metrics() {
        // Malformed Prometheus text: metric not found -> neutral defaults.
        let body = "not_prometheus_text {invalid} NaN\n";
        let base_url = start_test_server(ok_response(body)).await;
        let provider = provider_fixture("prov-a", &base_url, Some(mc_with_queue("my_queue")));

        let result = collect_provider_metrics("net", &[provider], &empty_cache(), Instant::now(), None).await;
        assert!(
            result.metrics.contains_key("prov-a"),
            "malformed body must still produce a metrics entry"
        );
        let bm = result
            .metrics
            .get("prov-a")
            .copied()
            .unwrap_or_else(|| std::process::abort());
        assert!(
            bm.queue_depth.is_finite(),
            "malformed body must produce finite queue_depth"
        );
        assert!(
            bm.kv_cache_utilization.is_finite(),
            "malformed body must produce finite kv_cache_utilization"
        );
        assert!(
            bm.latency_p99_ms.is_finite(),
            "malformed body must produce finite latency_p99_ms"
        );
    }

    #[tokio::test]
    async fn collect_metrics_pool_miss_scores_neutrally_and_caches() {
        // A 200 scrape whose only series belongs to a different pool is a pool miss.
        let body = "llm_d_epp_average_queue_size{name=\"pool-b\"} 7\n";
        let base_url = start_test_server(ok_response(body)).await;
        let mut mc = mc_with_queue("llm_d_epp_average_queue_size");
        mc.pool_name = Some("pool-a".to_owned());
        let provider = provider_fixture("prov-a", &base_url, Some(mc));
        let cache = empty_cache();

        let result = collect_provider_metrics("net", &[provider], &cache, Instant::now(), None).await;

        let bm = result
            .metrics
            .get("prov-a")
            .copied()
            .unwrap_or_else(|| std::process::abort());
        let neutral = PartialMetrics::default().into_backend_metrics();
        assert!(
            (bm.queue_depth - neutral.queue_depth).abs() < f64::EPSILON,
            "a pool miss scores neutrally, not from the other pool's series"
        );
        assert!(
            cache
                .lock()
                .await
                .entries
                .contains_key(&("net".to_owned(), "prov-a".to_owned())),
            "a neutral pool-miss sample is written to the cache"
        );
    }

    #[tokio::test]
    async fn collect_metrics_multiple_providers_all_present() {
        let body_a = "my_queue 0.1\n";
        let body_b = "my_queue 0.9\n";
        let url_a = start_test_server(ok_response(body_a)).await;
        let url_b = start_test_server(ok_response(body_b)).await;

        let prov_a = provider_fixture("prov-a", &url_a, Some(mc_with_queue("my_queue")));
        let prov_b = provider_fixture("prov-b", &url_b, Some(mc_with_queue("my_queue")));

        let result = collect_provider_metrics("net", &[prov_a, prov_b], &empty_cache(), Instant::now(), None).await;
        assert!(result.metrics.contains_key("prov-a"), "prov-a must be in metrics map");
        assert!(result.metrics.contains_key("prov-b"), "prov-b must be in metrics map");
        assert!(
            result
                .metrics
                .get("prov-a")
                .copied()
                .unwrap_or_else(|| std::process::abort())
                .queue_depth
                < result
                    .metrics
                    .get("prov-b")
                    .copied()
                    .unwrap_or_else(|| std::process::abort())
                    .queue_depth,
            "prov-a (queue=0.1) must have lower queue_depth than prov-b (queue=0.9)"
        );
    }

    #[tokio::test]
    async fn refresh_interval_reuses_generation_for_watch_reconcile() {
        let url = start_test_server(ok_response("my_queue 0.9\n")).await;
        let provider = provider_fixture("prov-a", &url, Some(mc_with_queue("my_queue")));
        let cache = empty_cache();
        let t0 = Instant::now();

        let first = collect_provider_metrics_with_refresh_interval(
            "net",
            std::slice::from_ref(&provider),
            &cache,
            t0,
            Duration::from_secs(10),
            None,
        )
        .await;
        let second = collect_provider_metrics_with_refresh_interval(
            "net",
            std::slice::from_ref(&provider),
            &cache,
            t0 + Duration::from_secs(1),
            Duration::from_secs(10),
            None,
        )
        .await;

        assert_eq!(
            first.generations.get("prov-a"),
            second.generations.get("prov-a"),
            "a watch-triggered reconcile within the refresh interval must reuse the scrape generation"
        );
    }

    #[tokio::test]
    async fn refresh_interval_allocates_new_generation_after_expiry() {
        let url = start_test_server(ok_response("my_queue 0.9\n")).await;
        let provider = provider_fixture("prov-a", &url, Some(mc_with_queue("my_queue")));
        let cache = empty_cache();
        let t0 = Instant::now();

        let first = collect_provider_metrics_with_refresh_interval(
            "net",
            std::slice::from_ref(&provider),
            &cache,
            t0,
            Duration::from_secs(10),
            None,
        )
        .await;
        let second = collect_provider_metrics_with_refresh_interval(
            "net",
            std::slice::from_ref(&provider),
            &cache,
            t0 + Duration::from_secs(11),
            Duration::from_secs(10),
            None,
        )
        .await;

        assert_ne!(
            first.generations.get("prov-a"),
            second.generations.get("prov-a"),
            "a later metrics refresh must receive a new scrape generation"
        );
    }

    #[tokio::test]
    async fn collect_metrics_skips_providers_from_other_networks() {
        let body = "my_queue 0.2\n";
        let base_url = start_test_server(ok_response(body)).await;
        let provider = provider_fixture("prov-a", &base_url, Some(mc_with_queue("my_queue")));

        let result = collect_provider_metrics("other-net", &[provider], &empty_cache(), Instant::now(), None).await;
        assert!(
            result.metrics.is_empty(),
            "provider from a different GridNetwork must not be scraped"
        );
    }

    // -----------------------------------------------------------------------
    // Stale metrics cache
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn stale_cache_used_within_ttl_on_scrape_failure() {
        // Seed the cache with a known good sample from T=0.
        let t0 = Instant::now();
        let provider = provider_fixture("prov-a", "http://127.0.0.1:1", Some(mc_with_queue_and_ttl("q", 60)));
        let cache = empty_cache();
        {
            let mut guard = cache.lock().await;
            guard.entries.insert(
                ("net".to_owned(), "prov-a".to_owned()),
                TimestampedMetrics {
                    metrics: scoring::BackendMetrics {
                        queue_depth: 0.42,
                        healthy: true,
                        kv_cache_utilization: 0.5,
                        latency_p99_ms: 2500.0,
                        prefix_cache_hit_ratio: 0.5,
                        error_rate: 0.0,
                    },
                    scraped_at: t0,
                    generation: 0,
                    observation: None,
                },
            );
        }
        // Scrape fails (port 1 is always refused); cache is within 60 s TTL.
        let result = collect_provider_metrics("net", &[provider], &cache, t0, None).await;
        assert!(
            result.metrics.contains_key("prov-a"),
            "failed scrape within TTL must use cached metrics"
        );
        let cached_bm = result
            .metrics
            .get("prov-a")
            .copied()
            .unwrap_or_else(|| std::process::abort());
        assert!(
            (cached_bm.queue_depth - 0.42).abs() < f64::EPSILON,
            "cached queue_depth must be returned"
        );
    }

    #[tokio::test]
    async fn stale_cache_not_used_after_ttl_expires() {
        let t0 = Instant::now();
        // The TTL is 1 s; advance the clock by 2 s to make the cache stale.
        let t_after_ttl = t0.checked_add(Duration::from_secs(2)).unwrap_or(t0);
        let provider = provider_fixture("prov-a", "http://127.0.0.1:1", Some(mc_with_queue_and_ttl("q", 1)));
        let cache = empty_cache();
        {
            let mut guard = cache.lock().await;
            guard.entries.insert(
                ("net".to_owned(), "prov-a".to_owned()),
                TimestampedMetrics {
                    metrics: scoring::BackendMetrics {
                        queue_depth: 0.42,
                        healthy: true,
                        kv_cache_utilization: 0.5,
                        latency_p99_ms: 2500.0,
                        prefix_cache_hit_ratio: 0.5,
                        error_rate: 0.0,
                    },
                    scraped_at: t0,
                    generation: 0,
                    observation: None,
                },
            );
        }
        // Clock advanced past TTL -> cache entry is expired -> plaintext provider omitted.
        let result = collect_provider_metrics("net", &[provider], &cache, t_after_ttl, None).await;
        assert!(
            !result.metrics.contains_key("prov-a"),
            "expired cache on plaintext provider must not insert unobservable metrics"
        );
    }

    #[tokio::test]
    async fn no_ttl_configured_scrape_failure_plaintext_omits_entry() {
        let provider = provider_fixture("prov-a", "http://127.0.0.1:1", Some(mc_with_queue("q")));
        let cache = empty_cache();
        {
            let mut guard = cache.lock().await;
            guard.entries.insert(
                ("net".to_owned(), "prov-a".to_owned()),
                TimestampedMetrics {
                    metrics: scoring::BackendMetrics {
                        queue_depth: 0.42,
                        healthy: true,
                        kv_cache_utilization: 0.5,
                        latency_p99_ms: 2500.0,
                        prefix_cache_hit_ratio: 0.5,
                        error_rate: 0.0,
                    },
                    scraped_at: Instant::now(),
                    generation: 0,
                    observation: None,
                },
            );
        }
        let result = collect_provider_metrics("net", &[provider], &cache, Instant::now(), None).await;
        assert!(
            !result.metrics.contains_key("prov-a"),
            "plaintext provider without stale_metrics_seconds must not insert unobservable metrics"
        );
    }

    #[tokio::test]
    async fn successful_scrape_updates_cache() {
        let body = "my_queue 0.33\n";
        let base_url = start_test_server(ok_response(body)).await;
        let provider = provider_fixture("prov-a", &base_url, Some(mc_with_queue_and_ttl("my_queue", 30)));
        let cache = empty_cache();
        let t0 = Instant::now();

        let _unused = collect_provider_metrics("net", &[provider], &cache, t0, None).await;

        // Read from the cache: extract the queue_depth while holding the lock,
        // then release the guard so the MutexGuard does not live across the assert.
        let cached_queue_depth = cache
            .lock()
            .await
            .entries
            .get(&("net".to_owned(), "prov-a".to_owned()))
            .map(|tm| tm.metrics.queue_depth);
        assert!(cached_queue_depth.is_some(), "successful scrape must write to cache");
        assert!(
            (cached_queue_depth.unwrap_or_else(|| std::process::abort()) - 0.33).abs() < f64::EPSILON,
            "cache must hold the scraped queue_depth value"
        );
    }

    #[tokio::test]
    async fn dynamo_adapter_scrape_keeps_frontend_counts_model_scoped_and_unit_typed() {
        let body = concat!(
            "dynamo_frontend_active_requests{model=\"model-a\"} 3\n",
            "dynamo_frontend_model_ready{model=\"model-a\"} 1\n",
            "dynamo_frontend_stage_requests{phase=\"aggregated\",stage=\"dispatch\"} 2\n",
        );
        let endpoint = start_test_server(ok_response(body)).await;
        let provider = provider_fixture("dynamo-a", &endpoint, Some(dynamo_adapter_config(None)));
        let collected = collect_provider_metrics_with_refresh_interval_for_site(
            "net",
            std::slice::from_ref(&provider),
            &empty_cache(),
            Instant::now(),
            Duration::ZERO,
            Some("site-a"),
            None,
        )
        .await;
        let observation = collected
            .observations
            .get("dynamo-a")
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(
            observation.source.site, "site-a",
            "the local site is part of source identity"
        );
        assert_eq!(
            observation.source.provider_uid, "uid-dynamo-a",
            "provider UID is revision-bound"
        );
        assert_eq!(
            observation.source.provider_generation, 1,
            "provider generation is revision-bound"
        );
        assert_eq!(observation.model, "model-a", "the primary model is canonical");
        assert_eq!(
            observation.readiness,
            grid_signals::provider_observation::ModelReadiness::Ready,
            "Dynamo model readiness remains an explicit boolean state"
        );
        assert_eq!(
            observation.active_requests.map(|count| (count.value, count.unit)),
            Some((3, grid_signals::provider_observation::CountUnit::Requests)),
            "Frontend active requests are not converted to queue depth"
        );
        assert_eq!(
            observation.stage_requests.as_ref().map(Vec::len),
            Some(1),
            "stage gauge remains a separate signal"
        );
        assert_eq!(
            collected.metrics.get("dynamo-a").map(|metrics| metrics.queue_depth),
            Some(0.5),
            "the legacy queueDepth field retains its neutral default"
        );
    }

    #[tokio::test]
    async fn deterministic_dynamo_scrapes_flow_through_canonical_observations_into_overlay_scores() {
        // This is a Grid contract test over controlled HTTP exposition. The
        // separate Kind smoke is the evidence for a real Dynamo runtime.
        let serve = |active| {
            let body = format!(
                "dynamo_frontend_active_requests{{model=\"model-a\"}} {active}\n\
                 dynamo_frontend_model_ready{{model=\"model-a\"}} 1\n"
            );
            start_test_server(ok_response(&body))
        };
        let (busier_endpoint, idler_endpoint) = tokio::join!(serve(6), serve(2));
        let configured = |endpoint: &str, name: &str| {
            let mut metrics = dynamo_adapter_config(None);
            if let Some(adapter) = metrics.adapter.as_mut() {
                adapter.concurrency = Some(crate::crd::inference_provider::MetricsConcurrencyConfig {
                    profile: "mock-concurrency-v1".to_owned(),
                    capacity: 10,
                });
            }
            provider_fixture(name, endpoint, Some(metrics))
        };
        let providers = [
            configured(&busier_endpoint, "pool-busier"),
            configured(&idler_endpoint, "pool-idle"),
        ];
        let collected = collect_provider_metrics_with_refresh_interval_for_site(
            "net",
            &providers,
            &empty_cache(),
            Instant::now(),
            Duration::ZERO,
            Some("site-a"),
            None,
        )
        .await;
        let network: crate::crd::grid_network::GridNetwork = serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis.fast/v1alpha1",
            "kind": "GridNetwork",
            "metadata": { "name": "net" },
            "spec": {
                "seeds": [],
                "routingPolicy": "scoreFirst",
                "scoringPolicy": { "strategy": "dynamoRequestPressure" }
            }
        }))
        .unwrap_or_else(|_| std::process::abort());
        let overlay = crate::resources::routing_overlay::render_routing_overlay_with_adapter_observations(
            &network,
            &[],
            &providers,
            &[],
            &[],
            &[],
            "grid-a",
            "site-a",
            None,
            None,
            None,
            &crate::crd::grid_network::ScoringStrategy::DynamoRequestPressure.weights(),
            None,
            &crate::served_models::ServedModelStore::new(),
            crate::resources::routing_overlay::CandidateExpirySupport::Supported,
            Some(&collected.observations),
        )
        .unwrap_or_else(|_| std::process::abort());

        assert_eq!(
            overlay
                .candidates
                .iter()
                .map(|candidate| candidate.cluster.as_str())
                .collect::<Vec<_>>(),
            ["pool-idle", "pool-busier"],
            "real HTTP scraping, adapter normalization, and overlay scoring must preserve pressure order"
        );
        let [idler, busier] = overlay.candidates.as_slice() else {
            std::process::abort();
        };
        assert_eq!(idler.score, Some(0.8));
        assert_eq!(busier.score, Some(0.4));
        assert!(
            overlay
                .candidates
                .iter()
                .all(|candidate| candidate.score_breakdown.is_none())
        );
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "the captured two-pool contract is intentionally exercised through scrape, normalization, and ranking"
    )]
    async fn captured_two_pool_frontend_samples_preserve_pressure_order_in_the_overlay_contract() {
        // These exact series were captured from two real pinned Frontend Pods
        // during the bounded Mocker load run. The test replays the exposition
        // through Grid's scraper/parser and overlay renderer; its test-only
        // denominator is not a production serving-capacity qualification.
        let loaded_endpoint = start_test_server(ok_response(include_str!(
            "../../tests/fixtures/dynamo-two-pool/pool-a-pressure.prom"
        )))
        .await;
        let idle_endpoint = start_test_server(ok_response(include_str!(
            "../../tests/fixtures/dynamo-two-pool/pool-b-idle.prom"
        )))
        .await;
        let configured = |endpoint: &str, name: &str| {
            let mut metrics = dynamo_adapter_config(None);
            if let Some(adapter) = metrics.adapter.as_mut() {
                adapter.primary_model = "Qwen/Qwen3-0.6B".to_owned();
                adapter.concurrency = Some(crate::crd::inference_provider::MetricsConcurrencyConfig {
                    profile: "captured-mocker-batch-4".to_owned(),
                    capacity: 4,
                });
            }
            let mut provider = provider_fixture(name, endpoint, Some(metrics));
            if let Some(model) = provider.spec.models.first_mut() {
                model.name = "Qwen/Qwen3-0.6B".to_owned();
            }
            provider
        };
        let providers = [
            configured(&loaded_endpoint, "pool-a"),
            configured(&idle_endpoint, "pool-b"),
        ];
        let collected = collect_provider_metrics_with_refresh_interval_for_site(
            "net",
            &providers,
            &empty_cache(),
            Instant::now(),
            Duration::ZERO,
            Some("site-a"),
            None,
        )
        .await;
        assert_eq!(
            collected
                .observations
                .get("pool-a")
                .and_then(|observation| observation.active_requests)
                .map(|count| count.value),
            Some(4),
            "the live pressure sample reaches the canonical observation"
        );
        assert_eq!(
            collected
                .observations
                .get("pool-b")
                .and_then(|observation| observation.active_requests)
                .map(|count| count.value),
            Some(0),
            "the simultaneously captured idle pool remains measured at zero"
        );
        let network: crate::crd::grid_network::GridNetwork = serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis.fast/v1alpha1",
            "kind": "GridNetwork",
            "metadata": { "name": "net" },
            "spec": {
                "seeds": [],
                "routingPolicy": "scoreFirst",
                "scoringPolicy": { "strategy": "dynamoRequestPressure" }
            }
        }))
        .unwrap_or_else(|_| std::process::abort());
        let overlay = crate::resources::routing_overlay::render_routing_overlay_with_adapter_observations(
            &network,
            &[],
            &providers,
            &[],
            &[],
            &[],
            "grid-a",
            "site-a",
            None,
            None,
            None,
            &crate::crd::grid_network::ScoringStrategy::DynamoRequestPressure.weights(),
            None,
            &crate::served_models::ServedModelStore::new(),
            crate::resources::routing_overlay::CandidateExpirySupport::Supported,
            Some(&collected.observations),
        )
        .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            overlay
                .candidates
                .iter()
                .map(|candidate| candidate.cluster.as_str())
                .collect::<Vec<_>>(),
            ["pool-b", "pool-a"],
            "the captured active-versus-idle observations determine local candidate order"
        );
        let [idle, loaded] = overlay.candidates.as_slice() else {
            std::process::abort();
        };
        assert_eq!(idle.score, Some(1.0), "zero active requests has zero pressure");
        assert_eq!(
            loaded.score,
            Some(0.0),
            "four observed requests meet the test denominator"
        );
    }

    #[tokio::test]
    async fn dynamo_adapter_preserves_not_ready_and_unknown_as_distinct_states() {
        let not_ready_endpoint =
            start_test_server(ok_response("dynamo_frontend_model_ready{model=\"model-a\"} 0\n")).await;
        let not_ready_provider = provider_fixture(
            "dynamo-not-ready",
            &not_ready_endpoint,
            Some(dynamo_adapter_config(None)),
        );
        let not_ready = collect_provider_metrics_with_refresh_interval_for_site(
            "net",
            &[not_ready_provider],
            &empty_cache(),
            Instant::now(),
            Duration::ZERO,
            Some("site-a"),
            None,
        )
        .await;
        assert_eq!(
            not_ready
                .observations
                .get("dynamo-not-ready")
                .map(|observation| observation.readiness),
            Some(grid_signals::provider_observation::ModelReadiness::NotReady),
            "zero readiness remains explicit"
        );
        assert!(
            not_ready
                .metrics
                .get("dynamo-not-ready")
                .is_some_and(|metrics| !metrics.healthy),
            "reported not-ready does not receive a healthy neutral metric sample"
        );

        let missing_endpoint =
            start_test_server(ok_response("dynamo_frontend_active_requests{model=\"model-a\"} 0\n")).await;
        let missing_provider = provider_fixture(
            "dynamo-missing-ready",
            &missing_endpoint,
            Some(dynamo_adapter_config(None)),
        );
        let missing = collect_provider_metrics_with_refresh_interval_for_site(
            "net",
            &[missing_provider],
            &empty_cache(),
            Instant::now(),
            Duration::ZERO,
            Some("site-a"),
            None,
        )
        .await;
        assert_eq!(
            missing
                .observations
                .get("dynamo-missing-ready")
                .map(|observation| observation.readiness),
            Some(grid_signals::provider_observation::ModelReadiness::Unknown),
            "missing readiness is never translated to ready-and-idle"
        );
        assert!(
            missing
                .metrics
                .get("dynamo-missing-ready")
                .is_some_and(|metrics| !metrics.healthy),
            "unknown readiness is not represented as a healthy idle metric sample"
        );
    }

    #[tokio::test]
    async fn failed_and_malformed_dynamo_scrapes_have_explicit_unknown_observations() {
        let failed_provider =
            provider_fixture("dynamo-failed", "http://127.0.0.1:1", Some(dynamo_adapter_config(None)));
        let failed = collect_provider_metrics_with_refresh_interval_for_site(
            "net",
            &[failed_provider],
            &empty_cache(),
            Instant::now(),
            Duration::ZERO,
            Some("site-a"),
            None,
        )
        .await;
        let missing = failed
            .observations
            .get("dynamo-failed")
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(
            missing.freshness,
            grid_signals::provider_observation::ObservationFreshness::Missing,
            "a failed scrape without a usable cache is explicitly missing"
        );
        assert_eq!(
            missing.readiness,
            grid_signals::provider_observation::ModelReadiness::Unknown,
            "a failed scrape cannot claim the model is ready"
        );
        assert!(
            failed
                .metrics
                .get("dynamo-failed")
                .is_some_and(|metrics| !metrics.healthy),
            "a missing observation fails closed"
        );

        let malformed_endpoint =
            start_test_server(ok_response("dynamo_frontend_model_ready{model=\"model-a\"} NaN\n")).await;
        let malformed_provider = provider_fixture(
            "dynamo-malformed",
            &malformed_endpoint,
            Some(dynamo_adapter_config(None)),
        );
        let malformed = collect_provider_metrics_with_refresh_interval_for_site(
            "net",
            &[malformed_provider],
            &empty_cache(),
            Instant::now(),
            Duration::ZERO,
            Some("site-a"),
            None,
        )
        .await;
        let invalid = malformed
            .observations
            .get("dynamo-malformed")
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(
            invalid.freshness,
            grid_signals::provider_observation::ObservationFreshness::Invalid,
            "a malformed current sample is distinct from a missing scrape"
        );
        assert_eq!(
            invalid.readiness,
            grid_signals::provider_observation::ModelReadiness::Unknown,
            "malformed readiness cannot claim the model is ready"
        );
        assert!(
            malformed
                .metrics
                .get("dynamo-malformed")
                .is_some_and(|metrics| !metrics.healthy),
            "an invalid observation fails closed"
        );
    }

    #[tokio::test]
    async fn failed_dynamo_scrape_keeps_stale_observation_marked_and_unroutable() {
        let config = dynamo_adapter_config(Some(60));
        let provider = provider_fixture("dynamo-a", "http://127.0.0.1:1", Some(config));
        let t0 = Instant::now();
        let mut cached_observation = grid_signals::dynamo_frontend::parse_dynamo_frontend_v1(
            "dynamo_frontend_model_ready{model=\"model-a\"} 1\n\
             dynamo_frontend_active_requests{model=\"model-a\"} 0\n",
            &grid_signals::dynamo_frontend::DynamoFrontendConfig {
                primary_model: "model-a",
                model_aliases: &[],
                concurrency_profile: None,
                concurrency_capacity: None,
            },
            grid_signals::dynamo_frontend::ProviderRevision {
                site: "site-a".to_owned(),
                provider_uid: "uid-dynamo-a".to_owned(),
                provider_generation: 1,
            },
            1_000,
        )
        .unwrap_or_else(|_| std::process::abort());
        cached_observation.freshness = grid_signals::provider_observation::ObservationFreshness::Fresh;
        let cache = empty_cache();
        {
            let mut guard = cache.lock().await;
            guard.entries.insert(
                ("net".to_owned(), "dynamo-a".to_owned()),
                TimestampedMetrics {
                    metrics: scoring::BackendMetrics::healthy_default(),
                    scraped_at: t0,
                    generation: 1,
                    observation: Some(cached_observation),
                },
            );
        }
        let collected = collect_provider_metrics_with_refresh_interval_for_site(
            "net",
            &[provider],
            &cache,
            t0 + Duration::from_secs(1),
            Duration::ZERO,
            Some("site-a"),
            None,
        )
        .await;
        assert_eq!(
            collected
                .observations
                .get("dynamo-a")
                .map(|observation| observation.freshness),
            Some(grid_signals::provider_observation::ObservationFreshness::Stale),
            "cached adapter values carry explicit stale state"
        );
        assert!(
            collected
                .metrics
                .get("dynamo-a")
                .is_some_and(|metrics| !metrics.healthy),
            "stale adapter telemetry never makes the provider routable"
        );
    }

    #[tokio::test]
    async fn zero_ttl_treated_as_absent_plaintext_omits_entry() {
        let provider = provider_fixture("prov-a", "http://127.0.0.1:1", Some(mc_with_queue_and_ttl("q", 0)));
        let cache = empty_cache();
        {
            let mut guard = cache.lock().await;
            guard.entries.insert(
                ("net".to_owned(), "prov-a".to_owned()),
                TimestampedMetrics {
                    metrics: scoring::BackendMetrics {
                        queue_depth: 0.99,
                        healthy: true,
                        kv_cache_utilization: 0.5,
                        latency_p99_ms: 2500.0,
                        prefix_cache_hit_ratio: 0.5,
                        error_rate: 0.0,
                    },
                    scraped_at: Instant::now(),
                    generation: 0,
                    observation: None,
                },
            );
        }
        let result = collect_provider_metrics("net", &[provider], &cache, Instant::now(), None).await;
        assert!(
            !result.metrics.contains_key("prov-a"),
            "plaintext provider with zero TTL must not insert unobservable metrics"
        );
    }

    // -----------------------------------------------------------------------
    // TlsFailureReason: stable status reason strings (via shared module)
    // -----------------------------------------------------------------------

    #[test]
    fn tls_failure_reason_metrics_prefix_stable_values() {
        use crate::resources::endpoint_tls::TlsFailureReason;

        assert_eq!(
            TlsFailureReason::SecretMissing.as_status_reason("Metrics"),
            "MetricsTlsSecretMissing",
            "stable status reason code"
        );
        assert_eq!(
            TlsFailureReason::KeyMissing.as_status_reason("Metrics"),
            "MetricsTlsKeyMissing",
            "stable status reason code"
        );
        assert_eq!(
            TlsFailureReason::MaterialInvalid.as_status_reason("Metrics"),
            "MetricsTlsMaterialInvalid",
            "stable status reason code"
        );
        assert_eq!(
            TlsFailureReason::IdentityMismatch.as_status_reason("Metrics"),
            "MetricsTlsIdentityMismatch",
            "stable status reason code"
        );
    }

    // -----------------------------------------------------------------------
    // classify_scrape_error: log-level classification
    // -----------------------------------------------------------------------

    #[test]
    fn classify_timeout_returns_scrape_timeout() {
        let err = metrics_scraper::MetricsScrapeError::Timeout(Duration::from_secs(2));
        assert_eq!(
            classify_scrape_error(&err),
            "MetricsScrapeTimeout",
            "timeout must classify as MetricsScrapeTimeout"
        );
    }

    #[test]
    fn classify_401_returns_unauthorized() {
        let err = metrics_scraper::MetricsScrapeError::NonOkStatus {
            status: 401,
            url: "http://x".to_owned(),
        };
        assert_eq!(
            classify_scrape_error(&err),
            "MetricsUnauthorized",
            "HTTP 401 must classify as MetricsUnauthorized"
        );
    }

    #[test]
    fn classify_403_returns_unauthorized() {
        let err = metrics_scraper::MetricsScrapeError::NonOkStatus {
            status: 403,
            url: "http://x".to_owned(),
        };
        assert_eq!(
            classify_scrape_error(&err),
            "MetricsUnauthorized",
            "HTTP 403 must classify as MetricsUnauthorized"
        );
    }

    #[test]
    fn classify_500_returns_generic() {
        let err = metrics_scraper::MetricsScrapeError::NonOkStatus {
            status: 500,
            url: "http://x".to_owned(),
        };
        assert_eq!(
            classify_scrape_error(&err),
            "MetricsScrapeError",
            "HTTP 500 has no specific failure category"
        );
    }

    #[test]
    fn classify_tls_material_error_returns_material_invalid() {
        let err = metrics_scraper::MetricsScrapeError::TlsMaterial("bad PEM".to_owned());
        assert_eq!(
            classify_scrape_error(&err),
            "MetricsTlsMaterialInvalid",
            "TLS material error must classify as MetricsTlsMaterialInvalid"
        );
    }

    #[test]
    fn classify_invalid_url_returns_generic() {
        let err = metrics_scraper::MetricsScrapeError::InvalidUrl("ftp://bad".to_owned());
        assert_eq!(
            classify_scrape_error(&err),
            "MetricsScrapeError",
            "invalid URL has no specific failure category"
        );
    }
}
