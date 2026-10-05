//! `grid-gateway`: the grid data-plane operand.
//!
//! A Praxis gateway assembled in the grid repo, deployed and configured by the
//! grid operator. Operator is the control plane. This binary is the operand it
//! manages.
//!
//! It links the Praxis library, registers the routing filters over the builtin
//! registry, and runs the Praxis server on the operator-supplied config. When
//! the operator sets `GRID_SERVING_CONFIG`, it also starts the cross-site pollers
//! and registers `grid_site_route` over the snapshot they keep fresh. This crate
//! is its own Cargo workspace so Praxis resolves independently of the operator's
//! Kubernetes client stack. See `deploy/gateway/Containerfile` and the
//! `gateway-image` make target.

use std::process::ExitCode;

use praxis_core::config::{Config, ConfigFile, DEFAULT_CONFIG};
use tracing::info;

/// Log line emitted once tracing is up; the startup test waits for it.
const STARTUP_MESSAGE: &str = "starting grid-gateway";
/// OTel-standard environment variable used as the OTLP endpoint fallback.
const OTLP_ENDPOINT_ENV_VAR: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";
/// OTel-standard environment variable containing exporter headers.
const OTLP_HEADERS_ENV_VAR: &str = "OTEL_EXPORTER_OTLP_HEADERS";

fn main() -> ExitCode {
    // Install the crypto provider before anything builds a TLS config.
    praxis::install_crypto_provider();

    // The operator writes the config. The path is `--config <path>` or the
    // positional argument, else the default search path. Read it once so the
    // reload watcher baselines on the bytes that run.
    let explicit = config_arg(std::env::args().skip(1)).unwrap_or_else(|err| praxis::fatal(&err));
    let config_file = praxis::resolve_config_path(explicit.as_deref())
        .as_deref()
        .map(ConfigFile::read)
        .transpose()
        .unwrap_or_else(|err| praxis::fatal(&err));
    let config = praxis::with_bootstrap_logging(|| Config::from_config_file_or(config_file.as_ref(), DEFAULT_CONFIG))
        .unwrap_or_else(|err| praxis::fatal(&err));

    validate_otlp_endpoint_transport(&config).unwrap_or_else(|err| praxis::fatal(&err));

    // Without a subscriber every log line, including reload results, is dropped.
    let tracing_guard = praxis::init_tracing(&config).unwrap_or_else(|err| praxis::fatal(&err));
    let log_level = Some(tracing_guard.log_level_state());
    let log_output = config.runtime.logging.output;
    info!(version = env!("CARGO_PKG_VERSION"), "{STARTUP_MESSAGE}");

    let mut registry = praxis_filter::FilterRegistry::with_builtins();
    praxis_ai_filters::register_ai_filters(&mut registry, None);

    // Grid cross-site routing is wired when the operator provides a serving
    // config. spawn_grid_routing starts one poller per peer and returns the
    // runtime holding their handles. grid_site_route registers over the snapshot
    // the pollers refresh. Dropping the runtime stops the pollers, so it is
    // bound until the server returns.
    let grid_runtime = match std::env::var("GRID_SERVING_CONFIG")
        .ok()
        .map(|path| start_grid_routing(&path, &mut registry))
    {
        Some(Err(err)) => return praxis::report_fatal(&err, log_output),
        Some(Ok(runtime)) => Some(runtime),
        None => None,
    };

    // Use the returning server path so both the routing runtime and tracing
    // provider can shut down cleanly after the listeners stop.
    let result = praxis::try_run_server_with_registry(config, registry, config_file, log_level);
    drop(grid_runtime);
    let exit_code = result.map_or_else(|err| praxis::report_fatal(&err, log_output), |()| ExitCode::SUCCESS);
    // The Praxis guard shuts down the OTLP provider and flushes queued spans.
    drop(tracing_guard);
    exit_code
}

/// Refuse to send configured OTLP headers to an unencrypted HTTP endpoint.
///
/// Praxis resolves the endpoint and headers from config first, then environment
/// variables. Validate the same effective values before initializing its
/// exporter so Secret-backed `OTEL_EXPORTER_OTLP_HEADERS` cannot be sent in
/// cleartext through either endpoint source.
fn validate_otlp_endpoint_transport(config: &Config) -> Result<(), &'static str> {
    let environment_endpoint = std::env::var(OTLP_ENDPOINT_ENV_VAR).ok();
    let environment_headers_present = std::env::var_os(OTLP_HEADERS_ENV_VAR).is_some_and(|value| !value.is_empty());
    let configured_headers_present = config.telemetry.otlp_headers.as_ref().map(|headers| !headers.is_empty());

    validate_otlp_endpoint_transport_values(
        config.telemetry.otlp_endpoint.as_deref(),
        environment_endpoint.as_deref(),
        configured_headers_present,
        environment_headers_present,
    )
}

/// Validate endpoint/header pairs using Praxis's config-before-environment precedence.
fn validate_otlp_endpoint_transport_values(
    configured_endpoint: Option<&str>,
    environment_endpoint: Option<&str>,
    configured_headers_present: Option<bool>,
    environment_headers_present: bool,
) -> Result<(), &'static str> {
    let endpoint = configured_endpoint.or_else(|| environment_endpoint.filter(|value| !value.trim().is_empty()));
    let headers_present = configured_headers_present.unwrap_or(environment_headers_present);
    let endpoint_uses_http = endpoint
        .and_then(|value| value.trim().split_once("://"))
        .is_some_and(|(scheme, _)| scheme.eq_ignore_ascii_case("http"));

    if headers_present && endpoint_uses_http {
        return Err("OTLP exporter headers require HTTPS; refusing to send OTLP credentials over HTTP");
    }

    Ok(())
}

/// Start the cross-site pollers and register `grid_site_route` over their snapshot.
///
/// # Errors
///
/// Returns the error from loading the serving config, starting the pollers, or
/// registering the filters.
fn start_grid_routing(
    path: &str,
    registry: &mut praxis_filter::FilterRegistry,
) -> Result<ai_grid_filters::GridRuntime, praxis_filter::FilterError> {
    let config = ai_grid_filters::load_serving_config(path)?;
    let mut runtime = ai_grid_filters::spawn_grid_routing(&config)?;
    ai_grid_filters::register_grid_filters(registry, runtime.snapshot())?;
    // The operator rewrites the file on membership and topology changes.
    runtime
        .watch(path, SERVING_RELOAD_INTERVAL)
        .map_err(|err| -> praxis_filter::FilterError { format!("grid: watching {path}: {err}").into() })?;
    Ok(runtime)
}

/// How often the grid serving config file is re-read.
const SERVING_RELOAD_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

/// Usage line for a malformed command line.
const USAGE: &str = "usage: grid-gateway [--config <path> | -c <path> | <path>]";

/// Config path from the arguments after the program name.
///
/// # Errors
///
/// Returns the usage line for a missing flag value, an unknown flag, or extra
/// arguments.
fn config_arg<I: IntoIterator<Item = String>>(args: I) -> Result<Option<String>, String> {
    let mut args = args.into_iter();
    let path = match args.next() {
        None => return Ok(None),
        Some(flag) if flag == "--config" || flag == "-c" => args.next().filter(|path| !path.starts_with('-')),
        Some(arg) => match arg.strip_prefix("--config=") {
            Some(path) => Some(path.to_owned()),
            None if !arg.starts_with('-') => Some(arg),
            None => None,
        },
    };
    match (path, args.next()) {
        (Some(path), None) if !path.is_empty() => Ok(Some(path)),
        _ => Err(USAGE.to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::{USAGE, config_arg, validate_otlp_endpoint_transport_values};

    fn parse(args: &[&str]) -> Result<Option<String>, String> {
        config_arg(args.iter().map(|arg| (*arg).to_owned()))
    }

    #[test]
    fn accepts_every_config_form() {
        for args in [
            &["/etc/grid/gateway.yaml"][..],
            &["--config", "/etc/grid/gateway.yaml"],
            &["-c", "/etc/grid/gateway.yaml"],
            &["--config=/etc/grid/gateway.yaml"],
        ] {
            assert_eq!(parse(args), Ok(Some("/etc/grid/gateway.yaml".to_owned())), "{args:?}");
        }
    }

    #[test]
    fn no_arguments_uses_the_default_search_path() {
        assert_eq!(parse(&[]), Ok(None), "no arguments");
    }

    #[test]
    fn rejects_malformed_command_lines() {
        for args in [
            &["--config"][..],
            &["--config="],
            &["--validate"],
            &["a.yaml", "b.yaml"],
            &["--config", "a.yaml", "b.yaml"],
            &["--config", "--validate"],
            &["-c", "--config"],
        ] {
            assert_eq!(parse(args), Err(USAGE.to_owned()), "{args:?}");
        }
    }

    #[test]
    fn rejects_http_endpoint_when_otlp_headers_are_configured() {
        assert!(
            validate_otlp_endpoint_transport_values(Some("http://collector:4317"), None, Some(true), false).is_err(),
            "an explicitly configured HTTP endpoint must not receive headers"
        );
    }

    #[test]
    fn rejects_http_environment_fallback_when_otlp_headers_are_configured() {
        assert!(
            validate_otlp_endpoint_transport_values(None, Some("http://collector:4317"), None, true).is_err(),
            "the OTEL_EXPORTER_OTLP_ENDPOINT fallback must not receive headers over HTTP"
        );
    }

    #[test]
    fn accepts_https_endpoints_with_otlp_headers() {
        for (configured, fallback, configured_headers, environment_headers) in [
            (Some("https://collector:4317"), None, Some(true), false),
            (None, Some("https://collector:4317"), None, true),
        ] {
            assert!(
                validate_otlp_endpoint_transport_values(
                    configured,
                    fallback,
                    configured_headers,
                    environment_headers,
                )
                .is_ok(),
                "HTTPS endpoints must remain usable with headers"
            );
        }
    }

    #[test]
    fn preserves_http_behavior_when_otlp_headers_are_absent() {
        assert!(
            validate_otlp_endpoint_transport_values(Some("http://collector:4317"), None, None, false).is_ok(),
            "HTTP without exporter headers remains supported"
        );
    }

    #[test]
    fn configured_endpoint_takes_precedence_over_environment_fallback() {
        assert!(
            validate_otlp_endpoint_transport_values(
                Some("https://configured:4317"),
                Some("http://fallback:4317"),
                None,
                true,
            )
            .is_ok(),
            "an unused HTTP fallback must not reject the configured HTTPS endpoint"
        );
    }
}
