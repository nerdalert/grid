//! Real, GPU-free Dynamo Frontend + Mocker qualification.

use std::{
    fmt::Write as _,
    fs,
    io::Write as _,
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde::Serialize;
use sha2::{Digest as _, Sha256};

use crate::env::verify::{self, PortForwardGuard};

/// Human-readable Dynamo image reference used when loading into Kind.
const IMAGE_TAG: &str = "nvcr.io/nvidia/ai-dynamo/dynamo-planner:1.6.1";
/// Immutable Dynamo v1.6.1 linux/amd64 image digest.
const IMAGE_DIGEST: &str = "sha256:f69ac8f2165289e0f9d06aa125fac8bda3a401b42f640235fca28b0c852b9875";
/// Dynamo image tag pinned to the qualification digest.
const IMAGE_PIN: &str = "nvcr.io/nvidia/ai-dynamo/dynamo-planner:1.6.1@sha256:f69ac8f2165289e0f9d06aa125fac8bda3a401b42f640235fca28b0c852b9875";
/// Kind control-plane node image.
const NODE_IMAGE: &str = "kindest/node:v1.34.3";
/// Single tokenizer/model identity advertised by each Frontend.
const MODEL: &str = "Qwen/Qwen3-0.6B";
/// Immutable Hugging Face revision for the tokenizer-only asset.
const TOKENIZER_REVISION: &str = "c1899de289a04d12100db370d81485cdf75e47ca";
/// Required tokenizer/config files and their pinned SHA-256 values.
const TOKENIZER_FILES: [(&str, &str); 5] = [
    (
        "config.json",
        "660db3b73d788119c04535e48cf9be5f55bc3100841a718637ae695b442f27dd",
    ),
    (
        "merges.txt",
        "8831e4f1a044471340f7c0a83d7bd71306a5b867e95fd870f74d0c5308a904d5",
    ),
    (
        "tokenizer.json",
        "aeb13307a71acd8fe81861d94ad54ab689df773318809eed3cbe794b4492dae4",
    ),
    (
        "tokenizer_config.json",
        "d5d09f07b48c3086c508b30d1c9114bd1189145b74e982a265350c923acd8101",
    ),
    (
        "vocab.json",
        "ca10d7e9fb3ed18575dd1e277a2579c16d108e32f27439684afa0e10b1440910",
    ),
];
/// Prefix for run-owned Kubernetes namespaces.
const NAMESPACE_PREFIX: &str = "dplg-";
/// Prefix for run-owned Kind clusters.
const KIND_CLUSTER_PREFIX: &str = "grid-dplg-";
/// Small, non-streaming OpenAI-compatible request used for real Mocker traffic.
const CHAT_REQUEST: &str = r#"{"model":"Qwen/Qwen3-0.6B","messages":[{"role":"user","content":"Please respond with a short greeting."}],"max_tokens":128,"stream":false}"#;

/// One named qualification assertion and its evidence.
#[derive(Serialize)]
struct Gate {
    /// Stable gate name.
    name: String,
    /// Whether the gate passed.
    passed: bool,
    /// Evidence supporting the result.
    evidence: String,
}

/// Machine-readable evidence envelope for one real Dynamo runtime run.
#[expect(
    clippy::struct_excessive_bools,
    reason = "separate booleans report distinct qualification scope and result gates"
)]
#[derive(Serialize)]
struct QualificationEvidence {
    /// Evidence schema version.
    schema_version: String,
    /// Stable qualification mode name.
    mode: String,
    /// Exact runtime boundary exercised by this command.
    qualification_scope: String,
    /// Unique UTC run identifier.
    run_id: String,
    /// Run start time.
    started_at_utc: String,
    /// Run completion time.
    completed_at_utc: String,
    /// Grid worktree root.
    repository_root: String,
    /// Parent containing run-scoped state and evidence.
    task_root: String,
    /// Grid source commit tested.
    source_revision: String,
    /// Hash of the tracked diff against the source commit.
    tracked_diff_sha256: String,
    /// Hash over tracked changes and untracked file contents.
    source_state_sha256: String,
    /// Unique Kind cluster names for the two runtime sites.
    kind_clusters: Vec<String>,
    /// Unique Kubernetes namespaces for the two runtime sites.
    namespaces: Vec<String>,
    /// Dynamo image reference used by the two pools.
    dynamo_image: String,
    /// Dynamo image digest checked at runtime.
    dynamo_image_digest: String,
    /// Tokenizer asset source revision.
    tokenizer_revision: String,
    /// Whether both sites passed their real chat-completion runtime check.
    dynamo_runtime_tested: bool,
    /// Whether Grid operator overlay distribution was tested in this run.
    local_grid_overlay_tested: bool,
    /// Whether peer relay and cross-site routing were tested in this run.
    cross_site_tested: bool,
    /// Whether all runtime and cleanup gates passed.
    success: bool,
    /// First runtime or cleanup error, if any.
    error: Option<String>,
    /// Individual runtime and cleanup gate results.
    gates: Vec<Gate>,
    /// Evidence file names captured by this run.
    artifacts: Vec<String>,
}

/// One isolated site owned by the Dynamo runtime qualification.
#[derive(Clone)]
struct SiteRun {
    /// Stable site label for evidence and manifests.
    name: String,
    /// Unique Kind cluster name.
    cluster: String,
    /// Unique Kubernetes namespace.
    namespace: String,
    /// Ephemeral kubeconfig for this cluster.
    kubeconfig: PathBuf,
    /// Whether this run's cluster is known to have been created.
    cluster_created: bool,
    /// Whether Kind cluster creation was attempted, for partial-create cleanup.
    cluster_create_attempted: bool,
}

/// Mutable state owned by one isolated Dynamo runtime qualification.
struct Run {
    /// Parent containing state and evidence roots.
    task_root: PathBuf,
    /// Persistent state files for the run.
    state_dir: PathBuf,
    /// Logs and machine-readable output for the run.
    evidence_dir: PathBuf,
    /// Unique UTC run identifier.
    id: String,
    /// The two independent Grid sites, each backed by one real Dynamo pool.
    sites: Vec<SiteRun>,
    /// Empty Docker client config preventing credential lookup.
    docker_config: PathBuf,
    /// Pinned tokenizer files mounted into the Kind node.
    tokenizer_dir: PathBuf,
    /// Run start time.
    started_at_utc: String,
    /// Grid source commit.
    source_revision: String,
    /// Hash of tracked changes against the source commit.
    tracked_diff_sha256: String,
    /// Hash over tracked changes and untracked file contents.
    source_state_sha256: String,
    /// Assertions captured as the runtime phase progresses.
    gates: Vec<Gate>,
}

/// Parsed readiness, request, and stage values from one Frontend scrape.
#[derive(Clone, Copy, Debug)]
struct MetricsSample {
    /// Number of active requests reported by the Frontend.
    active_requests: f64,
    /// Whether the configured model is ready.
    model_ready: f64,
    /// Highest stage request gauge observed in the scrape.
    stage_max: f64,
}

/// One bounded concurrent HTTP request owned by the pressure phase.
struct LoadCall {
    /// Child process while the request is in flight.
    child: Option<std::process::Child>,
    /// Captured Chat Completions response body.
    response_path: PathBuf,
    /// Captured HTTP status code.
    status_path: PathBuf,
    /// Captured curl stdout and stderr.
    log_path: PathBuf,
    /// Whether this request completed with a valid response.
    completed: bool,
}

/// Metric samples collected for one real pressure/recovery phase.
struct PressureEvidence {
    /// Active-request samples from the loaded Frontend.
    pressure_pool_active: Vec<f64>,
    /// Active-request samples from the idle comparison Frontend.
    control_pool_active: Vec<f64>,
    /// Number of loaded Frontend scrapes with a nonzero stage request gauge.
    pressure_stage_nonzero: usize,
    /// Number of Kind CPU/memory resource samples captured during traffic.
    resource_samples: usize,
    /// Minimum host memory available while real requests were outstanding.
    host_memory_available_min_bytes: u64,
    /// Minimum host filesystem space available while real requests were outstanding.
    host_disk_available_min_bytes: u64,
}

impl Drop for LoadCall {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            drop(child.kill());
            drop(child.wait());
        }
    }
}

/// Run one real Dynamo Frontend + Mocker pool in each of two isolated Kind sites.
///
/// The qualification intentionally stops at the two-site Dynamo runtime boundary. Its JSON reports
/// Grid overlay and cross-site routing as untested so runtime traffic cannot imply either gate.
pub(crate) fn run(evidence_parent: Option<&Path>) -> Result<(), Box<dyn std::error::Error>> {
    let repository_root = repository_root()?;
    let task_root = task_root(&repository_root);
    let run_id = unique_run_id();
    let state_dir = task_root.join("state").join(&run_id);
    let evidence_parent = evidence_parent.map_or_else(|| task_root.join("evidence"), Path::to_path_buf);
    let evidence_dir = evidence_parent.join(&run_id);
    if state_dir.exists() || evidence_dir.exists() {
        return Err(format!("refusing to reuse run-owned path for {run_id}").into());
    }
    fs::create_dir_all(&state_dir)?;
    fs::create_dir_all(&evidence_dir)?;

    let (source_revision, tracked_diff_sha256, source_state_sha256) =
        capture_source_lock(&repository_root, &state_dir)?;
    let cluster_suffix = run_id.to_ascii_lowercase().replace(['z', 't'], "");
    let sites = ["pool-a", "pool-b"]
        .into_iter()
        .map(|name| {
            let site_suffix = name.strip_prefix("pool-").unwrap_or(name);
            SiteRun {
                name: name.to_owned(),
                cluster: format!("{KIND_CLUSTER_PREFIX}{cluster_suffix}-{site_suffix}"),
                namespace: format!("{NAMESPACE_PREFIX}{cluster_suffix}-{site_suffix}"),
                kubeconfig: state_dir.join(format!("{name}.kubeconfig")),
                cluster_created: false,
                cluster_create_attempted: false,
            }
        })
        .collect();
    let tokenizer_dir = state_dir.join("assets/qwen3-0.6b");
    let mut run = Run {
        task_root,
        state_dir: state_dir.clone(),
        evidence_dir,
        id: run_id,
        sites,
        docker_config: state_dir.join("docker-config-empty"),
        tokenizer_dir,
        started_at_utc: utc_iso_timestamp(),
        source_revision,
        tracked_diff_sha256,
        source_state_sha256,
        gates: Vec::new(),
    };

    eprintln!("Grid Dynamo pluggable qualification: dynamo-runtime-full");
    for site in &run.sites {
        eprintln!("{} Kind cluster: {}", site.name, site.cluster);
        eprintln!("{} namespace: {}", site.name, site.namespace);
    }
    eprintln!("Evidence: {}", run.evidence_dir.display());
    fs::create_dir_all(&run.docker_config)?;
    fs::write(run.docker_config.join("config.json"), "{}\n")?;

    let execution = run_inner(&mut run);
    finish_run(&repository_root, run, &execution)
}

/// Record runtime results, clean up the owned cluster, and write the final evidence envelope.
fn finish_run(
    repository_root: &Path,
    mut run: Run,
    execution: &Result<(), Box<dyn std::error::Error>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let error = execution.as_ref().err().map(ToString::to_string);
    if let Some(error) = error.as_deref() {
        record_gate(&mut run, "dynamo-runtime-execution", false, error);
    } else {
        record_gate(
            &mut run,
            "dynamo-runtime-execution",
            true,
            "all real-runtime checks passed",
        );
    }
    let cleanup = cleanup(&mut run);
    let cleanup_error = cleanup.as_ref().err().map(ToString::to_string);
    record_gate(
        &mut run,
        "run-owned-kind-cleanup",
        cleanup.is_ok(),
        cleanup_error
            .as_deref()
            .unwrap_or("both unique run-owned Kind clusters were deleted"),
    );

    let success = execution.is_ok() && cleanup.is_ok();
    let mut artifacts = fs::read_dir(&run.evidence_dir)?
        .filter_map(Result::ok)
        .filter_map(|entry| entry.file_name().into_string().ok())
        .collect::<Vec<_>>();
    artifacts.extend(["qualification.json".to_owned(), "phase-summary.md".to_owned()]);
    artifacts.sort();
    let evidence = QualificationEvidence {
        schema_version: "1".to_owned(),
        mode: "dynamo-runtime-full".to_owned(),
        qualification_scope: "one independent Dynamo v1.6.1 Frontend+Mocker pool per Kind site (pool-a and pool-b); real chat completions at both sites; live Frontend active_requests pressure and recovery comparison. Grid overlay and cross-site request routing are not exercised".to_owned(),
        run_id: run.id.clone(),
        started_at_utc: run.started_at_utc.clone(),
        completed_at_utc: utc_iso_timestamp(),
        repository_root: repository_root.display().to_string(),
        task_root: run.task_root.display().to_string(),
        source_revision: run.source_revision.clone(),
        tracked_diff_sha256: run.tracked_diff_sha256.clone(),
        source_state_sha256: run.source_state_sha256.clone(),
        kind_clusters: run.sites.iter().map(|site| site.cluster.clone()).collect(),
        namespaces: run.sites.iter().map(|site| site.namespace.clone()).collect(),
        dynamo_image: IMAGE_PIN.to_owned(),
        dynamo_image_digest: IMAGE_DIGEST.to_owned(),
        tokenizer_revision: TOKENIZER_REVISION.to_owned(),
        dynamo_runtime_tested: runtime_was_tested(&run.gates),
        local_grid_overlay_tested: false,
        cross_site_tested: false,
        success,
        error: error.or(cleanup_error),
        gates: run.gates,
        artifacts,
    };
    let qualification_path = run.evidence_dir.join("qualification.json");
    fs::write(&qualification_path, serde_json::to_vec_pretty(&evidence)?)?;
    write_phase_summary(&evidence, &run.evidence_dir)?;
    eprintln!("[DYNAMO-RUNTIME][{}]", if success { "PASS" } else { "FAIL" });
    eprintln!("Evidence: {}", qualification_path.display());

    if success {
        Ok(())
    } else {
        Err(format!(
            "Dynamo runtime qualification failed; see {}",
            qualification_path.display()
        )
        .into())
    }
}

/// Execute each bounded phase of the two-site runtime qualification.
fn run_inner(run: &mut Run) -> Result<(), Box<dyn std::error::Error>> {
    capture_host_snapshot(run, "host-before.txt")?;
    ensure_cluster_name_available(run)?;
    prepare_tokenizer(run)?;
    verify_dynamo_image(run)?;
    for site in &run.sites {
        write_kind_config(run, site)?;
        write_pool_manifest(run, site)?;
    }
    for index in 0..run.sites.len() {
        create_cluster_and_pool(run, index)?;
    }
    record_gate(
        run,
        "dynamo-pools-ready-at-both-sites-with-pinned-image",
        true,
        "both independent Kind sites have one Ready Frontend+Mocker Pod with matching pinned v1.6.1 imageIDs and zero restarts",
    );
    let mut forwards = start_pool_port_forwards(run)?;
    record_readiness_and_chat(run, &forwards)?;
    qualify_live_pressure_and_recovery(run, &forwards)?;
    for site in &run.sites {
        capture_container_memory_peaks(run, site)?;
    }
    forwards.pressure_forward.stop();
    forwards.control_forward.stop();
    Ok(())
}

/// Refuse a cluster-name collision with any cluster in the existing Kind inventory.
fn ensure_cluster_name_available(run: &Run) -> Result<(), Box<dyn std::error::Error>> {
    let clusters_before = logged_command(run, "kind-clusters-before.txt", "kind", &["get", "clusters"])?;
    let existing = output_text(&clusters_before);
    for site in &run.sites {
        if existing.lines().any(|line| line.trim() == site.cluster) {
            return Err(format!("refusing to reuse existing Kind cluster {}", site.cluster).into());
        }
    }
    Ok(())
}

/// Create one unique site cluster, load Dynamo, and wait for its pool Pod to become ready.
fn create_cluster_and_pool(run: &mut Run, site_index: usize) -> Result<(), Box<dyn std::error::Error>> {
    let site = run
        .sites
        .get(site_index)
        .ok_or_else(|| format!("Dynamo site index {site_index} is invalid"))?
        .clone();
    let kind_create_started = Instant::now();
    run.sites
        .get_mut(site_index)
        .ok_or_else(|| format!("Dynamo site index {site_index} is invalid"))?
        .cluster_create_attempted = true;
    let kind_config = run.state_dir.join(format!("kind-{}.yaml", site.name));
    logged_command(
        run,
        &format!("{}-kind-create.log", site.name),
        "kind",
        &[
            "create",
            "cluster",
            "--name",
            &site.cluster,
            "--config",
            path_string(&kind_config)?.as_str(),
            "--kubeconfig",
            path_string(&site.kubeconfig)?.as_str(),
            "--wait",
            "180s",
        ],
    )?;
    run.sites
        .get_mut(site_index)
        .ok_or_else(|| format!("Dynamo site index {site_index} is invalid"))?
        .cluster_created = true;
    fs::write(
        run.evidence_dir.join(format!("{}-kind-create-duration.txt", site.name)),
        format!(
            "site={}\ncluster={}\nseconds={:.3}\n",
            site.name,
            site.cluster,
            kind_create_started.elapsed().as_secs_f64()
        ),
    )?;
    capture_node_storage(run, &site, &format!("{}-node-storage-before-image.txt", site.name));
    logged_command(
        run,
        &format!("{}-kind-load-dynamo-image.log", site.name),
        "kind",
        &["load", "docker-image", IMAGE_TAG, "--name", &site.cluster],
    )?;
    capture_node_storage(run, &site, &format!("{}-node-storage-after-image.txt", site.name));
    capture_host_snapshot(run, &format!("host-after-{}-image-load.txt", site.name))?;

    let context = format!("kind-{}", site.cluster);
    let manifest_path = run.state_dir.join(format!("dynamo-{}.yaml", site.name));
    let pod_name = format!("pod/dynamo-{}", site.name);
    logged_command(
        run,
        &format!("{}-kubectl-apply.log", site.name),
        "kubectl",
        &[
            "--kubeconfig",
            path_string(&site.kubeconfig)?.as_str(),
            "apply",
            "-f",
            path_string(&manifest_path)?.as_str(),
        ],
    )?;
    let ready_started = Instant::now();
    logged_command(
        run,
        &format!("{}-pods-ready.log", site.name),
        "kubectl",
        &[
            "--kubeconfig",
            path_string(&site.kubeconfig)?.as_str(),
            "--context",
            &context,
            "-n",
            &site.namespace,
            "wait",
            "--for=condition=Ready",
            &pod_name,
            "--timeout=600s",
        ],
    )?;
    fs::write(
        run.evidence_dir.join(format!("{}-startup-duration.txt", site.name)),
        format!(
            "site={}\ncluster={}\napply_to_pool_pod_ready_seconds={:.3}\n",
            site.name,
            site.cluster,
            ready_started.elapsed().as_secs_f64()
        ),
    )?;
    validate_pod_identities(run, &site, &context)?;
    Ok(())
}

/// Keep both site Frontend services reachable until the qualification finishes.
struct PoolPortForwards {
    /// Host port forwarded to the pressure pool.
    pressure_port: u16,
    /// Host port forwarded to the idle comparison pool.
    control_port: u16,
    /// Port-forward process for the pressure pool.
    pressure_forward: PortForwardGuard,
    /// Port-forward process for the idle comparison pool.
    control_forward: PortForwardGuard,
}

/// Allocate distinct local ports and start logged forwards to both site Frontend services.
fn start_pool_port_forwards(run: &Run) -> Result<PoolPortForwards, Box<dyn std::error::Error>> {
    let pressure_site = run.sites.first().ok_or("pool-a site is missing")?;
    let control_site = run.sites.get(1).ok_or("pool-b site is missing")?;
    let pressure_port = verify::find_free_port()?;
    let control_port = verify::find_free_port()?;
    if pressure_port == control_port {
        return Err("port allocator returned one port for both Dynamo sites".into());
    }
    fs::write(
        run.evidence_dir.join("local-ports.tsv"),
        format!("pool-a\t{pressure_port}\npool-b\t{control_port}\n"),
    )?;
    let pressure_context = format!("kind-{}", pressure_site.cluster);
    let control_context = format!("kind-{}", control_site.cluster);
    let mut pressure_forward = PortForwardGuard::start_with_kubeconfig_diagnostics(verify::KubePortForwardRequest {
        context: &pressure_context,
        kubeconfig: &pressure_site.kubeconfig,
        namespace: &pressure_site.namespace,
        service: &format!("dynamo-{}", pressure_site.name),
        local_port: pressure_port,
        service_port: 8000,
        stderr: Stdio::from(fs::File::create(run.evidence_dir.join("port-forward-a.log"))?),
    })?;
    let mut control_forward = PortForwardGuard::start_with_kubeconfig_diagnostics(verify::KubePortForwardRequest {
        context: &control_context,
        kubeconfig: &control_site.kubeconfig,
        namespace: &control_site.namespace,
        service: &format!("dynamo-{}", control_site.name),
        local_port: control_port,
        service_port: 8000,
        stderr: Stdio::from(fs::File::create(run.evidence_dir.join("port-forward-b.log"))?),
    })?;
    pressure_forward.wait_until_ready(pressure_port)?;
    control_forward.wait_until_ready(control_port)?;
    Ok(PoolPortForwards {
        pressure_port,
        control_port,
        pressure_forward,
        control_forward,
    })
}

/// Prove that both sites register their model and serve valid Chat Completions traffic.
fn record_readiness_and_chat(run: &mut Run, forwards: &PoolPortForwards) -> Result<(), Box<dyn std::error::Error>> {
    let readiness_a = wait_for_model_ready(run, "pool-a", forwards.pressure_port)?;
    let readiness_b = wait_for_model_ready(run, "pool-b", forwards.control_port)?;
    fs::write(run.evidence_dir.join("pool-a-metrics-readiness.txt"), readiness_a.raw)?;
    fs::write(run.evidence_dir.join("pool-b-metrics-readiness.txt"), readiness_b.raw)?;
    record_gate(
        run,
        "frontend-model-ready-both-sites",
        true,
        &format!("dynamo_frontend_model_ready{{model=\"{MODEL}\"}} was exactly 1 on both real Frontends"),
    );

    write_chat_request(run)?;
    send_and_validate_chat(run, "pool-a", forwards.pressure_port)?;
    send_and_validate_chat(run, "pool-b", forwards.control_port)?;
    record_gate(
        run,
        "real-chat-completions-both-sites",
        true,
        "both sites returned HTTP 200 with a non-empty assistant message from their Mocker worker",
    );
    Ok(())
}

/// Sample concurrent Mocker traffic, then require both Frontends to return to the idle ready state.
fn qualify_live_pressure_and_recovery(
    run: &mut Run,
    forwards: &PoolPortForwards,
) -> Result<(), Box<dyn std::error::Error>> {
    let baseline_a = scrape_metrics(forwards.pressure_port)?;
    let baseline_b = scrape_metrics(forwards.control_port)?;
    fs::write(
        run.evidence_dir.join("pool-a-metrics-before-pressure.txt"),
        &baseline_a.raw,
    )?;
    fs::write(
        run.evidence_dir.join("pool-b-metrics-before-pressure.txt"),
        &baseline_b.raw,
    )?;
    let baseline_loaded = parse_metrics(&baseline_a.raw)?;
    let baseline_control = parse_metrics(&baseline_b.raw)?;
    if !is_model_ready(baseline_loaded.model_ready) || !is_model_ready(baseline_control.model_ready) {
        return Err("one or both Frontends lost model_ready=1 before the pressure phase".into());
    }

    let load_request = run.state_dir.join("load-request.json");
    fs::write(&load_request, CHAT_REQUEST.replace("128", "512"))?;
    let load_calls = start_load_calls(run, forwards.pressure_port, &load_request)?;
    let pressure = sample_pressure(run, forwards.pressure_port, forwards.control_port, load_calls)?;
    let pressure_peak = pressure.pressure_pool_active.iter().copied().fold(0.0_f64, f64::max);
    let control_peak = pressure.control_pool_active.iter().copied().fold(0.0_f64, f64::max);
    if pressure.pressure_pool_active.len() < 2 || pressure.control_pool_active.len() < 2 {
        return Err(format!(
            "insufficient paired Frontend samples under load: pool-a={} pool-b={}",
            pressure.pressure_pool_active.len(),
            pressure.control_pool_active.len()
        )
        .into());
    }
    if pressure_peak <= 0.0 || !is_idle(control_peak) {
        return Err(format!(
            "live Frontend pressure did not isolate to pool-a: pool-a peak={pressure_peak}, pool-b peak={control_peak}"
        )
        .into());
    }
    record_gate(
        run,
        "live-frontend-pressure-isolated-to-pool-a",
        true,
        &format!(
            "{} paired metric samples; pool-a active_requests peak={pressure_peak}; pool-b peak={control_peak}; pool-a nonzero stage samples={}",
            pressure
                .pressure_pool_active
                .len()
                .min(pressure.control_pool_active.len()),
            pressure.pressure_stage_nonzero
        ),
    );
    fs::write(
        run.evidence_dir.join("pressure-sample-summary.txt"),
        format!(
            "paired_samples={}\npool_a_active_requests_peak={pressure_peak}\npool_b_active_requests_peak={control_peak}\npool_a_nonzero_stage_samples={}\nkind_resource_samples={}\nhost_memory_available_min_bytes={}\nhost_disk_available_min_bytes={}\n",
            pressure
                .pressure_pool_active
                .len()
                .min(pressure.control_pool_active.len()),
            pressure.pressure_stage_nonzero,
            pressure.resource_samples,
            pressure.host_memory_available_min_bytes,
            pressure.host_disk_available_min_bytes,
        ),
    )?;

    let recovered_a = wait_for_recovery(forwards.pressure_port)?;
    let recovered_b = wait_for_recovery(forwards.control_port)?;
    fs::write(run.evidence_dir.join("pool-a-metrics-recovery.txt"), recovered_a.raw)?;
    fs::write(run.evidence_dir.join("pool-b-metrics-recovery.txt"), recovered_b.raw)?;
    if !is_model_ready(recovered_a.sample.model_ready)
        || !is_model_ready(recovered_b.sample.model_ready)
        || !is_idle(recovered_a.sample.active_requests)
        || !is_idle(recovered_b.sample.active_requests)
    {
        return Err(format!(
            "Frontend metrics did not recover to ready=1, active_requests=0: pool-a={:?}, pool-b={:?}",
            recovered_a.sample, recovered_b.sample
        )
        .into());
    }
    record_gate(
        run,
        "frontend-pressure-recovery",
        true,
        "both Frontends returned model_ready=1 and active_requests=0 after all real requests completed",
    );
    Ok(())
}

/// Materialize the tokenizer-only asset from the verified cache or pinned source files.
fn prepare_tokenizer(run: &mut Run) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(&run.tokenizer_dir)?;
    let cache = find_pinned_tokenizer_cache(&run.task_root);
    let mut source = cache.as_deref().map(Path::to_path_buf);
    if source.is_none() {
        let source_dir = run.state_dir.join("tokenizer-download");
        fs::create_dir_all(&source_dir)?;
        for (file, _) in TOKENIZER_FILES {
            let url = format!("https://huggingface.co/Qwen/Qwen3-0.6B/resolve/{TOKENIZER_REVISION}/{file}");
            logged_command(
                run,
                &format!("tokenizer-download-{file}.log"),
                "curl",
                &[
                    "--fail",
                    "--location",
                    "--silent",
                    "--show-error",
                    "--connect-timeout",
                    "15",
                    "--max-time",
                    "120",
                    &url,
                    "-o",
                    path_string(&source_dir.join(file))?.as_str(),
                ],
            )?;
        }
        source = Some(source_dir);
    }
    let source = source.ok_or("pinned tokenizer asset source is unavailable")?;
    let mut lock = String::new();
    for (file, expected_sha256) in TOKENIZER_FILES {
        let source_path = source.join(file);
        let content = fs::read(&source_path)
            .map_err(|error| format!("read pinned tokenizer file {}: {error}", source_path.display()))?;
        let actual_sha256 = hex::encode(Sha256::digest(&content));
        if actual_sha256 != expected_sha256 {
            return Err(
                format!("tokenizer {file} digest mismatch: expected {expected_sha256}, got {actual_sha256}").into(),
            );
        }
        fs::write(run.tokenizer_dir.join(file), &content)?;
        writeln!(lock, "{actual_sha256}  {file}")?;
    }
    fs::write(run.state_dir.join("tokenizer-lock.txt"), &lock)?;
    fs::write(
        run.state_dir.join("tokenizer-source.txt"),
        format!(
            "repository=Qwen/Qwen3-0.6B\nrevision={TOKENIZER_REVISION}\nsource={}\nmodel_weights_downloaded=false\n",
            source.display()
        ),
    )?;
    record_gate(
        run,
        "pinned-tokenizer-asset",
        true,
        &format!(
            "all five tokenizer/config files match Qwen/Qwen3-0.6B revision {TOKENIZER_REVISION}; model weights were not downloaded"
        ),
    );
    Ok(())
}

/// Find a cached tokenizer directory whose required files match the locked hashes.
fn find_pinned_tokenizer_cache(task_root: &Path) -> Option<PathBuf> {
    let state_root = task_root.join("state");
    let mut candidates = fs::read_dir(state_root)
        .ok()?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .collect::<Vec<_>>();
    candidates.sort();
    candidates
        .into_iter()
        .rev()
        .map(|dir| dir.join("assets/qwen3-0.6b"))
        .find(|dir| tokenizer_dir_is_pinned(dir))
}

/// Check whether each required tokenizer file has its expected SHA-256 digest.
fn tokenizer_dir_is_pinned(dir: &Path) -> bool {
    TOKENIZER_FILES.iter().all(|(file, expected)| {
        fs::read(dir.join(file))
            .ok()
            .is_some_and(|bytes| hex::encode(Sha256::digest(bytes)) == *expected)
    })
}

/// Verify and record the immutable Dynamo image identity used by both pools.
fn verify_dynamo_image(run: &mut Run) -> Result<(), Box<dyn std::error::Error>> {
    let inspect_args = [
        "image",
        "inspect",
        IMAGE_TAG,
        "--format",
        "{{.Id}} {{.Os}}/{{.Architecture}} {{json .RepoDigests}}",
    ];
    let inspect = docker_command(run).args(inspect_args).output()?;
    if !inspect.status.success() {
        logged_command(run, "dynamo-image-pull.log", "docker", &["pull", IMAGE_PIN])?;
        logged_command(run, "dynamo-image-tag.log", "docker", &["tag", IMAGE_PIN, IMAGE_TAG])?;
    }
    let output = logged_command(run, "dynamo-image-inspect.txt", "docker", &inspect_args)?;
    let identity = output_text(&output);
    if !identity.contains("linux/amd64") || !identity.contains(IMAGE_DIGEST) {
        return Err(format!(
            "Dynamo image does not match pinned linux/amd64 digest {IMAGE_DIGEST}: {}",
            identity.trim()
        )
        .into());
    }
    record_gate(run, "pinned-dynamo-image", true, identity.trim());
    Ok(())
}

/// Write one site's Kind configuration with the run-owned tokenizer mount.
fn write_kind_config(run: &Run, site: &SiteRun) -> Result<(), Box<dyn std::error::Error>> {
    let host_path = yaml_quote(&path_string(&run.tokenizer_dir)?)?;
    let kind_config = format!(
        "kind: Cluster\napiVersion: kind.x-k8s.io/v1alpha4\nnodes:\n- role: control-plane\n  image: {NODE_IMAGE}\n  extraMounts:\n  - hostPath: {host_path}\n    containerPath: /mnt/grid-dynamo-pluggable-{}/qwen3-0.6b\n    readOnly: true\n",
        run.id.to_ascii_lowercase()
    );
    fs::write(run.state_dir.join(format!("kind-{}.yaml", site.name)), kind_config)?;
    Ok(())
}

/// Write one Frontend and Mocker pool with shared file discovery and pinned assets.
fn write_pool_manifest(run: &Run, site: &SiteRun) -> Result<(), Box<dyn std::error::Error>> {
    let pool = site
        .name
        .strip_prefix("pool-")
        .ok_or_else(|| format!("Dynamo site name {:?} must use a pool-* label", site.name))?;
    let run_label = yaml_quote(&run.id)?;
    let token_path = format!("/mnt/grid-dynamo-pluggable-{}/qwen3-0.6b", run.id.to_ascii_lowercase());
    let mut manifest = format!(
        "apiVersion: v1\nkind: Namespace\nmetadata:\n  name: {}\n  labels:\n    praxis.fast/run-id: {}\n    app.kubernetes.io/part-of: grid-dynamo-pluggable\n---\n",
        site.namespace, run_label
    );
    write!(
        manifest,
        "apiVersion: v1\nkind: Pod\nmetadata:\n  name: dynamo-pool-{pool}\n  namespace: {}\n  labels:\n    app: dynamo-pool\n    pool: pool-{pool}\n    praxis.fast/run-id: {}\nspec:\n  automountServiceAccountToken: false\n  restartPolicy: Always\n  containers:\n  - name: frontend\n    image: {IMAGE_PIN}\n    imagePullPolicy: IfNotPresent\n    workingDir: /workspace\n    command: [python3, -m, dynamo.frontend]\n    args: [--http-port, \"8000\", --discovery-backend, file]\n    env:\n    - {{name: DYN_FILE_KV, value: /var/lib/dynamo/file-kv}}\n    - {{name: HF_HUB_OFFLINE, value: \"1\"}}\n    - {{name: TRANSFORMERS_OFFLINE, value: \"1\"}}\n    - {{name: HF_HOME, value: /tmp/hf-home}}\n    ports:\n    - {{name: http, containerPort: 8000}}\n    resources:\n      requests: {{cpu: 100m, memory: 512Mi}}\n      limits: {{cpu: \"2\", memory: 1Gi}}\n    volumeMounts:\n    - {{name: dynamo-file-kv, mountPath: /var/lib/dynamo/file-kv}}\n    - {{name: tokenizer, mountPath: /models/qwen3-0.6b, readOnly: true}}\n  - name: mocker\n    image: {IMAGE_PIN}\n    imagePullPolicy: IfNotPresent\n    workingDir: /workspace\n    command: [python3, -m, dynamo.mocker]\n    args: [--model-path, /models/qwen3-0.6b, --model-name, {MODEL}, --discovery-backend, file, --speedup-ratio, \"0.1\"]\n    env:\n    - {{name: DYN_FILE_KV, value: /var/lib/dynamo/file-kv}}\n    - {{name: HF_HUB_OFFLINE, value: \"1\"}}\n    - {{name: TRANSFORMERS_OFFLINE, value: \"1\"}}\n    - {{name: HF_HOME, value: /tmp/hf-home}}\n    resources:\n      requests: {{cpu: 50m, memory: 128Mi}}\n      limits: {{cpu: \"1\", memory: 512Mi}}\n    volumeMounts:\n    - {{name: dynamo-file-kv, mountPath: /var/lib/dynamo/file-kv}}\n    - {{name: tokenizer, mountPath: /models/qwen3-0.6b, readOnly: true}}\n  volumes:\n  - name: dynamo-file-kv\n    emptyDir: {{}}\n  - name: tokenizer\n    hostPath:\n      path: {token_path}\n      type: Directory\n---\napiVersion: v1\nkind: Service\nmetadata:\n  name: dynamo-pool-{pool}\n  namespace: {}\n  labels:\n    praxis.fast/run-id: {}\nspec:\n  selector:\n    app: dynamo-pool\n    pool: pool-{pool}\n  ports:\n  - {{name: http, port: 8000, targetPort: http}}\n  type: ClusterIP\n---\n",
        site.namespace, run_label, site.namespace, run_label
    )?;
    fs::write(run.state_dir.join(format!("dynamo-{}.yaml", site.name)), manifest)?;
    Ok(())
}

/// Require both containers in one site's pool to use the pinned image without restarts.
fn validate_pod_identities(run: &Run, site: &SiteRun, context: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut identity_file = String::new();
    if !site.name.starts_with("pool-") {
        return Err(format!("Dynamo site name {:?} must use a pool-* label", site.name).into());
    }
    let output = kubectl_output(
        run,
        site,
        context,
        &[
            "-n",
            &site.namespace,
            "get",
            "pod",
            &format!("dynamo-{}", site.name),
            "-o",
            "jsonpath={range .status.containerStatuses[*]}{.name}{\"\\t\"}{.imageID}{\"\\t\"}{.restartCount}{\"\\n\"}{end}",
        ],
        &format!("{}-imageids.tsv", site.name),
    )?;
    let identity = output_text(&output);
    for line in identity.lines() {
        let columns = line.split('\t').collect::<Vec<_>>();
        let [container, image_id, restart_count] = columns.as_slice() else {
            return Err(format!("{} container identity is malformed: {line}", site.name).into());
        };
        if !matches!(*container, "frontend" | "mocker") || !image_id.contains(IMAGE_DIGEST) || *restart_count != "0" {
            return Err(format!("{} container identity/restart check failed: {line}", site.name).into());
        }
    }
    if identity.lines().count() != 2 {
        return Err(format!(
            "{} did not report exactly Frontend and Mocker imageIDs: {}",
            site.name,
            identity.trim()
        )
        .into());
    }
    writeln!(identity_file, "{}\n{identity}", site.name)?;
    let mut image_identities = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(run.evidence_dir.join("runtime-image-identities.tsv"))?;
    image_identities.write_all(identity_file.as_bytes())?;
    Ok(())
}

/// Persist the non-streaming request used for baseline Chat Completions traffic.
fn write_chat_request(run: &Run) -> Result<(), Box<dyn std::error::Error>> {
    fs::write(run.state_dir.join("chat-request.json"), CHAT_REQUEST)?;
    Ok(())
}

/// Send one real request and validate its HTTP status and assistant response.
fn send_and_validate_chat(run: &Run, pool: &str, port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let request_path = path_string(&run.state_dir.join("chat-request.json"))?;
    let url = format!("http://127.0.0.1:{port}/v1/chat/completions");
    let output = Command::new("curl")
        .args([
            "--silent",
            "--show-error",
            "--connect-timeout",
            "5",
            "--max-time",
            "60",
            "-H",
            "Content-Type: application/json",
            "-H",
            &format!("X-Request-ID: {}-{pool}-chat", run.id),
            "--data-binary",
            &format!("@{request_path}"),
            "-o",
            path_string(&run.evidence_dir.join(format!("{pool}-chat-response.json")))?.as_str(),
            "-w",
            "%{http_code}",
            &url,
        ])
        .output()?;
    fs::write(
        run.evidence_dir.join(format!("{pool}-chat-curl.log")),
        command_log("curl --data-binary @chat-request.json /v1/chat/completions", &output),
    )?;
    let status = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    fs::write(
        run.evidence_dir.join(format!("{pool}-chat-status.txt")),
        format!("{status}\n"),
    )?;
    if !output.status.success() || status != "200" {
        return Err(format!(
            "{pool} real chat request failed: process={:?}, HTTP {status}",
            output.status.code()
        )
        .into());
    }
    let response_path = run.evidence_dir.join(format!("{pool}-chat-response.json"));
    validate_chat_response(&fs::read_to_string(response_path)?)?;
    Ok(())
}

/// Start four bounded concurrent real requests against one Mocker-backed Frontend.
fn start_load_calls(run: &Run, port: u16, request_path: &Path) -> Result<Vec<LoadCall>, Box<dyn std::error::Error>> {
    let mut calls = Vec::with_capacity(4);
    for index in 1..=4 {
        let response_path = run.evidence_dir.join(format!("pool-a-load-{index:02}.json"));
        let status_path = run.evidence_dir.join(format!("pool-a-load-{index:02}.status"));
        let log_path = run.evidence_dir.join(format!("pool-a-load-{index:02}.log"));
        let output = Command::new("curl")
            .args([
                "--silent",
                "--show-error",
                "--connect-timeout",
                "5",
                "--max-time",
                "180",
                "-H",
                "Content-Type: application/json",
                "-H",
                &format!("X-Request-ID: {}-pool-a-load-{index:02}", run.id),
                "--data-binary",
                &format!("@{}", path_string(request_path)?),
                "-o",
                path_string(&response_path)?.as_str(),
                "-w",
                "%{http_code}",
                &format!("http://127.0.0.1:{port}/v1/chat/completions"),
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        calls.push(LoadCall {
            child: Some(output),
            response_path,
            status_path,
            log_path,
            completed: false,
        });
    }
    Ok(calls)
}

#[expect(
    clippy::disallowed_methods,
    reason = "xtask uses bounded synchronous polling during the real runtime pressure phase"
)]
/// Sample Frontend gauges and Kind resources until the concurrent requests finish.
fn sample_pressure(
    run: &Run,
    pressure_port: u16,
    control_port: u16,
    mut calls: Vec<LoadCall>,
) -> Result<PressureEvidence, Box<dyn std::error::Error>> {
    let mut evidence = PressureEvidence {
        pressure_pool_active: Vec::new(),
        control_pool_active: Vec::new(),
        pressure_stage_nonzero: 0,
        resource_samples: 0,
        host_memory_available_min_bytes: u64::MAX,
        host_disk_available_min_bytes: u64::MAX,
    };
    let mut load_metrics = fs::File::create(run.evidence_dir.join("metrics-under-load.tsv"))?;
    let mut resource_file = fs::File::create(run.evidence_dir.join("kind-resource-samples.tsv"))?;
    let mut host_resource_file = fs::File::create(run.evidence_dir.join("host-resource-samples.tsv"))?;
    writeln!(
        host_resource_file,
        "timestamp_utc\tmemory_available_bytes\tdisk_available_bytes"
    )?;
    let deadline = Instant::now() + Duration::from_secs(180);
    while calls.iter().any(|call| !call.completed) {
        if Instant::now() >= deadline {
            for call in &mut calls {
                if let Some(child) = call.child.as_mut() {
                    drop(child.kill());
                }
            }
            return Err("bounded live-load request window exceeded 180 seconds".into());
        }
        for (label, port, samples) in [
            ("pressure-pool", pressure_port, &mut evidence.pressure_pool_active),
            ("control-pool", control_port, &mut evidence.control_pool_active),
        ] {
            let timestamp = utc_iso_timestamp();
            writeln!(load_metrics, "# {timestamp}\t{label}")?;
            match scrape_metrics(port) {
                Ok(scrape) => {
                    load_metrics.write_all(scrape.raw.as_bytes())?;
                    if !scrape.raw.ends_with('\n') {
                        writeln!(load_metrics)?;
                    }
                    if let Ok(sample) = parse_metrics(&scrape.raw) {
                        samples.push(sample.active_requests);
                        if label == "pressure-pool" && sample.stage_max > 0.0 {
                            evidence.pressure_stage_nonzero += 1;
                        }
                    }
                },
                Err(error) => writeln!(load_metrics, "# scrape_error={error}")?,
            }
        }
        let nodes = run
            .sites
            .iter()
            .map(|site| format!("{}-control-plane", site.cluster))
            .collect::<Vec<_>>();
        let resource_stats = docker_command(run)
            .args([
                "stats",
                "--no-stream",
                "--format",
                "{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}",
            ])
            .args(&nodes)
            .output()?;
        writeln!(
            resource_file,
            "{}\t{}",
            utc_iso_timestamp(),
            String::from_utf8_lossy(&resource_stats.stdout).trim()
        )?;
        evidence.resource_samples += usize::from(resource_stats.status.success());
        sample_host_resources(run, &mut host_resource_file, &mut evidence)?;

        for call in &mut calls {
            if call.completed {
                continue;
            }
            let Some(running_child) = call.child.as_mut() else {
                call.completed = true;
                continue;
            };
            if running_child.try_wait()?.is_some() {
                let completed_child = call.child.take().ok_or("load request child disappeared")?;
                let output = completed_child.wait_with_output()?;
                let http_status = String::from_utf8_lossy(&output.stdout).trim().to_owned();
                fs::write(&call.status_path, format!("{http_status}\n"))?;
                fs::write(
                    &call.log_path,
                    command_log("curl --data-binary @load-request.json /v1/chat/completions", &output),
                )?;
                if !output.status.success() || http_status != "200" {
                    return Err(format!(
                        "pool-a concurrent request failed: HTTP {http_status}; see {}",
                        call.log_path.display()
                    )
                    .into());
                }
                validate_chat_response(&fs::read_to_string(&call.response_path)?)?;
                call.completed = true;
            }
        }
        thread::sleep(Duration::from_millis(400));
    }
    Ok(evidence)
}

/// Record the host memory and disk headroom for one in-flight request sample.
fn sample_host_resources(
    run: &Run,
    sample_file: &mut fs::File,
    evidence: &mut PressureEvidence,
) -> Result<(), Box<dyn std::error::Error>> {
    let memory_available = host_memory_available_bytes()?;
    let disk_available = host_disk_available_bytes(&run.task_root)?;
    evidence.host_memory_available_min_bytes = evidence.host_memory_available_min_bytes.min(memory_available);
    evidence.host_disk_available_min_bytes = evidence.host_disk_available_min_bytes.min(disk_available);
    writeln!(
        sample_file,
        "{}\t{memory_available}\t{disk_available}",
        utc_iso_timestamp()
    )?;
    Ok(())
}

/// Read the Linux host's currently available memory in bytes.
fn host_memory_available_bytes() -> Result<u64, Box<dyn std::error::Error>> {
    let meminfo = fs::read_to_string("/proc/meminfo")?;
    let available_kib = meminfo
        .lines()
        .find_map(|line| {
            let mut fields = line.split_whitespace();
            (fields.next()? == "MemAvailable:").then(|| fields.next())?
        })
        .ok_or("/proc/meminfo has no MemAvailable field")?
        .parse::<u64>()?;
    Ok(available_kib.saturating_mul(1024))
}

/// Read the available bytes on the filesystem holding the task's evidence and state.
fn host_disk_available_bytes(path: &Path) -> Result<u64, Box<dyn std::error::Error>> {
    let output = Command::new("df").args(["-Pk"]).arg(path).output()?;
    if !output.status.success() {
        return Err(format!("df -Pk failed: {}", String::from_utf8_lossy(&output.stderr)).into());
    }
    let available_kib = String::from_utf8_lossy(&output.stdout)
        .lines()
        .last()
        .and_then(|line| line.split_whitespace().nth(3))
        .ok_or("df -Pk output has no available-space field")?
        .parse::<u64>()?;
    Ok(available_kib.saturating_mul(1024))
}

#[expect(
    clippy::disallowed_methods,
    reason = "xtask uses bounded synchronous polling for the Frontend readiness gate"
)]
/// Wait up to five minutes for the pinned model readiness gauge to become one.
fn wait_for_model_ready(run: &Run, pool: &str, port: u16) -> Result<Scrape, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(300);
    let mut last_error: Option<String>;
    loop {
        last_error = match scrape_metrics(port) {
            Ok(scrape) => match readiness_value(&scrape.raw) {
                Ok(value) if is_model_ready(value) => return Ok(scrape),
                Ok(value) => Some(format!("model_ready is {value} for {MODEL}")),
                Err(error) => Some(error.to_string()),
            },
            Err(error) => Some(error.to_string()),
        };
        if Instant::now() >= deadline {
            let last_error = last_error
                .as_deref()
                .unwrap_or("metrics endpoint has not returned a sample");
            fs::write(run.evidence_dir.join(format!("{pool}-readiness-error.txt")), last_error)?;
            return Err(
                format!("{pool} Frontend did not publish model_ready=1 within 300 seconds: {last_error}").into(),
            );
        }
        thread::sleep(Duration::from_secs(1));
    }
}

/// One raw Prometheus exposition response from a Dynamo Frontend.
struct Scrape {
    /// The unmodified response body.
    raw: String,
}

/// A parsed response retained with its original Prometheus exposition body.
struct ParsedScrape {
    /// The required readiness and pressure values.
    sample: MetricsSample,
    /// The unmodified response body.
    raw: String,
}

/// Fetch one actual Prometheus exposition response from a local Frontend port.
fn scrape_metrics(port: u16) -> Result<Scrape, Box<dyn std::error::Error>> {
    let output = Command::new("curl")
        .args([
            "--fail",
            "--silent",
            "--show-error",
            "--connect-timeout",
            "2",
            "--max-time",
            "5",
            &format!("http://127.0.0.1:{port}/metrics"),
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "GET /metrics failed with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    Ok(Scrape {
        raw: String::from_utf8(output.stdout)?,
    })
}

/// Parse required Frontend readiness, active-request, and stage request series.
fn parse_metrics(raw: &str) -> Result<MetricsSample, Box<dyn std::error::Error>> {
    let ready = one_model_value(raw, "dynamo_frontend_model_ready")?;
    let active = one_model_value(raw, "dynamo_frontend_active_requests")?;
    let mut stage_values = Vec::new();
    for line in raw
        .lines()
        .filter(|line| line.starts_with("dynamo_frontend_stage_requests{"))
    {
        stage_values.push(metric_value(line)?);
    }
    if stage_values.is_empty() {
        return Err("metrics response has no dynamo_frontend_stage_requests series".into());
    }
    Ok(MetricsSample {
        active_requests: active,
        model_ready: ready,
        stage_max: stage_values.into_iter().fold(0.0_f64, f64::max),
    })
}

/// Find exactly one series for the pinned model and return its validated value.
fn one_model_value(raw: &str, name: &str) -> Result<f64, Box<dyn std::error::Error>> {
    let prefix = format!("{name}{{");
    let matches = raw
        .lines()
        .filter(|line| line.starts_with(&prefix) && line.contains(&format!("model=\"{MODEL}\"")))
        .collect::<Vec<_>>();
    if matches.len() != 1 {
        return Err(format!(
            "expected exactly one {name} series for {MODEL}, found {}",
            matches.len()
        )
        .into());
    }
    metric_value(
        matches
            .first()
            .ok_or("model metric disappeared after cardinality check")?,
    )
}

/// Read the pinned model readiness value before other metric series exist.
fn readiness_value(raw: &str) -> Result<f64, Box<dyn std::error::Error>> {
    one_model_value(raw, "dynamo_frontend_model_ready")
}

/// Check the exact ready value defined by Dynamo's model readiness gauge.
#[expect(
    clippy::float_cmp,
    reason = "Dynamo encodes readiness as the exact gauge values zero and one"
)]
fn is_model_ready(value: f64) -> bool {
    value == 1.0
}

/// Check whether Dynamo reports no active Frontend requests.
fn is_idle(value: f64) -> bool {
    value == 0.0
}

/// Parse one Prometheus sample and reject malformed or invalid numeric values.
fn metric_value(line: &str) -> Result<f64, Box<dyn std::error::Error>> {
    let value = line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| format!("malformed Prometheus sample: {line}"))?;
    let parsed = value
        .parse::<f64>()
        .map_err(|error| format!("invalid metric value in {line}: {error}"))?;
    if !parsed.is_finite() || parsed < 0.0 {
        return Err(format!("non-finite or negative metric sample: {line}").into());
    }
    Ok(parsed)
}

#[expect(
    clippy::disallowed_methods,
    reason = "xtask uses bounded synchronous polling for Frontend recovery"
)]
/// Poll until a Frontend is ready and idle or the bounded timeout expires.
fn wait_for_recovery(port: u16) -> Result<ParsedScrape, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut last_error: Option<String>;
    loop {
        last_error = match scrape_metrics(port) {
            Ok(scrape) => match parse_metrics(&scrape.raw) {
                Ok(sample) if is_model_ready(sample.model_ready) && is_idle(sample.active_requests) => {
                    return Ok(ParsedScrape {
                        sample,
                        raw: scrape.raw,
                    });
                },
                Ok(sample) => Some(format!(
                    "model_ready={} active_requests={}",
                    sample.model_ready, sample.active_requests
                )),
                Err(error) => Some(error.to_string()),
            },
            Err(error) => Some(error.to_string()),
        };
        if Instant::now() >= deadline {
            return Err(format!(
                "Frontend did not recover within 60 seconds: {}",
                last_error.as_deref().unwrap_or("no recovery sample")
            )
            .into());
        }
        thread::sleep(Duration::from_millis(500));
    }
}

/// Require a Chat Completions response with non-empty assistant content.
fn validate_chat_response(body: &str) -> Result<(), Box<dyn std::error::Error>> {
    let value: serde_json::Value = serde_json::from_str(body)?;
    let message = value
        .get("choices")
        .and_then(serde_json::Value::as_array)
        .and_then(|choices| choices.first())
        .and_then(|choice| choice.get("message"))
        .and_then(|message| message.get("content"))
        .and_then(serde_json::Value::as_str)
        .ok_or("response is not a non-empty Chat Completions success response")?;
    if message.trim().is_empty() {
        return Err("Chat Completions assistant content is empty".into());
    }
    Ok(())
}

/// Record Frontend and Mocker cgroup memory peaks after pressure and recovery.
fn capture_container_memory_peaks(run: &Run, site: &SiteRun) -> Result<(), Box<dyn std::error::Error>> {
    let mut peaks = String::new();
    let context = format!("kind-{}", site.cluster);
    for container in ["frontend", "mocker"] {
        let output = kubectl_output(
            run,
            site,
            &context,
            &[
                "-n",
                &site.namespace,
                "exec",
                &format!("dynamo-{}", site.name),
                "-c",
                container,
                "--",
                "python3",
                "-c",
                "from pathlib import Path; print(Path('/sys/fs/cgroup/memory.peak').read_text().strip())",
            ],
            &format!("{}-{container}-memory-peak.log", site.name),
        );
        match output {
            Ok(output) => writeln!(peaks, "{}/{container}\t{}", site.name, output_text(&output).trim())?,
            Err(error) => writeln!(peaks, "{}/{container}\tunavailable: {error}", site.name)?,
        }
    }
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(run.evidence_dir.join("pod-memory-peaks.tsv"))?;
    file.write_all(peaks.as_bytes())?;
    Ok(())
}

/// Capture filesystem capacity and unpacked image storage from one Kind node.
fn capture_node_storage(run: &Run, site: &SiteRun, filename: &str) {
    let node = format!("{}-control-plane", site.cluster);
    let output = docker_command(run)
        .args([
            "exec",
            &node,
            "sh",
            "-c",
            "df -B1 /; df -B1 /var/lib/containerd; du -sb /var/lib/containerd 2>/dev/null",
        ])
        .output();
    if let Ok(output) = output
        && let Err(error) = fs::write(
            run.evidence_dir.join(filename),
            command_log("docker exec sh -c 'df/du container storage'", &output),
        )
    {
        eprintln!("could not write {filename}: {error}");
    }
}

/// Record host memory and filesystem headroom in a named evidence file.
fn capture_host_snapshot(run: &Run, filename: &str) -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new("sh")
        .args(["-c", "date -u; free -b; df -B1 ."])
        .current_dir(&run.task_root)
        .output()?;
    fs::write(run.evidence_dir.join(filename), command_log("sh", &output))?;
    Ok(())
}

/// Remove only the two run-owned clusters and their temporary kubeconfigs after saving diagnostics.
fn cleanup(run: &mut Run) -> Result<(), Box<dyn std::error::Error>> {
    let mut errors = Vec::new();
    if let Err(error) = cleanup_inner(run) {
        errors.push(error.to_string());
    }
    for site in &run.sites {
        if site.kubeconfig.exists()
            && let Err(error) = fs::remove_file(&site.kubeconfig)
        {
            errors.push(format!(
                "could not remove {} task-owned kubeconfig {}: {error}",
                site.name,
                site.kubeconfig.display()
            ));
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; ").into())
    }
}

/// Inspect and delete only the unique clusters whose names belong to this run.
fn cleanup_inner(run: &mut Run) -> Result<(), Box<dyn std::error::Error>> {
    let clusters_before = kind_command(run).args(["get", "clusters"]).output()?;
    fs::write(
        run.evidence_dir.join("kind-clusters-pre-delete.txt"),
        command_log("kind get clusters", &clusters_before),
    )?;
    if !clusters_before.status.success() {
        return Err("could not inspect Kind inventory for the run-owned clusters during cleanup".into());
    }
    let inventory_before = output_text(&clusters_before);
    let mut errors = Vec::new();
    for index in 0..run.sites.len() {
        let site = run
            .sites
            .get(index)
            .ok_or_else(|| format!("Dynamo site index {index} is invalid"))?
            .clone();
        let should_clean = site.cluster_created || site.cluster_create_attempted;
        let exists = inventory_before.lines().any(|line| line.trim() == site.cluster);
        if !should_clean || !exists {
            continue;
        }
        capture_site_diagnostics(run, &site);
        let delete = kind_command(run)
            .args(["delete", "cluster", "--name", &site.cluster, "--kubeconfig"])
            .arg(&site.kubeconfig)
            .output()?;
        fs::write(
            run.evidence_dir.join(format!("{}-kind-delete.log", site.name)),
            command_log(
                &format!("kind delete cluster --name {} --kubeconfig <run-owned>", site.cluster),
                &delete,
            ),
        )?;
        if delete.status.success() {
            if let Some(cleaned_site) = run.sites.get_mut(index) {
                cleaned_site.cluster_created = false;
            }
        } else {
            errors.push(format!("failed to delete run-owned Kind cluster {}", site.cluster));
        }
    }
    let clusters_after = kind_command(run).args(["get", "clusters"]).output()?;
    fs::write(
        run.evidence_dir.join("kind-clusters-after.txt"),
        command_log("kind get clusters", &clusters_after),
    )?;
    if !clusters_after.status.success() {
        return Err("could not verify Kind cluster inventory after cleanup".into());
    }
    let inventory_after = output_text(&clusters_after);
    for site in &run.sites {
        if inventory_after.lines().any(|line| line.trim() == site.cluster) {
            errors.push(format!("run-owned Kind cluster {} remains after cleanup", site.cluster));
        }
    }
    let docker = docker_command(run)
        .args([
            "ps",
            "--filter",
            "label=io.x-k8s.kind.cluster",
            "--format",
            "{{.Names}} {{.Status}}",
        ])
        .output()?;
    fs::write(
        run.evidence_dir.join("docker-kind-after.txt"),
        command_log("docker ps", &docker),
    )?;
    capture_host_snapshot(run, "host-after.txt")?;
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; ").into())
    }
}

/// Save Pods, events, descriptions, logs, and manifests before deletion.
fn capture_site_diagnostics(run: &Run, site: &SiteRun) {
    let context = format!("kind-{}", site.cluster);
    let pod = format!("dynamo-{}", site.name);
    for (args, filename, description) in [
        (
            vec!["-n", site.namespace.as_str(), "get", "pods,services", "-o", "wide"],
            format!("{}-pods-services.txt", site.name),
            "Pods and Services",
        ),
        (
            vec![
                "-n",
                site.namespace.as_str(),
                "get",
                "events",
                "--sort-by=.lastTimestamp",
            ],
            format!("{}-events.txt", site.name),
            "events",
        ),
        (
            vec!["-n", site.namespace.as_str(), "describe", "pod", pod.as_str()],
            format!("{}-describe.txt", site.name),
            "Pod description",
        ),
        (
            vec!["-n", site.namespace.as_str(), "get", "pod", pod.as_str(), "-o", "yaml"],
            format!("{}-pod.yaml", site.name),
            "Pod YAML",
        ),
    ] {
        if kubectl_output(run, site, &context, &args, &filename).is_err() {
            eprintln!("could not capture {} {description}", site.name);
        }
    }
    for container in ["frontend", "mocker"] {
        if kubectl_output(
            run,
            site,
            &context,
            &[
                "-n",
                &site.namespace,
                "logs",
                &format!("pod/{pod}"),
                "-c",
                container,
                "--timestamps",
            ],
            &format!("{}-{container}.log", site.name),
        )
        .is_err()
        {
            eprintln!("could not capture {} {container} logs", site.name);
        }
    }
}

/// Run a task-scoped kubectl command and persist its complete output.
fn kubectl_output(
    run: &Run,
    site: &SiteRun,
    context: &str,
    args: &[&str],
    log_name: &str,
) -> Result<Output, Box<dyn std::error::Error>> {
    let mut command = Command::new("kubectl");
    command
        .arg("--kubeconfig")
        .arg(&site.kubeconfig)
        .arg("--context")
        .arg(context)
        .args(args);
    let output = command.output()?;
    fs::write(
        run.evidence_dir.join(log_name),
        command_log(
            &format!(
                "kubectl --kubeconfig <run-owned> --context {context} {}",
                args.join(" ")
            ),
            &output,
        ),
    )?;
    if !output.status.success() {
        return Err(format!(
            "kubectl {} failed; see {}",
            args.join(" "),
            run.evidence_dir.join(log_name).display()
        )
        .into());
    }
    Ok(output)
}

/// Run a command and persist its stdout, stderr, and exit status.
fn logged_command(
    run: &Run,
    log_name: &str,
    program: &str,
    args: &[&str],
) -> Result<Output, Box<dyn std::error::Error>> {
    let mut command = Command::new(program);
    if matches!(program, "docker" | "kind") {
        command.env("DOCKER_CONFIG", &run.docker_config);
    }
    let output = command.args(args).output()?;
    fs::write(
        run.evidence_dir.join(log_name),
        command_log(&format!("{program} {}", args.join(" ")), &output),
    )?;
    if !output.status.success() {
        return Err(format!(
            "{program} {} failed; see {}",
            args.join(" "),
            run.evidence_dir.join(log_name).display()
        )
        .into());
    }
    Ok(output)
}

/// Build a Docker command with this run’s empty credential configuration.
fn docker_command(run: &Run) -> Command {
    let mut command = Command::new("docker");
    command.env("DOCKER_CONFIG", &run.docker_config);
    command
}

/// Build a Kind command with this run’s empty credential configuration.
fn kind_command(run: &Run) -> Command {
    let mut command = Command::new("kind");
    command.env("DOCKER_CONFIG", &run.docker_config);
    command
}

/// Format a command, its exit status, and both output streams as evidence.
fn command_log(command: &str, output: &Output) -> Vec<u8> {
    let mut log = format!("$ {command}\nexit_code={:?}\n--- stdout ---\n", output.status.code()).into_bytes();
    log.extend_from_slice(&output.stdout);
    log.extend_from_slice(b"\n--- stderr ---\n");
    log.extend_from_slice(&output.stderr);
    log
}

/// Decode command stdout lossily for status and inventory parsing.
fn output_text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).to_string()
}

/// Append a named assertion and its supporting evidence.
fn record_gate(run: &mut Run, name: &str, passed: bool, evidence: &str) {
    run.gates.push(Gate {
        name: name.to_owned(),
        passed,
        evidence: evidence.to_owned(),
    });
}

/// Whether both site Frontends passed the real request check.
fn runtime_was_tested(gates: &[Gate]) -> bool {
    gates
        .iter()
        .any(|gate| gate.name == "real-chat-completions-both-sites" && gate.passed)
}

/// Write the concise summary for a completed machine-readable run.
fn write_phase_summary(
    evidence: &QualificationEvidence,
    evidence_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut summary = format!(
        "# Dynamo pluggable runtime qualification\n\nRun: `{}`\nResult: **{}**\nDynamo runtime tested: **{}**\nGrid overlay tested: **{}**\nCross-site routing tested: **{}**\n\nWorktree: `{}` at `{}`.\nDynamo image: `{}`\nTokenizer revision: `{}`\nKind clusters: `{}`\nNamespaces: `{}`\n\nScope: {}.\n\n",
        evidence.run_id,
        if evidence.success { "PASS" } else { "FAIL" },
        if evidence.dynamo_runtime_tested { "YES" } else { "NO" },
        if evidence.local_grid_overlay_tested {
            "YES"
        } else {
            "NO"
        },
        if evidence.cross_site_tested { "YES" } else { "NO" },
        evidence.repository_root,
        evidence.source_revision,
        evidence.dynamo_image,
        evidence.tokenizer_revision,
        evidence.kind_clusters.join(", "),
        evidence.namespaces.join(", "),
        evidence.qualification_scope,
    );
    for gate in &evidence.gates {
        writeln!(
            summary,
            "- {}: **{}** — {}",
            gate.name,
            if gate.passed { "PASS" } else { "FAIL" },
            gate.evidence
        )?;
    }
    if let Some(error) = &evidence.error {
        writeln!(summary, "\nExecution error: `{error}`")?;
    }
    summary.push_str("\nThis result is a Dynamo runtime qualification; it does not qualify Grid overlay distribution or live cross-site routing.\n");
    fs::write(evidence_dir.join("phase-summary.md"), summary)?;
    Ok(())
}

/// Record source revision and content hashes before runtime qualification.
fn capture_source_lock(
    repository_root: &Path,
    state_dir: &Path,
) -> Result<(String, String, String), Box<dyn std::error::Error>> {
    let source = super::capture_source_identity(repository_root)?;
    let branch = super::git_output(repository_root, &["branch", "--show-current"])?;
    let remotes = super::git_output(repository_root, &["remote", "-v"])?;
    let status = super::git_output(repository_root, &["status", "--porcelain=v1", "--untracked-files=all"])?;
    let untracked_paths = source
        .untracked_files
        .iter()
        .map(|file| file.path.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    let untracked_hashes = source
        .untracked_files
        .iter()
        .map(|file| format!("{}  {}", file.sha256, file.path))
        .collect::<Vec<_>>()
        .join("\n");
    let lock = format!(
        "worktree={}\nbranch={}\nhead={}\nremotes=\n{}\nstatus=\n{}\ntracked_diff_sha256={}\nsource_state_sha256={}\nuntracked_paths=\n{}\nuntracked_hashes=\n{}\n",
        repository_root.display(),
        branch,
        source.source_revision,
        remotes.trim(),
        status.trim(),
        source.tracked_diff_sha256,
        source.source_state_sha256,
        untracked_paths,
        untracked_hashes,
    );
    fs::write(state_dir.join("source-lock.txt"), lock)?;
    fs::write(state_dir.join("source.diff"), &source.tracked_diff)?;
    fs::write(state_dir.join("untracked-hashes.txt"), untracked_hashes)?;
    Ok((
        source.source_revision,
        source.tracked_diff_sha256,
        source.source_state_sha256,
    ))
}

/// Resolve the Grid checkout containing this xtask crate.
fn repository_root() -> Result<PathBuf, Box<dyn std::error::Error>> {
    Ok(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").canonicalize()?)
}

/// Resolve the parent that owns run-scoped state and evidence.
fn task_root(repository_root: &Path) -> PathBuf {
    repository_root
        .ancestors()
        .find(|path| path.join("sources/dynamo").is_dir() && path.join("worktrees").is_dir())
        .map_or_else(|| repository_root.to_path_buf(), Path::to_path_buf)
}

/// Convert a generated task-owned path to UTF-8 for subprocess arguments.
fn path_string(path: &Path) -> Result<String, Box<dyn std::error::Error>> {
    Ok(path
        .to_str()
        .ok_or_else(|| format!("non-UTF-8 path {} is not supported by the Kind runner", path.display()))?
        .to_owned())
}

/// Quote one scalar for embedding in generated YAML.
fn yaml_quote(value: &str) -> Result<String, Box<dyn std::error::Error>> {
    Ok(serde_yaml::to_string(value)?.trim().to_owned())
}

/// Generate a unique UTC identifier for this qualification run.
fn unique_run_id() -> String {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    format!(
        "{}-{}-{:03}",
        utc_timestamp().to_ascii_lowercase(),
        std::process::id(),
        now.subsec_millis()
    )
}

/// Format the current UTC time for filesystem-safe run names.
fn utc_timestamp() -> String {
    let now = time::OffsetDateTime::now_utc();
    format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        now.year(),
        u8::from(now.month()),
        now.day(),
        now.hour(),
        now.minute(),
        now.second()
    )
}

/// Format the current UTC time as an ISO-8601 evidence timestamp.
fn utc_iso_timestamp() -> String {
    let now = time::OffsetDateTime::now_utc();
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        now.year(),
        u8::from(now.month()),
        now.day(),
        now.hour(),
        now.minute(),
        now.second()
    )
}

#[cfg(test)]
mod tests {
    use super::{
        Gate, MODEL, metric_value, parse_metrics, readiness_value, runtime_was_tested, validate_chat_response,
    };

    #[test]
    fn runtime_evidence_flag_requires_the_successful_two_site_chat_gate() {
        let absent = Vec::new();
        assert!(
            !runtime_was_tested(&absent),
            "missing runtime gate is not a runtime pass"
        );
        let failed = vec![Gate {
            name: "real-chat-completions-both-sites".to_owned(),
            passed: false,
            evidence: "HTTP request failed".to_owned(),
        }];
        assert!(!runtime_was_tested(&failed), "failed requests are not a runtime pass");
        let passed = vec![Gate {
            name: "real-chat-completions-both-sites".to_owned(),
            passed: true,
            evidence: "both sites returned valid responses".to_owned(),
        }];
        assert!(
            runtime_was_tested(&passed),
            "successful requests record the runtime test"
        );
    }

    #[test]
    fn parses_frontend_readiness_pressure_and_stage_metrics() -> Result<(), Box<dyn std::error::Error>> {
        let metrics = format!(
            "dynamo_frontend_model_ready{{model=\"{MODEL}\"}} 1\ndynamo_frontend_active_requests{{model=\"{MODEL}\"}} 3\ndynamo_frontend_stage_requests{{phase=\"aggregated\",stage=\"dispatch\"}} 2\n"
        );
        let parsed = parse_metrics(&metrics)?;
        assert!((parsed.model_ready - 1.0).abs() <= f64::EPSILON);
        assert!((parsed.active_requests - 3.0).abs() <= f64::EPSILON);
        assert!((parsed.stage_max - 2.0).abs() <= f64::EPSILON);
        Ok(())
    }

    #[test]
    fn accepts_ready_metric_before_request_gauges_exist() -> Result<(), Box<dyn std::error::Error>> {
        let metrics = format!("dynamo_frontend_model_ready{{model=\"{MODEL}\"}} 1\n");
        assert!((readiness_value(&metrics)? - 1.0).abs() <= f64::EPSILON);
        Ok(())
    }

    #[test]
    fn rejects_duplicate_invalid_or_negative_runtime_samples() {
        let duplicate = format!(
            "dynamo_frontend_model_ready{{model=\"{MODEL}\"}} 1\ndynamo_frontend_active_requests{{model=\"{MODEL}\"}} 0\ndynamo_frontend_active_requests{{model=\"{MODEL}\",zone=\"a\"}} 0\ndynamo_frontend_stage_requests{{stage=\"dispatch\"}} 0\n"
        );
        assert!(parse_metrics(&duplicate).err().is_some());
        assert!(metric_value("metric{model=\"m\"} -1").err().is_some());
        assert!(metric_value("metric{model=\"m\"} NaN").err().is_some());
        assert!(metric_value("metric{model=\"m\"}").err().is_some());
    }

    #[test]
    fn validates_nonempty_chat_completion_responses() {
        assert!(
            validate_chat_response(r#"{"choices":[{"message":{"content":"hello"}}]}"#)
                .err()
                .is_none()
        );
        assert!(
            validate_chat_response(r#"{"choices":[{"message":{"content":" "}}]}"#)
                .err()
                .is_some()
        );
        assert!(validate_chat_response(r#"{"error":"unavailable"}"#).err().is_some());
    }
}
