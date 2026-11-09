//! Dynamo pluggable-signal qualification commands.

use std::{
    fs,
    io::Write as _,
    path::{Path, PathBuf},
    process::Command,
    time::Instant,
};

use serde::Serialize;
use sha2::{Digest as _, Sha256};
use time::OffsetDateTime;

pub(crate) mod runtime;

/// Evidence schema for the Grid-only quick qualification.
const EVIDENCE_SCHEMA_VERSION: &str = "1";

/// One deterministic Grid-only test command and its result.
#[derive(Serialize)]
struct CommandResult {
    /// Stable command label.
    name: String,
    /// Command line recorded for reproduction.
    command: String,
    /// Exit status, or `None` when the process could not start.
    exit_code: Option<i32>,
    /// Wall time in seconds.
    wall_seconds: f64,
    /// Whether the command passed.
    passed: bool,
    /// Process launch error, if any.
    error: Option<String>,
    /// Command log path relative to this run's evidence directory.
    log: String,
}

/// Content identity for one untracked source file included in a local run.
#[derive(Serialize)]
struct UntrackedFileDigest {
    /// Repository-relative file path.
    path: String,
    /// SHA-256 of the file contents.
    sha256: String,
}

/// Complete source identity shared by quick and full qualification modes.
struct SourceIdentity {
    /// Grid source commit tested.
    source_revision: String,
    /// Full diff from `HEAD`, including staged and unstaged tracked changes.
    tracked_diff: Vec<u8>,
    /// Hash of the tracked diff.
    tracked_diff_sha256: String,
    /// Hash covering the tracked diff plus untracked paths and contents.
    source_state_sha256: String,
    /// Content hashes for untracked source files.
    untracked_files: Vec<UntrackedFileDigest>,
}

/// Evidence envelope for the Grid-only quick qualification.
#[expect(
    clippy::struct_excessive_bools,
    reason = "the stable JSON envelope records two separate scope flags and the overall result"
)]
#[derive(Serialize)]
struct QuickEvidence {
    /// Evidence format version.
    schema_version: String,
    /// Mode label distinct from the real Dynamo runtime mode.
    mode: String,
    /// Unique UTC run identifier.
    run_id: String,
    /// Qualification start time.
    started_at_utc: String,
    /// Qualification completion time.
    completed_at_utc: String,
    /// Grid repository root.
    repository_root: String,
    /// Grid source commit tested.
    source_revision: String,
    /// Hash of the tracked source diff tested.
    tracked_diff_sha256: String,
    /// Hash covering the tracked diff plus untracked paths and contents.
    source_state_sha256: String,
    /// Hashes for untracked files included in `source_state_sha256`.
    untracked_files: Vec<UntrackedFileDigest>,
    /// Whether any Dynamo runtime was started.
    dynamo_runtime_tested: bool,
    /// Whether cross-site routing was tested.
    cross_site_tested: bool,
    /// Overall command result.
    success: bool,
    /// Commands executed by this qualification.
    commands: Vec<CommandResult>,
}

/// Run deterministic Grid-only contract tests for Dynamo signal routing.
///
/// This mode runs the parser, operator, and Gateway test suites and records each command.
/// It never starts a Kind cluster, Dynamo Frontend, or Mocker and cannot qualify live routing.
///
/// # Errors
///
/// Returns an error if evidence cannot be written or one or more test commands fail.
pub(crate) fn run_grid_only_quick(evidence_root: Option<&Path>) -> Result<(), Box<dyn std::error::Error>> {
    let repository_root = repository_root()?;
    let run_id = format!("{}-{}", utc_timestamp(), std::process::id());
    let evidence_parent = evidence_root.map_or_else(
        || repository_root.join("tests/e2e/topologies/grid-dynamo-pluggable/evidence"),
        Path::to_path_buf,
    );
    let evidence_dir = evidence_parent.join(&run_id);
    fs::create_dir_all(&evidence_dir)?;

    let started_at_utc = iso_timestamp();
    let source_identity = capture_source_identity(&repository_root)?;
    eprintln!("Grid Dynamo pluggable qualification: grid-only-quick");
    eprintln!("Evidence: {}", evidence_dir.display());

    let commands = execute_quick_commands(&repository_root, &evidence_dir)?;
    let success = commands.iter().all(|command| command.passed);
    let evidence = QuickEvidence {
        schema_version: EVIDENCE_SCHEMA_VERSION.to_owned(),
        mode: "grid-only-quick".to_owned(),
        run_id,
        started_at_utc,
        completed_at_utc: iso_timestamp(),
        repository_root: repository_root.display().to_string(),
        source_revision: source_identity.source_revision,
        tracked_diff_sha256: source_identity.tracked_diff_sha256,
        source_state_sha256: source_identity.source_state_sha256,
        untracked_files: source_identity.untracked_files,
        dynamo_runtime_tested: false,
        cross_site_tested: false,
        success,
        commands,
    };
    let evidence_path = evidence_dir.join("qualification.json");
    fs::write(&evidence_path, serde_json::to_vec_pretty(&evidence)?)?;
    eprintln!("[QUICK][{}] grid-only-quick", if success { "PASS" } else { "FAIL" });
    eprintln!("Evidence: {}", evidence_path.display());
    if success {
        Ok(())
    } else {
        Err(format!("Grid-only quick qualification failed; see {}", evidence_path.display()).into())
    }
}

/// Run the four deterministic parser, operator, and Gateway commands and retain their logs.
fn execute_quick_commands(
    repository_root: &Path,
    evidence_dir: &Path,
) -> Result<Vec<CommandResult>, Box<dyn std::error::Error>> {
    let specs: [(&str, &[&str]); 4] = [
        ("signals-contract", &["test", "--locked", "-p", "grid-signals"]),
        (
            "operator-dynamo-contract",
            &["test", "--locked", "-p", "operator", "--lib", "dynamo"],
        ),
        (
            "operator-readiness-policy",
            &[
                "test",
                "--locked",
                "-p",
                "operator",
                "--lib",
                "only_fresh_explicit_model_not_ready_excludes_by_adapter_readiness",
            ],
        ),
        (
            "gateway-filter-contract",
            &[
                "test",
                "--locked",
                "--manifest-path",
                "crates/gateway/Cargo.toml",
                "-p",
                "ai-grid-filters",
            ],
        ),
    ];

    let mut commands = Vec::with_capacity(specs.len());
    for (index, (name, args)) in specs.iter().enumerate() {
        let log_name = format!("{index:02}-{name}.log");
        let command_line = format!("cargo {}", args.join(" "));
        eprintln!("[QUICK][START] {command_line}");
        let started = Instant::now();
        let result = Command::new("cargo").args(*args).current_dir(repository_root).output();
        let elapsed = started.elapsed().as_secs_f64();
        let (exit_code, passed, error, log) = match result {
            Ok(output) => {
                let status = output.status.code();
                let passed = output.status.success();
                let mut log = Vec::with_capacity(output.stdout.len() + output.stderr.len() + 128);
                writeln!(log, "$ {command_line}")?;
                writeln!(log, "exit_code={status:?}")?;
                writeln!(log, "--- stdout ---")?;
                log.extend_from_slice(&output.stdout);
                writeln!(log, "\n--- stderr ---")?;
                log.extend_from_slice(&output.stderr);
                (status, passed, None, log)
            },
            Err(error) => {
                let message = format!("failed to start cargo: {error}");
                (None, false, Some(message.clone()), message.into_bytes())
            },
        };
        fs::write(evidence_dir.join(&log_name), log)?;
        eprintln!(
            "[QUICK][{}] {name} ({elapsed:.2}s)",
            if passed { "PASS" } else { "FAIL" }
        );
        commands.push(CommandResult {
            name: (*name).to_owned(),
            command: command_line,
            exit_code,
            wall_seconds: elapsed,
            passed,
            error,
            log: log_name,
        });
    }

    Ok(commands)
}

/// Run the real Dynamo Frontend + Mocker runtime qualification.
pub(crate) fn run_full_runtime(evidence_root: Option<&Path>) -> Result<(), Box<dyn std::error::Error>> {
    runtime::run(evidence_root)
}

/// Resolve the Grid repository containing the xtask crate.
fn repository_root() -> Result<PathBuf, Box<dyn std::error::Error>> {
    Ok(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").canonicalize()?)
}

/// Run a Git command and trim its UTF-8 output.
fn git_output(root: &Path, args: &[&str]) -> Result<String, Box<dyn std::error::Error>> {
    let output = Command::new("git").args(args).current_dir(root).output()?;
    if !output.status.success() {
        return Err(format!("git {} failed", args.join(" ")).into());
    }
    Ok(String::from_utf8(output.stdout)?.trim().to_owned())
}

/// Run a Git command and return its raw output.
fn git_bytes(root: &Path, args: &[&str]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let output = Command::new("git").args(args).current_dir(root).output()?;
    if !output.status.success() {
        return Err(format!("git {} failed", args.join(" ")).into());
    }
    Ok(output.stdout)
}

/// Capture the full source identity used by both Dynamo qualification modes.
fn capture_source_identity(root: &Path) -> Result<SourceIdentity, Box<dyn std::error::Error>> {
    let source_revision = git_output(root, &["rev-parse", "HEAD"])?;
    let tracked_diff = git_bytes(root, &["diff", "HEAD", "--binary"])?;
    let untracked = git_bytes(root, &["ls-files", "--others", "--exclude-standard", "-z"])?;
    let mut untracked_contents = Vec::new();
    let mut untracked_files = Vec::new();
    for raw_path in untracked.split(|byte| *byte == 0).filter(|path| !path.is_empty()) {
        let path = std::str::from_utf8(raw_path)?.to_owned();
        let contents = fs::read(root.join(&path))?;
        untracked_files.push(UntrackedFileDigest {
            path: path.clone(),
            sha256: hex::encode(Sha256::digest(&contents)),
        });
        untracked_contents.push((path, contents));
    }
    let tracked_diff_sha256 = hex::encode(Sha256::digest(&tracked_diff));
    let source_state_sha256 = source_state_sha256(&tracked_diff, &untracked_contents);
    Ok(SourceIdentity {
        source_revision,
        tracked_diff,
        tracked_diff_sha256,
        source_state_sha256,
        untracked_files,
    })
}

/// Hash tracked changes and each untracked path/content pair in Git's sorted order.
fn source_state_sha256(tracked_diff: &[u8], untracked: &[(String, Vec<u8>)]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(tracked_diff);
    for (path, contents) in untracked {
        hasher.update(path.as_bytes());
        hasher.update(contents);
    }
    hex::encode(hasher.finalize())
}

/// Format a UTC run identifier.
fn utc_timestamp() -> String {
    let now = OffsetDateTime::now_utc();
    format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        now.year(),
        u8::from(now.month()),
        now.day(),
        now.hour(),
        now.minute(),
        now.second(),
    )
}

/// Format an ISO UTC timestamp for evidence.
fn iso_timestamp() -> String {
    let now = OffsetDateTime::now_utc();
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        now.year(),
        u8::from(now.month()),
        now.day(),
        now.hour(),
        now.minute(),
        now.second(),
    )
}

#[cfg(test)]
mod tests {
    use super::source_state_sha256;

    #[test]
    fn source_state_hash_covers_untracked_path_and_content() {
        let first = vec![("src/new.rs".to_owned(), b"first".to_vec())];
        let changed_content = vec![("src/new.rs".to_owned(), b"second".to_vec())];
        let changed_path = vec![("src/other.rs".to_owned(), b"first".to_vec())];

        let first_hash = source_state_sha256(b"tracked diff", &first);
        assert_ne!(
            first_hash,
            source_state_sha256(b"tracked diff", &changed_content),
            "untracked contents affect the source identity"
        );
        assert_ne!(
            first_hash,
            source_state_sha256(b"tracked diff", &changed_path),
            "untracked paths affect the source identity"
        );
    }
}
