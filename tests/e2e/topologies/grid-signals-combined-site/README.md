# Persistent llm-d signals environment

This topology creates three independent Kind clusters with llm-d-sim providers,
authenticated EPP metrics, Grid scoring, cross-site overlays, and Praxis AI
consumer Gateways. The setup command stops after a bounded pressure and recovery
proof and leaves the environment running for Gridzilla or another performance
harness. It does not run sustained traffic or performance measurements.

## Requirements

- Docker, Kind, kubectl, Helm, `praxis-forge`, and the repository's Rust toolchain.
- Registry access for Praxis AI `ghcr.io/praxis-proxy/ai:0.7.0`, llm-d-sim
  `ghcr.io/llm-d/llm-d-inference-sim:v0.10.2`, and EPP
  `ghcr.io/llm-d/llm-d-router-endpoint-picker:v0.11.0`.
- Enough local disk and memory for three Kind clusters and the image builds.

The setup builds Grid operator and overlay-sync images from the current Grid
checkout, gives them run-unique tags, and locks all five image references before
provisioning. It loads those exact images into each cluster and sets
`imagePullPolicy: Never`; a missing image or mismatched runtime image causes a
failed, retained run. The image lock, full running Pod `imageID`s, Grid source
SHA, and dirty-tree fingerprint are recorded in the evidence directory.

## Start

From the Grid repository root, choose a fresh lowercase run ID (up to 32
characters) and start the persistent environment:

```bash
RUN_ID="run-$(date -u +%Y%m%d%H%M%S)"
EVIDENCE_ROOT="evidence/grid-signals-combined-site"
cargo xtask env start-grid-combined-site-signals \
  --run-id "$RUN_ID" \
  --evidence-root "$EVIDENCE_ROOT"
```

The command refuses an existing run ID, cluster name, or Forge network name. It
uses a run-owned kubeconfig and certificates under
`$EVIDENCE_ROOT/$RUN_ID/`, never the default kubeconfig. A successful run writes
`handoff.json`, `image-lock.json`, runtime image identities, baseline and
recovery metrics, overlay revisions, attributed request results, and a signal
timeline. A setup or qualification failure retains the run-owned resources and
diagnostics for inspection; it never silently tears the environment down.

The stable handoff for Gridzilla is:

```text
$EVIDENCE_ROOT/$RUN_ID/handoff.json
```

### VCR infrastructure only

The default VCR image is `ghcr.io/neuralmagic/vllm-vcr:0.2.5-vllm0.31`;
the setup checks its published OCI digest before creating clusters. The
Praxis AI gateway continues to use the topology's default image.

To start the same three-site infrastructure with CPU-only vllm-vcr backends,
without the llm-d-sim fake-metric pressure/recovery qualification, use a fresh
run ID and:

```bash
cargo xtask env start-grid-combined-site-signals \
  --run-id "$RUN_ID" --evidence-root "$EVIDENCE_ROOT" \
  --workload vllm-vcr --infra-only
```

This writes `infra-handoff.json` with `qualification: NOT_RUN` and a separate
`endpoints/gridzilla-vcr.json` for Gridzilla's generic JSON endpoint scenario.
The normal Grid signals `handoff.json` is deliberately absent. The setup
checks workload and gateway deployment readiness; it does not prove that VCR
requests affect EPP metrics, Grid scoring, or Gateway selection. The clusters
remain running. Use the same run-owned cleanup manifest when finished.

The run-owned cluster names are `grid-signals-$RUN_ID-west`,
`grid-signals-$RUN_ID-central`, and `grid-signals-$RUN_ID-east`.

## Clean up one run

After consumers of the environment have finished, explicitly remove only the
clusters named in that run's manifest:

```bash
cargo xtask env clean-grid-combined-site-signals \
  --manifest "$EVIDENCE_ROOT/$RUN_ID/run-manifest.json"
```

The cleanup command validates the manifest and run-owned evidence paths before
deleting clusters. It does not delete other Kind clusters, networks, or
worktrees. Preserve the evidence directory if the run is needed for later
analysis.

## Limits

The simulator pressure is synthetic scheduler input; this environment does not
measure GPU saturation, model quality, or token throughput. The qualification
proves a small llm-d-sim → EPP → Grid score/overlay → Gateway selection shift
and recovery. It does not prove sustained-load behavior, production capacity,
or a performance SLO. Request attribution uses the provider Gateway's site
header, Praxis AI's selected provider-route header, and the simulator's backend
route and Pod headers. The simulator echoes `X-Request-Id`; together with the
Pod identity, this provides a request-correlated backend response receipt
without enabling HTTP request-body logging or exposing a separate receipt API.
