# llm-d pool-metrics operator coverage

This inventory records what the Grid-native `run-grid-llmd-pool-metrics-demo`
actually exercises. It is based on the PR #398 source at
`9f16ab995eb0f514624045c41d66919537063a3d` and the corresponding topology
README, Forge configuration, and Triggered Integration Test workflow.

## Signal-to-request paths

| Stage | Deployed path and evidence | Boundary |
|---|---|---|
| Signal source | Each Kind site runs `llm-d-inference-sim` backends, an `InferencePool`, and the pinned llm-d EPP. The EPP exposes queue, KV-cache, and ready-endpoint metrics. Simulator `fake-metrics` is deliberately configured through run-owned ConfigMaps or its admin API; this is deterministic synthetic telemetry, not load-induced production pressure. | Proves the EPP/operator integration against the pinned fixture and metric contract, not arbitrary EPP implementations or real serving load. |
| Local operator and publication | `proof_pressure_and_flip` changes Pool A's persistent simulator signal, reads the EPP values, triggers normal GridNetwork reconciliation, and waits for the generated consumer overlay to rank Pool B first. `proof_recovery` returns the signal to baseline and waits for Pool A to regain rank 0. | Uses the local operator-to-overlay path. It does not test inter-operator signal relay. |
| Consumer Gateway request | Both proofs issue actual inference requests through the consumer Gateway and require both provider-Gateway and demo-attribution headers to identify the expected pool. Score/rank observations alone do not pass the traffic assertions. | This is the Praxis consumer `intelligent_route`/provider path, distinct from Grid's embedded `grid_site_route` scoring. |
| Cross-site signal relay | Full mode starts the embedded Grid Gateway, polls local operator metrics, and verifies EPP signals are relayed from the peer operator over the configured mTLS poll path. Attributed request batches are checked against the live two-site weighted share; the Gateway pod UID and serving-config reload counter must remain stable for signal-only updates. | Proves the fixture's two-site topology and current selection inputs, not a general multi-site or geography policy. |
| Signal loss and trust | Full mode removes Pool A's EPP, checks its measured Gateway gauges clear while SWIM remains connected and the provider pair remains in topology, then verifies traffic shifts. It restores the signal. A separate wrong-peer-pin phase checks rejected signal updates do not replace the trusted value. | `authorize_discovered_sites()` patches identity trust on run-owned auto-discovered GridSites after checking the expected certificate fingerprint. This is fixture authorization/bootstrap; it does not qualify automatic trust enrollment. |
| Network partition | Full mode disconnects only the run-owned remote Kind nodes from their identified shared Docker network, probes while the peer is unavailable, and restores the exact network ID, addresses, and aliases before verifying recovery. | Not a test of unrelated host/network failures. |
| Capacity lifecycle | Full mode checks learned soft/hard ceilings, idle persistence, exact serving-pair removal and restoration, KV-only routing, endpoint shrink, and relearning. EPP, both operator views, Gateway metrics/config revisions, attributed responses, and cleanup are used as separate observations. | These are deterministic simulator/EPP lifecycle scenarios, not KServe qualification. |

## Quick/full contract

Before this change, the README described quick mode as proving score changes,
overlay propagation, routing, and recovery, but the xtask dispatcher only ran
provenance and baseline in quick mode. The pressure/flip and recovery proofs
were inside the full-only branch, so the daily quick workflow provided no
signal-driven route change despite that documentation.

The proof plan now makes the contract explicit:

| Mode | Proofs |
|---|---|
| Quick | Image/metric provenance; idle baseline and attributed request; persistent local EPP pressure → Pool B rank/attribution; recovery → Pool A rank/attribution. |
| Full | All quick proofs; embedded Gateway and cross-site mTLS signal relay; live signal-only route shift; stale-signal exclusion; wrong-pin rejection/recovery; network partition/reconvergence; learned-capacity and KV-cache lifecycles. |

The full plan retains the previous pressure/recovery assertions and ordering.
The quick plan reuses those same bounded proofs; it does not require or install
the full-mode Grid Gateway. A focused `proof_plan` unit test prevents the mode
matrix from silently regressing.

The Triggered Integration Test accepts both
`llmd-pool-metrics-quick` and `llmd-pool-metrics-full`. The scheduled workflow
is quick-only; full remains manual because it is substantially longer and has
a 75-minute runner timeout. No full run is scheduled. This audit uses the full
mode locally and on the fork so the complete matrix is exercised at each tested
source revision.

## CI recommendation

- Keep the daily scheduled quick run as the recurring deployed signal-to-route
  smoke. Quick now includes the Pool A → Pool B pressure/attribution proof and
  recovery, not just setup and an idle baseline.
- Keep full as an explicit manual qualification for changes to cross-site
  relay, mTLS identity, partition behavior, learned ceilings, or KV-cache
  scoring. Do not schedule it daily or make every PR pay for its two-cluster,
  multi-phase runtime without a separate cost decision.
- The full local run for this audit took about 25 minutes; the workflow allows
  75 minutes. The updated quick-mode duration has not yet been measured in a
  clean local run, so do not infer its runner cost from this full result.
- The workflow retains uploaded evidence for 14 days. Preserve the per-proof
  results, image/source identities, failure diagnostics, and teardown inventory
  with each run. A manually selected workflow run or scheduled pass is not
  automatically a required PR status check; branch-protection configuration
  would need to make that policy explicit.

## Explicitly unqualified

- KServe is not deployed by this topology. The fixture is llm-d EPP plus
  deterministic `llm-d-inference-sim`; no KServe behavior is claimed.
- The full cross-site proof restarts operators before installing the embedded
  Gateway to activate poll mode. It does not restart an operator during an
  already-active signal-routing window. The partition and EPP-loss cases test
  live interruption/recovery but are not a process-restart test.
- Auto-discovered-site trust is explicitly patched by the harness after
  certificate verification; automated trust onboarding is not qualified.
- The requests use the configured test identity and attribution headers. This
  is not a claim about all production authentication, tenancy, or user-policy
  configurations.
- The operator's in-memory/unit reconciliation tests are useful component
  evidence but do not replace this deployed two-cluster proof. Conversely, this
  proof does not claim general Kubernetes status-conformance across every
  condition or CRD field.

## Audit change

Quick mode now runs the already-existing pressure-and-flip and recovery proofs
after baseline, matching its documented purpose. Full mode retains them before
the cross-site and learned-ceiling proofs. No traffic threshold, attribution
oracle, timeout, or full-mode assertion was weakened.
