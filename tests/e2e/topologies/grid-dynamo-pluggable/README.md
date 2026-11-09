# Grid Dynamo pluggable-signal qualification

## Modes

- `cargo xtask env run-grid-dynamo-pluggable-quick` runs deterministic Grid signal, operator, and Gateway contract tests. It does not start Kind or Dynamo.
- `cargo xtask env run-grid-dynamo-pluggable-full --evidence-dir <run-owned-evidence-directory>` starts the pinned Dynamo v1.6.1 Frontend+Mocker runtime in uniquely named Kind clusters. It records registration, valid chat completions, Frontend pressure and recovery, source/image identity, resource samples, logs, metrics, and cleanup.

The full runtime mode does not test Grid overlay distribution or live cross-site request routing. Those require separate phase evidence. Local host measurements characterize only the machine used for a run.

## Run evidence

Keep each run's evidence in its own directory. Do not delete or reuse clusters that are not owned by the current run.
