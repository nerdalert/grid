# RHOAI GPU vLLM chart

This chart manages the KServe `ServingRuntime` and exactly one RawDeployment
`InferenceService` per configured GPU provider. It is intended for the
cloud-burst validation topology, where the local tier has two GPU providers and
the overflow tier is configured separately.

The runtime image must be supplied as a pullable, pinned image. The default
values use the validated RHOAI vLLM CUDA image and Qwen model. Override the two
provider node names for the target cluster; do not put credentials in values.

Example:

```bash
helm upgrade --install cloud-burst-gpu ./charts/rhoai-vllm-gpu \
  --namespace grid-cloud-burst-rhoai --create-namespace
```

The chart does not create a Grid overlay or configure cloud credentials. Grid
provider registration and the Praxis gateway routes remain separate concerns.

The default runtime caps concurrent sequences and batched tokens so a bounded
pressure run can produce an observable waiting queue without exhausting the
GPU. Adjust these values only as part of a measured pressure test.

## RHOAI validation procedure

Use a temporary `KUBECONFIG` supplied by the cluster administrator. Do not put
the kubeconfig, passwords, model-pull credentials, or Valkey credentials in
values files, manifests, logs, or evidence.

Before installing, verify the target has two available NVIDIA GPUs:

```bash
oc get nodes -o wide
oc get nodes -o custom-columns='NAME:.metadata.name,GPU:.status.allocatable.nvidia\\.com/gpu'
oc get servingruntime -A
```

Install or update the GPU serving layer first:

```bash
helm lint ./charts/rhoai-vllm-gpu
helm upgrade --install cloud-burst-gpu ./charts/rhoai-vllm-gpu \\
  --namespace grid-cloud-burst-rhoai --create-namespace --wait
oc -n grid-cloud-burst-rhoai get inferenceservice,pods -o wide
```

Wait for both InferenceServices to report `Ready=True`. Verify pod node,
GPU request, vLLM readiness, and (where permitted) `nvidia-smi`. The predictor
Services are headless in this deployment; provider and metrics endpoints must
therefore include the explicit predictor port `:8080`.

The two local Grid providers use the live vLLM signal
`vllm:num_requests_waiting` and configured local weights. The RHOAI topology
has exactly two local provider gateways, one per GPU. A third provider gateway
must not remain without a GPU-backed provider.

Verify the operator-published overlay ConfigMaps and the accepted Praxis
serving revision. A live overlay must show the two local candidates and
`selection_policy.mode: weightedRandom`; do not silently fall back to a
hand-authored static overlay. If GridSite discovery remains Pending because a
single-member SWIM instance treats its own identity as a seed, record that
limitation and do not describe the overlay as fully reconciled by Grid.

The optional tracing UI is deployed separately with a namespaced ServiceAccount
and least-privilege RoleBindings. It reads overlay and provider state
server-side; inference credentials remain server-side. For a real GPU backend,
static-metric controls are disabled: pressure must come from real traffic and
observed vLLM/EPP metrics. Test the UI route with a small authenticated request
before any pressure run.

Evidence should include provider readiness, GPU placement, a small successful
completion with usage, overlay and serving revisions, vLLM queue metrics, and
the final topology. Keep failed attempts separate. Do not claim cloud burst,
recovery, or quota continuity until those paths are observed live.

## Multi-quota UI and real-GPU pressure

The optional cloud-burst UI can run three independent application profiles,
each with its own authenticated principal and soft allocation. Store the
application passwords in a Secret and mount them only into the UI server. Do
not put those values in a ConfigMap, browser code, image, or evidence. The UI
uses the existing application palette to correlate each app's requests,
quota-charge timeline, provider attribution, and topology edges.

On a two-GPU RHOAI cluster, configure exactly two consumer/provider paths:
East and West. The UI must discover those paths from the accepted overlay and
must not render a third provider merely because the Kind demo has three. East
and West still share one quota ledger per principal and logical service.

Soft allocation is observable governance: requests continue successfully when
an app goes over its soft allocation, and the UI reports `over_allocation`.
Hard enforcement is a separate mode that can return 429. Never interpret an
over-allocation status as a provider failure or fabricate cloud usage when no
cloud route was selected.

For real GPU pressure, use the UI's sustained-pressure control. It sends a
bounded stream of authenticated requests through the consumer gateway and
observes the predictor metrics; it does not patch a fake queue value or scale
the deployment. Verify the queue builds, record
`vllm:num_requests_waiting`/`vllm:num_requests_running`, stop the stream, and
wait for the queue to drain. The static metric control is for Kind simulator
runs only.

The chart's default vLLM limits (`max-num-seqs=2`,
`max-num-batched-tokens=512`, `max-model-len=8192`, and
`gpu-memory-utilization=0.90`) are pressure-test safeguards. Keep the values
as unquoted numeric command-line arguments in the rendered ServingRuntime;
quoted numeric arguments are rejected by vLLM.

The single-site SWIM bootstrap currently expects literal `host:port` seed
addresses. A headless Service DNS name is not accepted by that parser, so a
headless-Service workaround must not be presented as a completed discovery
fix. If `GridSite` remains Pending, keep the static-overlay limitation
explicit and do not claim a fully Grid-reconciled pressure or burst result.

### RHOAI trust activation record

The live single-site probe required three configuration corrections:

1. Add the GridNetwork label
   `grid.praxis-proxy.io/auto-discover-sites: "true"`.
2. Reference the certificate Secret that contains the provider gateway SANs
   for the GridNetwork TLS material. In the validated deployment this was
   `consumer-gateway-tls-v3`; the earlier Secret did not contain the required
   SANs.
3. Set the GridSite egress TLS `serverName` to the SAN actually issued for the
   gateway, `provider-gateway.grid-system.svc`, rather than the longer
   cluster-local DNS name.

With the existing provider certificate fingerprint, the GridSite reached
`Active` with reason `TlsVerified`. This confirms the local mutual-TLS trust
path. A one-site SWIM table with no remote peers is normal; gateways are not
SWIM members.

The GridNetwork still reports `Initializing` because the running operator logs
`foca add_broadcast failed: Received data larger than maximum configured
limit` while publishing the self-site broadcast. Until that Grid/operator
issue is fixed, the installation must not claim full GridNetwork convergence
or a completed cloud-burst qualification. The local overlay is observable,
but its control-plane status remains explicitly incomplete.
