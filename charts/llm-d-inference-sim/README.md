# llm-d inference simulator chart

This chart deploys one or more inference backend pools for Grid validation.
The default `backend.type: simulator` mode uses deterministic
`llm-d-inference-sim` containers and reports configured `fake-metrics`,
including `vllm:num_requests_waiting`. The same chart can run a real vLLM
image with `backend.type: vllm`; the caller supplies the pinned image and
command/arguments.

The default values are CPU-only and suitable for Kind or OpenShift restricted
SCCs. The chart does not require a fixed UID. When a real GPU-serving image is
ready, set `backend.type: vllm`, pin `image.repository`/`image.tag` or
`digest`, enable `gpu`, and provide a node selector and tolerations for the
NVIDIA worker pool. The chart does not assume a cluster-specific model-serving
policy; vLLM's command and model arguments remain explicit values.

Example GPU values:

```yaml
image:
  repository: registry.example/vllm
  tag: pinned-version
backend:
  type: vllm
  command: ["vllm"]
  args: ["serve", "Qwen/Qwen2.5-3B-Instruct", "--served-model-name", "gpt-4o-mini"]
gpu:
  enabled: true
  resourceName: nvidia.com/gpu
  count: 1
nodeSelector:
  nvidia.com/gpu.present: "true"
tolerations:
  - key: nvidia.com/gpu
    operator: Exists
    effect: NoSchedule
```
