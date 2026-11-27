# Quickstart: a two-site grid on kind

> **Technical preview.** AI Grid Network (AGN) is a technical preview. Read the
> [technical preview scope and limitations](../technical-preview-scope.md)
> before you plan a deployment. This quickstart is for evaluation on one
> machine, not for production.

This quickstart runs a two-site grid on one Linux machine with kind: a hub
cluster and a site cluster, with SPIFFE peer trust and polled signals. The
repository's hub-and-site test script creates the clusters, runs the
[hub and site Helm install](../../examples/helm/hub-site/README.md), and checks
that the grid works. CI runs the same script on every pull request.

To install AGN on clusters you already run, use the
[existing-cluster Helm installation](../installation/existing-clusters.md)
instead.

## Prerequisites

- Linux with Docker. macOS is not verified: the images are `linux/amd64`, and
  kind LoadBalancer addresses are not reachable from a macOS host.
- On non-amd64 Linux hosts, the script loads the amd64-only VCR simulator image
  and requires container-runtime emulation.
- [kind](https://kind.sigs.k8s.io) v0.32.0, kubectl,
  [Helm](https://helm.sh) v3.17.3, Git, curl, and `jq`.
- Rust from `rust-toolchain.toml`, used to install [Forge](../developing/forge.md),
  the tool that creates the clusters.
- 4 vCPU, 16 GB memory, and 20 GB of free disk.

The script uses the fixed Kind cluster names `grid-hub-site-hub` and
`grid-hub-site-site`; the corresponding kubectl contexts are
`kind-grid-hub-site-hub` and `kind-grid-hub-site-site`. Check `kind get clusters`
before starting. The `down` command deletes both named clusters, so use these
names only when they are free and the clusters are owned by this evaluation.

## Create the grid

Clone the repository and install the pinned Forge:

```console
git clone https://github.com/praxis-proxy/grid && cd grid
./scripts/forge.sh install
```

Use the published images for the checked-out commit instead of building them:

```console
export SKIP_BUILD=1 IMAGE_PREFIX=ghcr.io/praxis-proxy/grid- \
  IMAGE_TAG=sha-$(git rev-parse HEAD | cut -c1-7)
for i in operator gateway enrollment; do
  docker pull "$IMAGE_PREFIX$i:$IMAGE_TAG"
done
```

The `Helm` workflow publishes these `sha-<commit>` images for commits pushed to
`main` and for release commits, after its tests pass. A commit whose run is
still in progress or failed has no images yet, and the pulls fail. Check out a
commit whose `Helm` run succeeded, or wait for its run to finish.

To build the images from source instead, clear the variables you exported:

```console
unset SKIP_BUILD IMAGE_PREFIX IMAGE_TAG
```

Create the hub and site clusters:

```console
./scripts/e2e-hub-site.sh up
```

Install AGN with SPIFFE trust (`MODES=spiffe`) and polled signals with
operator-generated routing (`HUB_ROUTER=operator`):

```console
MODES=spiffe HUB_ROUTER=operator ./scripts/e2e-hub-site.sh test
```

The script prints a `PASS` or `FAIL` line for each check it makes, including
enrollment, membership, peer trust, and requests served through the hub
gateway, and exits with a non-zero status when any check fails. It leaves the
grid running.

## Look around

List the grid resources on each cluster:

```console
kubectl --context kind-grid-hub-site-hub -n grid get gridsites,gridnetworks,inferenceproviders
kubectl --context kind-grid-hub-site-site -n grid get gridsites
```

Send a chat completion through the hub gateway. In one terminal, forward its
port:

```console
kubectl --context kind-grid-hub-site-hub -n grid port-forward service/grid-gateway 18080:8080
```

In another terminal:

```console
curl -sS -D - -H 'Content-Type: application/json' \
  -d '{"model":"Qwen/Qwen3-0.6B","messages":[{"role":"user","content":"ping"}],"max_tokens":8}' \
  http://127.0.0.1:18080/v1/chat/completions
```

A working grid returns HTTP 200 with a `choices` array. The backend is a
simulator that replays recorded responses, so the content is not a live model
answer. In this example, the `x-grid-provider-site: site-a` and
`x-grid-backend: vcr` response headers identify the route and simulator.
Stop the port-forward with Ctrl+C.

## What you just verified

This run exercises the Grid gateway's `grid_site_route` path with serving
configuration rendered by the operator. The script checks hub and site
membership, configured peer trust, and a request routed from the hub gateway to
the site provider gateway and its VCR simulator. It does not exercise Praxis
AI's separate `intelligent_route` overlay path; see the [routing
guide](../routing.md) for that integration. The response headers above are
evidence from this example's gateway and simulator, not headers guaranteed by
every Grid deployment.

This procedure demonstrates the configured route and response path. It does
not measure model capacity or inference performance, and signal polling is
enabled by this quickstart configuration.

## Clean up

Delete both clusters:

```console
./scripts/e2e-hub-site.sh down
```

## Notes

- The Helm commands the script runs are in
  [examples/helm/hub-site](../../examples/helm/hub-site/README.md).
- `grid.membership.source` picks where a site learns its members. `Swim`, the
  default, discovers them by SWIM gossip. `Local` means this site's
  configuration lists its peers as `GridSite` objects, polls their signals, and
  runs no SWIM. Both are supported peer options.
- The script's header documents its other modes and environment variables.

## Next steps

- [Existing-cluster Helm installation](../installation/existing-clusters.md):
  install AGN and Praxis on running Kubernetes clusters.
- [Adding an inference provider](../operations/adding-provider.md): add
  in-cluster, existing-service, and external HTTPS providers.
- [Routing guide](../routing.md): choose and configure routing behavior.
- [Architecture overview](../architecture/overview.md): what the operator
  does and where the data plane begins.
