# OpenTelemetry for Grid gateways

Grid gateway builds can export traces to an OTLP/gRPC collector. Export is
configured on the gateway process; routing overlays remain limited to routing
state.

## Enable export

The default configuration has no collector endpoint, so it starts without an
OTLP exporter and does not require a collector. To opt in, use either
`GatewayRef.consumerConfig.telemetry` for operator-generated consumer config or
`gatewayConfig.telemetry` for the Helm-generated Praxis config.

An operator example:

```yaml
spec:
  gatewayRefs:
    - name: edge
      namespace: grid
      consumerConfig:
        enabled: true
        telemetry:
          otlpEndpoint: http://otel-collector.observability:4317
          samplingRate: 0.1
          serviceName: grid-edge
          environment: production
```

For Helm, enable `gatewayConfig.telemetry` and use the Grid gateway image:

```yaml
image:
  repository: ghcr.io/praxis-proxy/grid-gateway
  flavor: grid-gateway
gatewayConfig:
  render: true
  telemetry:
    enabled: true
    otlpEndpoint: http://otel-collector.observability:4317
    samplingRate: 0.1
    serviceName: grid-edge
```

Both paths render a top-level Praxis `telemetry` block. The Helm chart adds the
`trace_context` filter when telemetry is enabled. The operator-generated
consumer config does the same. Neither path places exporter settings in the
routing overlay.

## Credentials and lifecycle

Do not put credentials in `otlpEndpoint`, telemetry fields, routing overlays,
or filter configuration. Praxis reads OTLP headers from
`OTEL_EXPORTER_OTLP_HEADERS`; provide that variable from a Deployment-managed
Secret. Helm exposes the container `env` list for a Secret reference:

```yaml
env:
  - name: OTEL_EXPORTER_OTLP_HEADERS
    valueFrom:
      secretKeyRef:
        name: collector-credentials
        key: headers
```

The Secret value uses the OpenTelemetry `key=value` header format. The operator
only creates the consumer `ConfigMap`; the deployment manager must add the same
Secret-backed environment reference to its gateway Deployment. Praxis redacts
OTLP header values from its config debug representation.

Exporter setup runs once at process startup. Helm rolls gateway pods when its
telemetry values change. An externally managed consumer Deployment must be
restarted after its generated ConfigMap changes, and a gateway must be restarted
after the collector endpoint or Secret-backed environment changes. On normal
server return, Grid drops Praxis's `TracingGuard`, which shuts down the OTLP
provider and flushes queued spans.

## Build and spans

The `grid-gateway` binary built by `deploy/gateway/Containerfile` includes the
Praxis 0.7.1 `otel` feature and the pinned Praxis AI v0.4.1
`opentelemetry` feature. Use an image built from that target, published as
`ghcr.io/praxis-proxy/grid-gateway`, for this configuration. The chart's default
`ghcr.io/praxis-proxy/ai:0.4.0` image does not include the Grid build features.

With the AI feature enabled, these short semantic spans are supported when the
corresponding filters run:

- `http_request` server spans and `upstream_exchange` internal spans from
  Praxis 0.7.1's HTTP proxy protocol.
- `routing.select` from `intelligent_route`, including the serving overlay
  semantic revision when that revision is available to the filter.
- `provider.route` from `provider_route`, including a validated edge overlay
  revision when present.

The pinned AI implementation projects bounded routing fields into those spans;
it does not add prompt or body contents, credential values, authorization
headers, cookies, session keys, or raw request IDs. These spans describe route
selection and resolution; `provider.route` does not prove the model backend
served the request.

## Trace linkage status in Praxis 0.7.1

Praxis 0.7.1 exports each gateway's HTTP server span and an internal
`upstream_exchange` span. The AI `routing.select` and `provider.route` spans
also appear as children of their local gateway's server span. The proxy's
upstream span is named `upstream_exchange` with `INTERNAL` kind; Praxis 0.7.1
does not export it as an HTTP client span.

The `trace_context` filter separately validates inbound W3C `traceparent`,
stores that value in a Praxis request extension, and forwards its trace ID and
flags with a newly generated hop span ID. The filter's own source documentation
states that this hop ID does not name an exported span. Praxis 0.7.1 also does
not extract that extension into the OpenTelemetry context used by the HTTP
server span. This leaves two parallel contexts: exported spans use the local
OpenTelemetry request context, while forwarded headers use the filter's W3C
context.

A collector-backed test used an incoming trace ID of
`aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa`. The edge exported a `POST` server span in
trace `89e767a01d701b4f04d1b7b12c022c8a`, and the provider exported a `POST`
server span in trace `abea135120a177a97b76273f37477f99`; both have no parent ID.
The edge `routing.select` span is a child of the edge server span, and the
provider `provider.route` span is a child of the provider server span. The
test backend received `traceparent` with the original `aaaa...` trace ID and a
fresh hop ID. The collector received spans only after normal SIGTERM shutdown,
confirming provider flush. Full request details and collector output are kept
outside the source tree in the PR evidence directory.

This proves header propagation and local span export. It also proves that the
exact pinned Praxis version does not create exported cross-gateway parent/child
relationships. Issue #260 remains incomplete until Praxis provides a shared
W3C/OpenTelemetry context for inbound server spans and outbound client spans,
then a collector test confirms the resulting parent IDs across both gateways.
See the accompanying Praxis issue draft for the proposed dependency change.
