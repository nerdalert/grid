# llm-d Pressure-Weighted Provider Distribution

This research note describes the opt-in pressure-weighted placement path. It
extends provider selection; it does not move Grid or llm-d work into the
request path.

## Ownership boundary

```text
llm-d EPP metrics                 Praxis request path
queue depth / KV pressure              Client
          |                               |
          v                               v
    Grid operator                 Consumer gateway
          |                               |
          | eligibility, groups,          | intelligent_route
          | smoothing, weights           v
          v                     weightedRandom from snapshot
  immutable routing overlay              |
          |                               v
          +--> overlay-sync       Provider gateway A/B/C
                    |                     |
                    +--> Praxis          +--> llm-d EPP selects
                         snapshot              an endpoint inside a pool
```

llm-d EPP selects an endpoint inside one provider pool. Grid asynchronously
compares eligible provider pools and publishes effective weights. Praxis uses
those weights locally for new selections. An individual request does not call
Grid, Kubernetes, ConfigMaps, EPP, Prometheus, or a remote scoring service.

## Policy modes

| Routing | Scoring | Placement and selection | Result |
|---|---|---|---|
| `scoreFirst` | `queueDepth` | `deterministic` | Strict preference for the highest-ranked viable provider. |
| `geographyFirst` | `noMetrics` | `roundRobin` | Equal selection in the nearest viable group. |
| `scoreFirst` | `noMetrics` | static `weightedRandom` | Distribute according to configured provider capacity weights. |
| `scoreFirst` | `queueDepth` | `pressureWeighted` + `weightedRandom` | Distribute according to smoothed inverse queue pressure. |
| `scoreFirst` | `kvCachePressure` | `pressureWeighted` + `weightedRandom` | Distribute according to smoothed available KV capacity. |

Deterministic selection remains the winner-takes-all option. A rank change can
move all new unbound traffic from one provider to another. Pressure weighting
is an explicit alternative, not an implicit conversion of scores into weights.

## Queue-depth configuration

```yaml
spec:
  routingPolicy: scoreFirst
  scoringPolicy:
    strategy: queueDepth
  placementPolicy:
    strategy: pressureWeighted
    pressureWeighted:
      signal: queueDepth
      minimumWeight: 1
      maximumWeight: 1000
      availabilityFloorPercent: 5
      smoothingFactor: 0.35
      changeThresholdPercent: 5
  selectionPolicy:
    mode: weightedRandom
```

The queue signal is read from the configured llm-d EPP metrics endpoint. A
positive `metricsConfig.queueCapacity` is required for queue-depth placement so
raw queue size can be normalized consistently between pools.

## KV-cache configuration

```yaml
spec:
  routingPolicy: scoreFirst
  scoringPolicy:
    strategy: kvCachePressure
  placementPolicy:
    strategy: pressureWeighted
    pressureWeighted:
      signal: kvCachePressure
  selectionPolicy:
    mode: weightedRandom
```

KV-cache pressure is provider capacity pressure. It is not request-specific
prefix affinity. Queue depth and KV-cache pressure are separate typed modes and
are never blended in one placement calculation.

## Calculation and churn control

```text
EPP sample
  -> validate freshness and range
  -> convert pressure to availability (1 - pressure)
  -> apply configured capacity and EWMA smoothing
  -> normalize independently inside each selection group
  -> round deterministically and enforce positive bounds
  -> compare with the last published weights
       | below change threshold -> keep the existing revision
       ` material change        -> publish a new semantic revision
                                      -> overlay-sync validates it
                                      -> Praxis loads it atomically
```

`metricsRefreshInterval` controls how often Grid can observe a new metric. It
does not mean that every observation rewrites a ConfigMap. The percentage
threshold and semantic no-op comparison suppress insignificant changes. A
changed accepted overlay can create fresh snapshot-local selection state; an
unchanged semantic revision must not continually reset it.

The exact timing of a route change is therefore bounded by metric collection,
reconciliation, overlay distribution, and snapshot acceptance. Existing
affinity bindings are checked before weighted selection and are not moved just
to improve the ratio.

## Groups and fallback

```text
Group 0: Provider A (weight 700), Provider B (weight 300)
         weighted selection operates here

Group 1: Provider C
         used only when Group 0 is not viable
```

Weights express distribution only among viable members of one active group.
They do not bypass health, admission, freshness, authorization, locality, or
fallback boundaries. If the active group cannot serve the request, Praxis moves
to the next viable group.

## Helm lifecycle

Pressure weighting is available through the normal Grid Helm values and CRD;
Forge only supplies demo orchestration. Existing installations remain
unweighted when `placementPolicy` is omitted. Enabling pressure weighting is an
upgrade that requires the matching `selectionPolicy.mode` and signal-specific
`scoringPolicy.strategy`. Disabling it removes effective traffic weights from
the next overlay without deleting providers. A rollback restores the previous
selection and placement fields, subject to the normal overlay acceptance delay.

Invalid combinations are rejected before activation: missing placement,
non-weighted selection with placement, mismatched signal and scorer, malformed
pressure controls, and unknown policy fields.

## What to measure

The qualifying runtime evidence must retain the complete causal chain:

```text
direct pressure on Pool A
  -> real EPP metric changes
  -> Grid consumes the selected signal
  -> effective weights change
  -> semantic overlay revision changes
  -> overlay-sync accepts the revision
  -> Praxis serves the revision
  -> new unbound consumer traffic redistributes
  -> pressure removal produces a later recovery redistribution
```

Queue-depth and KV-cache proofs are separate runs. Pressure traffic must be
sent directly to Pool A's provider path; measured traffic must enter through
the consumer gateway. Pressure failures are reported separately from normal
consumer request failures.

## Pressure scenario and claim boundary

The queue-depth scenario creates concurrent, in-flight work against one llm-d
pool and observes the queue signal exported by that pool's EPP. The pressure
payload may use a longer prompt or output length so the VCR backend retains
requests long enough for EPP and Grid to observe them. Changing this synthetic
workload duration does not change routing, scoring, placement, concurrency, or
acceptance thresholds.

VCR queue depth can drain quickly. Fast drainage does not invalidate
pressure-weighted placement, but it makes the pressure window short. Evidence
collection must sample frequently enough to record the queue change and must
align these values on one timeline:

```text
EPP queue depth
  -> Grid effective provider weights
  -> semantic overlay revision
  -> Praxis serving revision
  -> measured consumer traffic distribution
```

A qualifying pressure-weighted result supports this claim:

> Weighted routing responds to queue-depth pressure reported by llm-d EPP and
> shifts new unbound traffic according to the updated provider weights.

The result does not by itself prove sustained overload, long-term capacity
prediction, request-specific cache affinity, or movement of existing affinity
bindings.

The measured distribution must agree with the effective weights published in
the serving overlay. If pressure reduces Pool A's effective weight to zero or
makes it ineligible, all new traffic moving to Pool B is consistent with the
published state. Otherwise, an all-to-Pool-B result proves a preference or
winner-takes-all transition, not weighted distribution. In that case the run
must not be presented as evidence for pressure-weighted selection.

Recovery is a separate measured transition:

```text
pressure stops
  -> EPP queue depth returns to its documented baseline or healthy range
  -> Grid publishes materially recovered weights
  -> overlay-sync accepts the revision
  -> Praxis serves the recovered revision
  -> new unbound traffic follows the recovered distribution
```

Merely making Pool A preferable relative to Pool B is a relative crossover,
not complete recovery. If the scenario claims recovery, it must define and
measure the healthy threshold. If the short-lived VCR queue drains before Grid
observes it, the run is nonqualifying; the harness may lengthen the synthetic
request duration, but it must not lower assertions, fabricate metrics, or
change production routing behavior to force a result.
