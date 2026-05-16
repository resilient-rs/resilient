# Circuit Breaker

The circuit breaker pattern prevents cascading failures by monitoring operation outcomes and short-circuiting requests when the failure rate exceeds a threshold. This gives downstream services time to recover.

## State Machine

```
      ┌─────────────────────────────────────────────┐
      │                                             │
      │  ┌──────────┐  failures >= threshold  ┌─────┴───────┐
      │  │  Closed   │ ──────────────────────▶ │    Open     │
      │  │ (normal)  │                         │ (rejecting) │
      │  └─────┬─────┘                         └──────┬──────┘
      │        │                                      │
      │        │  successes >= threshold              │  timeout elapsed
      │        │                                      │
      │        │              ┌──────────┐            │
      │        └──────────────│ HalfOpen │◀───────────┘
      │                      │ (probing) │
      │                      └─────┬─────┘
      │                            │
      │                     any failure
      └────────────────────────────┘
```

- **Closed** — Normal operation. Requests pass through. When failures exceed the threshold, the circuit trips to Open.
- **Open** — Requests are rejected immediately. After `open_timeout` elapses, transitions to HalfOpen.
- **HalfOpen** — Limited probe requests are allowed through. If they succeed, the circuit closes. If any fail, it reopens.
- **ForcedOpen** — Manual override. All requests rejected until `force_close()` or `reset()`.

## Defaults

| Parameter | Default | Description |
|---|---|---|
| `failure_threshold` | 5 | Consecutive failures to trip |
| `success_threshold` | 3 | Consecutive successes in HalfOpen to close |
| `open_timeout` | 30 s | Base time before transitioning to HalfOpen |
| `half_open_max_calls` | 3 | Max concurrent probe requests |
| `mode` | `CountBased` | Failure-detection strategy |

## Modes

| Mode | Trip Condition | Best For |
|---|---|---|
| `CountBased` | N consecutive failures | Simple, deterministic |
| `SlidingWindow` | Failure rate ≥ 50% in a rolling time window | Transient-tolerant, burst-aware |
| `Adaptive` | N consecutive failures + exponential back-off on `open_timeout` | Volatile downstream services |

### CountBased

The simplest strategy. The circuit trips after `failure_threshold` consecutive failures. A single success resets the counter.

```rust
use resilient::circuit_breaker::{BreakerPolicy, CircuitBreakerMode};
use std::time::Duration;

let cb = BreakerPolicy::default()
    .with_mode(CircuitBreakerMode::CountBased)
    .with_failure_threshold(5)
    .with_open_timeout(Duration::from_secs(30));
```

### SlidingWindow

Trips when the failure rate in a rolling time window exceeds 50%. Old calls age out after `window_size`. More tolerant of transient errors than CountBased.

```rust
let cb = BreakerPolicy::default()
    .with_mode(CircuitBreakerMode::SlidingWindow)
    .with_failure_threshold(10)         // minimum calls in window
    .with_window_size(Duration::from_secs(60));
```

### Adaptive

Like CountBased, but doubles the `open_timeout` each time the circuit trips, clamped between `min_open_timeout` and `max_open_timeout`.

```rust
let cb = BreakerPolicy::default()
    .with_mode(CircuitBreakerMode::Adaptive)
    .with_failure_threshold(5)
    .with_open_timeout(Duration::from_secs(30))
    .with_adaptive_bounds(
        Duration::from_secs(10),   // min
        Duration::from_secs(300),  // max
    );
```

## Manual Controls

```rust
// Force the circuit open (reject all requests)
cb.force_open();

// Force the circuit closed (reset to normal)
cb.force_close();

// Reset all counters and state
cb.reset();
```

## Request Admission Logic

| State | Decision |
|---|---|
| Closed | Always allow |
| Open | Allow if `open_timeout` has elapsed (transitions to HalfOpen) |
| HalfOpen | Allow if fewer than `half_open_max_calls` have been made in this period |
| ForcedOpen | Always reject |

## Direct Usage

The circuit breaker can be used directly via the `Policy` trait:

```rust
use resilient::circuit_breaker::BreakerPolicy;

let cb = BreakerPolicy::default().with_failure_threshold(3);

let result: Result<String, String> = cb
    .call(&mut || async { Err("fail".to_string()) })
    .await;
// Err(CircuitError::CircuitOpen { ... })
```

When used through a [Pipeline](../core-concepts/pipeline), the breaker runs in two phases: check-before-execute and record-outcome-after.
