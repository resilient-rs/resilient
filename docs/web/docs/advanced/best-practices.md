# Best Practices

## Policy Order

The pipeline applies policies in a fixed order. Understanding why helps you reason about behavior:

1. **Rate Limiter first** — No point checking the circuit breaker or running the operation if we're already rate-limited.
2. **Circuit Breaker check** — Quick rejection if the downstream is known to be unhealthy.
3. **Operation execution** — Retry wraps timeout, timeout wraps the operation.
4. **Circuit Breaker feedback** — Record the outcome so the breaker can update its state.

This ordering means a rate-limited or circuit-open request is rejected in microseconds without ever touching the network.

## Sharing State Across Tasks

All policies use `Arc`-based interior mutability. Clone the pipeline or individual policies to share them across tasks:

```rust
let pipeline = Pipeline::default()
    .with_circuit_breaker(BreakerPolicy::default());

let p1 = pipeline.clone();
let p2 = pipeline.clone();

tokio::spawn(async move { p1.run(op1).await });
tokio::spawn(async move { p2.run(op2).await });
// Both tasks share the same circuit breaker state.
```

## Choosing a Circuit Breaker Mode

| Scenario | Recommended Mode |
|---|---|
| Simple, deterministic failure detection | `CountBased` |
| Tolerating occasional blips | `SlidingWindow` |
| Unstable downstream that needs cooldown | `Adaptive` |
| You don't know which to pick | `CountBased` (start simple) |

## Choosing a Retry Mode

| Scenario | Recommended Mode |
|---|---|
| Predictable delays, testing | `Linear` |
| Many clients, same downstream | `FullJitter` |
| Balance of spread and latency | `EqualJitter` |
| Variable network conditions | `DecorrelatedJitter` |

## Anti-Patterns

### Infinite retries
Always set a finite `max_retries` and a reasonable `max_duration`. The defaults (3 retries, 10s max) are a good starting point.

### Retrying on every error
Not all errors are retryable. Consider mapping your errors so that only transient failures trigger retries (e.g., network timeouts vs. 400 Bad Request).

### Large rate limiter bursts
A large `max_tokens` allows a burst of traffic that may overwhelm downstream services. Set `max_tokens` to a value your downstream can handle in a single burst.

### Disabling timeout cancellation
Setting `cancel = false` means the operation continues running even after the deadline. Use this only when you need to observe timing without aborting.
