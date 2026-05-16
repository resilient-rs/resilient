# resilient

**resilient** is a composable async resilience toolkit for Rust. It provides a set of modular, composable policies that make it easy to add retry, timeout, circuit breaking, and rate limiting to any async operation.

## Why resilient?

Building reliable distributed systems means handling failures gracefully. resilient gives you:

- **Composability** — Combine policies through a single `Pipeline` builder. Add or remove policies without changing your business logic.
- **Async-native** — Built for `tokio` with first-class async support. The `Policy` trait integrates naturally with `async fn` and `Future`.
- **Configurable** — Every policy exposes builder methods for fine-grained control over thresholds, timing, and behavior.
- **Thread-safe** — All policies use `Arc`-based interior mutability, making them cheap to clone and safe to share across tasks.

## Policies

| Policy | Description |
|---|---|
| [Retry](policies/retry) | Retry failed operations with configurable back-off and jitter strategies |
| [Timeout](policies/timeout) | Enforce a deadline on async operations with lifecycle hooks |
| [Circuit Breaker](policies/circuit-breaker) | Prevent cascading failures by monitoring error rates and short-circuiting |
| [Rate Limiter](policies/rate-limiter) | Control request rate with a token-bucket algorithm |

## Quick Example

```rust
use resilient::pipeline::Pipeline;
use resilient::retry_policy::RetryPolicy;
use resilient::timeout::TimeoutPolicy;
use resilient::circuit_breaker::BreakerPolicy;
use resilient::rate_limit::RateLimiter;
use std::time::Duration;

let pipeline = Pipeline::default()
    .with_retry(RetryPolicy::default().with_max_retries(3))
    .with_timeout(TimeoutPolicy::default().with_timeout(Duration::from_secs(5)))
    .with_circuit_breaker(BreakerPolicy::default())
    .with_rate_limiter(RateLimiter::default().with_max_tokens(100));

let result = pipeline
    .run(&mut || async { Ok::<_, String>("hello resilient!") })
    .await;
```

## Project Status

resilient is in early development. The API is stable enough for experimentation but may evolve based on feedback.
