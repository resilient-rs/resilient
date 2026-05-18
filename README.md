# resilient

A composable async resilience toolkit for Rust — retry, timeout, circuit breaker, rate limiting, bulkheads, and fallbacks.

[![Crates.io](https://img.shields.io/crates/v/resilient.svg)](https://crates.io/crates/resilient)
[![docs.rs](https://docs.rs/resilient/badge.svg)](https://docs.rs/resilient)
[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

---

## Installation

```toml
[dependencies]
resilient = "0.1"
tokio = { version = "1", features = ["time", "macros", "rt-multi-thread"] }
```

---

## Policies

| Policy | What it does |
|--------|--------------|
| **Retry** | Re-run a failed operation with configurable backoff |
| **Timeout** | Cancel if the operation takes too long |
| **Circuit Breaker** | Stop calling a broken dependency until it recovers |
| **Rate Limiter** | Cap how many calls per second reach a downstream |
| **Bulkhead** | Limit concurrent in-flight calls |
| **Fallback** | Return a default value when everything else fails |

---

## Quick Start

```rust
use resilient::pipeline::Pipeline;
use resilient::{RetryPolicy, TimeoutPolicy, BreakerPolicy, RateLimiter};

let pipeline = Pipeline::new()
    .with_retry(RetryPolicy::default().with_max_retries(3))
    .with_timeout(TimeoutPolicy::default().with_timeout_secs(5))
    .with_circuit_breaker(BreakerPolicy::default())
    .with_rate_limiter(RateLimiter::default());

let result: Result<String, Box<dyn std::error::Error + Send + Sync>> = pipeline
    .run(|| async {
        // Your async operation here
        Ok("success".to_string())
    })
    .await;
```

---

## Individual Policy Usage

Each policy can be used standalone:

### Retry

```rust
use resilient::RetryPolicy;

let policy = RetryPolicy::default().with_max_retries(3);
let result: Result<String, String> = policy
    .run(|| async { Ok("success".to_string()) })
    .await;
```

### Timeout

```rust
use resilient::TimeoutPolicy;
use std::time::Duration;

let policy = TimeoutPolicy::default().with_timeout(Duration::from_secs(2));
let result: Result<String, Box<dyn std::error::Error + Send + Sync>> = policy
    .run(|| async { Ok("success".to_string()) })
    .await;
```

### Circuit Breaker

```rust
use resilient::{BreakerPolicy, BreakerResult};

let policy = BreakerPolicy::default().with_failure_threshold(5);
let result: Result<String, BreakerResult<String>> = policy
    .run(|| async { Ok("success".to_string()) })
    .await;
```

### Bulkhead

```rust
use resilient::Bulkhead;

let bulkhead = Bulkhead::default().with_max_concurrent(3);
let result: Result<String, String> = bulkhead
    .run(|| async { Ok("success".to_string()) })
    .await;
```

### Rate Limiter

```rust
use resilient::RateLimiter;

let limiter = RateLimiter::default().with_capacity(10).with_refill(5);
let result: Result<String, String> = limiter
    .run(|| async { Ok("success".to_string()) })
    .await;
```

---

## Using Pipeline (Recommended)

The `Pipeline` composes multiple policies together in the correct order:

```rust
use resilient::pipeline::Pipeline;
use resilient::timeout::Builder as TimeoutBuilder;
use resilient::{RetryPolicy, BreakerPolicy, RateLimiter, Bulkhead};

let pipeline = Pipeline::new()
    .with_retry(RetryPolicy::default())
    .with_timeout(TimeoutBuilder::new().with_timeout_secs(5).build())
    .with_circuit_breaker(BreakerPolicy::default())
    .with_rate_limiter(RateLimiter::default())
    .with_bulkhead(Bulkhead::default());

let result = pipeline
    .run(|| async { Ok("result".to_string()) })
    .await;
```

**Note**: When using timeout with `Pipeline`, your error type must implement `From<resilient::timeout::TimeoutError>`. Use `Box<dyn std::error::Error + Send + Sync>` or a custom error enum.

---

## Fallback

Provide a fallback when the pipeline fails:

```rust
use resilient::pipeline::Pipeline;

let pipeline = Pipeline::default()
    .with_retry(RetryPolicy::default())
    .or_else(|| async { Ok("fallback value".to_string()) });

let result = pipeline
    .run(|| async { Err::<String, _>("error") })
    .await;

assert!(result.is_ok());
```

---

## Feature Flags

| Flag | Description |
|------|--------------|
| `async-closure` | Enables async closure syntax (nightly only) |

---

## License

MIT — see [LICENSE](LICENSE).