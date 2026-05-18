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
tokio = { version = "1", features = ["full"] }
```

---

## Policies

| Policy | What it does |
|---|---|
| **Retry** | Re-run a failed operation with configurable backoff |
| **Timeout** | Cancel if the operation takes too long |
| **Circuit Breaker** | Stop calling a broken dependency until it recovers |
| **Rate Limiter** | Cap how many calls per second reach a downstream |
| **Bulkhead** | Limit concurrent in-flight calls |
| **Fallback** | Return a default value when everything else fails |

---

## Usage

Each policy wraps an async closure:

```rust
// Retry up to 3 times with exponential backoff
let result = RetryPolicy::builder()
    .max_attempts(3)
    .backoff(Backoff::exponential(Duration::from_millis(100)))
    .build()
    .execute(|| async { call_service().await })
    .await?;
```

```rust
// Fail fast if the call takes more than 2 seconds
let result = TimeoutPolicy::new(Duration::from_secs(2))
    .execute(|| async { slow_query().await })
    .await?;
```

```rust
// Open the circuit after 5 consecutive failures
let result = CircuitBreakerPolicy::new(
    CircuitBreakerConfig::builder()
        .failure_threshold(5)
        .probe_timeout(Duration::from_secs(30))
        .build()
)
.execute(|| async { call_payment_api().await })
.await?;
```

### Composing policies

Policies compose by wrapping one inside another:

```rust
let result = FallbackPolicy::new(|_| async { Ok(Response::default()) })
    .execute(|| {
        retry.execute(|| {
            timeout.execute(|| {
                breaker.execute(|| async { call_upstream().await })
            })
        })
    })
    .await;
```

The recommended order from outermost to innermost is:
**Fallback → Retry → Timeout → Circuit Breaker → Rate Limiter → Bulkhead**

---

## Feature Flags

| Flag | Description |
|---|---|
| `async-closure` | Enables async closure syntax (nightly only). On stable, use `|| async { ... }`. |

---

## License

MIT — see [LICENSE](LICENSE).
