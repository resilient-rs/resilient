# Pipeline

The `Pipeline` combines multiple resilience policies into a single, ordered execution chain. Each policy is optional — only the ones you configure are applied.

## Builder Pattern

Construct a pipeline using the builder methods:

```rust
use resilient::pipeline::Pipeline;
use resilient::retry_policy::RetryPolicy;

let pipeline = Pipeline::new()
    .with_retry(RetryPolicy::default())
    .with_timeout(/* ... */)
    .with_circuit_breaker(/* ... */)
    .with_rate_limiter(/* ... */);
```

`Pipeline::default()` is equivalent to `Pipeline::new()`.

## Execution Order

When `run` is called, policies apply in this fixed order:

```
1. Rate Limiter ──► check token bucket
       │
       ▼
2. Circuit Breaker ──► check if circuit is open
       │
       ▼
3. Operation ┌── Retry (wraps the operation)
             ├── Timeout (wraps each attempt when combined with retry)
             └── Neither (operation runs once)
       │
       ▼
4. Circuit Breaker ──► record outcome (success/failure feedback)
```

### Key details

- **Rate limiter** runs first — no point checking other policies if we're already rate-limited.
- **Circuit breaker** checks state before running; outcome is recorded after.
- **Retry + Timeout together**: each retry attempt gets its own timeout.
- **Retry only**: operation is retried on failure without a deadline.
- **Timeout only**: single deadline for the operation, no retries.

## Running an Operation

```rust
let result = pipeline
    .run(&mut || async {
        my_fallible_operation().await
    })
    .await;
```

## Fallback

Use `or_else` to attach a fallback that runs when the pipeline fails:

```rust
let result = Pipeline::default()
    .or_else(|| async { Ok::<_, String>("cached response") })
    .run(&mut || async { Err::<String, _>("error") })
    .await;
```

The fallback runs **raw** — it does not re-apply any resilience policies. See [Fallback](../advanced/fallback) for details.

## Thread Safety

`Pipeline` derives `Clone` and is `Send + Sync`. The underlying policies use `Arc`-based interior mutability, so cloned pipelines share state. You can safely use the same pipeline from multiple tasks concurrently.
