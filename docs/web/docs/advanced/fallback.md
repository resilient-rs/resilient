# Fallback Pipeline

The fallback pipeline pairs a primary `Pipeline` with a fallback closure that runs when the primary produces any error.

## Usage

```rust
use resilient::pipeline::Pipeline;
use resilient::retry_policy::RetryPolicy;

let pipeline = Pipeline::default()
    .with_retry(RetryPolicy::default().with_max_retries(3));

let result = pipeline
    .or_else(|| async {
        Ok::<_, String>("cached response")
    })
    .run(&mut || async {
        Err::<String, _>("service unavailable")
    })
    .await;

assert_eq!(result.unwrap(), "cached response");
```

## Important

The fallback runs **raw** — it does not re-apply any resilience policies. This is intentional:

- **Fallbacks should be fast and reliable** — they typically return a cached value, a default, or an alternative code path.
- **Re-applying policies could cause infinite loops** — a fallback that fails and triggers retry/circuit-breaker would defeat the purpose.
