# Error Handling

resilient defines three error types, one for each policy that can reject a request without calling the operation.

## Error Types

### `CircuitError`

Returned when the circuit breaker rejects a request.

```rust
pub enum CircuitError {
    CircuitOpen { last_failure_time: Option<Instant>, failure_count: usize },
    ForcedOpen,
    HalfOpenRejected { calls_remaining: usize },
}
```

### `TimeoutError`

Returned when the operation exceeds the configured duration.

```rust
pub enum TimeoutError {
    Elapsed { duration: Duration, name: Option<String> },
}
```

### `RateLimitError`

Returned when the token bucket is empty.

```rust
pub enum RateLimitError {
    RateLimited,
}
```

## Error Conversion

The pipeline requires your error type `E` to implement `From` for all three error types:

```rust
pub async fn run<F, Fut, T, E>(&self, f: F) -> Result<T, E>
where
    E: From<CircuitError> + From<TimeoutError> + From<RateLimitError>,
```

### Using `thiserror`

The recommended approach is to derive the conversions with `thiserror`:

```rust
use thiserror::Error;

#[derive(Error, Debug)]
pub enum MyError {
    #[error("Operation failed: {0}")]
    Operation(String),
    #[error("Circuit breaker error: {0}")]
    Circuit(#[from] resilient::circuit_breaker::CircuitError),
    #[error("Timeout: {0}")]
    Timeout(#[from] resilient::timeout::TimeoutError),
    #[error("Rate limited: {0}")]
    RateLimit(#[from] resilient::rate_limit::RateLimitError),
}
```

### Using `String` errors

All three error types implement `From<X> for String`, so you can use `String` as your error type:

```rust
let result: Result<String, String> = pipeline
    .run(&mut || async { Ok::<_, String>("done") })
    .await;
```

This works because `String: From<CircuitError> + From<TimeoutError> + From<RateLimitError>`.
