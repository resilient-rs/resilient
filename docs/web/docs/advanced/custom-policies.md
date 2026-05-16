# Custom Policies

You can implement the `Policy` trait to create your own resilience strategies.

## Example: Logging Policy

```rust
use std::future::Future;
use resilient::policy::Policy;

struct LoggingPolicy;

impl<T, E> Policy<T, E> for LoggingPolicy
where
    E: std::fmt::Debug,
{
    fn call<F, Fut>(&self, f: &mut F) -> impl Future<Output = Result<T, E>> + Send
    where
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<T, E>> + Send,
        T: Send,
        E: Send,
    {
        async move {
            println!("Operation starting...");
            let result = f().await;
            match &result {
                Ok(_) => println!("Operation succeeded"),
                Err(e) => println!("Operation failed: {:?}", e),
            }
            result
        }
    }
}
```

## Using Custom Policies

Custom policies implement `Policy` so they can be used anywhere a policy is expected. However, the built-in `Pipeline` only accepts the four predefined policy types. For custom policies, use the `Policy` trait directly:

```rust
let result = LoggingPolicy
    .call(&mut || my_operation())
    .await;
```

Or wrap them inside a pipeline-compatible wrapper if you need the full pipeline composition.

## Example: Circuit Breaker with Custom Logic

You could implement a circuit breaker with different rules:

```rust
struct TimeWindowBreaker {
    window: Duration,
    max_failures: usize,
    // ... shared state via Arc<Mutex<...>>
}

impl<T, E> Policy<T, E> for TimeWindowBreaker
where
    E: From<MyBreakerError>,
{
    fn call<F, Fut>(&self, f: &mut F) -> impl Future<Output = Result<T, E>> + Send { ... }
}
```

The key requirement is that your error type `E` must implement `From<YourPolicyError>` so it can be converted through the pipeline's error channel.
