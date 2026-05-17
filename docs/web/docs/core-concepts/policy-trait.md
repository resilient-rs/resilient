# Policy Trait

The `Policy` trait is the core abstraction in resilient. Every resilience strategy implements this trait, allowing them to be composed interchangeably.

## Definition

```rust
pub trait Policy<T, E> {
    fn call<F, Fut>(&self, f: &mut F) -> impl Future<Output = Result<T, E>> + Send
    where
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<T, E>> + Send,
        T: Send,
        E: Send;
}
```

- **`T`** — The success type of the wrapped operation.
- **`E`** — The error type of the wrapped operation.
- **`f`** — A mutable closure that produces the async operation. `FnMut` is required because policies like retry may call `f` multiple times.

## Built-in Implementations

resilient provides four built-in policy implementations:

| Type | Behavior |
|---|---|
| `RetryPolicy` | Calls the operation up to N times on failure |
| `TimeoutPolicy` | Enforces a deadline on the operation |
| `BreakerPolicy` | Rejects calls when failure rate is too high |
| `RateLimiter` | Limits call frequency via token bucket |

## Usage

Policies can be used directly:

```rust
use resilient::RetryPolicy;

let policy = RetryPolicy::default().with_max_retries(3);
let result = policy
    .call(&mut || async { Ok::<_, String>("done") })
    .await;
```

Or combined through a [Pipeline](pipeline) which handles ordering and feedback loops automatically.
