# Rate Limiter

The rate limiter uses a **token-bucket** algorithm to control how often an operation may execute. Tokens are replenished at a fixed rate up to a maximum capacity. Each call consumes one token.

## How it works

1. Tokens are added at a configurable `refill_rate` (one token per interval).
2. The bucket holds at most `max_tokens` tokens — excess tokens are discarded.
3. Each call to `try_consume` deducts one token.
4. If the bucket is empty, the call is rejected.

## Defaults

| Parameter | Default | Description |
|---|---|---|
| `max_tokens` | 10 | Maximum bucket capacity |
| `refill_rate` | 1 s | One token added per this duration |

## Usage

```rust
use resilient::RateLimiter;
use std::time::Duration;

let rl = RateLimiter::default()
    .with_max_tokens(100)
    .with_refill_rate(Duration::from_millis(200));
// Allows up to 100 burst requests, then 5 requests/second sustained.
```

## Thread Safety

All clones of a `RateLimiter` share the same underlying bucket state via `Arc<Mutex<...>>`. The struct is cheap to clone and safe to use from multiple threads concurrently.

```rust
let shared = RateLimiter::default().with_max_tokens(50);

// Spawn multiple tasks sharing the same rate limiter
let rl1 = shared.clone_inner();
let rl2 = shared.clone_inner();

tokio::spawn(async move {
    rl1.call(&mut || my_operation()).await
});

tokio::spawn(async move {
    rl2.call(&mut || my_operation()).await
});
```

## Direct Usage

```rust
use resilient::RateLimiter;
use resilient::policy::Policy;

let rl = RateLimiter::default().with_max_tokens(5);

for _ in 0..10 {
    let result: Result<(), String> = rl
        .call(&mut || async { Ok(()) })
        .await;
    match result {
        Ok(_) => println!("Request allowed"),
        Err(_) => println!("Rate limited"),
    }
}
```

## Inspecting State

```rust
let tokens_left = rl.available_tokens();
println!("Available tokens: {}", tokens_left);
```

This is a snapshot — the value may change immediately after the call returns due to concurrent access.
