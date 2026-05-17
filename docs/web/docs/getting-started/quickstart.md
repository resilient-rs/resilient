# Quickstart

This guide walks through building a resilient async operation step by step.

## Setup

Create a new Rust binary project and add the dependencies:

```bash
cargo new resilient-example
cd resilient-example
cargo add resilient
cargo add tokio --features time,macros,rt-multi-thread
```

## Basic Pipeline

Create a `Pipeline` with all four policies:

```rust
use resilient::pipeline::Pipeline;
use resilient::RetryPolicy;
use resilient::TimeoutPolicy;
use resilient::BreakerPolicy;
use resilient::RateLimiter;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pipeline = Pipeline::default()
        .with_retry(
            RetryPolicy::default()
                .with_max_retries(3)
        )
        .with_timeout(
            TimeoutPolicy::default()
                .with_timeout(Duration::from_secs(5))
        )
        .with_circuit_breaker(
            BreakerPolicy::default()
        )
        .with_rate_limiter(
            RateLimiter::default()
                .with_max_tokens(100)
        );

    let result = pipeline
        .run(&mut || async {
            Ok::<_, String>("Success!")
        })
        .await;

    println!("Result: {:?}", result);
    Ok(())
}
```

## Operation with Retries

The pipeline automatically retries failed operations:

```rust
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use resilient::pipeline::Pipeline;
use resilient::RetryPolicy;

let pipeline = Pipeline::default()
    .with_retry(RetryPolicy::default().with_max_retries(3));

let attempts = Arc::new(AtomicU32::new(0));

let result = pipeline
    .run(&mut || {
        let attempts = attempts.clone();
        async move {
            let count = attempts.fetch_add(1, Ordering::Relaxed);
            if count < 2 {
                Err("Temporary error".to_string())
            } else {
                Ok("Recovered!".to_string())
            }
        }
    })
    .await;

println!("{:?}", result); // Ok("Recovered!")
```

## What's Next?

- Learn how the [Pipeline](../core-concepts/pipeline) orchestrates policies
- Dive into each [policy](../policies/retry) for detailed configuration
- Explore [advanced patterns](../advanced/fallback) like fallbacks and custom policies
