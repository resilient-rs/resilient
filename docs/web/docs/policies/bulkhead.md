---
sidebar_position: 4
---

# Bulkhead

The bulkhead pattern isolates failures and prevents resource exhaustion by limiting how many operations can run concurrently. When the limit is reached, new calls are rejected immediately rather than queuing indefinitely.

## How it works

The bulkhead uses a **semaphore** to track in-flight operations:

1. Each call acquires one permit before executing.
2. If permits are available, the operation runs.
3. When all permits are taken, new calls are rejected immediately.
4. When the operation completes, the permit is released.

This prevents cascading failures by ensuring that a misbehaving downstream doesn't consume all your available resources.

## Defaults

| Parameter | Default | Description |
|-----------|---------|--------------|
| `max_concurrent` | 10 | Maximum number of concurrent in-flight operations |

## Usage

```rust
use resilient::Bulkhead;

let bulkhead = Bulkhead::default().with_max_concurrent(4);

// Only 4 operations can run at once
let result: Result<String, String> = bulkhead
    .run(|| async { Ok("completed".to_string()) })
    .await;
```

## Direct Usage

```rust
use resilient::Bulkhead;
use resilient::policy::Policy;

let bulkhead = Bulkhead::default().with_max_concurrent(2);

// First two calls succeed, third is rejected
for i in 0..3 {
    let result = bulkhead
        .call(&mut || async { Ok::<_, String>("done") })
        .await;
    match result {
        Ok(_) => println!("Call {}: executed", i),
        Err(e) => println!("Call {}: rejected - {}", i, e),
    }
}
```

## Thread Safety

All clones of a `Bulkhead` share the same underlying semaphore state via `Arc`. The struct is cheap to clone and safe to use from multiple async tasks concurrently.

```rust
let shared = Bulkhead::default().with_max_concurrent(5);

// Clone and use from different tasks
let bh1 = shared.clone();
let bh2 = shared.clone();

tokio::spawn(async move {
    bh1.run(|| async { Ok(()) }).await;
});

tokio::spawn(async move {
    bh2.run(|| async { Ok(()) }).await;
});
```

## Pipeline Integration

The bulkhead is typically the innermost policy in the pipeline, closest to the actual call:

```rust
use resilient::pipeline::Pipeline;
use resilient::{RetryPolicy, TimeoutPolicy, BreakerPolicy, Bulkhead};

let pipeline = Pipeline::new()
    .with_retry(RetryPolicy::default())
    .with_timeout(TimeoutPolicy::default().with_timeout_secs(5))
    .with_circuit_breaker(BreakerPolicy::default())
    .with_bulkhead(Bulkhead::default().with_max_concurrent(10));
    // Bulkhead should be last - it limits actual execution

let result = pipeline
    .run(|| async { Ok("result".to_string()) })
    .await;
```

## Inspecting State

```rust
let available = bulkhead.available_permits();
println!("Available permits: {}", available);
```

This is a snapshot — the value may change immediately after the call returns due to concurrent access.