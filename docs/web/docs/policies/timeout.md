# Timeout Policy

The timeout policy enforces a deadline on async operations. If the operation does not complete within the configured duration, a `TimeoutError::Elapsed` is returned.

## Defaults

| Parameter | Default | Description |
|---|---|---|
| `duration` | 30 s | Maximum time allowed for the operation |
| `cancel` | `true` | Whether to cancel the future on timeout via `tokio::time::timeout` |
| `name` | `None` | Optional label shown in timeout error messages |

## Usage

```rust
use resilient::timeout::TimeoutPolicy;
use std::time::Duration;

let policy = TimeoutPolicy::default()
    .with_timeout(Duration::from_secs(5));
```

### Builder

A separate `Builder` provides additional configuration options:

```rust
use resilient::timeout::Builder;

let policy = Builder::new()
    .with_timeout_secs(10)
    .with_name("database-query")
    .with_cancel(true)
    .build();
```

## Lifecycle Hooks

The timeout policy supports async callbacks that fire on specific outcomes:

```rust
let policy = Builder::new()
    .with_timeout_secs(5)
    .with_on_timeout(|| async {
        eprintln!("Query timed out!");
    })
    .with_on_success(|| async {
        metrics::record_success();
    })
    .with_on_failure(|| async {
        metrics::record_failure();
    })
    .build();
```

### Hook execution

| Hook | When it fires |
|---|---|
| `on_timeout` | After the deadline is exceeded, before the error is returned |
| `on_success` | After the inner future returns `Ok` |
| `on_failure` | After the inner future returns `Err` (non-timeout) |

## Cancellation

When `cancel = true` (default), the policy uses `tokio::time::timeout` which drops the inner future when the deadline passes. When `cancel = false`, the operation runs to completion without interruption — useful when you want to observe timing but not abort long-running tasks.

```rust
let policy = Builder::new()
    .with_timeout_secs(5)
    .with_cancel(false)  // still fires on_timeout hook but lets operation finish
    .build();
```

## Convenience Setters

```rust
policy.with_timeout(Duration::from_secs(5));
policy.with_timeout_secs(10);
policy.with_timeout_millis(500);
policy.with_timeout_minutes(2);
policy.with_timeout_hours(1);
```
