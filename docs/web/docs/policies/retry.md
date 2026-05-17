# Retry Policy

The retry policy executes an async operation up to `N` times on failure, with configurable back-off and jitter between attempts.

## Defaults

| Parameter | Default | Description |
|---|---|---|
| `max_retries` | 3 | Maximum retry attempts (the first call + 3 retries = 4 total attempts) |
| `mode` | `Linear` | Jitter/back-off strategy |
| `min_delay` | 2 s | Base delay (floor for back-off) |
| `max_delay` | 6 s | Cap on any single delay |
| `max_duration` | 10 s | Hard cap on total elapsed time for all attempts |

## Usage

```rust
use resilient::retry::{RetryPolicy, RetryMode};
use std::time::Duration;

let policy = RetryPolicy::default()
    .with_max_retries(5)
    .with_mode(RetryMode::FullJitter)
    .with_min_delay(Duration::from_millis(500))
    .with_max_delay(Duration::from_secs(10))
    .with_max_duration(Duration::from_secs(30));
```

## Retry Modes

| Mode | Behavior | Best For |
|---|---|---|
| `Linear` | Pure linear back-off from `min_delay` to `max_delay` | Predictable, deterministic retry intervals |
| `FullJitter` | Random delay between `0` and the computed back-off | Breaking synchronization across many clients (thundering herd) |
| `EqualJitter` | Half the back-off + random jitter of the same range | Balance of predictability and randomization |
| `DecorrelatedJitter` | `min(cap, random(min, last × 3))` — adapts based on previous sleep | Naturally spreading retries over time |

## How it works

1. The operation is called.
2. On success → returns `Ok`.
3. On failure → computes a delay using the configured `RetryMode`, waits, and retries.
4. Panics are caught with `AssertUnwindSafe` and either resumed or retried.
5. Stops when the retry budget or `max_duration` is exhausted.

## Pipeline Integration

When combined with a [timeout](timeout), each retry attempt has its own deadline:

```rust
let pipeline = Pipeline::default()
    .with_retry(RetryPolicy::default().with_max_retries(3))
    .with_timeout(TimeoutPolicy::default().with_timeout(Duration::from_secs(5)));
// Each of the 4 attempts gets its own 5-second timeout.
```
