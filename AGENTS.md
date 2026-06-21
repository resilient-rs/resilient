# Repository Guidelines

## Project Structure

Core library is `src/`. Entrypoint: `src/lib.rs` re-exports all public types.
Shared abstraction: `src/policy.rs` defines the `Policy<T, E>` trait (method `call`).
Composition: `src/pipeline.rs` implements `Pipeline` (builder + fixed-order execution).

Each policy is a module with `mod.rs`, error types, and main implementation file:

| Module              | Impl file                   | Error types                          |
|---------------------|-----------------------------|--------------------------------------|
| `retry_policy`      | `retry_policy/retry.rs`     | —                                    |
| `timeout`           | `timeout/timeout_policy.rs` | `TimeoutError`                       |
| `circuit_breaker`   | `circuit_breaker/breaker.rs`| `CircuitError`, `BreakerResult`      |
| `rate_limit`        | `rate_limit/rate_limiter.rs`| `RateLimitError`, `RateLimitResult`  |
| `bulkhead`          | `bulkhead/bulkhead_policy.rs`| `BulkheadError`                     |

Runnable examples in `examples/` (one per policy + `basic_usage.rs` for full pipeline).

Doc site: Docusaurus app at `docs/web/`. Source pages in `docs/web/docs/`.

## Policies at a Glance

Every policy supports three invocation patterns:
1. **Standalone** — call `policy.run(|| op()).await` (return type varies per policy).
2. **Via `Policy` trait** — `policy.call(&mut || op()).await` (returns `Result<T, E>`).
3. **Composed** — attach to `Pipeline` via `with_*` builder methods.

### RetryPolicy (`RetryPolicy`)

Builder: `with_max_retries` (default 3), `with_mode`, `with_min_delay` (2s), `with_max_delay` (6s),
`with_max_duration` (10s), `retry_if(|e: &MyError| …)`.

`RetryMode`: `Linear`, `FullJitter`, `EqualJitter`, `DecorrelatedJitter`.

`run()` returns `Result<T, E>` directly (no wrapper error type).

Panics are caught by `AssertUnwindSafe` and retried (re-raised if budget exhausted).
Timeouts are NOT retried when inside a Pipeline (the pipeline sets an atomic flag).

### TimeoutPolicy (`TimeoutPolicy`, plus separate `Builder`)

Builder: `with_timeout` / `with_timeout_millis/secs/minutes/hours`, `with_cancel` (default true),
`with_name` (appears in error message), `with_on_timeout/success/failure` (async lifecycle hooks).

`TimeoutError::Elapsed { duration, name }` and `TimeoutError::Returning(Box<dyn Error>)`.

Two standalone run methods:
- `policy.run(|| op()).await` — requires `E: From<TimeoutError>`, returns `Result<T, E>`.
- `policy.run_with_timeout(|| op()).await` — returns `Result<T, TimeoutError>`.

**When used in `Pipeline`:** `Pipeline::run()` requires `E: From<TimeoutError>`.
Timeout errors are **not** retried — they propagate immediately.

### BreakerPolicy (`BreakerPolicy`)

Builder: `with_failure_threshold` (default 5), `with_success_threshold` (3),
`with_open_timeout` (30s), `with_half_open_max_calls` (3), `with_mode`, `with_window_size` (60s),
`with_adaptive_bounds` (min 10s, max 300s).

`CircuitBreakerMode`: `CountBased` (consecutive failures), `SlidingWindow` (≥50% failure rate in
rolling window), `Adaptive` (CountBased + exponential back-off on open timeout).

State machine: `Closed → Open → HalfOpen → Closed`. Manual overrides: `force_open()`, `force_close()`,
`reset()`. Shared state behind `Arc<Atomic*>` — clones share counters.

Standalone:
- `run()` returns `Result<T, BreakerResult<E>>`.
- `run_raw()` returns `Result<T, E>` (rejects by calling op anyway).

### RateLimiter (`RateLimiter`)

Token-bucket algorithm. Builder: `with_max_tokens` (default 10), `with_refill_rate` (1s).

`run()` returns `Result<T, RateLimitResult<E>>`.
`try_consume(n)` returns `bool`. `available_tokens()` snapshot.

Shared state behind `Arc<Mutex<…>>`. Clones share bucket.

### Bulkhead (`Bulkhead`)

Built on `tokio::sync::Semaphore`. Builder: `with_max_concurrent` (default 10, minimum 1).

`run()` returns `Result<T, E>` directly (no wrapper).
`try_acquire()` returns `Option<SemaphorePermit>`. `available_permits()`, `in_flight()`.

## Pipeline

Build via `Pipeline::new()` or `Pipeline::default()`, then chain:
`with_retry`, `with_timeout`, `with_circuit_breaker`, `with_rate_limiter`, `with_bulkhead`.

Fixed execution order:
1. **Circuit breaker check** — reject if open/forced-open.
2. **Bulkhead acquire** — blocks if at capacity.
3. **Operation** — optionally wrapped in retry + timeout (each retry has its own timeout).
   Rate limiting is applied per-attempt inside this step.
4. **Circuit breaker feedback** — success/failure recorded.

Attach fallback via `.or_else(|| async { … })`. The fallback runs **raw** (no policies re-applied).

**Key type constraint:** `Pipeline::run()` requires `E: From<TimeoutError>`. Use
`Box<dyn std::error::Error + Send + Sync>` or a custom enum that derives `From<TimeoutError>`.

`Pipeline` is `Clone + Send + Sync`. Clones share circuit breaker and rate limiter state.

## Build, Test, Lint

```sh
cargo build
cargo test                         # unit + doc tests
cargo fmt --all                    # format (CI uses --check)
cargo clippy -- -D warnings        # lint
cargo run --example <name>         # run an example
```

CI order (`.github/workflows/main.yml`): `cargo fmt --all -- --check` → `cargo clippy -- -D warnings` → `cargo test`.

## Docs Site

```sh
cd docs/web && bun install
cd docs/web && bun run start       # dev server
cd docs/web && bun run build       # production build
cd docs/web && bun run typecheck   # tsc
cd docs/web && bun run lint        # eslint src/
```

Generated artifacts: `docs/web/build/`, `docs/web/node_modules/`, `target/` — do not edit manually.

## Feature Flags

| Flag             | Description                                   |
|------------------|-----------------------------------------------|
| `async-closure`  | Enables async closure syntax (nightly only)   |

## Coding Conventions

- `with_*` builder methods return `Self` (consuming, not `&mut self`).
- Error/wrapper types follow the pattern `XxxResult<E>`, e.g., `BreakerResult<E>`.
- Tests: `tokio::test`, co-located near the implementation they test.
- Commits: Conventional Commit style, e.g., `feat: add adaptive circuit breaker mode`.

## Tests

Run `cargo test` before opening a PR. Tests use `tokio::test` and are inlined in impl files
under `#[cfg(test)] mod tests`. Test functions are named after observable behavior:
`releases_permit_after_completion`, `permanent_error_fails_without_extra_attempts`.
