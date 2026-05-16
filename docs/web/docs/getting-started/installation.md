# Installation

Add `resilient` to your `Cargo.toml`:

```toml
[dependencies]
resilient = "0.1.0"
tokio = { version = "1", features = ["time", "macros", "rt-multi-thread"] }
```

Or use `cargo add`:

```bash
cargo add resilient
```

## Minimum Supported Rust Version (MSRV)

resilient requires Rust **1.75** or later (edition 2021).

## Feature flags

resilient currently has no feature flags — all policies are included by default.

## Dependencies

resilient itself depends on:

- `tokio` — async runtime (timeouts, sleep between retries)
- `futures-util` — `FutureExt::catch_unwind` for panic-safe retries
- `fastrand` — fast, non-cryptographic randomness for jitter
- `thiserror` — ergonomic error types

Your application must provide a `tokio` runtime (typically via `#[tokio::main]`).
