# Repository Guidelines

## Project Structure & Module Organization

This repository is a Rust async resilience library. Core code lives in `src/`.
The public API is exported from `src/lib.rs`, shared behavior is in
`src/policy.rs`, and composition is implemented in `src/pipeline.rs`.
Individual policies are grouped by module: `src/retry_policy/`, `src/timeout/`,
`src/circuit_breaker/`, `src/rate_limit/`, and `src/bulkhead/`.

Runnable examples live in `examples/` and should show one policy or a small
composition. The documentation site is a Docusaurus app in
`docs/web/`; its Markdown pages are under `docs/web/docs/`, and styling lives in
`docs/web/src/css/`. Generated outputs such as `target/`, `docs/web/build/`, and
`docs/web/node_modules/` should not be edited manually.

## Build, Test, and Development Commands

- `cargo build`: compile the Rust crate.
- `cargo test`: run unit tests and doc tests.
- `cargo fmt --all`: format all Rust code.
- `cargo clippy -- -D warnings`: lint with warnings treated as errors.
- `cargo run --example retry`: run an example; replace `retry` with another file
  stem from `examples/`.
- `cd docs/web && bun install`: install docs dependencies.
- `cd docs/web && bun run start`: run the local Docusaurus docs server.
- `cd docs/web && bun run build`: build the docs site for production.

## Coding Style & Naming Conventions

Use standard Rust formatting via `rustfmt`; keep indentation and imports aligned
with `cargo fmt`. Prefer focused modules and builder-style methods
named `with_*` for configuration. Rust types and traits use `PascalCase`, modules
and functions use `snake_case`, and error/result wrapper types should be explicit,
for example `TimeoutError` or `BreakerResult`.

## Testing Guidelines

Tests use Rust's built-in test framework with `tokio::test` for async behavior.
Place unit tests near the implementation they validate, as in
`src/bulkhead/bulkhead_policy.rs`. Name tests after observable behavior, such as
`releases_permit_after_completion`. Run `cargo test` before opening a PR; run
`cargo clippy -- -D warnings` when touching shared policy logic.

## Commit & Pull Request Guidelines

Recent commits use short Conventional Commit-style prefixes such as `chore:`,
`docs:`, and `fix:`. Keep messages imperative and scoped, for example
`fix: handle timeout errors in pipeline`.

Pull requests should include a brief description, tests run, and any user-facing
API or documentation impact. Link related issues when applicable.
For docs UI changes, include screenshots or a short note confirming
`bun run build` passed.

## Security & Configuration Tips

Do not commit generated artifacts, secrets, local environment files, or registry
credentials. Keep dependency changes intentional and reflected in `Cargo.lock`
or `docs/web/bun.lock` as appropriate.
