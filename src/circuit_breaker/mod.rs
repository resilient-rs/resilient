//! # Circuit Breaker
//!
//! The circuit breaker pattern prevents cascading failures by monitoring
//! for failures and short-circuiting requests when the failure rate
//! exceeds a threshold. This gives downstream services time to recover.
//!
//! ## State Machine
//!
//! ```text
//!      ┌─────────────────────────────────────────────┐
//!      │                                             │
//!      │  ┌──────────┐  failures >= threshold  ┌─────┴───────┐
//!      │  │  Closed   │ ──────────────────────▶ │    Open     │
//!      │  │ (normal)  │                         │ (rejecting) │
//!      │  └─────┬─────┘                         └──────┬──────┘
//!      │        │                                      │
//!      │        │  successes >= threshold              │  timeout elapsed
//!      │        │                                      │
//!      │        │              ┌──────────┐            │
//!      │        └──────────────│ HalfOpen │◀───────────┘
//!      │                      │ (probing) │
//!      │                      └─────┬─────┘
//!      │                            │
//!      │                     any failure
//!      └────────────────────────────┘
//! ```
//!
//! In **Closed** state, requests pass through normally. When failures exceed
//! the configured threshold, the circuit **trips** to Open.
//!
//! In **Open** state, requests are rejected immediately without calling the
//! operation. After `open_timeout` elapses, the circuit transitions to HalfOpen.
//!
//! In **HalfOpen** state, a limited number of probe requests are allowed through.
//! If they succeed, the circuit closes (resuming normal operation). If any fail,
//! the circuit reopens.
//!
//! The **ForcedOpen** state is a manual override — all requests are rejected
//! until `force_close()` or `reset()` is called.
//!
//! ## Modes
//!
//! The circuit breaker supports three failure-detection strategies:
//! - [`CountBased`](CircuitBreakerMode::CountBased) — trips on N consecutive failures
//! - [`SlidingWindow`](CircuitBreakerMode::SlidingWindow) — trips when failure rate ≥ 50% in a rolling time window
//! - [`Adaptive`](CircuitBreakerMode::Adaptive) — like CountBased but with exponential back-off on the open timeout
//!
//! ## Thread Safety
//!
//! All mutable state is stored on the heap via `Arc<Atomic*>` and
//! `Arc<Mutex<...>>`, making `BreakerPolicy` cheap to clone and safe to
//! share across threads. The same `BreakerPolicy` instance can be used
//! from multiple tasks concurrently.

pub mod breaker;
pub mod errors;

pub use breaker::{BreakerPolicy, BreakerState, CircuitBreakerMode};
pub use errors::{BreakerResult, CircuitError};
