//! # Circuit Breaker Errors
//!
//! Error types returned when the circuit breaker rejects a request.
//! These errors implement `From<CircuitError>` so they integrate with
//! the [`Policy`](crate::policy::Policy) trait's error handling.

use thiserror::Error;

/// Errors produced by the circuit breaker when it rejects calls.
///
/// The most common variant is [`CircuitOpen`](CircuitError::CircuitOpen),
/// returned when the breaker is in the Open or HalfOpen state and refuses
/// to let a request through. The [`ForcedOpen`](CircuitError::ForcedOpen)
/// variant is returned when the breaker has been manually placed in the
/// forced-open state via [`force_open()`](crate::circuit_breaker::BreakerPolicy::force_open).
///
/// # Example
///
/// ```ignore
/// match err {
///     CircuitError::CircuitOpen { failure_count, .. } => {
///         eprintln!("Circuit open after {failure_count} failures");
///     }
///     CircuitError::ForcedOpen => {
///         eprintln!("Circuit manually forced open");
///     }
///     CircuitError::HalfOpenRejected { calls_remaining } => {
///         eprintln!("HalfOpen: {calls_remaining} probe slots left");
///     }
/// }
/// ```
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum CircuitError {
    /// The circuit is open and the request was rejected.
    ///
    /// Contains diagnostic information about the failure that caused the
    /// circuit to trip and how long ago it happened.
    #[error("Circuit breaker is open after {failure_count} failures")]
    CircuitOpen {
        /// When the last failure occurred (or `None` if the failure time
        /// was not recorded yet, which should not happen in practice).
        last_failure_time: Option<std::time::Instant>,
        /// The number of consecutive failures that tripped the circuit.
        failure_count: usize,
    },

    /// The circuit has been manually placed in forced-open mode via
    /// [`force_open()`](crate::circuit_breaker::BreakerPolicy::force_open).
    ///
    /// No requests are allowed through until
    /// [`force_close()`](crate::circuit_breaker::BreakerPolicy::force_close)
    /// or [`reset()`](crate::circuit_breaker::BreakerPolicy::reset) is called.
    #[error("Circuit breaker is in forced open state")]
    ForcedOpen,

    /// The circuit is in HalfOpen state but all probe slots are taken.
    ///
    /// Only [`half_open_max_calls`](crate::circuit_breaker::BreakerPolicy::half_open_max_calls)
    /// requests are allowed through while probing. This error is returned
    /// when all those slots are occupied by in-flight requests.
    #[error("Circuit breaker rejected call in half-open state (remaining: {calls_remaining})")]
    HalfOpenRejected {
        /// How many probe slots are still available (0 means all taken).
        calls_remaining: usize,
    },
}