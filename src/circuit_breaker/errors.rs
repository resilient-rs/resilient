use std::time::Instant;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum CircuitError {
    #[error("Circuit breaker is open after {failure_count} failures")]
    CircuitOpen {
        last_failure_time: Option<Instant>,
        failure_count: usize,
    },

    #[error("Circuit breaker is in forced open state")]
    ForcedOpen,

    #[error("Circuit breaker rejected call in half-open state (remaining: {calls_remaining})")]
    HalfOpenRejected { calls_remaining: usize },
}

#[derive(Error, Debug)]
pub enum BreakerResult<E> {
    #[error("Circuit breaker is open after {failure_count} failures")]
    CircuitOpen {
        last_failure_time: Option<Instant>,
        failure_count: usize,
    },

    #[error(transparent)]
    Inner(#[from] E),
}
