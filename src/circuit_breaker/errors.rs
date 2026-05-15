use thiserror::Error;

#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum CircuitError {
    #[error("Circuit breaker is open")]
    CircuitOpen {
        last_failure_time: Option<std::time::Instant>,
        failure_count: usize,
    },

    #[error("Circuit breaker is in forced open state")]
    ForcedOpen,

    #[error("Circuit breaker rejected call in half-open state")]
    HalfOpenRejected {
        calls_remaining: usize,
    },
}