use std::time::Duration;

use thiserror::Error;

/// Errors returned by the timeout policy.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum TimeoutError {
    /// The operation did not complete within the configured duration.
    #[error(
        "Operation timed out after {duration:?}{}",
        match .name {
            Some(n) => format!(" ({n})"),
            None => String::new(),
        }
    )]
    Elapsed {
        /// The duration that was exceeded.
        duration: Duration,
        /// Optional policy name shown in the error message.
        name: Option<String>,
    },
}

/// Allows treating a `TimeoutError` as a generic `String` error.
impl From<TimeoutError> for String {
    fn from(e: TimeoutError) -> Self {
        e.to_string()
    }
}
