//! Error types used by the retry policy.
//! Currently defines `RetryError` for when all retry attempts are exhausted.

use thiserror::Error;

/// Represents errors that can occur during retry operations.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum RetryError {
    /// Returned when the operation keeps failing and the retry budget is spent.
    #[error("Max retries of {max_retries} has been exceeded")]
    MaxRetriesExceeded {
        /// The maximum number of retries that was configured.
        max_retries: usize,
        /// The last error that occurred before giving up.
        last_error: Box<RetryError>,
    },
}
