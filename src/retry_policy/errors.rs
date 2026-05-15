use thiserror::Error;

#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum RetryError {
    #[error("Max retries of {max_retries} has been exceeded")]
    MaxRetriesExceeded {
        max_retries: usize,
        last_error: Box<RetryError>,
    },
}
