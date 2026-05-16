use thiserror::Error;

#[derive(Error, Debug)]
pub enum RetryError<E> {
    #[error("Max attempts ({max_attempts}) exceeded")]
    MaxRetriesExceeded { max_attempts: u32, source: E },
}
