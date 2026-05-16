use thiserror::Error;

#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum RateLimitError {
    #[error("Rate limit exceeded: no tokens available")]
    RateLimited,
}

impl From<RateLimitError> for String {
    fn from(e: RateLimitError) -> Self {
        e.to_string()
    }
}
