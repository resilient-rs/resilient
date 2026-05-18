use thiserror::Error;

#[derive(Error, Debug)]
pub enum RateLimitError {
    #[error("Rate limit exceeded: no tokens available")]
    RateLimited,
}

#[derive(Error, Debug)]
pub enum RateLimitResult<E> {
    #[error("Rate limit exceeded: no tokens available")]
    RateLimited,

    #[error(transparent)]
    Inner(#[from] E),
}