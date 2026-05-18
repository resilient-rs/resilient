//! Rate limiter — token-bucket based rate limiting for async operations.
//!
//! The rate limiter uses a token-bucket algorithm: tokens are added at a
//! configurable rate up to a maximum capacity. Each operation consumes one
//! token. When no tokens are available, the operation is rejected.
//!
//! # Example
//!
//! ```ignore
//! use resilient::rate_limit::RateLimiter;
//! use std::time::Duration;
//!
//! let rl = RateLimiter::default()
//!     .with_max_tokens(100)
//!     .with_refill_rate(Duration::from_secs(1));
//!
//! let result = rl.call(&mut || operation()).await;
//! ```

pub mod errors;
pub mod rate_limiter;

pub use errors::{RateLimitError, RateLimitResult};
pub use rate_limiter::RateLimiter;
