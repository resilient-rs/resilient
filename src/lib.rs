//! `resilient` is a Rust library for adding resilience to async operations.
//! It provides composable policies (retry, timeout, circuit breaker, etc.)
//! that can be layered via a pipeline.

pub mod circuit_breaker;
pub use circuit_breaker as breaker;
pub use breaker::BreakerPolicy;
pub mod pipeline;
pub mod policy;
pub mod rate_limit;
pub use rate_limit as limiter;
pub use limiter::RateLimiter;
pub mod retry_policy;
pub use retry_policy as retry;
pub use retry::RetryPolicy;
pub mod timeout;
pub use timeout::TimeoutPolicy;
