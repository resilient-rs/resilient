//! `resilient` is a Rust library for adding resilience to async operations.
//! It provides composable policies (retry, timeout, circuit breaker, bulkhead, etc.)
//! that can be layered via a pipeline.

pub mod bulkhead;
pub use bulkhead::Bulkhead;
pub mod circuit_breaker;
pub use breaker::BreakerPolicy;
pub use breaker::BreakerResult;
pub use circuit_breaker as breaker;
pub mod pipeline;
pub mod policy;
pub mod rate_limit;
pub use limiter::RateLimiter;
pub use limiter::RateLimitResult;
pub use rate_limit as limiter;
pub mod retry_policy;
pub use retry::RetryPolicy;
pub use retry_policy as retry;
pub mod timeout;
pub use timeout::TimeoutPolicy;
