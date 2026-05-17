//! Retry policy module — provides configurable retry logic for async operations.
//!
//! Supports:
//! - Configurable max retry count
//! - Multiple jitter strategies via [`RetryMode`](retry::RetryMode)
//! - Conditional retries via [`RetryPolicy::retry_if`](retry::RetryPolicy::retry_if)
//! - Overall duration cap via `max_duration`
//! - Panic catching to prevent unwinding across retries

pub mod retry;

pub use retry::{RetryMode, RetryPolicy};
