//! Retry policy module — provides configurable retry logic for async operations.
//!
//! Supports:
//! - Configurable max retry count
//! - Multiple jitter strategies via [`RetryMode`](retry::RetryMode)
//! - Overall duration cap via `max_duration`
//! - Panic catching to prevent unwinding across retries

pub mod errors;
pub mod retry;

pub use errors::RetryError;
pub use retry::{RetryMode, RetryPolicy};
