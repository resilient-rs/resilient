//! Composable async resilience utilities for Rust.
//!
//! # Retry
//!
//! ```
//! use std::time::Duration;
//!
//! use resilient::{retry, RetryPolicy};
//!
//! # async fn example() -> Result<(), resilient::RetryError<&'static str>> {
//! let policy = RetryPolicy::exponential(3)
//!     .base_delay(Duration::from_millis(50))
//!     .build();
//!
//! let value = retry(&policy, || async {
//!     // call your fallible async operation here
//!     Ok::<_, &str>(())
//! })
//! .await?;
//! # let _ = value;
//! # Ok(())
//! # }
//! ```

pub mod retry_policy;

pub use retry_policy::{
    BackoffStrategy, Exponential, Fixed, Linear, RetryBuilder, RetryError, RetryPolicy, retry,
};
