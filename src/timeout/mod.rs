//! Timeout policy — imposes a deadline on async operations.
//!
//! Provides [`TimeoutPolicy`] and its [`Builder`] to configure
//! duration, cancellation, interrupt behaviour, and lifecycle hooks.

pub mod errors;
pub mod timeout_policy;

pub use errors::TimeoutError;
pub use timeout_policy::{Builder, TimeoutPolicy};
