//! `resilient` is a Rust library for adding resilience to async operations.
//! It provides composable policies (retry, timeout, circuit breaker, etc.)
//! that can be layered via a pipeline.

pub mod circuit_breaker;
pub mod pipeline;
pub mod policy;
pub mod retry_policy;
pub mod timeout;
