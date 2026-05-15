//! Pipeline module — chains multiple resilience policies together.
//! A Pipeline wraps an operation and applies policies (retry, timeout, etc.)
//! in a configurable order before the operation is executed.

use std::future::Future;

use crate::policy::Policy;
use crate::retry_policy::RetryPolicy;

/// The main entry point for running operations with resilience policies.
/// Currently supports an optional retry policy; more policies will be added.
pub struct Pipeline {
    /// Optional retry policy applied before executing the operation.
    /// `None` means the operation runs without retry logic.
    retry_policy: Option<RetryPolicy>,
}

impl Pipeline {
    /// Creates a new Pipeline with no policies configured.
    /// Policies are added via builder methods like `.with_retry()`.
    pub fn new() -> Self {
        Pipeline { retry_policy: None }
    }
}

impl Default for Pipeline {
    /// Returns a default (empty) pipeline with no policies.
    fn default() -> Self {
        Self::new()
    }
}

impl Pipeline {
    /// Attaches a retry policy to the pipeline.
    ///
    /// # Arguments
    /// * `policy` - The retry configuration to use.
    ///
    /// # Returns
    /// `Self` with the retry policy set, enabling builder-style chaining.
    pub fn with_retry(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = Some(policy);
        self
    }

    /// Runs the given operation through the configured pipeline.
    /// If a retry policy is set, it will wrap the call; otherwise the
    /// operation runs directly.
    ///
    /// # Type Parameters
    /// - `F`: A closure that returns a future resolving to `Result<T, E>`.
    /// - `Fut`: The future type produced by `F`.
    /// - `T`: The success type.
    /// - `E`: The error type.
    ///
    /// # Arguments
    /// * `f` - The operation to run (can be called multiple times if retrying).
    ///
    /// # Returns
    /// `Ok(T)` on success, or `Err(E)` after all retries are exhausted.
    pub async fn run<F, Fut, T, E>(&self, mut f: F) -> Result<T, E>
    where
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<T, E>> + Send,
        T: Send,
        E: Send,
    {
        if let Some(ref policy) = self.retry_policy {
            policy.call(&mut f).await
        } else {
            f().await
        }
    }
}
