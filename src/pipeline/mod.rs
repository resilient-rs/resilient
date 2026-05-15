use std::future::Future;

use crate::policy::Policy;
use crate::retry_policy::RetryPolicy;

/// Pipeline is the main entry point for running operations with policies.
/// It orchestrates multiple policies (retry, timeout, etc.) in a configurable order.
pub struct Pipeline {
    /// The retry policy to use, if any.
    /// More policies (timeout, circuit breaker, etc.) will be added later.
    retry_policy: Option<RetryPolicy>,
}

impl Pipeline {
    /// Creates a new empty Pipeline with no policies configured.
    /// Use `.with_retry()`, `.with_timeout()`, etc. to add policies.
    pub fn new() -> Self {
        Pipeline { retry_policy: None }
    }
}

impl Default for Pipeline {
    /// Default pipeline has no policies configured.
    fn default() -> Self {
        Self::new()
    }
}

impl Pipeline {
    pub fn with_retry(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = Some(policy);
        self
    }

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
