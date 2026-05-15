use std::future::Future;

use crate::policy::Policy;
use crate::retry_policy::RetryPolicy;

pub struct Pipeline {
    retry_policy: Option<RetryPolicy>,
}

impl Pipeline {
    pub fn new() -> Self {
        Pipeline { retry_policy: None }
    }

    pub fn with_retry(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = Some(policy);
        self
    }

    pub async fn run<F, Fut, T, E>(&self, f: F) -> Result<T, E>
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<T, E>> + Send,
        T: Send,
        E: Send,
    {
        if let Some(ref policy) = self.retry_policy {
            policy.call(f).await
        } else {
            f().await
        }
    }
}
