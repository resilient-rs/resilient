use std::future::Future;

use crate::circuit_breaker::{BreakerPolicy, CircuitError};
use crate::policy::Policy;
use crate::retry_policy::RetryPolicy;

pub struct Pipeline {
    retry_policy: Option<RetryPolicy>,
    circuit_breaker: Option<BreakerPolicy>,
}

impl Pipeline {
    pub fn new() -> Self {
        Pipeline {
            retry_policy: None,
            circuit_breaker: None,
        }
    }
}

impl Default for Pipeline {
    fn default() -> Self {
        Self::new()
    }
}

impl Pipeline {
    pub fn with_retry(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = Some(policy);
        self
    }

    pub fn with_circuit_breaker(mut self, policy: BreakerPolicy) -> Self {
        self.circuit_breaker = Some(policy);
        self
    }

    pub async fn run<F, Fut, T, E>(&self, mut f: F) -> Result<T, E>
    where
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<T, E>> + Send,
        T: Send,
        E: Send + From<CircuitError>,
    {
        if let Some(ref cb) = self.circuit_breaker
            && !cb.should_allow_request()
        {
            return Err(CircuitError::CircuitOpen {
                last_failure_time: cb.last_failure_time(),
                failure_count: cb.consecutive_failures(),
            }
            .into());
        }

        let result = if let Some(ref retry) = self.retry_policy {
            retry.call(&mut f).await
        } else {
            f().await
        };

        if let Some(ref cb) = self.circuit_breaker {
            match &result {
                Ok(_) => cb.record_success(),
                Err(_) => cb.record_failure(),
            }
        }

        result
    }
}
