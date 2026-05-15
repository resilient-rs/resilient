use std::future::Future;

use crate::circuit_breaker::{BreakerPolicy, CircuitError};
use crate::policy::Policy;
use crate::retry_policy::RetryPolicy;
use crate::timeout::{TimeoutError, TimeoutPolicy};

pub struct Pipeline {
    retry_policy: Option<RetryPolicy>,
    circuit_breaker: Option<BreakerPolicy>,
    timeout: Option<TimeoutPolicy>,
}

impl Pipeline {
    pub fn new() -> Self {
        Pipeline {
            retry_policy: None,
            circuit_breaker: None,
            timeout: None,
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

    pub fn with_timeout(mut self, policy: TimeoutPolicy) -> Self {
        self.timeout = Some(policy);
        self
    }

    pub async fn run<F, Fut, T, E>(&self, mut f: F) -> Result<T, E>
    where
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<T, E>> + Send,
        T: Send,
        E: Send + From<CircuitError> + From<TimeoutError>,
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

        let result = match (self.retry_policy.as_ref(), self.timeout.as_ref()) {
            (Some(retry), Some(timeout)) => {
                let duration = timeout.duration;
                let name = &timeout.name;
                let on_success = &timeout.on_success;
                let on_failure = &timeout.on_failure;
                let on_timeout = &timeout.on_timeout;
                let mut timed = || {
                    let fut = f();
                    async move {
                        match tokio::time::timeout(duration, fut).await {
                            Ok(Ok(val)) => {
                                if let Some(cb) = on_success {
                                    cb().await;
                                }
                                Ok(val)
                            }
                            Ok(Err(e)) => {
                                if let Some(cb) = on_failure {
                                    cb().await;
                                }
                                Err(e)
                            }
                            Err(_elapsed) => {
                                if let Some(cb) = on_timeout {
                                    cb().await;
                                }
                                Err(TimeoutError::Elapsed {
                                    duration,
                                    name: name.clone(),
                                }
                                .into())
                            }
                        }
                    }
                };
                retry.call(&mut timed).await
            }
            (Some(retry), None) => retry.call(&mut f).await,
            (None, Some(timeout)) => timeout.call(&mut f).await,
            (None, None) => f().await,
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
