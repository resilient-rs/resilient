//! Pipeline — a composable execution chain for resilience policies.
//!
//! The `Pipeline` combines multiple resilience strategies (retry, timeout,
//! circuit breaker, rate limiting) into a single, ordered execution chain.
//! Each policy is optional — only the ones you configure are applied.
//!
//! # Policy Order
//!
//! When `run` is called, policies are applied in this fixed order:
//!
//! 1. **Rate Limiting** — checks the token bucket before anything else.
//! 2. **Circuit Breaker** — checks whether the circuit is open.
//! 3. **Retry & Timeout** — the operation itself, optionally wrapped in retry
//!    and/or timeout logic. When both are present, timeout wraps each retry
//!    attempt individually.
//! 4. **Circuit Breaker Feedback** — after the operation completes (or fails),
//!    the result is fed back to the circuit breaker so it can update its state.
//!
//! # Example
//!
//! ```ignore
//! use resilient::pipeline::Pipeline;
//! use resilient::retry_policy::RetryPolicy;
//! use resilient::timeout::TimeoutPolicy;
//! use resilient::circuit_breaker::BreakerPolicy;
//! use resilient::rate_limit::RateLimiter;
//! use std::time::Duration;
//!
//! let pipeline = Pipeline::default()
//!     .with_retry(RetryPolicy::default().with_max_retries(3))
//!     .with_timeout(TimeoutPolicy::default().with_duration(Duration::from_secs(5)))
//!     .with_circuit_breaker(BreakerPolicy::default())
//!     .with_rate_limiter(RateLimiter::default().with_max_tokens(100));
//!
//! let result = pipeline.run(&mut || async { Ok::<_, String>("done") }).await;
//! ```

use std::future::Future;

use crate::circuit_breaker::{BreakerPolicy, CircuitError};
use crate::policy::Policy;
use crate::rate_limit::{RateLimitError, RateLimiter};
use crate::retry_policy::RetryPolicy;
use crate::timeout::{TimeoutError, TimeoutPolicy};

/// A composable resilience pipeline that chains retry, circuit breaker,
/// timeout, and rate-limiting policies around an async operation.
///
/// Each policy is optional. Use the builder methods to attach the ones you need.
/// Policies execute in a fixed order (see [module docs](self) for details).
///
/// # Cloning
///
/// `Pipeline` derives `Clone`. The underlying policies use `Arc`-based interior
/// mutability (circuit breaker, rate limiter), so cloned pipelines share state.
///
/// # Thread Safety
///
/// `Pipeline` is `Send` and `Sync` when its policy types are, which they are
/// by design. The same pipeline can be used from multiple tasks concurrently.
#[derive(Clone)]
pub struct Pipeline {
    retry_policy: Option<RetryPolicy>,
    circuit_breaker: Option<BreakerPolicy>,
    timeout: Option<TimeoutPolicy>,
    rate_limiter: Option<RateLimiter>,
}

impl Pipeline {
    /// Creates a new `Pipeline` with no policies configured.
    ///
    /// Equivalent to calling [`Pipeline::default`].
    ///
    /// # Example
    ///
    /// ```ignore
    /// let pipeline = Pipeline::new()
    ///     .with_retry(RetryPolicy::default());
    /// ```
    pub fn new() -> Self {
        Pipeline {
            retry_policy: None,
            circuit_breaker: None,
            timeout: None,
            rate_limiter: None,
        }
    }
}

impl Default for Pipeline {
    /// Returns a default (empty) pipeline via [`Pipeline::new`].
    fn default() -> Self {
        Self::new()
    }
}

impl Pipeline {
    /// Attaches a retry policy.
    ///
    /// When set, the operation is retried according to the policy's back-off
    /// and jitter strategy on failure. See [`RetryPolicy`] for details.
    pub fn with_retry(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = Some(policy);
        self
    }

    /// Attaches a circuit breaker policy.
    ///
    /// When set, the circuit breaker monitors operation outcomes. If failures
    /// exceed the configured threshold, the circuit opens and subsequent calls
    /// are rejected immediately. See [`BreakerPolicy`](crate::circuit_breaker::BreakerPolicy)
    /// for details on the state machine.
    pub fn with_circuit_breaker(mut self, policy: BreakerPolicy) -> Self {
        self.circuit_breaker = Some(policy);
        self
    }

    /// Attaches a timeout policy.
    ///
    /// When set, each invocation (or each retry attempt) is bounded by the
    /// configured duration. If the operation does not complete in time, a
    /// [`TimeoutError`] is returned. See [`TimeoutPolicy`] for details.
    pub fn with_timeout(mut self, policy: TimeoutPolicy) -> Self {
        self.timeout = Some(policy);
        self
    }

    /// Attaches a rate limiter.
    ///
    /// When set, the operation is only executed if the rate limiter has
    /// a token available. Otherwise a [`RateLimitError`] is returned
    /// immediately without invoking the operation. See [`RateLimiter`] for
    /// details.
    pub fn with_rate_limiter(mut self, policy: RateLimiter) -> Self {
        self.rate_limiter = Some(policy);
        self
    }

    /// Runs the provided async operation through the configured resilience pipeline.
    ///
    /// The policies are applied in this order:
    ///
    /// 1. **Rate limiter check** — consumes a token; returns
    ///    [`RateLimitError`] if the bucket is empty.
    /// 2. **Circuit breaker check** — returns [`CircuitError::CircuitOpen`]
    ///    if the circuit is currently open.
    /// 3. **Operation execution** — the closure `f` is invoked. Depending on
    ///    the configured policies:
    ///    - If both retry and timeout are set, each retry attempt has its own
    ///      timeout.
    ///    - If only timeout is set, the operation is wrapped in a single
    ///      deadline.
    ///    - If only retry is set, the operation is retried on failure.
    ///    - If neither is set, the operation runs once as-is.
    /// 4. **Circuit breaker feedback** — the outcome is recorded so the
    ///    breaker can update its state (success increments the success count
    ///    in HalfOpen; failure increments the consecutive failure count).
    ///
    /// # Type Parameters
    ///
    /// * `F` — A callable (typically a closure or function pointer) that
    ///   produces a future when invoked. Must be `FnMut` because retry may
    ///   call it multiple times.
    /// * `Fut` — The future returned by `F`.
    /// * `T` — The success type of the operation.
    /// * `E` — The error type. Must implement `From` for [`CircuitError`],
    ///   [`TimeoutError`], and [`RateLimitError`] so the pipeline can return
    ///   those error variants through the same error channel.
    ///
    /// # Returns
    ///
    /// * `Ok(T)` — the operation succeeded (possibly after retries).
    /// * `Err(E)` — the operation ultimately failed, was rate-limited, was
    ///   rejected by the circuit breaker, or timed out.
    pub async fn run<F, Fut, T, E>(&self, mut f: F) -> Result<T, E>
    where
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<T, E>> + Send,
        T: Send,
        E: Send + From<CircuitError> + From<TimeoutError> + From<RateLimitError>,
    {
        if let Some(ref rl) = self.rate_limiter
            && !rl.try_consume(1)
        {
            return Err(RateLimitError::RateLimited.into());
        }

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
