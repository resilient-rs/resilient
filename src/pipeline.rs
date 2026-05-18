//! Pipeline — a composable execution chain for resilience policies.
//!
//! The `Pipeline` combines multiple resilience strategies (retry, timeout,
//! circuit breaker, bulkhead, rate limiting) into a single, ordered execution chain.
//! Each policy is optional — only the ones you configure are applied.
//!
//! # Policy Order
//!
//! When `run` is called, policies are applied in this fixed order:
//!
//! 1. **Circuit Breaker** — checks whether the circuit is open.
//! 2. **Bulkhead** — acquires a concurrency permit for the whole run.
//! 3. **Retry & Timeout** — the operation itself, optionally wrapped in retry
//!    and/or timeout logic. Rate limiting (when configured) is applied per
//!    execution attempt inside this step. When both retry and timeout are
//!    present, each retry attempt has its own timeout. Timeout errors are
//!    **not** retried — they propagate immediately to the caller.
//! 4. **Circuit Breaker Feedback** — after the operation completes (or fails),
//!    the result is fed back to the circuit breaker so it can update its state.
//!
//! # Fallback
//!
//! [`Pipeline::or_else`] attaches a fallback closure that runs when the
//! pipeline produces any error. The fallback executes **raw** — it does not
//! re-apply the pipeline's policies. This is useful for cached responses,
//! default values, or alternative code paths.
//!
//! # Examples
//!
//! Basic pipeline:
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
//!
//! Pipeline with a fallback:
//!
//! ```ignore
//! use resilient::pipeline::Pipeline;
//! use resilient::retry_policy::RetryPolicy;
//!
//! let pipeline = Pipeline::default()
//!     .with_retry(RetryPolicy::default().with_max_retries(3));
//!
//! let result = pipeline
//!     .or_else(|| async { Ok::<_, String>("cached response") })
//!     .run(&mut || async { Err::<String, _>("service unavailable") })
//!     .await;
//! ```

use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::bulkhead::Bulkhead;
use crate::circuit_breaker::BreakerPolicy;
use crate::policy::Policy;
use crate::rate_limit::RateLimiter;
use crate::retry_policy::RetryPolicy;
use crate::timeout::TimeoutPolicy;

/// A composable resilience pipeline that chains retry, circuit breaker,
/// timeout, bulkhead, and rate-limiting policies around an async operation.
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
    bulkhead: Option<Bulkhead>,
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
            bulkhead: None,
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
    /// are rejected immediately. See [`BreakerPolicy`]
    /// for details on the state machine.
    pub fn with_circuit_breaker(mut self, policy: BreakerPolicy) -> Self {
        self.circuit_breaker = Some(policy);
        self
    }

    /// Attaches a timeout policy.
    ///
    /// When set, each invocation (or each retry attempt) is bounded by the
    /// configured duration. See [`TimeoutPolicy`] for details.
    pub fn with_timeout(mut self, policy: TimeoutPolicy) -> Self {
        self.timeout = Some(policy);
        self
    }

    /// Attaches a rate limiter.
    ///
    /// When set, the operation is only executed if the rate limiter has
    /// a token available. See [`RateLimiter`] for details.
    pub fn with_rate_limiter(mut self, policy: RateLimiter) -> Self {
        self.rate_limiter = Some(policy);
        self
    }

    /// Attaches a bulkhead policy.
    ///
    /// When set, at most the configured number of pipeline runs may be in
    /// flight at once. The permit is held for the entire run,
    /// including all retry attempts.
    pub fn with_bulkhead(mut self, policy: Bulkhead) -> Self {
        self.bulkhead = Some(policy);
        self
    }

    /// Wraps this pipeline with a fallback operation.
    ///
    /// When the pipeline's [`run`](Pipeline::run) returns any error, the
    /// provided fallback closure is called instead. The fallback runs **raw**
    /// — it does not re-apply any of the pipeline's resilience policies.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let result = Pipeline::default()
    ///     .or_else(|| async { Ok::<_, String>("fallback") })
    ///     .run(&mut || async { Err::<String, _>("error") })
    ///     .await;
    ///
    /// assert_eq!(result.unwrap(), "fallback");
    /// ```
    pub fn or_else<F, Fut, T, E>(self, fallback: F) -> FallbackPipeline<F, Fut, T, E> {
        FallbackPipeline {
            primary: self,
            fallback,
            _marker: PhantomData,
        }
    }

    /// Runs the provided async operation through the configured resilience pipeline.
    ///
    /// The policies are applied in this order:
    ///
    /// 1. **Circuit breaker check** — checks if the circuit is open.
    /// 2. **Bulkhead acquire** — acquires a permit when max concurrency is reached.
    /// 3. **Operation execution** — the closure `f` is invoked. Depending on
    ///    the configured policies:
    ///    - If both retry and timeout are set, each retry attempt has its own
    ///      per-attempt timeout. Timeout errors are **not** retried — they
    ///      propagate immediately.
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
    /// * `E` — The error type of the wrapped operation.
    ///
    /// # Returns
    ///
    /// * `Ok(T)` — the operation succeeded (possibly after retries).
    /// * `Err(E)` — the operation ultimately failed.
    pub async fn run<F, Fut, T, E>(&self, mut f: F) -> Result<T, E>
    where
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<T, E>> + Send,
        T: Send,
        E: Send + 'static + From<crate::timeout::TimeoutError>,
    {
        if let Some(ref cb) = self.circuit_breaker {
            if !cb.should_allow_request() {
                return f().await;
            }
        }

        let _bulkhead_permit = if let Some(ref bulkhead) = self.bulkhead {
            bulkhead.try_acquire()
        } else {
            None
        };

        let result = match (self.retry_policy.as_ref(), self.timeout.as_ref()) {
            (Some(retry), Some(timeout)) => {
                let timeout_flag = Arc::new(AtomicBool::new(false));
                let mut retry = retry.clone();
                retry.timeout_occurred = Some(timeout_flag.clone());

                let duration = timeout.duration;
                let on_success = timeout.on_success.clone();
                let on_failure = timeout.on_failure.clone();
                let on_timeout = timeout.on_timeout.clone();
                let rate_limiter = self.rate_limiter.clone();

                let mut timed = || {
                    if let Some(ref rl) = rate_limiter {
                        if !rl.try_consume(1) {
                            return Box::pin(f())
                                as Pin<Box<dyn Future<Output = Result<T, E>> + Send>>;
                        }
                    }
                    timeout_flag.store(false, Ordering::Relaxed);
                    let fut = f();
                    let flag = timeout_flag.clone();
                    let os = on_success.clone();
                    let of = on_failure.clone();
                    let ot = on_timeout.clone();
                    Box::pin(async move {
                        match tokio::time::timeout(duration, fut).await {
                            Ok(Ok(val)) => {
                                if let Some(cb) = os {
                                    cb().await;
                                }
                                Ok(val)
                            }
                            Ok(Err(e)) => {
                                if let Some(cb) = of {
                                    cb().await;
                                }
                                Err(e)
                            }
                            Err(_elapsed) => {
                                if let Some(cb) = ot {
                                    cb().await;
                                }
                                flag.store(true, Ordering::Relaxed);
                                unreachable!("timeout future is dropped - retry should handle this")
                            }
                        }
                    }) as Pin<Box<dyn Future<Output = Result<T, E>> + Send>>
                };
                retry.call(&mut timed).await
            }
            (Some(retry), None) => {
                let mut g = || {
                    if let Some(ref rl) = self.rate_limiter {
                        if !rl.try_consume(1) {
                            return Box::pin(f())
                                as Pin<Box<dyn Future<Output = Result<T, E>> + Send>>;
                        }
                    }
                    Box::pin(f()) as Pin<Box<dyn Future<Output = Result<T, E>> + Send>>
                };
                retry.call(&mut g).await
            }
            (None, Some(timeout)) => {
                let duration = timeout.duration;
                let on_success = timeout.on_success.clone();
                let on_failure = timeout.on_failure.clone();
                let on_timeout = timeout.on_timeout.clone();
                let name = timeout.name.clone();
                let rate_limiter = self.rate_limiter.clone();

                let g = move || {
                    if let Some(ref rl) = rate_limiter {
                        if !rl.try_consume(1) {
                            return Box::pin(f())
                                as Pin<Box<dyn Future<Output = Result<T, E>> + Send>>;
                        }
                    }
                    let fut = f();
                    let os = on_success.clone();
                    let of = on_failure.clone();
                    let ot = on_timeout.clone();
                    Box::pin(async move {
                        match tokio::time::timeout(duration, fut).await {
                            Ok(Ok(val)) => {
                                if let Some(cb) = os {
                                    cb().await;
                                }
                                Ok(val)
                            }
                            Ok(Err(e)) => {
                                if let Some(cb) = of {
                                    cb().await;
                                }
                                Err(e)
                            }
                            Err(_elapsed) => {
                                if let Some(cb) = ot {
                                    cb().await;
                                }
                                Err(crate::timeout::TimeoutError::Elapsed { duration, name }.into())
                            }
                        }
                    }) as Pin<Box<dyn Future<Output = Result<T, E>> + Send>>
                };
                g().await
            }
            (None, None) => {
                if let Some(ref rl) = self.rate_limiter {
                    let _ = rl.try_consume(1);
                }
                f().await
            }
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

/// A pipeline paired with a fallback operation.
///
/// When the primary pipeline returns any error, the fallback closure is
/// invoked directly, bypassing all resilience policies. This is useful for
/// providing default values, cached responses, or alternative code paths.
///
/// Constructed via [`Pipeline::or_else`].
pub struct FallbackPipeline<F, Fut, T, E> {
    primary: Pipeline,
    fallback: F,
    _marker: PhantomData<(Fut, T, E)>,
}

impl<F, Fut, T, E> FallbackPipeline<F, Fut, T, E>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = Result<T, E>> + Send,
    T: Send,
    E: Send + 'static,
{
    /// Runs the primary pipeline and falls back on error.
    ///
    /// The primary operation runs through all configured pipeline policies
    /// (rate limiter, circuit breaker, retry, timeout). If any of those
    /// produce an error, the fallback closure is invoked instead of
    /// propagating the error.
    ///
    /// The fallback runs **raw** — it does not re-apply any resilience
    /// policies.
    ///
    /// # Type Parameters
    ///
    /// * `G` — The primary operation callable. Must be `FnMut` so retry can
    ///   call it multiple times.
    /// * `GFut` — The future returned by the primary operation.
    ///
    /// # Returns
    ///
    /// * `Ok(T)` — the primary operation succeeded, or the fallback succeeded.
    /// * `Err(E)` — both the primary operation and the fallback failed.
    pub async fn run<G, GFut>(&self, op: &mut G) -> Result<T, E>
    where
        G: FnMut() -> GFut + Send,
        GFut: Future<Output = Result<T, E>> + Send,
        E: Send + 'static + From<crate::timeout::TimeoutError>,
    {
        match self.primary.run(op).await {
            Ok(val) => Ok(val),
            Err(_) => (self.fallback)().await,
        }
    }
}
