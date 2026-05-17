//! Core retry logic — implements the [`Policy`] trait with configurable back-off and jitter.
//!
//! The retry algorithm:
//! 1. Calls the operation.
//! 2. On success → returns `Ok`.
//! 3. On failure  → if [`RetryPolicy::retry_if`] is set and the predicate returns `false`,
//!    the error is returned immediately; otherwise a delay is computed, the policy waits,
//!    and the operation is retried.
//! 4. On panic   → catches it with `AssertUnwindSafe` and either resumes or retries.
//! 5. Stops when the retry budget or `max_duration` is exhausted.

use std::{
    any::Any,
    fmt,
    future::Future,
    panic::AssertUnwindSafe,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time,
};

use futures_util::FutureExt;

use crate::policy::Policy;

/// Determines how retry delays are computed.
///
/// Each variant implements a different jitter strategy to avoid
/// thundering-herd problems when many clients retry simultaneously.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryMode {
    /// Pure linear back-off from `min_delay` to `max_delay`, no randomness.
    Linear,
    /// Random delay uniformly chosen between zero and the computed back-off.
    /// Best at breaking synchronization across clients.
    FullJitter,
    /// Half the computed back-off plus a random jitter of the same range.
    /// Provides a balance of predictability and randomization.
    EqualJitter,
    /// `min(cap, random(min_delay, last_delay × 3))`.
    /// Adapts the delay based on the previous sleep time, spreading retries
    /// naturally over time.
    DecorrelatedJitter,
}

/// Predicate that decides whether a failed attempt should be retried.
type RetryIf = Arc<dyn Fn(&(dyn Any + 'static)) -> bool + Send + Sync>;

/// Configurable retry policy that executes an operation up to `max_retries` times.
///
/// Delays between attempts are computed according to [`RetryMode`].
/// The entire retry sequence is bounded by `max_duration`.
/// Panics during execution are caught so they don't skip remaining retries.
///
/// Use [`RetryPolicy::retry_if`] to skip retries for errors that are not transient.
#[derive(Clone)]
pub struct RetryPolicy {
    /// Maximum number of times the operation will be attempted.
    pub max_retries: usize,

    /// The jitter / back-off strategy to use.
    pub mode: RetryMode,

    /// Base delay used as the floor for back-off calculations.
    pub min_delay: time::Duration,

    /// Cap on any single delay between retries.
    pub max_delay: time::Duration,

    /// Hard cap on the total elapsed time for all retry attempts combined.
    pub max_duration: time::Duration,

    /// Internal flag set by the pipeline's timed closure when a timeout occurs.
    /// When set, the current attempt's error will not be retried.
    pub(crate) timeout_occurred: Option<Arc<AtomicBool>>,

    /// When set, only errors for which this returns `true` are retried.
    retry_if: Option<RetryIf>,
}

impl fmt::Debug for RetryPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetryPolicy")
            .field("max_retries", &self.max_retries)
            .field("mode", &self.mode)
            .field("min_delay", &self.min_delay)
            .field("max_delay", &self.max_delay)
            .field("max_duration", &self.max_duration)
            .field("retry_if", &self.retry_if.as_ref().map(|_| "Fn"))
            .finish()
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            mode: RetryMode::Linear,
            max_delay: time::Duration::from_secs(6),
            min_delay: time::Duration::from_secs(2),
            max_duration: time::Duration::from_secs(10),
            timeout_occurred: None,
            retry_if: None,
        }
    }
}

impl RetryPolicy {
    /// Builder-style setter for the retry mode.
    pub fn with_mode(mut self, mode: RetryMode) -> Self {
        self.mode = mode;
        self
    }

    /// Sets the maximum number of retry attempts.
    pub fn with_max_retries(mut self, max: usize) -> Self {
        self.max_retries = max;
        self
    }

    /// Sets the minimum delay between retries (floor for back-off).
    pub fn with_min_delay(mut self, delay: time::Duration) -> Self {
        self.min_delay = delay;
        self
    }

    /// Sets the cap on any single delay between retries.
    pub fn with_max_delay(mut self, delay: time::Duration) -> Self {
        self.max_delay = delay;
        self
    }

    /// Sets the hard cap on total elapsed time across all retry attempts.
    pub fn with_max_duration(mut self, duration: time::Duration) -> Self {
        self.max_duration = duration;
        self
    }

    /// Retries only when `predicate` returns `true` for the error.
    ///
    /// Without this, every error is retried until the attempt or duration budget
    /// is exhausted. Non-retryable errors fail immediately and do not consume
    /// further attempts.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use resilient::retry_policy::RetryPolicy;
    ///
    /// #[derive(Debug, PartialEq)]
    /// enum ApiError {
    ///     Transient,
    ///     BadRequest,
    /// }
    ///
    /// let policy = RetryPolicy::default()
    ///     .retry_if(|e: &ApiError| matches!(e, ApiError::Transient));
    /// ```
    pub fn retry_if<E, F>(mut self, predicate: F) -> Self
    where
        E: Send + Sync + 'static,
        F: Fn(&E) -> bool + Send + Sync + 'static,
    {
        let predicate = Arc::new(predicate);
        self.retry_if = Some(Arc::new(move |err: &(dyn Any + 'static)| {
            err.downcast_ref::<E>()
                .map(|e| predicate(e))
                .unwrap_or(false)
        }));
        self
    }

    /// Linear interpolation between `min_delay` and `max_delay` for the given attempt.
    fn base_delay(&self, attempt: usize, max_retries: usize) -> time::Duration {
        if max_retries <= 1 {
            return self.min_delay;
        }
        let t = attempt as f64 / (max_retries - 1) as f64;
        let secs = self.min_delay.as_secs_f64()
            + (self.max_delay.as_secs_f64() - self.min_delay.as_secs_f64()) * t;
        time::Duration::from_secs_f64(secs)
    }

    /// Applies the configured jitter strategy to produce the actual delay.
    fn jittered_delay(
        &self,
        base: time::Duration,
        last_delay: &mut time::Duration,
    ) -> time::Duration {
        match self.mode {
            RetryMode::Linear => base,

            RetryMode::FullJitter => {
                let secs = fastrand::f64() * base.as_secs_f64();
                time::Duration::from_secs_f64(secs)
            }

            RetryMode::EqualJitter => {
                let half = base.as_secs_f64() / 2.0;
                let secs = half + fastrand::f64() * half;
                time::Duration::from_secs_f64(secs)
            }

            RetryMode::DecorrelatedJitter => {
                let min_s = self.min_delay.as_secs_f64();
                let last = last_delay.as_secs_f64().max(min_s);
                let cap = self.max_delay.as_secs_f64();
                let next = (min_s + fastrand::f64() * (last * 3.0 - min_s)).min(cap);
                *last_delay = time::Duration::from_secs_f64(next);
                *last_delay
            }
        }
    }

    fn should_stop_retrying(&self, attempt: usize, start: time::Instant) -> bool {
        attempt >= self.max_retries || start.elapsed() >= self.max_duration
    }

    fn timed_out(&self) -> bool {
        self.timeout_occurred
            .as_ref()
            .map(|f| f.load(Ordering::Relaxed))
            .unwrap_or(false)
    }
}

// ── Convenience `run` method (same signature pattern as Pipeline::run) ─────

impl RetryPolicy {
    /// Executes the operation with retry logic.
    ///
    /// Behaves exactly like running a `Pipeline` configured with only a retry
    /// policy: calls the operation up to `max_retries + 1` times, applying
    /// the configured back-off and jitter between attempts.
    pub async fn run<F, Fut, T, E>(&self, mut f: F) -> Result<T, E>
    where
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<T, E>> + Send,
        T: Send,
        E: Send + 'static,
    {
        let max_retries = self.max_retries;
        let max_duration = self.max_duration;
        let total_attempts = max_retries + 1;
        let retry_if = self.retry_if.clone();

        let start = time::Instant::now();
        let mut last_delay = self.min_delay;

        for attempt in 0..=max_retries {
            let result = AssertUnwindSafe(f()).catch_unwind().await;

            match result {
                Ok(Ok(val)) => return Ok(val),
                Ok(Err(e)) => {
                    if let Some(ref should_retry) = retry_if {
                        if !should_retry(&e as &(dyn Any + 'static)) {
                            return Err(e);
                        }
                    }
                    if self.should_stop_retrying(attempt, start) || self.timed_out() {
                        return Err(e);
                    }
                }
                Err(panic) => {
                    if self.should_stop_retrying(attempt, start) {
                        std::panic::resume_unwind(panic);
                    }
                }
            }

            let base = self.base_delay(attempt, total_attempts);
            let mut delay = self.jittered_delay(base, &mut last_delay);
            let remaining = max_duration.saturating_sub(start.elapsed());
            delay = delay.min(remaining);

            tokio::time::sleep(delay).await;
        }

        unreachable!()
    }
}

impl<T, E: 'static> Policy<T, E> for RetryPolicy {
    fn call<F, Fut>(&self, f: &mut F) -> impl Future<Output = Result<T, E>> + Send
    where
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<T, E>> + Send,
        T: Send,
        E: Send,
    {
        let max_retries = self.max_retries;
        let max_duration = self.max_duration;
        let total_attempts = max_retries + 1;
        let retry_if = self.retry_if.clone();

        async move {
            let start = time::Instant::now();

            let mut last_delay = self.min_delay;

            for attempt in 0..=max_retries {
                let result = AssertUnwindSafe(f()).catch_unwind().await;

                match result {
                    Ok(Ok(val)) => return Ok(val),
                    Ok(Err(e)) => {
                        if let Some(ref should_retry) = retry_if {
                            if !should_retry(&e as &(dyn Any + 'static)) {
                                return Err(e);
                            }
                        }
                        if self.should_stop_retrying(attempt, start) || self.timed_out() {
                            return Err(e);
                        }
                    }
                    Err(panic) => {
                        if self.should_stop_retrying(attempt, start) {
                            std::panic::resume_unwind(panic);
                        }
                    }
                }

                let base = self.base_delay(attempt, total_attempts);
                let mut delay = self.jittered_delay(base, &mut last_delay);

                let remaining = max_duration.saturating_sub(start.elapsed());
                delay = delay.min(remaining);

                tokio::time::sleep(delay).await;
            }

            unreachable!()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use crate::policy::Policy;

    #[derive(Debug, PartialEq, Eq)]
    enum TestError {
        Transient,
        Permanent,
    }

    fn fast_policy() -> RetryPolicy {
        RetryPolicy {
            max_retries: 5,
            min_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(1),
            max_duration: Duration::from_secs(10),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn retries_all_errors_by_default() {
        let policy = fast_policy();
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_clone = Arc::clone(&attempts);

        let result = policy
            .call(&mut || {
                let attempts = Arc::clone(&attempts_clone);
                async move {
                    let n = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                    if n < 3 {
                        Err(TestError::Transient)
                    } else {
                        Ok(42)
                    }
                }
            })
            .await;

        assert_eq!(result, Ok(42));
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn permanent_error_fails_without_extra_attempts() {
        let policy = fast_policy().retry_if(|e: &TestError| matches!(e, TestError::Transient));
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_clone = Arc::clone(&attempts);

        let result: Result<(), TestError> = policy
            .call(&mut || {
                let attempts = Arc::clone(&attempts_clone);
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Err(TestError::Permanent)
                }
            })
            .await;

        assert_eq!(result, Err(TestError::Permanent));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn retries_only_when_predicate_matches() {
        let policy = fast_policy().retry_if(|e: &TestError| matches!(e, TestError::Transient));
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_clone = Arc::clone(&attempts);

        let result: Result<&str, TestError> = policy
            .call(&mut || {
                let attempts = Arc::clone(&attempts_clone);
                async move {
                    let n = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                    if n < 3 {
                        Err(TestError::Transient)
                    } else {
                        Ok("ok")
                    }
                }
            })
            .await;

        assert_eq!(result, Ok("ok"));
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }
}
