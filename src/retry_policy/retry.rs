//! Core retry logic — implements the [`Policy`] trait with configurable back-off and jitter.
//!
//! The retry algorithm:
//! 1. Calls the operation.
//! 2. On success → returns `Ok`.
//! 3. On failure  → computes a delay using the configured [`RetryMode`], waits, and retries.
//! 4. On panic   → catches it with `AssertUnwindSafe` and either resumes or retries.
//! 5. Stops when the retry budget or `max_duration` is exhausted.

use std::{future::Future, panic::AssertUnwindSafe, time};

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

/// Configurable retry policy that executes an operation up to `max_retries` times.
///
/// Delays between attempts are computed according to [`RetryMode`].
/// The entire retry sequence is bounded by `max_duration`.
/// Panics during execution are caught so they don't skip remaining retries.
#[derive(Debug)]
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
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            mode: RetryMode::Linear,
            max_delay: time::Duration::from_secs(6),
            min_delay: time::Duration::from_secs(2),
            max_duration: time::Duration::from_secs(10),
        }
    }
}

impl RetryPolicy {
    /// Builder-style setter for the retry mode.
    pub fn with_mode(mut self, mode: RetryMode) -> Self {
        self.mode = mode;
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
}

impl<T, E> Policy<T, E> for RetryPolicy {
    fn call<F, Fut>(&self, f: &mut F) -> impl Future<Output = Result<T, E>> + Send
    where
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<T, E>> + Send,
        T: Send,
        E: Send,
    {
        let max_retries = self.max_retries.max(1);
        let max_duration = self.max_duration;

        async move {
            let start = std::time::Instant::now();

            // DecorrelatedJitter tracks the previous delay across iterations.
            let mut last_delay = self.min_delay;

            for attempt in 0..max_retries {
                let result = AssertUnwindSafe(f()).catch_unwind().await;

                match result {
                    Ok(Ok(val)) => return Ok(val),
                    Ok(Err(e)) => {
                        if attempt + 1 >= max_retries || start.elapsed() >= max_duration {
                            return Err(e);
                        }
                    }
                    Err(panic) => {
                        if attempt + 1 >= max_retries || start.elapsed() >= max_duration {
                            std::panic::resume_unwind(panic);
                        }
                    }
                }

                let base = self.base_delay(attempt, max_retries);
                let mut delay = self.jittered_delay(base, &mut last_delay);

                // Clamp so we never exceed the total duration budget.
                let remaining = max_duration.saturating_sub(start.elapsed());
                delay = delay.min(remaining);

                tokio::time::sleep(delay).await;
            }

            f().await
        }
    }
}
