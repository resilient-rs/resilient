//! Core retry logic — implements the `Policy` trait with configurable back-off.
//!
//! The retry algorithm:
//! 1. Calls the operation.
//! 2. On success → returns `Ok`.
//! 3. On error  → waits with a linearly increasing delay, then retries.
//! 4. On panic → catches it with `AssertUnwindSafe` and either resumes or retries.
//! 5. Stops when the retry budget or the max duration is exhausted.

use std::{future::Future, panic::AssertUnwindSafe, sync::atomic::AtomicUsize, time};

use futures_util::FutureExt;

use crate::policy::Policy;

/// Configurable retry policy that executes an operation up to `max_retries` times.
///
/// Delays between attempts grow linearly from `min_delay` to `max_delay`.
/// The entire retry sequence is bounded by `max_duration`.
/// Panics during execution are caught so they don't skip remaining retries.
#[derive(Debug)]
pub struct RetryPolicy {
    /// Maximum number of times the operation will be attempted.
    pub max_retries: AtomicUsize,

    /// Minimum delay between retries (used for the first retry).
    pub min_delay: time::Duration,

    /// Maximum delay between retries (used for the last retry before the cap).
    pub max_delay: time::Duration,

    /// Hard cap on the total elapsed time for all retry attempts combined.
    pub max_duration: time::Duration,

    /// Minimum bound on total elapsed time (reserved for future use / symmetry).
    pub min_duration: time::Duration,
}

impl Default for RetryPolicy {
    /// Defaults:
    /// - max_retries: 3
    /// - max_delay:   6 seconds
    /// - min_delay:   2 seconds
    /// - max_duration: 10 seconds
    /// - min_duration: 3 seconds
    fn default() -> Self {
        Self {
            max_retries: AtomicUsize::new(3),
            max_delay: time::Duration::from_secs(6),
            min_delay: time::Duration::from_secs(2),
            max_duration: time::Duration::from_secs(10),
            min_duration: time::Duration::from_secs(3),
        }
    }
}

impl<T, E> Policy<T, E> for RetryPolicy {
    /// Calls the operation through the retry policy.
    ///
    /// The returned future will:
    /// - Return `Ok(T)` immediately if the operation succeeds.
    /// - Retry with linear back-off if the operation returns `Err`.
    /// - Catch panics and either retry or resume the panic.
    /// - Stop retrying when the max retry count or duration cap is reached.
    fn call<F, Fut>(&self, f: &mut F) -> impl Future<Output = Result<T, E>> + Send
    where
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<T, E>> + Send,
        T: Send,
        E: Send,
    {
        let max_retries = self
            .max_retries
            .load(std::sync::atomic::Ordering::Relaxed)
            .max(1);
        let min_delay = self.min_delay;
        let max_delay = self.max_delay;
        let max_duration = self.max_duration;

        async move {
            let start = std::time::Instant::now();

            // Core retry loop — try up to `max_retries` times.
            for attempt in 0..max_retries {
                // Catch panics so we can retry instead of crashing the caller.
                let result = AssertUnwindSafe(f()).catch_unwind().await;

                match result {
                    // Operation succeeded — return immediately.
                    Ok(Ok(val)) => return Ok(val),
                    // Operation returned an error.
                    Ok(Err(e)) => {
                        // If no retries remain or the total time budget is spent, give up.
                        if attempt + 1 >= max_retries || start.elapsed() >= max_duration {
                            return Err(e);
                        }
                    }
                    // Operation panicked.
                    Err(panic) => {
                        // If we can't retry anymore, resume the panic.
                        if attempt + 1 >= max_retries || start.elapsed() >= max_duration {
                            std::panic::resume_unwind(panic);
                        }
                    }
                }

                // Compute linear back-off delay between min_delay and max_delay.
                let delay = if max_retries > 1 {
                    let t = attempt as f64 / (max_retries - 1) as f64;
                    let secs = min_delay.as_secs_f64()
                        + (max_delay.as_secs_f64() - min_delay.as_secs_f64()) * t;
                    std::time::Duration::from_secs_f64(secs)
                } else {
                    min_delay
                };

                // Clamp delay so we never exceed max_duration.
                let remaining = max_duration.saturating_sub(start.elapsed());
                let delay = delay.min(remaining);

                // Wait before the next attempt.
                tokio::time::sleep(delay).await;
            }

            // Final attempt after loop exhaustion (should rarely be reached).
            f().await
        }
    }
}
