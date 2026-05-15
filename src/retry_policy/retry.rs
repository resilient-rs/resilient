use std::{future::Future, panic::AssertUnwindSafe, sync::atomic::AtomicUsize, time};

use futures_util::FutureExt;

use crate::policy::Policy;

// Retry policy is used to define the rules for the retry runner
// this is the one injected on the async runner for adaptive retry
// based runs.
#[derive(Debug)]
pub struct RetryPolicy {
    // This one defines the maximum number of calls
    // this will make to the service or the function
    pub max_retries: AtomicUsize,

    //max_delay defines the maximum time it needs to wait before
    //starting the duration counter
    pub max_delay: time::Duration,

    //min_delay defines the minimum time it needs to wait before
    //starting the duration counter
    pub min_delay: time::Duration,

    //max and min durations define the time it needs to wait
    // before running the next service call.
    pub max_duration: time::Duration,
    pub min_duration: time::Duration,
}

impl Default for RetryPolicy {
    // Defaults:
    // max_retries: 3 seconds
    // max_delay: 6 seconds
    // min_delay: 2 seconds
    // max_duration: 10 seconds
    // min_duration: 3 seconds
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

                let delay = if max_retries > 1 {
                    let t = attempt as f64 / (max_retries - 1) as f64;
                    let secs = min_delay.as_secs_f64()
                        + (max_delay.as_secs_f64() - min_delay.as_secs_f64()) * t;
                    std::time::Duration::from_secs_f64(secs)
                } else {
                    min_delay
                };

                let remaining = max_duration.saturating_sub(start.elapsed());
                let delay = delay.min(remaining);

                tokio::time::sleep(delay).await;
            }

            f().await
        }
    }
}
