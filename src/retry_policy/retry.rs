use std::{future::Future, sync::atomic::AtomicUsize, time};

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
    fn call<F, Fut>(&self, f: F) -> impl Future<Output = Result<T, E>> + Send
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<T, E>> + Send,
        T: Send,
        E: Send,
    {
        async move { f().await }
    }
}
