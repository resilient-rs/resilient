use std::{sync::atomic::AtomicUsize, time};

pub struct RetryPolicy {
    pub max_retries: AtomicUsize,
    pub max_delay: time::Duration,
    pub min_delay: time::Duration,
    pub max_duration: time::Duration,
    pub min_duration: time::Duration,
}
