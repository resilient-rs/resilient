use std::time::Duration;

pub struct Fixed;

pub struct Exponential;

pub struct Linear;

pub struct RetryBuilder<S> {
    pub max_attempts: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
    pub jitter: Option<f64>,
    _strategy: std::marker::PhantomData<S>,
}

impl RetryBuilder<Exponential> {
    pub fn exponential(max_attempts: u32) -> RetryBuilder<Exponential> {
        RetryBuilder {
            max_attempts,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            jitter: None,
            _strategy: std::marker::PhantomData,
        }
    }
}