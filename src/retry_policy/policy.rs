use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub struct Fixed;
pub struct Exponential;
pub struct Linear;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackoffStrategy {
    Fixed,
    Exponential,
    Linear,
}

#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
    pub jitter: Option<f64>,
    strategy: BackoffStrategy,
}

impl RetryPolicy {
    pub fn exponential(max_attempts: u32) -> RetryBuilder<Exponential> {
        RetryBuilder::exponential(max_attempts)
    }

    pub fn fixed(max_attempts: u32) -> RetryBuilder<Fixed> {
        RetryBuilder::fixed(max_attempts)
    }

    pub fn linear(max_attempts: u32) -> RetryBuilder<Linear> {
        RetryBuilder::linear(max_attempts)
    }

    pub(crate) fn delay_before_retry(&self, retry_index: u32) -> Duration {
        let raw = match self.strategy {
            BackoffStrategy::Fixed => self.base_delay,
            BackoffStrategy::Exponential => {
                let factor = 2u32.saturating_pow(retry_index);
                self.base_delay.saturating_mul(factor)
            }
            BackoffStrategy::Linear => self
                .base_delay
                .saturating_mul(retry_index.saturating_add(1)),
        };

        let capped = raw.min(self.max_delay);
        apply_jitter(capped, self.jitter)
    }
}

pub struct RetryBuilder<S> {
    pub max_attempts: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
    pub jitter: Option<f64>,
    _strategy: std::marker::PhantomData<S>,
}

impl<S> RetryBuilder<S> {
    pub fn base_delay(mut self, delay: Duration) -> Self {
        self.base_delay = delay;
        self
    }

    pub fn max_delay(mut self, delay: Duration) -> Self {
        self.max_delay = delay;
        self
    }

    /// Jitter factor in `[0.0, 1.0]`. Each delay is scaled by `1.0 ± factor`.
    pub fn jitter(mut self, factor: f64) -> Self {
        self.jitter = Some(factor.clamp(0.0, 1.0));
        self
    }

    fn into_policy(self, strategy: BackoffStrategy) -> RetryPolicy {
        RetryPolicy {
            max_attempts: self.max_attempts,
            base_delay: self.base_delay,
            max_delay: self.max_delay,
            jitter: self.jitter,
            strategy,
        }
    }
}

impl RetryBuilder<Exponential> {
    pub fn exponential(max_attempts: u32) -> Self {
        Self {
            max_attempts,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            jitter: None,
            _strategy: std::marker::PhantomData,
        }
    }

    pub fn build(self) -> RetryPolicy {
        self.into_policy(BackoffStrategy::Exponential)
    }
}

impl RetryBuilder<Fixed> {
    pub fn fixed(max_attempts: u32) -> Self {
        Self {
            max_attempts,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            jitter: None,
            _strategy: std::marker::PhantomData,
        }
    }

    pub fn build(self) -> RetryPolicy {
        self.into_policy(BackoffStrategy::Fixed)
    }
}

impl RetryBuilder<Linear> {
    pub fn linear(max_attempts: u32) -> Self {
        Self {
            max_attempts,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            jitter: None,
            _strategy: std::marker::PhantomData,
        }
    }

    pub fn build(self) -> RetryPolicy {
        self.into_policy(BackoffStrategy::Linear)
    }
}

fn apply_jitter(delay: Duration, jitter: Option<f64>) -> Duration {
    let Some(factor) = jitter else {
        return delay;
    };

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let r = (nanos % 1000) as f64 / 1000.0;
    let multiplier = 1.0 + factor * (r * 2.0 - 1.0);
    Duration::from_secs_f64(delay.as_secs_f64() * multiplier.max(0.0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exponential_backoff_caps_at_max_delay() {
        let policy = RetryPolicy::exponential(5)
            .base_delay(Duration::from_millis(100))
            .max_delay(Duration::from_millis(250))
            .build();

        assert_eq!(policy.delay_before_retry(0), Duration::from_millis(100));
        assert_eq!(policy.delay_before_retry(1), Duration::from_millis(200));
        assert_eq!(policy.delay_before_retry(2), Duration::from_millis(250));
    }

    #[test]
    fn fixed_backoff_is_constant() {
        let policy = RetryPolicy::fixed(3)
            .base_delay(Duration::from_millis(50))
            .build();

        assert_eq!(policy.delay_before_retry(0), Duration::from_millis(50));
        assert_eq!(policy.delay_before_retry(4), Duration::from_millis(50));
    }

    #[test]
    fn linear_backoff_grows() {
        let policy = RetryPolicy::linear(5)
            .base_delay(Duration::from_millis(10))
            .max_delay(Duration::from_secs(1))
            .build();

        assert_eq!(policy.delay_before_retry(0), Duration::from_millis(10));
        assert_eq!(policy.delay_before_retry(1), Duration::from_millis(20));
        assert_eq!(policy.delay_before_retry(2), Duration::from_millis(30));
    }
}
