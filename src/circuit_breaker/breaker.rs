use std::{
    collections::VecDeque,
    future::Future,
    sync::{atomic::{AtomicU64, AtomicU8, Ordering}, Arc},
    time,
};

use crate::policy::Policy;

const STATE_CLOSED: u8 = 0;
const STATE_OPEN: u8 = 1;
const STATE_HALF_OPEN: u8 = 2;
const STATE_FORCED_OPEN: u8 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitBreakerMode {
    CountBased,
    SlidingWindow,
    Adaptive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BreakerState {
    Closed,
    Open,
    HalfOpen,
    ForcedOpen,
}

impl From<u8> for BreakerState {
    fn from(state: u8) -> Self {
        match state {
            STATE_CLOSED => BreakerState::Closed,
            STATE_OPEN => BreakerState::Open,
            STATE_HALF_OPEN => BreakerState::HalfOpen,
            STATE_FORCED_OPEN => BreakerState::ForcedOpen,
            _ => BreakerState::Closed,
        }
    }
}

impl From<BreakerState> for u8 {
    fn from(state: BreakerState) -> Self {
        match state {
            BreakerState::Closed => STATE_CLOSED,
            BreakerState::Open => STATE_OPEN,
            BreakerState::HalfOpen => STATE_HALF_OPEN,
            BreakerState::ForcedOpen => STATE_FORCED_OPEN,
        }
    }
}

#[derive(Debug)]
pub struct BreakerPolicy {
    pub failure_threshold: usize,
    pub success_threshold: usize,
    pub open_timeout: time::Duration,
    pub half_open_max_calls: usize,
    pub mode: CircuitBreakerMode,
    pub window_size: time::Duration,
    pub min_open_timeout: time::Duration,
    pub max_open_timeout: time::Duration,

    state: Arc<AtomicU8>,
    failure_count: Arc<AtomicU64>,
    success_count: Arc<AtomicU64>,
    consecutive_failures: Arc<AtomicU64>,
    consecutive_successes: Arc<AtomicU64>,
    last_failure_time: Arc<std::sync::Mutex<Option<time::Instant>>>,
    open_transition_count: Arc<AtomicU64>,
    window_calls: Arc<std::sync::Mutex<VecDeque<(time::Instant, bool)>>>,
    half_open_calls_made: Arc<AtomicU64>,
}

impl Default for BreakerPolicy {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            success_threshold: 3,
            open_timeout: time::Duration::from_secs(30),
            half_open_max_calls: 3,
            mode: CircuitBreakerMode::CountBased,
            window_size: time::Duration::from_secs(60),
            min_open_timeout: time::Duration::from_secs(10),
            max_open_timeout: time::Duration::from_secs(300),
            state: Arc::new(AtomicU8::new(STATE_CLOSED)),
            failure_count: Arc::new(AtomicU64::new(0)),
            success_count: Arc::new(AtomicU64::new(0)),
            consecutive_failures: Arc::new(AtomicU64::new(0)),
            consecutive_successes: Arc::new(AtomicU64::new(0)),
            last_failure_time: Arc::new(std::sync::Mutex::new(None)),
            open_transition_count: Arc::new(AtomicU64::new(0)),
            window_calls: Arc::new(std::sync::Mutex::new(VecDeque::new())),
            half_open_calls_made: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl BreakerPolicy {
    pub fn with_failure_threshold(mut self, threshold: usize) -> Self {
        self.failure_threshold = threshold;
        self
    }

    pub fn with_success_threshold(mut self, threshold: usize) -> Self {
        self.success_threshold = threshold;
        self
    }

    pub fn with_open_timeout(mut self, timeout: time::Duration) -> Self {
        self.open_timeout = timeout;
        self
    }

    pub fn with_half_open_max_calls(mut self, max: usize) -> Self {
        self.half_open_max_calls = max;
        self
    }

    pub fn with_mode(mut self, mode: CircuitBreakerMode) -> Self {
        self.mode = mode;
        self
    }

    pub fn with_window_size(mut self, window: time::Duration) -> Self {
        self.window_size = window;
        self
    }

    pub fn with_adaptive_bounds(mut self, min: time::Duration, max: time::Duration) -> Self {
        self.min_open_timeout = min;
        self.max_open_timeout = max;
        self
    }

    pub fn state(&self) -> BreakerState {
        self.state.load(Ordering::SeqCst).into()
    }

    pub fn consecutive_failures(&self) -> usize {
        self.consecutive_failures.load(Ordering::SeqCst) as usize
    }

    pub fn last_failure_time(&self) -> Option<time::Instant> {
        *self.last_failure_time.lock().unwrap()
    }

    fn calculate_open_timeout(&self) -> time::Duration {
        let count = self.open_transition_count.load(Ordering::SeqCst);
        let min_secs = self.min_open_timeout.as_secs_f64();
        let max_secs = self.max_open_timeout.as_secs_f64();
        let calculated = min_secs * 2_f64.powf(count as f64);
        time::Duration::from_secs_f64(calculated.min(max_secs))
    }

    fn try_transition_to_open(&self) {
        let current = self.state.load(Ordering::SeqCst);
        if current != STATE_HALF_OPEN {
            self.state.store(STATE_OPEN, Ordering::SeqCst);
            self.open_transition_count.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn try_transition_to_half_open(&self) {
        let current = self.state.load(Ordering::SeqCst);
        if current == STATE_OPEN {
            self.state.store(STATE_HALF_OPEN, Ordering::SeqCst);
            self.half_open_calls_made.store(0, Ordering::SeqCst);
            self.consecutive_successes.store(0, Ordering::SeqCst);
        }
    }

    fn try_transition_to_closed(&self) {
        self.state.store(STATE_CLOSED, Ordering::SeqCst);
        self.consecutive_failures.store(0, Ordering::SeqCst);
        self.failure_count.store(0, Ordering::SeqCst);
    }

    pub fn reset(&self) {
        self.state.store(STATE_CLOSED, Ordering::SeqCst);
        self.failure_count.store(0, Ordering::SeqCst);
        self.success_count.store(0, Ordering::SeqCst);
        self.consecutive_failures.store(0, Ordering::SeqCst);
        self.consecutive_successes.store(0, Ordering::SeqCst);
        self.open_transition_count.store(0, Ordering::SeqCst);
        self.half_open_calls_made.store(0, Ordering::SeqCst);
        *self.last_failure_time.lock().unwrap() = None;
        self.window_calls.lock().unwrap().clear();
    }

    pub fn force_open(&self) {
        self.state.store(STATE_FORCED_OPEN, Ordering::SeqCst);
    }

    pub fn force_close(&self) {
        self.reset();
    }

    pub fn record_success(&self) {
        let state = self.state.load(Ordering::SeqCst);

        if state == STATE_HALF_OPEN {
            self.half_open_calls_made.fetch_add(1, Ordering::SeqCst);
            self.consecutive_successes.fetch_add(1, Ordering::SeqCst);
            self.consecutive_failures.store(0, Ordering::SeqCst);

            let calls_made = self.half_open_calls_made.load(Ordering::SeqCst);
            let successes = self.consecutive_successes.load(Ordering::SeqCst);

            if calls_made >= self.half_open_max_calls as u64 && successes >= self.success_threshold as u64 {
                self.try_transition_to_closed();
            }
        } else if state == STATE_CLOSED {
            self.success_count.fetch_add(1, Ordering::SeqCst);
            self.consecutive_successes.fetch_add(1, Ordering::SeqCst);
            self.consecutive_failures.store(0, Ordering::SeqCst);
        }

        self.window_calls.lock().unwrap().push_back((time::Instant::now(), true));
    }

    pub fn record_failure(&self) {
        let state = self.state.load(Ordering::SeqCst);

        *self.last_failure_time.lock().unwrap() = Some(time::Instant::now());
        self.failure_count.fetch_add(1, Ordering::SeqCst);
        self.consecutive_failures.fetch_add(1, Ordering::SeqCst);
        self.consecutive_successes.store(0, Ordering::SeqCst);

        self.window_calls.lock().unwrap().push_back((time::Instant::now(), false));

        if state == STATE_HALF_OPEN {
            self.half_open_calls_made.fetch_add(1, Ordering::SeqCst);
            self.try_transition_to_open();
        } else if state == STATE_CLOSED {
            match self.mode {
                CircuitBreakerMode::CountBased => {
                    let failures = self.consecutive_failures.load(Ordering::SeqCst);
                    if failures >= self.failure_threshold as u64 {
                        self.try_transition_to_open();
                    }
                }
                CircuitBreakerMode::SlidingWindow => {
                    self.check_sliding_window_and_trip();
                }
                CircuitBreakerMode::Adaptive => {
                    let failures = self.consecutive_failures.load(Ordering::SeqCst);
                    if failures >= self.failure_threshold as u64 {
                        self.try_transition_to_open();
                    }
                }
            }
        }
    }

    fn check_sliding_window_and_trip(&self) {
        let mut calls = self.window_calls.lock().unwrap();
        let now = time::Instant::now();

        while let Some((timestamp, _)) = calls.front() {
            if now.duration_since(*timestamp) > self.window_size {
                calls.pop_front();
            } else {
                break;
            }
        }

        let total = calls.len();
        if total >= self.failure_threshold {
            let failures = calls.iter().filter(|(_, is_failure)| !*is_failure).count();
            let failure_rate = failures as f64 / total as f64;
            if failure_rate >= 0.5 {
                self.try_transition_to_open();
            }
        }
    }

    pub fn should_allow_request(&self) -> bool {
        let state = self.state.load(Ordering::SeqCst);

        match state {
            STATE_CLOSED => true,
            STATE_OPEN => {
                if let Some(last_failure) = *self.last_failure_time.lock().unwrap() {
                    let timeout = self.calculate_open_timeout();
                    if last_failure.elapsed() >= timeout {
                        self.try_transition_to_half_open();
                        return true;
                    }
                }
                false
            }
            STATE_HALF_OPEN => {
                let calls_made = self.half_open_calls_made.load(Ordering::SeqCst);
                (calls_made as usize) < self.half_open_max_calls
            }
            STATE_FORCED_OPEN => false,
            _ => false,
        }
    }
}

impl<T, E> Policy<T, E> for BreakerPolicy
where
    E: From<crate::circuit_breaker::CircuitError>,
{
    fn call<F, Fut>(&self, f: &mut F) -> impl Future<Output = Result<T, E>> + Send
    where
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<T, E>> + Send,
        T: Send,
        E: Send,
    {
        let policy = self.clone_inner();

        async move {
            if !policy.should_allow_request() {
                return Err(crate::circuit_breaker::CircuitError::CircuitOpen {
                    last_failure_time: policy.last_failure_time(),
                    failure_count: policy.consecutive_failures(),
                }
                .into());
            }

            let result = f().await;

            match &result {
                Ok(_) => policy.record_success(),
                Err(_) => policy.record_failure(),
            }

            result
        }
    }
}

impl BreakerPolicy {
    pub fn clone_inner(&self) -> Self {
        Self {
            failure_threshold: self.failure_threshold,
            success_threshold: self.success_threshold,
            open_timeout: self.open_timeout,
            half_open_max_calls: self.half_open_max_calls,
            mode: self.mode,
            window_size: self.window_size,
            min_open_timeout: self.min_open_timeout,
            max_open_timeout: self.max_open_timeout,
            state: Arc::clone(&self.state),
            failure_count: Arc::clone(&self.failure_count),
            success_count: Arc::clone(&self.success_count),
            consecutive_failures: Arc::clone(&self.consecutive_failures),
            consecutive_successes: Arc::clone(&self.consecutive_successes),
            last_failure_time: Arc::clone(&self.last_failure_time),
            open_transition_count: Arc::clone(&self.open_transition_count),
            window_calls: Arc::clone(&self.window_calls),
            half_open_calls_made: Arc::clone(&self.half_open_calls_made),
        }
    }
}

fn _assert_send() {
    fn is_send<T: Send>() {}
    is_send::<BreakerPolicy>();
}