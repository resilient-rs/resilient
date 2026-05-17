//! # Circuit Breaker Core Implementation
//!
//! The circuit breaker wraps an async operation and monitors its outcomes.
//! When failures exceed a configurable threshold the circuit **trips**,
//! rejecting subsequent requests without calling the operation. After a
//! cooldown period it transitions to **HalfOpen** where a limited number
//! of probes are let through to test whether the downstream has recovered.
//!
//! ## Architecture
//!
//! All mutable state lives on the heap behind `Arc` pointers so that a
//! single [`BreakerPolicy`] can be shared across tasks and cheaply cloned.
//! The current state is stored as an `AtomicU8` for lock-free reads.
//! Counters use `AtomicU64` for concurrent updates without contention.
//! The sliding-window call log uses `Mutex<VecDeque>` since it involves
//! heap-allocated data that must be pruned atomically.
//!
//! ## State Transitions
//!
//! | From       | Event                          | To         |
//! |------------|---------------------------------|------------|
//! | Closed     | consecutive failures ≥ threshold| Open       |
//! | Open       | timeout elapsed                 | HalfOpen   |
//! | HalfOpen   | success ≥ threshold            | Closed     |
//! | HalfOpen   | any failure                    | Open       |
//! | Any        | `force_open()` called           | ForcedOpen |
//! | ForcedOpen | `force_close()` / `reset()`     | Closed     |
//!
//! ## Usage
//!
//! ```ignore
//! use resilient::circuit_breaker::{BreakerPolicy, CircuitBreakerMode};
//!
//! let cb = BreakerPolicy::default()
//!     .with_mode(CircuitBreakerMode::CountBased)
//!     .with_failure_threshold(5)
//!     .with_open_timeout(Duration::from_secs(30));
//!
//! // Use directly with the Policy trait:
//! let result: Result<String, MyError> = cb.call(&mut || operation()).await;
//!
//! // Or attach to a pipeline (recording only, no rejection):
//! let pipeline = Pipeline::new().with_circuit_breaker(cb);
//! let result = pipeline.run(|| operation()).await;
//! ```

use std::{
    collections::VecDeque,
    future::Future,
    sync::{
        atomic::{AtomicU64, AtomicU8, Ordering},
        Arc,
    },
    time,
};

use crate::policy::Policy;

// ── Internal constants for the atomically-stored state ────────────────────

/// The circuit is closed — requests pass through normally.
const STATE_CLOSED: u8 = 0;
/// The circuit is open — requests are rejected.
const STATE_OPEN: u8 = 1;
/// The circuit is half-open — probe requests are allowed through.
const STATE_HALF_OPEN: u8 = 2;
/// The circuit has been manually forced open — all requests rejected.
const STATE_FORCED_OPEN: u8 = 3;

// ── CircuitBreakerMode ────────────────────────────────────────────────────

/// Determines how the circuit breaker decides to trip from **Closed** to **Open**.
///
/// Each variant implements a different failure-detection strategy, trading off
/// between simplicity, sensitivity to transient errors, and adaptability.
///
/// # Variants
///
/// | Mode            | Trip condition                                      | Best for                        |
/// |-----------------|-----------------------------------------------------|---------------------------------|
/// | `CountBased`    | N consecutive failures                              | Simple, deterministic           |
/// | `SlidingWindow` | Failure rate ≥ 50% in a rolling time window         | Transient-tolerant, burst-aware |
/// | `Adaptive`      | N consecutive failures + exponential back-off       | Volatile downstream services    |
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitBreakerMode {
    /// **Count-based** — trips when `failure_threshold` consecutive failures
    /// have occurred. This is the simplest and most predictable strategy.
    ///
    /// Use this when you want a hard limit on sequential failures before
    /// opening the circuit. A single success resets the consecutive-failure
    /// counter, so brief glitches do not trip the breaker.
    CountBased,

    /// **Sliding window** — trips when the failure rate in a rolling time
    /// window exceeds 50 %. Old calls age out of the window after
    /// `window_size` elapses.
    ///
    /// Use this when you want to tolerate occasional failures but trip on
    /// sustained degradation. Unlike CountBased, a success does not
    /// immediately reset the window — the failure rate is evaluated
    /// continuously based on recent history.
    SlidingWindow,

    /// **Adaptive** — trips on consecutive failures like CountBased, but
    /// additionally doubles the `open_timeout` each time the circuit trips.
    /// The timeout is clamped between `min_open_timeout` and
    /// `max_open_timeout`.
    ///
    /// Use this for downstream services that need increasingly long recovery
    /// periods. The exponential back-off prevents tight open→half-open→open
    /// cycles when the service is consistently unhealthy.
    Adaptive,
}

// ── BreakerState ──────────────────────────────────────────────────────────

/// Represents the current state of the circuit breaker.
///
/// The state is stored internally as a `u8` in an `AtomicU8` for lock-free
/// reads. The variants correspond to:
///
/// | Variant     | Meaning                                                    |
/// |-------------|------------------------------------------------------------|
/// | `Closed`    | Normal operation — requests pass through.                  |
/// | `Open`      | Rejecting requests — the failure threshold was exceeded.   |
/// | `HalfOpen`  | Probing — limited requests allowed to test recovery.       |
/// | `ForcedOpen`| Manual override — all requests rejected until reset.       |
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BreakerState {
    /// Normal operation — all requests are allowed through. When failures
    /// exceed the threshold the circuit transitions to `Open`.
    Closed,

    /// The circuit is open — requests are rejected immediately without
    /// calling the wrapped operation. After `open_timeout` elapses the
    /// circuit transitions to `HalfOpen` to probe for recovery.
    Open,

    /// The circuit is probing — a limited number of requests are allowed
    /// through. If enough succeed the circuit closes; if any fail it
    /// reopens.
    HalfOpen,

    /// Manual override — all requests are rejected until
    /// [`force_close`](BreakerPolicy::force_close) or
    /// [`reset`](BreakerPolicy::reset) is called.
    ForcedOpen,
}

// ── State conversions (u8 ↔ BreakerState) ────────────────────────────────

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

// ── BreakerPolicy ─────────────────────────────────────────────────────────

/// Configurable circuit breaker policy that monitors operation outcomes and
/// short-circuits when failures exceed the configured threshold.
///
/// # Configurable Parameters
///
/// | Field               | Default  | Purpose                          |
/// |---------------------|----------|----------------------------------|
/// | `failure_threshold` | 5        | Consecutive failures to trip     |
/// | `success_threshold` | 3        | Consecutive successes to close   |
/// | `open_timeout`      | 30 s     | Time before transitioning to     |
/// |                     |          | HalfOpen (base, without back-off)|
/// | `half_open_max_calls`| 3       | Max concurrent probe requests    |
/// | `mode`              | CountBased| Failure-detection strategy      |
/// | `window_size`       | 60 s     | Rolling window for SlidingWindow |
/// | `min_open_timeout`  | 10 s     | Floor for adaptive back-off      |
/// | `max_open_timeout`  | 300 s    | Ceiling for adaptive back-off    |
///
/// # Thread Safety
///
/// `BreakerPolicy` can be shared across threads. All mutable state is stored
/// in `Arc<Atomic*>` or `Arc<Mutex<...>>` cells on the heap, so cloning is
/// cheap and the shared state is visible to all clones.
///
/// # Example
///
/// ```ignore
/// use resilient::circuit_breaker::BreakerPolicy;
///
/// let cb = BreakerPolicy::default()
///     .with_failure_threshold(10)
///     .with_open_timeout(Duration::from_secs(60));
///
/// // Use via the Policy trait:
/// let result = cb.call(&mut || some_fallible_op()).await;
///
/// // Check state externally:
/// if cb.state() == BreakerState::Open {
///     eprintln!("Circuit is open!");
/// }
/// ```
#[derive(Debug, Clone)]
pub struct BreakerPolicy {
    /// Number of consecutive failures that triggers the Open state.
    pub failure_threshold: usize,

    /// Number of consecutive successes in HalfOpen needed to close the circuit.
    pub success_threshold: usize,

    /// How long to wait in Open before transitioning to HalfOpen.
    /// In `Adaptive` mode, this is the base value that gets multiplied
    /// by `2^trip_count`.
    pub open_timeout: time::Duration,

    /// Maximum number of concurrent requests allowed through in HalfOpen.
    pub half_open_max_calls: usize,

    /// The failure-detection strategy to use.
    pub mode: CircuitBreakerMode,

    /// Width of the rolling window used by [`SlidingWindow`](CircuitBreakerMode::SlidingWindow)
    /// mode. Older calls are evicted from the window before evaluating the
    /// failure rate.
    pub window_size: time::Duration,

    /// Minimum back-off duration in [`Adaptive`](CircuitBreakerMode::Adaptive) mode.
    /// The actual open timeout is never shorter than this.
    pub min_open_timeout: time::Duration,

    /// Maximum back-off duration in [`Adaptive`](CircuitBreakerMode::Adaptive) mode.
    /// The actual open timeout is never longer than this.
    pub max_open_timeout: time::Duration,

    // ── Internal heap-allocated state (shared across clones) ───────────────
    /// The current circuit breaker state stored as a `u8` for atomic access.
    state: Arc<AtomicU8>,
    /// Total number of failures recorded (across all states).
    failure_count: Arc<AtomicU64>,
    /// Total number of successes recorded (across all states).
    success_count: Arc<AtomicU64>,
    /// Current streak of consecutive failures (reset on any success).
    consecutive_failures: Arc<AtomicU64>,
    /// Current streak of consecutive successes in HalfOpen (reset on any failure).
    consecutive_successes: Arc<AtomicU64>,
    /// Timestamp of the most recent failure (used to compute timeout expiry).
    last_failure_time: Arc<std::sync::Mutex<Option<time::Instant>>>,
    /// Number of times the circuit has transitioned to Open (used for
    /// adaptive back-off multiplier).
    open_transition_count: Arc<AtomicU64>,
    /// Rolling log of call outcomes for SlidingWindow mode.
    window_calls: Arc<std::sync::Mutex<VecDeque<(time::Instant, bool)>>>,
    /// Number of probe requests made in the current HalfOpen period.
    half_open_calls_made: Arc<AtomicU64>,
}

// ── Default ───────────────────────────────────────────────────────────────

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

// ── Builder methods & public API ──────────────────────────────────────────

impl BreakerPolicy {
    // ── Builder setters ───────────────────────────────────────────────────

    /// Sets the number of consecutive failures required to trip the circuit.
    ///
    /// Once this threshold is reached, the state transitions from Closed to Open.
    /// A single success resets the consecutive-failure counter (except in
    /// SlidingWindow mode where the window is evaluated as a whole).
    pub fn with_failure_threshold(mut self, threshold: usize) -> Self {
        self.failure_threshold = threshold;
        self
    }

    /// Sets the number of consecutive successes in HalfOpen required to
    /// close the circuit.
    ///
    /// Once this threshold is reached AND `half_open_max_calls` have been
    /// made, the state transitions from HalfOpen back to Closed.
    pub fn with_success_threshold(mut self, threshold: usize) -> Self {
        self.success_threshold = threshold;
        self
    }

    /// Sets the base duration the circuit stays Open before transitioning
    /// to HalfOpen.
    ///
    /// In [`Adaptive`](CircuitBreakerMode::Adaptive) mode, the actual
    /// timeout is multiplied by `2^trip_count` and clamped to
    /// [`min_open_timeout`](BreakerPolicy::min_open_timeout) /
    /// [`max_open_timeout`](BreakerPolicy::max_open_timeout).
    pub fn with_open_timeout(mut self, timeout: time::Duration) -> Self {
        self.open_timeout = timeout;
        self
    }

    /// Sets the maximum number of concurrent probe requests allowed in
    /// HalfOpen.
    ///
    /// If more requests arrive while all slots are taken, they are rejected.
    /// Each probe result (success or failure) is counted toward this limit
    /// so that it is decremented correctly.
    pub fn with_half_open_max_calls(mut self, max: usize) -> Self {
        self.half_open_max_calls = max;
        self
    }

    /// Sets the failure-detection strategy.
    ///
    /// See [`CircuitBreakerMode`] for details on each variant.
    pub fn with_mode(mut self, mode: CircuitBreakerMode) -> Self {
        self.mode = mode;
        self
    }

    /// Sets the rolling window width for
    /// [`SlidingWindow`](CircuitBreakerMode::SlidingWindow) mode.
    ///
    /// Calls older than this duration are evicted before evaluating the
    /// failure rate. Has no effect in other modes.
    pub fn with_window_size(mut self, window: time::Duration) -> Self {
        self.window_size = window;
        self
    }

    /// Sets the min and max bounds for the adaptive open-timeout back-off.
    ///
    /// The actual timeout is `min_open_timeout × 2^trip_count`, capped at
    /// `max_open_timeout`. Only used in
    /// [`Adaptive`](CircuitBreakerMode::Adaptive) mode.
    pub fn with_adaptive_bounds(mut self, min: time::Duration, max: time::Duration) -> Self {
        self.min_open_timeout = min;
        self.max_open_timeout = max;
        self
    }

    // ── Accessors ─────────────────────────────────────────────────────────

    /// Returns the current circuit breaker state.
    ///
    /// This is a lock-free read of the internal atomic state.
    pub fn state(&self) -> BreakerState {
        self.state.load(Ordering::SeqCst).into()
    }

    /// Returns the current consecutive failure count.
    pub fn consecutive_failures(&self) -> usize {
        self.consecutive_failures.load(Ordering::SeqCst) as usize
    }

    /// Returns the timestamp of the most recent failure, if any.
    pub fn last_failure_time(&self) -> Option<time::Instant> {
        *self
            .last_failure_time
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }

    // ── Timeout calculation ───────────────────────────────────────────────

    /// Computes the actual open timeout, applying exponential back-off when
    /// in [`Adaptive`](CircuitBreakerMode::Adaptive) mode.
    ///
    /// For `CountBased` and `SlidingWindow` modes, the base `open_timeout`
    /// is returned directly. For `Adaptive`, the timeout is:
    ///
    /// ```text
    /// clamp(min_open_timeout × 2^trip_count, min_open_timeout, max_open_timeout)
    /// ```
    fn calculate_open_timeout(&self) -> time::Duration {
        match self.mode {
            CircuitBreakerMode::Adaptive => {
                let count = self.open_transition_count.load(Ordering::SeqCst);
                let min_secs = self.min_open_timeout.as_secs_f64();
                let max_secs = self.max_open_timeout.as_secs_f64();
                let calculated = min_secs * 2_f64.powf(count as f64);
                time::Duration::from_secs_f64(calculated.min(max_secs))
            }
            _ => self.open_timeout,
        }
    }

    // ── State transitions ─────────────────────────────────────────────────

    /// Attempts to transition from Closed or HalfOpen to Open.
    /// Only transitions if the current state is Closed or HalfOpen.
    /// Records the failure timestamp and increments the open transition
    /// counter for adaptive back-off.
    fn try_transition_to_open(&self) {
        let current = self.state.load(Ordering::SeqCst);
        if current == STATE_CLOSED || current == STATE_HALF_OPEN {
            self.state.store(STATE_OPEN, Ordering::SeqCst);
            self.open_transition_count.fetch_add(1, Ordering::SeqCst);
            *self
                .last_failure_time
                .lock()
                .unwrap_or_else(|e| e.into_inner()) = Some(time::Instant::now());
        }
    }

    /// Attempts to transition from Open to HalfOpen when the timeout has
    /// elapsed. Resets the HalfOpen tracking counters (calls made and
    /// consecutive successes) so that each HalfOpen period starts fresh.
    fn try_transition_to_half_open(&self) {
        let current = self.state.load(Ordering::SeqCst);
        if current == STATE_OPEN {
            self.state.store(STATE_HALF_OPEN, Ordering::SeqCst);
            self.half_open_calls_made.store(0, Ordering::SeqCst);
            self.consecutive_successes.store(0, Ordering::SeqCst);
        }
    }

    /// Transitions from HalfOpen to Closed. Resets the consecutive-failure
    /// and total-failure counters since the circuit is now healthy.
    fn try_transition_to_closed(&self) {
        let prev = self.state.swap(STATE_CLOSED, Ordering::SeqCst);
        if prev == STATE_HALF_OPEN || prev == STATE_FORCED_OPEN {
            self.consecutive_failures.store(0, Ordering::SeqCst);
            self.failure_count.store(0, Ordering::SeqCst);
        }
    }

    // ── Manual controls ───────────────────────────────────────────────────

    /// Resets the circuit breaker to its initial Closed state and clears
    /// all counters and the sliding window.
    pub fn reset(&self) {
        self.state.store(STATE_CLOSED, Ordering::SeqCst);
        self.failure_count.store(0, Ordering::SeqCst);
        self.success_count.store(0, Ordering::SeqCst);
        self.consecutive_failures.store(0, Ordering::SeqCst);
        self.consecutive_successes.store(0, Ordering::SeqCst);
        self.open_transition_count.store(0, Ordering::SeqCst);
        self.half_open_calls_made.store(0, Ordering::SeqCst);
        *self
            .last_failure_time
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = None;
        self.window_calls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
    }

    /// Manually forces the circuit into the Open state. All requests will
    /// be rejected until [`force_close`](BreakerPolicy::force_close) or
    /// [`reset`](BreakerPolicy::reset) is called.
    pub fn force_open(&self) {
        self.state.store(STATE_FORCED_OPEN, Ordering::SeqCst);
    }

    /// Manually closes the circuit. Equivalent to [`reset`](BreakerPolicy::reset).
    pub fn force_close(&self) {
        self.reset();
    }

    // ── Outcome recording ─────────────────────────────────────────────────

    /// Records a successful operation outcome.
    ///
    /// Behavior depends on the current state:
    /// - **Closed**: increments the success counter and resets consecutive failures.
    /// - **HalfOpen**: increments the probe-call counter and consecutive successes.
    ///   If both the probe limit and success threshold are met, transitions to Closed.
    /// - **Open / ForcedOpen**: no-op (requests should not reach here, but
    ///   recording a success in Open is harmless).
    ///
    /// Also appends the outcome to the sliding window for `SlidingWindow` mode.
    pub fn record_success(&self) {
        let state = self.state.load(Ordering::SeqCst);

        if state == STATE_HALF_OPEN {
            self.half_open_calls_made.fetch_add(1, Ordering::SeqCst);
            self.consecutive_successes.fetch_add(1, Ordering::SeqCst);
            self.consecutive_failures.store(0, Ordering::SeqCst);

            let calls_made = self.half_open_calls_made.load(Ordering::SeqCst);
            let successes = self.consecutive_successes.load(Ordering::SeqCst);

            if calls_made >= self.half_open_max_calls as u64 {
                if successes >= self.success_threshold as u64 {
                    self.try_transition_to_closed();
                } else {
                    self.try_transition_to_open();
                }
            }
        } else if state == STATE_CLOSED {
            self.success_count.fetch_add(1, Ordering::SeqCst);
            self.consecutive_successes.fetch_add(1, Ordering::SeqCst);
            self.consecutive_failures.store(0, Ordering::SeqCst);
        }

        self.window_calls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push_back((time::Instant::now(), true));
    }

    /// Records a failed operation outcome.
    ///
    /// Behavior depends on the current state and mode:
    /// - **HalfOpen**: immediately transitions back to Open (even a single
    ///   probe failure reopens the circuit).
    /// - **Closed**: checks the failure-detection strategy:
    ///   - `CountBased`: transitions to Open if consecutive failures ≥ threshold.
    ///   - `SlidingWindow`: evicts old entries and checks the failure rate.
    ///   - `Adaptive`: same as CountBased.
    /// - **Open / ForcedOpen**: no-op (failures in Open are expected).
    ///
    /// Also updates the last-failure timestamp and appends to the sliding window.
    pub fn record_failure(&self) {
        let state = self.state.load(Ordering::SeqCst);

        self.failure_count.fetch_add(1, Ordering::SeqCst);
        self.consecutive_failures.fetch_add(1, Ordering::SeqCst);
        self.consecutive_successes.store(0, Ordering::SeqCst);

        self.window_calls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push_back((time::Instant::now(), false));

        if state == STATE_HALF_OPEN {
            self.half_open_calls_made.fetch_add(1, Ordering::SeqCst);
            self.try_transition_to_open();
        } else if state == STATE_CLOSED {
            match self.mode {
                CircuitBreakerMode::CountBased | CircuitBreakerMode::Adaptive => {
                    let failures = self.consecutive_failures.load(Ordering::SeqCst);
                    if failures >= self.failure_threshold as u64 {
                        self.try_transition_to_open();
                    }
                }
                CircuitBreakerMode::SlidingWindow => {
                    self.check_sliding_window_and_trip();
                }
            }
        }
    }

    // ── SlidingWindow helpers ─────────────────────────────────────────────

    /// Evaluates the sliding window and trips the circuit if the failure
    /// rate exceeds 50 %.
    ///
    /// Steps:
    /// 1. Evict all entries older than `window_size`.
    /// 2. Count successes (`true`) and failures (`false`).
    /// 3. If failure count / total ≥ 0.5, transition to Open.
    fn check_sliding_window_and_trip(&self) {
        let mut calls = self.window_calls.lock().unwrap_or_else(|e| e.into_inner());
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
            let failures = calls.iter().filter(|(_, is_success)| !*is_success).count();
            let failure_rate = failures as f64 / total as f64;
            if failure_rate >= 0.5 {
                self.try_transition_to_open();
            }
        }
    }

    // ── Request admission ─────────────────────────────────────────────────

    /// Checks whether a request should be allowed through based on the
    /// current circuit state.
    ///
    /// Returns `true` if the request can proceed, `false` if it should be
    /// rejected.
    ///
    /// | State       | Decision                                                  |
    /// |-------------|-----------------------------------------------------------|
    /// | Closed      | Always allow.                                             |
    /// | Open        | Allow if `open_timeout` has elapsed (transitions to        |
    /// |             | HalfOpen automatically).                                   |
    /// | HalfOpen    | Allow if fewer than `half_open_max_calls` have been made   |
    /// |             | during this HalfOpen period.                               |
    /// | ForcedOpen  | Always reject.                                             |
    pub fn should_allow_request(&self) -> bool {
        let state = self.state.load(Ordering::SeqCst);

        match state {
            STATE_CLOSED => true,
            STATE_OPEN => {
                if let Some(last_failure) = *self
                    .last_failure_time
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                {
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

// ── Policy trait implementation ───────────────────────────────────────────

impl<T, E> Policy<T, E> for BreakerPolicy
where
    E: From<crate::circuit_breaker::CircuitError>,
{
    /// Executes the operation through the circuit breaker.
    ///
    /// 1. Calls [`should_allow_request`](BreakerPolicy::should_allow_request).
    /// 2. If rejected, returns `Err(CircuitError::CircuitOpen)`.
    /// 3. If allowed, runs the operation and records the outcome via
    ///    [`record_success`](BreakerPolicy::record_success) or
    ///    [`record_failure`](BreakerPolicy::record_failure).
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

// ── Convenience `run` method (same signature pattern as Pipeline::run) ─────

impl BreakerPolicy {
    /// Executes the operation through the circuit breaker.
    ///
    /// Behaves exactly like running a `Pipeline` configured with only a
    /// circuit breaker: checks [`should_allow_request`](BreakerPolicy::should_allow_request),
    /// rejects with `CircuitError` if the circuit is open, otherwise runs
    /// the operation and records the outcome.
    pub async fn run<F, Fut, T, E>(&self, mut f: F) -> Result<T, E>
    where
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<T, E>> + Send,
        T: Send,
        E: Send + From<crate::circuit_breaker::CircuitError>,
    {
        let policy = self.clone_inner();

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

// ── Cloning helper ────────────────────────────────────────────────────────

impl BreakerPolicy {
    /// Creates a new `BreakerPolicy` handle that shares the same underlying
    /// heap state (counters, state, window, etc.) with the original.
    ///
    /// This is used internally by the `Policy` implementation to move a
    /// shared handle into the returned future, and by the pipeline to share
    /// the same breaker state across multiple policy layers.
    ///
    /// The clone is cheap — only `Arc` pointers are copied.
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

// ── Send / Sync assertion ─────────────────────────────────────────────────

/// Compile-time check that `BreakerPolicy` implements `Send`.
fn _assert_send() {
    fn is_send<T: Send>() {}
    is_send::<BreakerPolicy>();
}
