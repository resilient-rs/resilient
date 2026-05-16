//! Core token-bucket rate limiter — implements the [`Policy`] trait.
//!
//! The token-bucket algorithm provides a configurable rate limit: tokens
//! are replenished at a fixed [`refill_rate`](RateLimiter::refill_rate) up
//! to [`max_tokens`](RateLimiter::max_tokens). Each call consumes one token.
//! When the bucket is empty, requests are rejected.
//!
//! ## Thread safety
//!
//! Mutable state (current token count and last-refill timestamp) is stored
//! behind an `Arc<Mutex<...>>`.  The struct is cheap to clone and all clones
//! share the same underlying bucket state.

use std::{
    future::Future,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use crate::policy::Policy;
use crate::rate_limit::RateLimitError;

// ── Internal state ─────────────────────────────────────────────────────────

/// Mutable state shared across all clones of a [`RateLimiter`].
struct Inner {
    /// Current number of tokens in the bucket (never exceeds `max_tokens`).
    /// Stored as `f64` to track fractional tokens from partial refill intervals.
    available_tokens: f64,
    /// Timestamp of the most recent token refill.
    last_refill: Instant,
}

// ── RateLimiter ────────────────────────────────────────────────────────────

/// Token-bucket rate limiter that controls how often an operation may execute.
///
/// Tokens are replenished at `refill_rate` intervals up to `max_tokens`.
/// Each call to [`try_consume`](RateLimiter::try_consume) deducts one token
/// (or more, if requested).  When no tokens remain, requests are rejected.
///
/// All clones of a `RateLimiter` share the same underlying bucket and are
/// safe to use from multiple threads concurrently.
///
/// # Defaults
///
/// | Field          | Default |
/// |----------------|---------|
/// | `max_tokens`   | 10      |
/// | `refill_rate`  | 1 s     |
///
/// # Example
///
/// ```ignore
/// use resilient::rate_limit::RateLimiter;
/// use std::time::Duration;
///
/// let rl = RateLimiter::default()
///     .with_max_tokens(100)
///     .with_refill_rate(Duration::from_secs(1));
///
/// rl.call(&mut || my_operation()).await;
/// ```
#[derive(Clone)]
pub struct RateLimiter {
    /// Maximum number of tokens the bucket can hold.
    pub max_tokens: usize,
    /// Time between individual token refills (one token added per period).
    pub refill_rate: Duration,
    /// Shared mutable bucket state.
    inner: Arc<Mutex<Inner>>,
}

// ── Default ────────────────────────────────────────────────────────────────

impl Default for RateLimiter {
    fn default() -> Self {
        let now = Instant::now();
        Self {
            max_tokens: 10,
            refill_rate: Duration::from_secs(1),
            inner: Arc::new(Mutex::new(Inner {
                available_tokens: 10.0,
                last_refill: now,
            })),
        }
    }
}

// ── Builder methods & public API ──────────────────────────────────────────

impl RateLimiter {
    /// Sets the maximum bucket capacity.
    ///
    /// This is the upper bound on how many tokens can accumulate.  Excess
    /// tokens from refilling beyond this cap are discarded.
    pub fn with_max_tokens(mut self, max: usize) -> Self {
        self.max_tokens = max;
        self
    }

    /// Sets the refill rate — one token is added every `rate` duration.
    ///
    /// For example, `Duration::from_millis(200)` would add one token every
    /// 200 ms (5 tokens per second).
    pub fn with_refill_rate(mut self, rate: Duration) -> Self {
        self.refill_rate = rate;
        self
    }

    /// Attempts to consume `tokens` from the bucket.
    ///
    /// 1. Refills the bucket based on elapsed time since the last refill.
    /// 2. If at least `tokens` are available, deducts them and returns `true`.
    /// 3. Otherwise returns `false` (the bucket state is still updated with
    ///    any refilled tokens so the caller can inspect the new level).
    pub fn try_consume(&self, tokens: usize) -> bool {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        let elapsed = now
            .checked_duration_since(inner.last_refill)
            .unwrap_or(Duration::ZERO);
        let tokens_to_add = elapsed.as_secs_f64() / self.refill_rate.as_secs_f64();

        inner.available_tokens =
            (inner.available_tokens + tokens_to_add).min(self.max_tokens as f64);
        inner.last_refill = now;

        if inner.available_tokens >= tokens as f64 {
            inner.available_tokens -= tokens as f64;
            true
        } else {
            false
        }
    }

    /// Returns the current number of available tokens.
    ///
    /// This is a snapshot — the value may change immediately after the call
    /// returns due to concurrent access from another task.
    pub fn available_tokens(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .available_tokens as usize
    }

    /// Creates a new handle that shares the same underlying bucket state.
    ///
    /// Cheap — only an `Arc` pointer is copied.
    pub fn clone_inner(&self) -> Self {
        self.clone()
    }
}

// ── Policy trait implementation ────────────────────────────────────────────

impl<T, E> Policy<T, E> for RateLimiter
where
    E: From<RateLimitError>,
{
    /// Executes the operation through the rate limiter.
    ///
    /// 1. Attempts to consume one token via [`try_consume`](RateLimiter::try_consume).
    /// 2. On success, runs the wrapped operation and returns its result.
    /// 3. On failure (no tokens), returns [`RateLimitError::RateLimited`].
    fn call<F, Fut>(&self, f: &mut F) -> impl Future<Output = Result<T, E>> + Send
    where
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<T, E>> + Send,
        T: Send,
        E: Send,
    {
        let this = self.clone();

        async move {
            if !this.try_consume(1) {
                return Err(RateLimitError::RateLimited.into());
            }
            f().await
        }
    }
}

// ── Send / Sync assertion ─────────────────────────────────────────────────

/// Compile-time check that `RateLimiter` implements `Send`.
fn _assert_send() {
    fn is_send<T: Send>() {}
    is_send::<RateLimiter>();
}
