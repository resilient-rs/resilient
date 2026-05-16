use std::{future::Future, sync::Arc, time::Duration};

use futures_util::future::BoxFuture;

use crate::policy::Policy;
use crate::timeout::TimeoutError;

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Shared, boxed, async callback used by lifecycle hooks.
type Hook = Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>;

// ---------------------------------------------------------------------------
// TimeoutPolicy — the main policy struct
// ---------------------------------------------------------------------------

/// A resilience policy that enforces a timeout on every operation.
///
/// When the configured duration elapses before the operation completes,
/// the policy returns a [`TimeoutError::Elapsed`].
///
/// # Defaults
///
/// | Setting      | Default  |
/// |--------------|----------|
/// | duration     | 30 s     |
/// | cancel       | `true`   |
///
/// # Example
///
/// ```ignore
/// use resilient::timeout::TimeoutPolicy;
/// use std::time::Duration;
///
/// let policy = TimeoutPolicy::default()
///     .with_timeout_secs(5)
///     .with_cancel(true);
/// ```
#[derive(Clone)]
pub struct TimeoutPolicy {
    /// Maximum time allowed for the operation.
    pub(crate) duration: Duration,
    /// If `true`, the future is cancelled on timeout (uses
    /// `tokio::time::timeout`).
    pub(crate) cancel: bool,
    /// Optional name used in timeout error messages for easier
    /// debugging.
    pub(crate) name: Option<String>,
    /// Async callback invoked when a timeout occurs.
    pub(crate) on_timeout: Option<Hook>,
    /// Async callback invoked when the operation succeeds.
    pub(crate) on_success: Option<Hook>,
    /// Async callback invoked when the operation fails (before
    /// timeout).
    pub(crate) on_failure: Option<Hook>,
}

// ---------------------------------------------------------------------------
// Builder — separate construction type
// ---------------------------------------------------------------------------

/// Builder for [`TimeoutPolicy`].
///
/// Every field has a sensible default so you only need to set what
/// differs from the defaults shown below.
///
/// | Field          | Default  |
/// |----------------|----------|
/// | `duration`     | 30 s     |
/// | `cancel`       | `true`   |
///
/// # Example
///
/// ```ignore
/// use resilient::timeout::Builder;
/// use std::time::Duration;
///
/// let policy = Builder::new()
///     .with_timeout_secs(10)
///     .with_on_timeout(|| async { eprintln!("timed out!") })
///     .build();
/// ```
pub struct Builder {
    duration: Duration,
    cancel: bool,
    name: Option<String>,
    on_timeout: Option<Hook>,
    on_success: Option<Hook>,
    on_failure: Option<Hook>,
}

impl Builder {
    /// Creates a new `Builder` with the default settings.
    ///
    /// Defaults:
    /// - `duration`: 30 seconds
    /// - `cancel`: `true`
    pub fn new() -> Self {
        Self {
            duration: Duration::from_secs(30),
            cancel: true,
            name: None,
            on_timeout: None,
            on_success: None,
            on_failure: None,
        }
    }

    /// Sets the timeout duration from a [`std::time::Duration`].
    pub fn with_timeout(mut self, duration: Duration) -> Self {
        self.duration = duration;
        self
    }

    /// Sets the timeout duration in **milliseconds**.
    pub fn with_timeout_millis(mut self, millis: u64) -> Self {
        self.duration = Duration::from_millis(millis);
        self
    }

    /// Sets the timeout duration in **seconds**.
    pub fn with_timeout_secs(mut self, secs: u64) -> Self {
        self.duration = Duration::from_secs(secs);
        self
    }

    /// Sets the timeout duration in **minutes**.
    pub fn with_timeout_minutes(mut self, mins: u64) -> Self {
        self.duration = Duration::from_secs(mins * 60);
        self
    }

    /// Sets the timeout duration in **hours**.
    pub fn with_timeout_hours(mut self, hours: u64) -> Self {
        self.duration = Duration::from_secs(hours * 3600);
        self
    }

    /// Whether the underlying `tokio::time::timeout` mechanism is
    /// used to cancel the future on timeout.  Default: `true`.
    pub fn with_cancel(mut self, cancel: bool) -> Self {
        self.cancel = cancel;
        self
    }

    /// Attaches a human-readable name to the policy.
    ///
    /// The name appears inside the error message of
    /// [`TimeoutError::Elapsed`], which helps identify which policy
    /// triggered when multiple timeouts are in play.
    pub fn with_name(mut self, name: impl ToString) -> Self {
        self.name = Some(name.to_string());
        self
    }

    /// Registers an async callback that fires **on timeout**.
    ///
    /// The callback runs **after** the timeout error has occurred but
    /// before it is returned to the caller.  It receives no
    /// arguments; use it for logging, metrics, or side-effects.
    pub fn with_on_timeout<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.on_timeout = Some(Arc::new(move || -> BoxFuture<'static, ()> {
            Box::pin(f())
        }));
        self
    }

    /// Registers an async callback that fires **on success**.
    ///
    /// The callback runs after the inner future returns `Ok`.
    pub fn with_on_success<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.on_success = Some(Arc::new(move || -> BoxFuture<'static, ()> {
            Box::pin(f())
        }));
        self
    }

    /// Registers an async callback that fires **on failure** (non-timeout
    /// error).
    ///
    /// The callback runs after the inner future returns `Err` (but
    /// before the timeout would have fired).
    pub fn with_on_failure<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.on_failure = Some(Arc::new(move || -> BoxFuture<'static, ()> {
            Box::pin(f())
        }));
        self
    }

    /// Consumes the `Builder` and produces a ready-to-use
    /// [`TimeoutPolicy`].
    pub fn build(self) -> TimeoutPolicy {
        TimeoutPolicy {
            duration: self.duration,
            cancel: self.cancel,
            name: self.name,
            on_timeout: self.on_timeout,
            on_success: self.on_success,
            on_failure: self.on_failure,
        }
    }
}

// ---------------------------------------------------------------------------
// TimeoutPolicy convenience setters (builder-style on the struct itself)
// ---------------------------------------------------------------------------

impl TimeoutPolicy {
    /// Overrides the timeout duration from a [`std::time::Duration`].
    pub fn with_timeout(mut self, duration: Duration) -> Self {
        self.duration = duration;
        self
    }

    /// Overrides the timeout duration in **milliseconds**.
    pub fn with_timeout_millis(mut self, millis: u64) -> Self {
        self.duration = Duration::from_millis(millis);
        self
    }

    /// Overrides the timeout duration in **seconds**.
    pub fn with_timeout_secs(mut self, secs: u64) -> Self {
        self.duration = Duration::from_secs(secs);
        self
    }

    /// Overrides the timeout duration in **minutes**.
    pub fn with_timeout_minutes(mut self, mins: u64) -> Self {
        self.duration = Duration::from_secs(mins * 60);
        self
    }

    /// Overrides the timeout duration in **hours**.
    pub fn with_timeout_hours(mut self, hours: u64) -> Self {
        self.duration = Duration::from_secs(hours * 3600);
        self
    }
}

// ---------------------------------------------------------------------------
// Default implementations
// ---------------------------------------------------------------------------

impl Default for TimeoutPolicy {
    fn default() -> Self {
        Builder::new().build()
    }
}

impl Default for Builder {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Policy trait implementation
// ---------------------------------------------------------------------------

impl<T, E> Policy<T, E> for TimeoutPolicy
where
    E: From<TimeoutError>,
{
    fn call<F, Fut>(&self, f: &mut F) -> impl Future<Output = Result<T, E>> + Send
    where
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<T, E>> + Send,
        T: Send,
        E: Send,
    {
        let this = self.clone();

        async move {
            if this.cancel {
                match tokio::time::timeout(this.duration, f()).await {
                    Ok(Ok(val)) => {
                        // Operation completed within the deadline.
                        if let Some(ref cb) = this.on_success {
                            cb().await;
                        }
                        Ok(val)
                    }
                    Ok(Err(e)) => {
                        // Operation completed within the deadline
                        // but returned a user-level error.
                        if let Some(ref cb) = this.on_failure {
                            cb().await;
                        }
                        Err(e)
                    }
                    Err(_elapsed) => {
                        // Deadline was exceeded – fire the timeout
                        // hook and return an error.
                        if let Some(ref cb) = this.on_timeout {
                            cb().await;
                        }
                        Err(TimeoutError::Elapsed {
                            duration: this.duration,
                            name: this.name,
                        }
                        .into())
                    }
                }
            } else {
                // cancel is disabled – run the operation without any
                // timeout at all and just fire lifecycle hooks.
                let result = f().await;
                match &result {
                    Ok(_) => {
                        if let Some(ref cb) = this.on_success {
                            cb().await;
                        }
                    }
                    Err(_) => {
                        if let Some(ref cb) = this.on_failure {
                            cb().await;
                        }
                    }
                }
                result
            }
        }
    }
}
