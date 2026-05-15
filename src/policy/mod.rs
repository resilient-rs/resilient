use std::future::Future;

/// Policy is the core trait that all resilience policies implement.
/// Each policy wraps an async operation and applies its own logic (retry, timeout, etc.).
///
/// # Type Parameters
/// - `T`: The success type returned by the operation
/// - `E`: The error type returned when the operation fails
///
/// # Generic Parameters
/// - `F`: The callable that produces the future (e.g., a closure)
/// - `Fut`: The future returned by calling `F`
///
/// # Example
/// A retry policy might call the operation multiple times on failure,
/// while a timeout policy might enforce a time limit.
pub trait Policy<T, E> {
    /// Executes the given operation through this policy.
    ///
    /// # Arguments
    /// * `f` - A mutable reference to a callable that returns a future
    ///
    /// # Returns
    /// A future that resolves to `Result<T, E>`:
    /// - `Ok(T)` when the operation succeeds
    /// - `Err(E)` when the operation fails (after all retries, etc.)
    fn call<F, Fut>(&self, f: &mut F) -> impl Future<Output = Result<T, E>> + Send
    where
        F: FnMut() -> Fut + Send,         // FnMut because we need to call multiple times
        Fut: Future<Output = Result<T, E>> + Send,  // The future must be Send
        T: Send,  // Success type must be Send
        E: Send;  // Error type must be Send
}
