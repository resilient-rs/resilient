//! Policy trait — the core abstraction that all resilience strategies implement.
//!
//! A Policy wraps an async operation and adds cross-cutting behavior such as
//! retrying on failure, enforcing timeouts, or breaking the circuit.

use std::future::Future;

/// Defines the interface for a resilience policy.
///
/// # Type Parameters
/// - `T`: The success type of the wrapped operation.
/// - `E`: The error type of the wrapped operation.
///
/// Implementors provide custom logic that runs before, after, or around
/// the operation (e.g., retrying on failure, measuring latency, etc.).
pub trait Policy<T, E> {
    /// Executes the given operation through this policy.
    ///
    /// # Arguments
    /// * `f` - A mutable reference to a callable that returns a future.
    ///   `FnMut` is required because policies like retry may call `f`
    ///   multiple times.
    ///
    /// # Returns
    /// A future that resolves to `Ok(T)` on success or `Err(E)` on failure.
    fn call<F, Fut>(&self, f: &mut F) -> impl Future<Output = Result<T, E>> + Send
    where
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<T, E>> + Send,
        T: Send,
        E: Send;
}
