//! Simple async runner that executes a closure and awaits its future.
//! This acts as the base execution layer before any policies are applied.

use std::future::Future;

/// Runs an async closure by calling it and awaiting the returned future.
///
/// # Type Parameters
/// - `F`: A closure that takes no arguments and returns a future.
/// - `Fut`: The future type produced by `F`.
/// - `T`: The output type of the future (and therefore the return type).
///
/// # Arguments
/// * `f` - The closure to invoke.
///
/// # Returns
/// The value produced by the future.
pub async fn run<F, Fut, T>(f: F) -> T
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = T>,
{
    f().await
}
