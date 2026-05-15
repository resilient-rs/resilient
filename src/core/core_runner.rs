use std::future::Future;
// This module provides the async runner implementation.

pub async fn run<F, Fut, T>(f: F) -> T
where
    // T = the output type of the future
    // Fut = the future type returned by f
    // F = the function type that returns Fut
    F: FnOnce() -> Fut,
    Fut: Future<Output = T>,
{
    // Call the function and await the result
    f().await
}
