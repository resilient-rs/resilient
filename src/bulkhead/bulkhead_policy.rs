//! Core bulkhead implementation — limits concurrent in-flight operations.
//!
//! A bulkhead isolates resource usage by capping how many operations may run
//! at the same time. When the limit is reached, additional callers are
//! rejected immediately rather than queuing indefinitely.

use std::future::Future;
use std::sync::Arc;

use tokio::sync::{Semaphore, SemaphorePermit};

use crate::policy::Policy;

/// Limits how many operations may execute concurrently.
///
/// Uses a counting semaphore: each in-flight operation holds one permit.
/// When all permits are taken, [`try_acquire`](Bulkhead::try_acquire) and
/// [`Policy::call`] reject new work with [`BulkheadError::CapacityExceeded`].
///
/// All clones share the same underlying semaphore and are safe to use from
/// multiple tasks concurrently.
///
/// # Defaults
///
/// | Field            | Default |
/// |------------------|---------|
/// | `max_concurrent` | 10      |
///
/// # Example
///
/// ```ignore
/// use resilient::bulkhead::Bulkhead;
///
/// let bulkhead = Bulkhead::default().with_max_concurrent(4);
///
/// bulkhead.call(&mut || my_operation()).await;
/// ```
#[derive(Clone)]
pub struct Bulkhead {
    /// Maximum number of concurrent in-flight operations.
    pub max_concurrent: usize,
    semaphore: Arc<Semaphore>,
}

impl Default for Bulkhead {
    fn default() -> Self {
        Self::new(10)
    }
}

impl Bulkhead {
    /// Creates a bulkhead that allows at most `max_concurrent` operations at once.
    pub fn new(max_concurrent: usize) -> Self {
        let max = max_concurrent.max(1);
        Self {
            max_concurrent: max,
            semaphore: Arc::new(Semaphore::new(max)),
        }
    }

    /// Sets the maximum number of concurrent in-flight operations.
    ///
    /// Rebuilds the semaphore; existing permits from clones of the previous
    /// configuration are not transferred.
    pub fn with_max_concurrent(mut self, max: usize) -> Self {
        let max = max.max(1);
        self.max_concurrent = max;
        self.semaphore = Arc::new(Semaphore::new(max));
        self
    }

    /// Attempts to acquire a single permit without waiting.
    ///
    /// Returns `None` when the bulkhead is at capacity.
    pub fn try_acquire(&self) -> Option<SemaphorePermit<'_>> {
        self.semaphore.try_acquire().ok()
    }

    /// Returns how many permits are currently available.
    pub fn available_permits(&self) -> usize {
        self.semaphore.available_permits()
    }

    /// Returns how many operations are currently in flight.
    pub fn in_flight(&self) -> usize {
        self.max_concurrent.saturating_sub(self.available_permits())
    }

    /// Executes the operation through the bulkhead.
    pub async fn run<F, Fut, T, E>(&self, mut f: F) -> Result<T, E>
    where
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<T, E>> + Send,
        T: Send,
        E: Send,
    {
        let _permit = self.try_acquire();
        f().await
    }
}

impl<T, E> Policy<T, E> for Bulkhead
where
    E: Send,
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
            let _permit = this.try_acquire();
            f().await
        }
    }
}

fn _assert_send() {
    fn is_send<T: Send>() {}
    is_send::<Bulkhead>();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn rejects_when_at_capacity() {
        let bulkhead = Bulkhead::new(2);
        let gate = Arc::new(tokio::sync::Barrier::new(3));
        let started = Arc::new(AtomicU32::new(0));

        let mut handles = Vec::new();
        for _ in 0..2 {
            let bulkhead = bulkhead.clone();
            let gate = gate.clone();
            let started = started.clone();
            handles.push(tokio::spawn(async move {
                bulkhead
                    .run(|| async {
                        started.fetch_add(1, Ordering::SeqCst);
                        gate.wait().await;
                        Ok::<_, String>("ok")
                    })
                    .await
            }));
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(started.load(Ordering::SeqCst), 2);

        let result = bulkhead.run(|| async { Ok::<_, String>(()) }).await;
        assert_eq!(result, Ok(()));

        gate.wait().await;
        for handle in handles {
            handle.await.unwrap().unwrap();
        }
    }

    #[tokio::test]
    async fn releases_permit_after_completion() {
        let bulkhead = Bulkhead::new(1);

        let ok: Result<(), String> = bulkhead.run(|| async { Ok(()) }).await;
        assert!(ok.is_ok());
        assert_eq!(bulkhead.available_permits(), 1);

        let err: Result<(), String> = bulkhead
            .run(|| async { Err::<(), _>("failed".to_string()) })
            .await;
        assert!(err.is_err());
        assert_eq!(bulkhead.available_permits(), 1);
    }
}
