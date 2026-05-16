use std::future::Future;

use crate::retry_policy::{RetryError, RetryPolicy};

/// Runs `operation` up to `policy.max_attempts` times, sleeping between failures.
///
/// `max_attempts` is the total number of tries (e.g. `3` = one initial call plus two retries).
pub async fn retry<F, Fut, T, E>(policy: &RetryPolicy, mut operation: F) -> Result<T, RetryError<E>>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let mut attempt = 0u32;

    loop {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(error) => {
                attempt += 1;
                if attempt >= policy.max_attempts {
                    return Err(RetryError::MaxRetriesExceeded {
                        max_attempts: policy.max_attempts,
                        source: error,
                    });
                }

                let delay = policy.delay_before_retry(attempt - 1);
                tokio::time::sleep(delay).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicU32, Ordering},
        },
        time::Duration,
    };

    use super::*;
    use crate::retry_policy::RetryPolicy;

    #[tokio::test]
    async fn succeeds_on_first_attempt() {
        let policy = RetryPolicy::exponential(3).build();
        let result = retry(&policy, || async { Ok::<_, u32>(42) }).await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn succeeds_after_transient_failures() {
        let policy = RetryPolicy::exponential(3)
            .base_delay(Duration::from_millis(1))
            .build();
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_clone = Arc::clone(&attempts);

        let result = retry(&policy, || {
            let attempts = Arc::clone(&attempts_clone);
            async move {
                let n = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                if n < 3 { Err("transient") } else { Ok("ok") }
            }
        })
        .await;

        assert_eq!(result.unwrap(), "ok");
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn returns_max_retries_exceeded() {
        let policy = RetryPolicy::exponential(2)
            .base_delay(Duration::from_millis(1))
            .build();
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_clone = Arc::clone(&attempts);

        let result: Result<&str, RetryError<&str>> = retry(&policy, || {
            let attempts = Arc::clone(&attempts_clone);
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err::<&str, _>("always fails")
            }
        })
        .await;

        let err = result.unwrap_err();
        assert!(matches!(
            err,
            RetryError::MaxRetriesExceeded {
                max_attempts: 2,
                source: "always fails",
            }
        ));
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }
}
