use resilient::RetryPolicy;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

#[tokio::main]
async fn main() {
    let policy = RetryPolicy::default().with_max_retries(3);
    let attempts = Arc::new(AtomicU32::new(0));

    let result: Result<String, String> = policy
        .run(|| {
            let a = attempts.clone();
            async move {
                let n = a.fetch_add(1, Ordering::Relaxed);
                if n < 2 {
                    Err(format!("attempt {} failed", n + 1))
                } else {
                    Ok("finally succeeded!".to_string())
                }
            }
        })
        .await;

    println!("{:?}", result);
}
