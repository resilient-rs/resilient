use resilient::pipeline::Pipeline;
use resilient::timeout::Builder as TimeoutBuilder;
use resilient::BreakerPolicy;
use resilient::Bulkhead;
use resilient::RateLimiter;
use resilient::RetryPolicy;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Resilient Library Example ===\n");

    let _attempt_count = Arc::new(AtomicU32::new(0));

    let retry_policy = RetryPolicy::default();
    let timeout_policy = TimeoutBuilder::new().with_timeout_secs(5).build();
    let breaker_policy = BreakerPolicy::default();
    let rate_limiter = RateLimiter::default();
    let bulkhead = Bulkhead::default();

    let pipeline = Pipeline::new()
        .with_retry(retry_policy)
        .with_timeout(timeout_policy)
        .with_circuit_breaker(breaker_policy)
        .with_rate_limiter(rate_limiter)
        .with_bulkhead(bulkhead);

    println!("Example 1: Successful operation");
    let result: Result<String, Box<dyn std::error::Error + Send + Sync>> = pipeline
        .run(|| async {
            println!("  Executing operation...");
            Ok("Success!".to_string())
        })
        .await;
    println!("  Result: {:?}\n", result);

    println!("Example 2: Operation with retries (fails then succeeds)");
    let attempts = Arc::new(AtomicU32::new(0));
    let attempts_clone = attempts.clone();

    let result = pipeline
        .run({
            let attempts_clone = attempts_clone;
            move || {
                let attempts_clone = attempts_clone.clone();
                async move {
                    let count = attempts_clone.fetch_add(1, Ordering::Relaxed);
                    if count < 2 {
                        println!("  Attempt {}: Failed", count + 1);
                        Err(Box::new(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "Temporary error",
                        ))
                            as Box<dyn std::error::Error + Send + Sync>)
                    } else {
                        println!("  Attempt {}: Success!", count + 1);
                        Ok("Recovered!".to_string())
                    }
                }
            }
        })
        .await;
    println!("  Result: {:?}\n", result);

    println!("Example 3: Multiple requests (rate limited)");
    for i in 1..=3 {
        let result: Result<String, Box<dyn std::error::Error + Send + Sync>> = pipeline
            .run(move || async move {
                println!("  Request {}: processed", i);
                Ok(format!("Response {}", i))
            })
            .await;
        println!("  Result: {:?}", result);
    }

    println!("\n=== Example completed successfully! ===");
    Ok(())
}
