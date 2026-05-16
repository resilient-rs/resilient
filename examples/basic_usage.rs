use resilient::pipeline::Pipeline;
use resilient::retry_policy::RetryPolicy;
use resilient::timeout::Builder as TimeoutBuilder;
use resilient::circuit_breaker::BreakerPolicy;
use resilient::rate_limit::RateLimiter;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Resilient Library Example ===\n");

    // Create a simple counter to track attempts
    let _attempt_count = Arc::new(AtomicU32::new(0));
    
    // Create resilience policies
    let retry_policy = RetryPolicy::default();

    let timeout_policy = TimeoutBuilder::new()
        .with_timeout_secs(5)
        .build();

    let breaker_policy = BreakerPolicy::default();

    let rate_limiter = RateLimiter::default();

    // Create a pipeline combining all policies
    let pipeline = Pipeline::new()
        .with_retry(retry_policy)
        .with_timeout(timeout_policy)
        .with_circuit_breaker(breaker_policy)
        .with_rate_limiter(rate_limiter);

    // Example 1: Successful operation
    println!("Example 1: Successful operation");
    let result = pipeline.run(|| async {
        println!("  Executing operation...");
        Ok::<_, String>("Success!".to_string())
    })
    .await;
    println!("  Result: {:?}\n", result);

    // Example 2: Operation with retries
    println!("Example 2: Operation with retries (fails then succeeds)");
    let attempts = Arc::new(AtomicU32::new(0));
    let attempts_clone = attempts.clone();
    
    let result = pipeline.run(|| {
        let attempts = attempts_clone.clone();
        async move {
            let count = attempts.fetch_add(1, Ordering::Relaxed);
            if count < 2 {
                println!("  Attempt {}: Failed", count + 1);
                Err("Temporary error".to_string())
            } else {
                println!("  Attempt {}: Success!", count + 1);
                Ok::<_, String>("Recovered!".to_string())
            }
        }
    })
    .await;
    println!("  Result: {:?}\n", result);

    // Example 3: Rate limiting
    println!("Example 3: Multiple requests (rate limited)");
    for i in 1..=3 {
        let result = pipeline.run(|| async {
            println!("  Request {}: processed", i);
            Ok::<_, String>(format!("Response {}", i))
        })
        .await;
        println!("  Result: {:?}", result);
    }

    println!("\n=== Example completed successfully! ===");
    Ok(())
}
