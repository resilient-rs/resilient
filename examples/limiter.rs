use resilient::RateLimiter;
use std::time::Duration;

#[tokio::main]
async fn main() {
    let policy = RateLimiter::default()
        .with_max_tokens(3)
        .with_refill_rate(Duration::from_secs(10));

    for i in 0..6 {
        let result: Result<String, String> = policy
            .run(|| async {
                Ok::<_, String>("allowed".to_string())
            })
            .await;
        println!("call {}: {:?}", i + 1, result);
    }
}
