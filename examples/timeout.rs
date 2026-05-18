use resilient::{pipeline::Pipeline, TimeoutPolicy};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() {
    let timeout = TimeoutPolicy::default().with_timeout(Duration::from_secs(1));
    let pipeline = Pipeline::default().with_timeout(timeout);

    let start = std::time::Instant::now();
    let result: Result<String, Box<dyn std::error::Error + Send + Sync>> = pipeline
        .run(|| async {
            sleep(Duration::from_secs(3)).await;
            Ok("done".to_string())
        })
        .await;

    let elapsed = start.elapsed();
    println!("result: {:?}", result);
    println!("elapsed: {:?} (should be ~1 second)", elapsed);
}
