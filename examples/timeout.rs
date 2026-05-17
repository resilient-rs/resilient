use resilient::TimeoutPolicy;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() {
    let policy = TimeoutPolicy::default().with_timeout(Duration::from_secs(1));

    let result: Result<String, String> = policy
        .run(|| async {
            sleep(Duration::from_secs(3)).await;
            Ok::<_, String>("done".to_string())
        })
        .await;

    println!("{:?}", result);
}
