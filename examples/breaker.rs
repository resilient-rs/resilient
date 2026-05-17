use resilient::BreakerPolicy;

#[tokio::main]
async fn main() {
    let policy = BreakerPolicy::default().with_failure_threshold(3);

    for i in 0..6 {
        let result: Result<String, String> = policy
            .run(|| async {
                Err::<String, _>("something went wrong".to_string())
            })
            .await;
        println!("call {}: {:?}", i + 1, result);
    }
}
