use resilient::timeout::TimeoutPolicy;
use std::time::Duration;

#[tokio::test]
async fn timeout_policy_times_out() {
    let policy = TimeoutPolicy::default().with_timeout(Duration::from_millis(1));

    let result = policy
        .run(&mut || async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            Ok::<_, resilient::timeout::TimeoutError>("ok")
        })
        .await;

    assert!(result.is_err(), "Expected timeout policy to return an error");
}
