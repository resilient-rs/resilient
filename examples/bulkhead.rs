use resilient::Bulkhead;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Barrier;

#[tokio::main]
async fn main() {
    let bulkhead = Bulkhead::default().with_max_concurrent(2);
    let gate = Arc::new(Barrier::new(3));

    let mut handles = Vec::new();
    for i in 0..2 {
        let bulkhead = bulkhead.clone();
        let gate = gate.clone();
        handles.push(tokio::spawn(async move {
            let result: Result<String, String> = bulkhead
                .run(|| {
                    let gate = gate.clone();
                    async move {
                        println!("call {}: started", i + 1);
                        gate.wait().await;
                        Ok(format!("call {} done", i + 1))
                    }
                })
                .await;
            println!("call {}: {:?}", i + 1, result);
            result
        }));
    }

    tokio::time::sleep(Duration::from_millis(50)).await;

    let rejected: Result<String, String> = bulkhead
        .run(|| async { Ok("should not run".to_string()) })
        .await;
    println!("call 3 (rejected): {:?}", rejected);

    gate.wait().await;
    for handle in handles {
        let _ = handle.await;
    }
}
