mod common;

use anyhow::Result;
use common::{find_leader, rafters::IntegerArg, start_frontend, ClusterGuard};
use tokio::time::Duration;
use tonic::Request;

#[tokio::test]
async fn test_cluster_startup() -> Result<()> {
    let (child, mut client) = start_frontend().await;
    let _guard = ClusterGuard { frontend: child };

    let n = 5;

    let mut startup_request = Request::new(IntegerArg { arg: n });
    startup_request.set_timeout(Duration::from_millis(1000));
    client.start_raft(startup_request).await?;

    tokio::time::sleep(Duration::from_secs(3)).await;

    let leader = find_leader(n).await?;

    println!("Got raft leader as {}", leader);

    Ok(())
}
