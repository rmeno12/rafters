mod common;

use anyhow::Result;
use common::TestCluster;
use tokio::time::Duration;

#[tokio::test]
async fn test_cluster_startup() -> Result<()> {
    let mut cluster = TestCluster::new();
    let mut client = cluster.start_frontend().await?;

    cluster.start_cluster(5, &mut client).await?;

    tokio::time::sleep(Duration::from_secs(3)).await;

    let leader = cluster.find_leader().await?;

    println!("Got raft leader as {}", leader);

    Ok(())
}
