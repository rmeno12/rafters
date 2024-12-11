mod common;

use anyhow::Result;
use common::{
    rafters::{GetKey, KeyValue},
    validate_series, TestCluster,
};
use serial_test::serial;
use tokio::time::Duration;

#[tokio::test]
#[serial]
async fn test_cluster_startup() -> Result<()> {
    let mut cluster = TestCluster::new();
    let mut client = cluster.start_frontend().await?;

    cluster.start_cluster(5, &mut client).await?;

    tokio::time::sleep(Duration::from_secs(3)).await;

    let leader = cluster.find_leader().await?;

    println!("Got raft leader as {}", leader);

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_single_put_get() -> Result<()> {
    let mut cluster = TestCluster::new();
    let mut client = cluster.start_frontend().await?;
    cluster.start_cluster(5, &mut client).await?;

    tokio::time::sleep(Duration::from_secs(3)).await;

    let key = String::from("abcd");
    let value = String::from("1234");

    client
        .put(KeyValue {
            key: key.clone(),
            value: value.clone(),
            client_id: 0,
            request_id: 0,
        })
        .await?;

    let reply = client
        .get(GetKey {
            key,
            client_id: 0,
            request_id: 0,
        })
        .await?
        .into_inner();

    assert!(!reply.wrong_leader);
    assert_eq!(reply.value, value);

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_many_single_thread() -> Result<()> {
    let mut cluster = TestCluster::new();
    let mut client = cluster.start_frontend().await?;
    cluster.start_cluster(5, &mut client).await?;

    tokio::time::sleep(Duration::from_secs(3)).await;

    validate_series(client, 0, 50, 0).await
}

#[tokio::test]
#[serial]
async fn test_many_multi_thread() -> Result<()> {
    let mut cluster = TestCluster::new();
    let mut client = cluster.start_frontend().await?;
    cluster.start_cluster(5, &mut client).await?;

    tokio::time::sleep(Duration::from_secs(3)).await;

    let tasks: Vec<_> = (0..10)
        .map(|id: i64| {
            let task_client = client.clone();
            tokio::spawn(
                async move { validate_series(task_client, id * 25, (id + 1) * 25, id).await },
            )
        })
        .collect();

    let results = futures::future::join_all(tasks).await;
    for res in results {
        res??;
    }

    Ok(())
}
