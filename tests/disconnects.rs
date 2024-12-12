mod common;

use anyhow::Result;
use common::{
    rafters::{GetKey, KeyValue},
    TestCluster,
};
use serial_test::serial;
use tokio::time::Duration;

#[tokio::test]
#[serial]
async fn test_disconnect_nonleader() -> Result<()> {
    let mut cluster = TestCluster::new();
    let mut client = cluster.start_frontend().await?;
    cluster.start_cluster(5, &mut client).await?;

    tokio::time::sleep(Duration::from_secs(3)).await;

    let leader = cluster.find_leader().await?;

    let to_disconnect = if leader == 1 { 2 } else { 1 };

    println!("Disconnecting node {}", to_disconnect);
    for i in 1..=5 {
        if i != to_disconnect {
            cluster.block_conn(to_disconnect, i);
            cluster.block_conn(i, to_disconnect);
        }
    }
    println!("Disconnected");

    // should not need any delay here
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

    println!("Reconnecting node {}", to_disconnect);
    for i in 1..=5 {
        if i != to_disconnect {
            cluster.unblock_conn(to_disconnect, i);
            cluster.unblock_conn(i, to_disconnect);
        }
    }

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_disconnect_leader() -> Result<()> {
    let mut cluster = TestCluster::new();
    let mut client = cluster.start_frontend().await?;
    cluster.start_cluster(5, &mut client).await?;

    tokio::time::sleep(Duration::from_secs(3)).await;

    let to_disconnect = cluster.find_leader().await?;

    println!("Disconnecting node {}", to_disconnect);
    for i in 1..=5 {
        if i != to_disconnect {
            cluster.block_conn(to_disconnect, i);
            cluster.block_conn(i, to_disconnect);
        }
    }
    println!("Disconnected");

    // wait a little for the new leader election to happen
    tokio::time::sleep(Duration::from_millis(1000)).await;

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

    println!("Reconnecting node {}", to_disconnect);
    for i in 1..=5 {
        if i != to_disconnect {
            cluster.unblock_conn(to_disconnect, i);
            cluster.unblock_conn(i, to_disconnect);
        }
    }

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_disconnect_less_than_quorum() -> Result<()> {
    let mut cluster = TestCluster::new();
    let mut client = cluster.start_frontend().await?;
    cluster.start_cluster(5, &mut client).await?;

    tokio::time::sleep(Duration::from_secs(3)).await;

    for to_disconnect in 1..=2 {
        println!("Disconnecting node {}", to_disconnect);
        for i in 1..=5 {
            if i != to_disconnect {
                cluster.block_conn(to_disconnect, i);
                cluster.block_conn(i, to_disconnect);
            }
        }
    }
    println!("Disconnected");

    // wait a little for the new leader election to happen
    tokio::time::sleep(Duration::from_millis(1000)).await;

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

    for to_disconnect in 1..=2 {
        println!("Reconnecting node {}", to_disconnect);
        for i in 1..=5 {
            if i != to_disconnect {
                cluster.unblock_conn(to_disconnect, i);
                cluster.unblock_conn(i, to_disconnect);
            }
        }
    }

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_disconnect_quorum() -> Result<()> {
    let mut cluster = TestCluster::new();
    let mut client = cluster.start_frontend().await?;
    cluster.start_cluster(5, &mut client).await?;

    tokio::time::sleep(Duration::from_secs(3)).await;

    for to_disconnect in 1..=3 {
        println!("Disconnecting node {}", to_disconnect);
        for i in 1..=5 {
            if i != to_disconnect {
                cluster.block_conn(to_disconnect, i);
                cluster.block_conn(i, to_disconnect);
            }
        }
    }
    println!("Disconnected");

    // wait a little for the new leader election to happen
    tokio::time::sleep(Duration::from_millis(1000)).await;

    let key = String::from("abcd");
    let value = String::from("1234");

    client
        .put(KeyValue {
            key: key.clone(),
            value: value.clone(),
            client_id: 0,
            request_id: 0,
        })
        .await
        .unwrap_err();

    for to_disconnect in 1..=3 {
        println!("Reconnecting node {}", to_disconnect);
        for i in 1..=5 {
            if i != to_disconnect {
                cluster.unblock_conn(to_disconnect, i);
                cluster.unblock_conn(i, to_disconnect);
            }
        }
    }

    Ok(())
}
