pub mod rafters {
    tonic::include_proto!("raftkv");
}

use anyhow::Result;
use std::{
    collections::HashMap,
    process::{Child, Command},
};
use tonic::transport::Channel;

use rafters::front_end_client::FrontEndClient;
use rafters::key_value_store_client::KeyValueStoreClient;
use rafters::Empty;

#[derive(Debug)]
enum RaftersTestingError {
    NoLeader,
    MultipleLeaders,
}

impl std::error::Error for RaftersTestingError {}

impl std::fmt::Display for RaftersTestingError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            RaftersTestingError::NoLeader => {
                write!(f, "No leader found in cluster")
            }
            RaftersTestingError::MultipleLeaders => {
                write!(f, "Multiple leaders found in cluster for latest term")
            }
        }
    }
}

pub struct ClusterGuard {
    pub frontend: Child,
}

impl Drop for ClusterGuard {
    fn drop(&mut self) {
        if let Err(e) = Command::new("pkill").arg("raftserver").status() {
            eprintln!("Unable to kill raftservers: {}", e);
        }
        if let Err(e) = self.frontend.kill() {
            eprintln!("Unable to kill frontend: {}", e);
        }
    }
}

pub async fn start_frontend() -> (Child, FrontEndClient<Channel>) {
    let child = Command::new("target/debug/frontend")
        .spawn()
        .expect("Failed to start frontend");
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let client = FrontEndClient::connect("http://127.0.0.1:8001")
        .await
        .expect("Failed to connect to frontend server");

    (child, client)
}

pub async fn find_leader(num_servers: i32) -> Result<i32> {
    let mut terms_to_leaders: HashMap<i32, Vec<i32>> = HashMap::new();
    for id in 1..=num_servers {
        let mut client =
            match KeyValueStoreClient::connect(format!("http://127.0.0.1:{}", 9000 + id)).await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("Unable to connect to Raft server {}. Err: {}", id, e);
                    continue;
                }
            };
        let node_status = match client.get_state(Empty {}).await {
            Ok(resp) => resp.into_inner(),
            Err(e) => {
                eprintln!("Unable to query node {} for state. Err: {}", id, e);
                continue;
            }
        };
        if node_status.is_leader {
            terms_to_leaders
                .entry(node_status.term)
                .or_default()
                .push(id);
        }
    }

    let last_term = terms_to_leaders.keys().copied().max().unwrap_or_default();
    if last_term == 0 {
        Err(RaftersTestingError::NoLeader.into())
    } else if terms_to_leaders[&last_term].len() != 1 {
        Err(RaftersTestingError::MultipleLeaders.into())
    } else {
        Ok(terms_to_leaders[&last_term][0])
    }
}
