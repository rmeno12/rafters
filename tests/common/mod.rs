pub mod rafters {
    tonic::include_proto!("raftkv");
}

use anyhow::{Context, Result};
use std::{
    collections::HashMap,
    process::{Child, Command},
};
use thiserror::Error;
use tokio::time::Duration;
use tonic::transport::Channel;
use tonic::Request;

use rafters::front_end_client::FrontEndClient;
use rafters::key_value_store_client::KeyValueStoreClient;
use rafters::{Empty, IntegerArg};

#[derive(Debug, Error)]
enum RaftersTestingError {
    #[error("no leader found in cluster")]
    NoLeader,
    #[error("multiple leaders found in cluster in latest term")]
    MultipleLeaders,
}

pub struct TestCluster {
    num_servers: i32,
    frontend: Option<Child>,
}

impl Drop for TestCluster {
    fn drop(&mut self) {
        if let Some(ref mut child) = &mut self.frontend {
            if let Err(e) = Command::new("pkill").arg("raftserver").status() {
                eprintln!("Unable to kill raftservers: {}", e);
            }
            if let Err(e) = child.kill() {
                eprintln!("Unable to kill frontend: {}", e);
            }
        }
    }
}

impl TestCluster {
    pub fn new() -> Self {
        Self {
            num_servers: 0,
            frontend: None,
        }
    }

    pub async fn start_frontend(&mut self) -> Result<FrontEndClient<Channel>> {
        let child = Command::new("target/debug/frontend")
            .spawn()
            .context("Failed to start frontend")?;
        self.frontend = Some(child);

        // wait a tiny bit for the frontend to actually start
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let client = FrontEndClient::connect("http://127.0.0.1:8001")
            .await
            .context("Failed to connect to frontend server")?;

        Ok(client)
    }

    pub async fn start_cluster(
        &mut self,
        num_servers: i32,
        client: &mut FrontEndClient<Channel>,
    ) -> Result<()> {
        let mut startup_request = Request::new(IntegerArg { arg: num_servers });
        startup_request.set_timeout(Duration::from_millis(1000));
        client.start_raft(startup_request).await?;
        self.num_servers = num_servers;

        Ok(())
    }

    pub async fn find_leader(&self) -> Result<i32> {
        let mut terms_to_leaders: HashMap<i32, Vec<i32>> = HashMap::new();
        for id in 1..=self.num_servers {
            let mut client =
                match KeyValueStoreClient::connect(format!("http://127.0.0.1:{}", 9000 + id)).await
                {
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
}
