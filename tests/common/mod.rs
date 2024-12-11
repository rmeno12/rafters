use std::process::{Child, Command};

pub mod rafters {
    tonic::include_proto!("raftkv");
}

use rafters::front_end_client::FrontEndClient;
use tonic::transport::Channel;

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

pub fn kill_cluster(mut frontend: Child) -> Result<(), Box<dyn std::error::Error>> {
    Command::new("pkill").arg("raftserver").status()?;

    frontend.kill()?;

    Ok(())
}
