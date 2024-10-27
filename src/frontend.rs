use log::{error, info, warn};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{
    transport::{Channel, Endpoint, Server},
    Request, Response, Status,
};

pub mod rafters {
    tonic::include_proto!("rafters");
}

use rafters::frontend_server::{Frontend, FrontendServer};
use rafters::raft_client::RaftClient;
use rafters::{Empty, IntegerArg, KeyValue, State};

type ChildMap = HashMap<i32, (std::process::Child, RaftClient<Channel>)>;

pub struct RaftersFrontend {
    term: i32,
    servers: Arc<Mutex<ChildMap>>,
}

impl Default for RaftersFrontend {
    fn default() -> Self {
        Self {
            term: -1,
            servers: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[tonic::async_trait]
impl Frontend for RaftersFrontend {
    async fn get_state(&self, _request: Request<Empty>) -> Result<Response<State>, Status> {
        Ok(Response::new(State {
            term: self.term,
            is_leader: false,
        }))
    }

    async fn start_raft(&self, request: Request<IntegerArg>) -> Result<Response<Empty>, Status> {
        let child_binary = if cfg!(debug_assertions) {
            "target/debug/raftserver"
        } else {
            "target/release/raftserver"
        };
        let num_servers = request.into_inner().arg;
        let mut servers = self.servers.lock().await;
        let children: Vec<std::process::Child> = (1..=num_servers)
            .map(|i| {
                let child = std::process::Command::new(child_binary)
                    .arg(i.to_string())
                    .spawn()
                    .unwrap_or_else(|_| panic!("Couldn't start raft node {}", i));
                info!("Started child raft node {} (pid {})", i, child.id());
                child
            })
            .collect();
        // wait for raft nodees to come online before sending them messages
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        for (i, child) in (1..=num_servers).zip(children) {
            let endpoint = Endpoint::from_shared(format!("http://[::1]:{}", 9000 + i)).unwrap();
            info!("Connecting to node {} on {:?}", i, endpoint.uri());
            let client = RaftClient::connect(endpoint).await.unwrap_or_else(|_| {
                error!("Couldn't connect to raft node {}", i);
                panic!()
            });
            servers.insert(i, (child, client));
        }
        for (num, (_, client)) in servers.iter_mut() {
            for i in 1..=num_servers {
                if i != *num {
                    client.add_server(IntegerArg { arg: i }).await?;
                }
            }
        }
        Ok(Response::new(Empty {}))
    }

    async fn add_server(&self, request: Request<IntegerArg>) -> Result<Response<Empty>, Status> {
        todo!()
    }

    async fn remove_server(&self, request: Request<IntegerArg>) -> Result<Response<Empty>, Status> {
        todo!()
    }

    async fn get(&self, request: Request<IntegerArg>) -> Result<Response<KeyValue>, Status> {
        // ask leader for info
        todo!()
    }

    async fn put(&self, request: Request<KeyValue>) -> Result<Response<Empty>, Status> {
        // tell leader
        todo!()
    }

    async fn replace(&self, request: Request<KeyValue>) -> Result<Response<Empty>, Status> {
        // tell leader
        todo!()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let env = env_logger::Env::default().default_filter_or("info");
    env_logger::init_from_env(env);

    let addr = "[::1]:8001".parse().unwrap();
    let frontend = RaftersFrontend::default();

    info!("Starting frontend. Listening on {}", addr);

    Server::builder()
        .add_service(FrontendServer::new(frontend))
        .serve(addr)
        .await?;

    Ok(())
}
