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
use rafters::key_value_store_client::KeyValueStoreClient;
use rafters::{GetKey, IntegerArg, KeyValue, Reply};

type ChildMap = HashMap<i32, (std::process::Child, KeyValueStoreClient<Channel>)>;

struct FrontendState {
    servers: ChildMap,
    current_leader: i32,
}

impl FrontendState {
    fn new() -> Self {
        Self {
            servers: ChildMap::new(),
            current_leader: 0,
        }
    }
}

pub struct RaftersFrontend {
    state: Arc<Mutex<FrontendState>>,
}

impl RaftersFrontend {
    fn new(state: Arc<Mutex<FrontendState>>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl Frontend for RaftersFrontend {
    async fn get(&self, request: Request<GetKey>) -> Result<Response<Reply>, Status> {
        // ask leader for info
        todo!()
    }

    async fn put(&self, request: Request<KeyValue>) -> Result<Response<Reply>, Status> {
        // tell leader
        todo!()
    }

    async fn replace(&self, request: Request<KeyValue>) -> Result<Response<Reply>, Status> {
        // tell leader
        todo!()
    }

    async fn start_raft(&self, request: Request<IntegerArg>) -> Result<Response<Reply>, Status> {
        let child_binary = if cfg!(debug_assertions) {
            "target/debug/raftserver"
        } else {
            "target/release/raftserver"
        };
        let mut state = self.state.lock().await;
        let num_servers = request.into_inner().arg;
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
            let client = KeyValueStoreClient::connect(endpoint)
                .await
                .unwrap_or_else(|_| {
                    error!("Couldn't connect to raft node {}", i);
                    panic!()
                });
            state.servers.insert(i, (child, client));
        }
        for (num, (_, client)) in state.servers.iter_mut() {
            for i in 1..=num_servers {
                if i != *num {
                    client.add_server(IntegerArg { arg: i }).await?;
                }
            }
        }
        Ok(Response::new(Reply {
            wrong_leader: false,
            error: String::from(""),
            value: String::from(""),
        }))
    }
}
// TODO: track some data by periodically getting all node states to find which is the leader, maybe
// once every 3-4 seconds?

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let env = env_logger::Env::default().default_filter_or("info");
    env_logger::init_from_env(env);

    let addr = "[::1]:8001".parse().unwrap();
    let frontend_state = Arc::new(Mutex::new(FrontendState::new()));
    let frontend = RaftersFrontend::new(frontend_state);

    info!("Starting frontend. Listening on {}", addr);

    Server::builder()
        .add_service(FrontendServer::new(frontend))
        .serve(addr)
        .await?;

    Ok(())
}
