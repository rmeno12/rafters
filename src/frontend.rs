use log::{error, info, warn};
use std::collections::HashMap;
use std::os::unix::process::CommandExt;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{timeout, Duration};
use tonic::{
    transport::{Channel, Endpoint, Server},
    Request, Response, Status,
};

pub mod rafters {
    tonic::include_proto!("raftkv");
}

use rafters::front_end_server::{FrontEnd, FrontEndServer};
use rafters::key_value_store_client::KeyValueStoreClient;
use rafters::{GetKey, IntegerArg, KeyValue, Reply};

type ChildMap = HashMap<i32, (std::process::Child, KeyValueStoreClient<Channel>)>;

const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("../proto/rafters_descriptor.pb");

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
impl FrontEnd for RaftersFrontend {
    async fn get(&self, request: Request<GetKey>) -> Result<Response<Reply>, Status> {
        // ask leader for info
        let mut state = self.state.lock().await;
        let leader_id = state.current_leader;
        if leader_id == 0 {
            return Err(Status::unavailable("Raft cluster not started yet!"));
        }
        let (_, leader_client) = &state.servers[&leader_id];
        let mut leader_client = leader_client.clone();
        let getkey = request.into_inner();
        match leader_client.get(Request::new(getkey.clone())).await {
            Ok(response) => {
                let response = response.into_inner();
                if response.wrong_leader {
                    // only retrying once to avoid looping a lot and save on complexity
                    let new_leader: i32 = response.value.parse().unwrap();
                    state.current_leader = new_leader;
                    state.servers[&new_leader]
                        .1
                        .clone()
                        .get(Request::new(getkey))
                        .await
                } else {
                    Ok(Response::new(response))
                }
            }
            Err(err) => Err(Status::internal(err.to_string())),
        }
    }

    async fn put(&self, request: Request<KeyValue>) -> Result<Response<Reply>, Status> {
        // tell leader
        let mut state = self.state.lock().await;
        let leader_id = state.current_leader;
        if leader_id == 0 {
            return Err(Status::unavailable("Raft cluster not started yet!"));
        }
        let (_, leader_client) = &state.servers[&leader_id];
        let mut leader_client = leader_client.clone();
        let getkey = request.into_inner();
        match leader_client.put(Request::new(getkey.clone())).await {
            Ok(response) => {
                let response = response.into_inner();
                if response.wrong_leader {
                    // only retrying once to avoid looping a lot and save on complexity
                    let new_leader: i32 = response.value.parse().unwrap();
                    state.current_leader = new_leader;
                    state.servers[&new_leader]
                        .1
                        .clone()
                        .put(Request::new(getkey))
                        .await
                } else {
                    Ok(Response::new(response))
                }
            }
            Err(err) => Err(Status::internal(err.to_string())),
        }
    }

    async fn replace(&self, request: Request<KeyValue>) -> Result<Response<Reply>, Status> {
        self.put(request).await
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
                    .arg0(format!("raftserver{}", i))
                    .arg(i.to_string())
                    .arg(num_servers.to_string())
                    .spawn()
                    .unwrap_or_else(|_| panic!("Couldn't start raft node {}", i));
                info!("Started child raft node {} (pid {})", i, child.id());
                child
            })
            .collect();

        // wait for raft nodees to come online before sending them messages
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        for (i, child) in (1..=num_servers).zip(children) {
            let endpoint = Endpoint::from_shared(format!("http://127.0.0.1:{}", 9000 + i)).unwrap();
            info!("Connecting to node {} on {:?}", i, endpoint.uri());
            let client = KeyValueStoreClient::connect(endpoint)
                .await
                .unwrap_or_else(|_| {
                    error!("Couldn't connect to raft node {}", i);
                    panic!()
                });
            state.servers.insert(i, (child, client));
        }

        state.current_leader = 1; // Guess that node 1 is the leader
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
    env_logger::Builder::from_env(env)
        .format_timestamp_millis()
        .init();

    let addr = "127.0.0.1:8001".parse().unwrap();
    let frontend_state = Arc::new(Mutex::new(FrontendState::new()));
    let frontend = RaftersFrontend::new(frontend_state);

    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
        .build()?;

    info!("Starting frontend. Listening on {}", addr);

    Server::builder()
        .add_service(reflection_service)
        .add_service(FrontEndServer::new(frontend))
        .serve(addr)
        .await?;

    Ok(())
}
