use log::{error, info, trace, warn};
use std::collections::HashMap;
use std::os::unix::process::CommandExt;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::Duration;
use tonic::{
    transport::{Channel, Endpoint, Server},
    Request, Response, Status,
};

pub mod rafters {
    tonic::include_proto!("raftkv");
}

use rafters::front_end_server::{FrontEnd, FrontEndServer};
use rafters::key_value_store_client::KeyValueStoreClient;
use rafters::{Empty, GetKey, IntegerArg, KeyValue, Reply};

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

    async fn find_leader(&mut self) -> i32 {
        let mut terms_to_leaders: HashMap<i32, Vec<i32>> = HashMap::new();
        for (id, (_, ref mut client)) in &mut self.servers {
            let node_status = client.get_state(Empty {}).await;
            if node_status.is_err() {
                warn!(
                    "Unable to query node {} for state. Err: {}",
                    id,
                    node_status.unwrap_err()
                );
                continue;
            }
            let resp = node_status.unwrap().into_inner();
            if resp.is_leader {
                terms_to_leaders.entry(resp.term).or_default().push(*id);
            }
        }

        let last_term = terms_to_leaders.keys().copied().max().unwrap_or_default();
        if last_term == 0 {
            warn!("Trying to get leader but wasn't able to find any!");
            0
        } else if terms_to_leaders[&last_term].len() != 1 {
            error!("Multiple leaders in last term!");
            0
        } else {
            let leader = terms_to_leaders[&last_term][0];
            info!("Found leader as {}", leader);
            leader
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
        state.current_leader = state.find_leader().await;
        let leader_id = state.current_leader;
        if leader_id == 0 {
            return Err(Status::unavailable("Unable to find leader!"));
        }
        let (_, leader_client) = &state.servers[&leader_id];
        let mut leader_client = leader_client.clone();
        leader_client.get(request).await
    }

    async fn put(&self, request: Request<KeyValue>) -> Result<Response<Reply>, Status> {
        // tell leader
        let mut state = self.state.lock().await;
        state.current_leader = state.find_leader().await;
        let mut leader_id = state.current_leader;
        if leader_id == 0 {
            leader_id = 1; // just start at 1 and loop if we can't find the real leader
        }

        // go through all nodes twice if necessary
        let max_iter = state.servers.len() as i32 * 2;
        let mut iter = 0;
        let getkey = request.into_inner();
        while iter < max_iter {
            let (_, leader_client) = &state.servers[&leader_id];
            let mut leader_client = leader_client.clone();
            let resp_result = leader_client.put(Request::new(getkey.clone())).await;
            match resp_result {
                Ok(response) => {
                    let reply = response.into_inner();
                    if !reply.wrong_leader {
                        state.current_leader = leader_id;
                        return Ok(Response::new(reply));
                    }
                }
                Err(e) => return Err(Status::unavailable(e.message())),
            }
            trace!("{} was not leader, will try next server", leader_id);
            iter += 1;
            leader_id += 1;
            if leader_id > state.servers.len() as i32 {
                leader_id = 1;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        warn!("tried all servers twice and got no positive responses or errors!");

        Err(Status::unavailable("Unable to get responses from servers"))
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

        let node_state = self.state.clone();
        tokio::spawn(async move {
            let mut state = node_state.lock().await;
            // wait for raft nodees to come online before sending them messages
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
            for (i, child) in (1..=num_servers).zip(children) {
                let endpoint =
                    Endpoint::from_shared(format!("http://127.0.0.1:{}", 9000 + i)).unwrap();
                info!("Connecting to node {} on {:?}", i, endpoint.uri());
                let client = KeyValueStoreClient::connect(endpoint)
                    .await
                    .unwrap_or_else(|_| {
                        error!("Couldn't connect to raft node {}", i);
                        panic!()
                    });
                state.servers.insert(i, (child, client));
            }

            // wait some time for leader election to happen
            tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
            state.current_leader = state.find_leader().await;
        });

        Ok(Response::new(Reply {
            wrong_leader: false,
            error: String::from(""),
            value: String::from(""),
        }))
    }
}

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
        .build_v1()?;

    info!("Starting frontend. Listening on {}", addr);

    Server::builder()
        .add_service(reflection_service)
        .add_service(FrontEndServer::new(frontend))
        .serve(addr)
        .await?;

    Ok(())
}
