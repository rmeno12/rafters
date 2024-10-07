use rand::prelude::Distribution;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::{
    sync::Mutex,
    time::{Duration, Instant},
};
use tonic::{
    transport::{Channel, Endpoint, Server},
    Request, Response, Status,
};

pub mod rafters {
    tonic::include_proto!("rafters"); // The string specified here must match the proto package name
}

use rafters::raft_client::RaftClient;
use rafters::raft_server::{Raft, RaftServer};
use rafters::{
    AppendEntriesRequest, AppendEntriesResponse, Empty, IntegerArg, KeyValue, State, VoteRequest,
    VoteResponse,
};

#[derive(Clone, Copy, PartialEq, Eq)]
enum RaftNodeKind {
    Leader,
    Follower,
    Candidate,
}

enum LogCommand {
    Put,
    Remove,
}

struct LogEntry {
    term: i32,
    key: i32,
    value: String,
    command: LogCommand,
}

struct RaftNodeState {
    id: i32,
    servers: HashMap<i32, RaftClient<Channel>>,
    election_timeout_end: Instant,
    kind: RaftNodeKind,
    term: i32,
    voted_for: i32,
    log: Vec<LogEntry>,
    last_log_term: i32,
    kv: HashMap<i32, String>,
}

impl RaftNodeState {
    fn new(id: i32) -> Self {
        let election_timeout_dist = rand::distributions::Uniform::from(0.8..=1.2);
        let mut rng = rand::thread_rng();
        let election_timeout = election_timeout_dist.sample(&mut rng);
        Self {
            id,
            servers: HashMap::new(),
            election_timeout_end: Instant::now() + Duration::from_secs_f64(election_timeout),
            kind: RaftNodeKind::Follower,
            term: 1,
            voted_for: 0,
            log: vec![],
            last_log_term: 0,
            kv: HashMap::new(),
        }
    }
}

struct RaftersServer {
    node_state: Arc<Mutex<RaftNodeState>>,
}

impl RaftersServer {
    fn new(node_state: Arc<Mutex<RaftNodeState>>) -> Self {
        Self { node_state }
    }
}

#[tonic::async_trait]
impl Raft for RaftersServer {
    async fn get_state(&self, _request: Request<Empty>) -> Result<Response<State>, Status> {
        todo!()
    }

    async fn get(&self, request: Request<IntegerArg>) -> Result<Response<KeyValue>, Status> {
        // only do following as leader
        // read from kv and send data back
        let state = self.node_state.lock().await;
        if state.kind == RaftNodeKind::Leader {
            let req = request.into_inner();
            let key = req.arg;
            match state.kv.get(&key) {
                Some(val) => Ok(Response::new(KeyValue {
                    key,
                    value: val.clone(),
                })),
                None => Err(Status::invalid_argument(format!(
                    "Key {} not in store!",
                    key
                ))),
            }
        } else {
            todo!()
        }
    }

    async fn put(&self, request: Request<KeyValue>) -> Result<Response<Empty>, Status> {
        // only do following as leader
        // add add command to log
        // send append entry rpcs to other servers in parallel
        // keep sending until they accept (some error handling described in paper)
        // once they all accept, add new thing to kv
        // send response to client
        let mut state = self.node_state.lock().await;
        if state.kind == RaftNodeKind::Leader {
            let req = request.into_inner();
            let new_entry = LogEntry {
                term: state.term,
                key: req.key,
                value: req.value,
                command: LogCommand::Put,
            };
            state.log.push(new_entry);
            Ok(Response::new(Empty{}))
        } else {
            todo!()
        }
    }

    async fn add_server(&self, request: Request<IntegerArg>) -> Result<Response<Empty>, Status> {
        let server_num = request.into_inner().arg;
        let endpoint =
            Endpoint::from_shared(format!("http://[::1]:{}", 9000 + server_num)).unwrap();
        let client = RaftClient::connect(endpoint)
            .await
            .unwrap_or_else(|_| panic!("Couldn't connect to new node {}", server_num));
        let servers = &mut self.node_state.lock().await.servers;
        servers.insert(server_num, client);
        Ok(Response::new(Empty {}))
    }

    async fn remove_server(&self, request: Request<IntegerArg>) -> Result<Response<Empty>, Status> {
        let server_num = request.into_inner().arg;
        let servers = &mut self.node_state.lock().await.servers;
        servers.remove(&server_num);
        Ok(Response::new(Empty {}))
    }

    async fn request_vote(
        &self,
        request: Request<VoteRequest>,
    ) -> Result<Response<VoteResponse>, Status> {
        let mut state = self.node_state.lock().await;
        let req = request.into_inner();
        println!("{}: Got vote request from {}", state.id, req.candidate_id);
        // Vote if request is not for an older term, and the candidate's log is at least as recent
        // as this node's
        let grant_vote = req.term >= state.term
            && req.last_log_index >= state.log.len() as i32
            && req.last_log_term >= state.last_log_term;
        if grant_vote {
            state.voted_for = req.candidate_id;
            state.election_timeout_end = Instant::now() + Duration::from_secs(100);
        }
        Ok(Response::new(VoteResponse {
            term: state.term,
            vote_granted: grant_vote,
        }))
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        let mut state = self.node_state.lock().await;
        let req = request.into_inner();
        match state.kind {
            RaftNodeKind::Follower => {
                if !req.entries.is_empty() {
                    todo!()
                } else {
                    // TODO: handle bad requests?
                    state.election_timeout_end = Instant::now() + Duration::from_secs(1);
                    Ok(Response::new(AppendEntriesResponse {
                        term: state.term,
                        success: true,
                    }))
                }
            }
            RaftNodeKind::Candidate => {
                todo!()
            }
            RaftNodeKind::Leader => {
                todo!()
            }
        }
    }
}

async fn leader_loop(node_state: Arc<Mutex<RaftNodeState>>) {
    loop {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let mut state = node_state.lock().await;
        let term = state.term;
        let id = state.id;
        let kind = state.kind;
        if kind != RaftNodeKind::Leader {
            drop(state);
            continue;
        }
        for client in state.servers.values_mut() {
            let resp = client
                .append_entries(AppendEntriesRequest {
                    term,
                    leader_id: id,
                    prev_log_index: 0,
                    prev_log_term: 0,
                    entries: vec![],
                    leader_commit_index: 0,
                })
                .await
                .unwrap();
        }
    }
}

async fn follower_candidate_loop(node_state: Arc<Mutex<RaftNodeState>>) {
    loop {
        let election_timeout_end = node_state.lock().await.election_timeout_end;
        tokio::time::sleep_until(election_timeout_end).await;
        // Check again to see if the election timeout was delayed by a heartbeat
        let mut state = node_state.lock().await;
        let election_timeout_end = state.election_timeout_end;
        let kind = state.kind;
        if election_timeout_end > Instant::now() {
            drop(state); // is this necessary?
            continue;
        } else if kind == RaftNodeKind::Leader {
            state.election_timeout_end = Instant::now() + Duration::from_secs(1);
            drop(state);
            continue;
        }

        // Election timed out and we are a follower, so time to start a new
        // election and request votes from all other raft nodes
        println!("{}: Timed out, starting election", state.id);
        state.term += 1;
        let new_term = state.term;
        let id = state.id;
        state.kind = RaftNodeKind::Candidate;
        let mut votes = 1;
        for client in state.servers.values_mut() {
            // TODO: do this in parallel with tokio::spawn
            let resp = client
                .request_vote(VoteRequest {
                    term: new_term,
                    candidate_id: id,
                    last_log_index: 0,
                    last_log_term: 0,
                })
                .await
                .unwrap();
            if resp.into_inner().vote_granted {
                votes += 1;
            }
        }
        if votes > state.servers.len() / 2 {
            println!("{}: Won the election. Becoming leader.", state.id);
            state.kind = RaftNodeKind::Leader;
            for client in state.servers.values_mut() {
                let resp = client
                    .append_entries(AppendEntriesRequest {
                        term: new_term,
                        leader_id: id,
                        prev_log_index: 0,
                        prev_log_term: 0,
                        entries: vec![],
                        leader_commit_index: 0,
                    })
                    .await
                    .unwrap();
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let node_num: i32 = args.get(1).unwrap().parse()?;

    let addr = format!("[::1]:{}", 9000 + node_num).parse().unwrap();
    println!("Starting raft node {}. Listening on {}", node_num, addr);

    let raft_node_state = Arc::new(Mutex::new(RaftNodeState::new(node_num)));
    let raftserver = RaftersServer::new(raft_node_state.clone());

    tokio::spawn(leader_loop(raft_node_state.clone()));
    tokio::spawn(follower_candidate_loop(raft_node_state.clone()));

    Server::builder()
        .add_service(RaftServer::new(raftserver))
        .serve(addr)
        .await?;

    Ok(())
}
