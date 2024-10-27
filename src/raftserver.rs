use log::{error, info, warn};
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

type NodeId = i32;

struct RaftNodeState {
    id: NodeId,
    kind: RaftNodeKind,
    term: i32,
    current_leader: NodeId,
    election_timeout_end: Instant,
    voted_for: NodeId,
    votes_received: Vec<NodeId>,
    log: Vec<LogEntry>,
    kv: HashMap<i32, String>,
    other_nodes: HashMap<NodeId, RaftClient<Channel>>,
}

impl RaftNodeState {
    fn new(id: i32) -> Self {
        let election_timeout_dist = rand::distributions::Uniform::from(0.8..=1.2);
        let mut rng = rand::thread_rng();
        let election_timeout = election_timeout_dist.sample(&mut rng);
        Self {
            id,
            other_nodes: HashMap::new(),
            election_timeout_end: Instant::now() + Duration::from_secs_f64(election_timeout),
            kind: RaftNodeKind::Follower,
            term: 1,
            voted_for: 0,
            votes_received: vec![],
            current_leader: 0,
            log: vec![],
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
            Ok(Response::new(Empty {}))
        } else {
            todo!()
        }
    }

    async fn add_server(&self, request: Request<IntegerArg>) -> Result<Response<Empty>, Status> {
        let server_num = request.into_inner().arg;
        let endpoint =
            Endpoint::from_shared(format!("http://[::1]:{}", 9000 + server_num)).unwrap();
        let client = RaftClient::connect(endpoint).await.unwrap_or_else(|_| {
            error!("Couldn't connect to new node {}", server_num);
            panic!()
        });
        let servers = &mut self.node_state.lock().await.other_nodes;
        servers.insert(server_num, client);
        Ok(Response::new(Empty {}))
    }

    async fn remove_server(&self, request: Request<IntegerArg>) -> Result<Response<Empty>, Status> {
        let server_num = request.into_inner().arg;
        let servers = &mut self.node_state.lock().await.other_nodes;
        servers.remove(&server_num);
        Ok(Response::new(Empty {}))
    }

    async fn request_vote(
        &self,
        request: Request<VoteRequest>,
    ) -> Result<Response<VoteResponse>, Status> {
        let mut state = self.node_state.lock().await;
        let req = request.into_inner();
        info!("{}: Got vote request from {}", state.id, req.candidate_id);

        // If we're behind, become a follower
        if req.term > state.term {
            state.term = req.term;
            state.kind = RaftNodeKind::Follower;
            state.voted_for = 0;
        }
        let last_log_term = if !state.log.is_empty() {
            state.log.last().unwrap().term
        } else {
            0
        };
        // Either have a more recent log, or have log from the same term but candidate has at least
        // as many values in their log
        let log_ok = req.last_log_term > last_log_term
            || (req.last_log_term == last_log_term
                && req.last_log_index >= state.log.len() as i32);
        // If we're in the same term, and their log is okay, and we haven't already voted for
        // someone else
        let grant_vote = req.term == state.term
            && log_ok
            && (state.voted_for == req.candidate_id || state.voted_for == 0);
        if grant_vote {
            state.voted_for = req.candidate_id;
            state.election_timeout_end = Instant::now() + Duration::from_secs(1);
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
        let state = node_state.lock().await;
        if state.kind != RaftNodeKind::Leader {
            continue;
        }
        let id = state.id;
        let req = AppendEntriesRequest {
            term: state.term,
            leader_id: id,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit_index: 0,
        };
        let _vec_resps: Vec<_> = state
            .other_nodes
            .clone()
            .into_iter()
            .map(|(client_id, client)| {
                let mut client = client.clone();
                let req_val = req.clone();
                tokio::spawn(async move {
                    let req_result = client.append_entries(req_val);
                    if (tokio::time::timeout(Duration::from_millis(100), req_result).await).is_err()
                    {
                        warn!("{}: Unable to send heartbeat to {}", id, client_id);
                    }
                });
            })
            .collect();
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
            continue;
        } else if kind == RaftNodeKind::Leader {
            state.election_timeout_end = Instant::now() + Duration::from_secs(1);
            continue;
        }

        // Election timed out and we are a follower, so time to start a new
        // election and request votes from all other raft nodes
        info!("{}: Election timer ran out, starting election", state.id);
        state.term += 1;
        state.kind = RaftNodeKind::Candidate;
        state.voted_for = state.id;
        state.votes_received = vec![state.id];
        let last_log_term = if !state.log.is_empty() {
            state.log.last().unwrap().term
        } else {
            0
        };
        let vote_req = VoteRequest {
            term: state.term,
            candidate_id: state.id,
            last_log_index: state.log.len() as i32,
            last_log_term,
        };
        let id = state.id;
        let majority = (state.other_nodes.len() + 2) / 2;
        let _vote_req_tasks: Vec<_> = state
            .other_nodes
            .clone()
            .into_iter()
            .map(|(client_id, client)| {
                let mut client = client.clone();
                let this_state = node_state.clone();
                tokio::spawn(async move {
                    let req_result = client.request_vote(vote_req);
                    match tokio::time::timeout(Duration::from_millis(500), req_result).await {
                        Ok(Ok(resp)) => {
                            let resp = resp.into_inner();
                            let mut state = this_state.lock().await;
                            if state.kind == RaftNodeKind::Candidate
                                && resp.term == state.term
                                && resp.vote_granted
                            {
                                state.votes_received.push(client_id);
                                if state.votes_received.len() >= majority {
                                    // won the election
                                    state.kind = RaftNodeKind::Leader;
                                    state.current_leader = state.id;
                                    state.election_timeout_end += Duration::from_secs(1);
                                    info!("{}: Won the election! Becoming leader", state.id);

                                    // TODO: Replicate logs onto followers
                                }
                            } else if resp.term > state.term {
                                // our term is behind, step out of the election and wait for
                                // someone to start the next one
                                state.term = resp.term;
                                state.kind = RaftNodeKind::Follower;
                                state.voted_for = 0;
                                state.election_timeout_end += Duration::from_secs(1);
                            }
                        }
                        _ => {
                            warn!("{} Couldn't send VoteRequest to {}", id, client_id)
                        }
                    }
                })
            })
            .collect();
        state.election_timeout_end = Instant::now() + Duration::from_secs(1);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let node_num: i32 = args.get(1).unwrap().parse()?;

    let env = env_logger::Env::default().default_filter_or("info");
    env_logger::init_from_env(env);

    let addr = format!("[::1]:{}", 9000 + node_num).parse().unwrap();
    info!("Starting raft node {}. Listening on {}", node_num, addr);

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
