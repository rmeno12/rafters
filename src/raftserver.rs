use log::{error, info, trace, warn};
use rand::prelude::Distribution;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::{From, Into};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::process;
use std::sync::Arc;
use tokio::{
    sync::Mutex,
    time::{timeout, Duration, Instant},
};
use tonic::{
    transport::{Channel, Endpoint, Server},
    Request, Response, Status,
};

pub mod rafters {
    tonic::include_proto!("raftkv"); // The string specified here must match the proto package name
}

use rafters::key_value_store_client::KeyValueStoreClient;
use rafters::key_value_store_server::{KeyValueStore, KeyValueStoreServer};
use rafters::{
    AppendEntriesRequest, AppendEntriesResponse, Empty, GetKey, IntegerArg, KeyValue, Reply, State,
    VoteRequest, VoteResponse,
};

#[derive(Clone, Copy, PartialEq, Eq)]
enum RaftNodeKind {
    Leader,
    Follower,
    Candidate,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
enum LogCommand {
    Put = 0,
    Remove = 1,
}

impl From<i32> for LogCommand {
    fn from(item: i32) -> Self {
        match item {
            0 => LogCommand::Put,
            1 => LogCommand::Remove,
            _ => panic!("Unknown command type!"),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct LogEntry {
    term: i32,
    key: String,
    value: String,
    command: LogCommand,
}

impl From<rafters::LogEntry> for LogEntry {
    fn from(entry: rafters::LogEntry) -> Self {
        LogEntry {
            term: entry.term,
            key: entry.key,
            value: entry.value,
            command: LogCommand::from(entry.command),
        }
    }
}

type NodeId = i64;

struct RaftNodeState {
    id: NodeId,
    kind: RaftNodeKind,
    term: i32,
    current_leader: NodeId,

    election_timeout_end: Instant,
    voted_for: NodeId,
    votes_received: Vec<NodeId>,

    log: Vec<LogEntry>,

    committed_index: usize,

    kv: HashMap<String, String>,

    total_nodes: i64,
    other_nodes: HashMap<NodeId, KeyValueStoreClient<Channel>>,
    sent_len: HashMap<NodeId, usize>,
    acked_len: HashMap<NodeId, usize>,

    log_file_path: String,
}

struct Timeouts;
impl Timeouts {
    const HEARTBEAT: Duration = Duration::from_millis(100);
    const LOG_REPLICATE: Duration = Duration::from_millis(250);
    const VOTE_RESPONSE: Duration = Duration::from_millis(250);
}

// TODO: rethink all unwraps

impl RaftNodeState {
    fn new(id: NodeId, total_nodes: i64, log_file_path: String) -> Self {
        let election_timeout_dist = rand::distributions::Uniform::from(1.2..=1.6);
        let mut rng = rand::thread_rng();
        let election_timeout = election_timeout_dist.sample(&mut rng);
        let mut acked_len = HashMap::new();
        acked_len.insert(id, 0);

        let mut state = Self {
            id,
            kind: RaftNodeKind::Follower,
            term: 0,
            current_leader: 0,
            election_timeout_end: Instant::now() + Duration::from_secs_f64(election_timeout),
            voted_for: 0,
            votes_received: vec![],
            log: vec![],
            committed_index: 0,
            kv: HashMap::new(),
            total_nodes,
            other_nodes: HashMap::new(),
            sent_len: HashMap::new(),
            acked_len,
            log_file_path,
        };
        state.recover();
        state.persist();
        state
    }

    async fn connect_to(
        &mut self,
        other: NodeId,
    ) -> Result<KeyValueStoreClient<Channel>, Box<dyn std::error::Error>> {
        let server_addr = format!("127.0.0.1:{}", 9000 + other);

        let src_port_base = (self.id - 1) * self.total_nodes + 7000;
        let src_port = (src_port_base + other) as u16;
        let local_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), src_port);
        info!(
            "{}: connecting to {} ({} -> {})",
            self.id, other, local_addr, server_addr
        );

        let channel = Channel::from_shared("http://".to_string() + &server_addr.clone())?
            .connect_with_connector(tower::service_fn(move |_| {
                let server_addr = server_addr.clone();
                let local_addr = local_addr;

                async move {
                    let tcp = tokio::net::TcpSocket::new_v4()?;
                    tcp.set_reuseport(true)?; // hopefully this isn't a bad thing
                    tcp.bind(local_addr)?;
                    let stream = tcp.connect(server_addr.parse().unwrap()).await?;
                    Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
                }
            }))
            .await?;

        let client = KeyValueStoreClient::new(channel);
        Ok(client)
    }

    async fn connect_to_others(&mut self) {
        for other_server_id in 1..=self.total_nodes {
            if other_server_id != self.id {
                let client = self
                    .connect_to(other_server_id)
                    .await
                    .unwrap_or_else(|err| {
                        error!(
                            "{}: unable to connect to {}! Err: {:?}",
                            self.id, other_server_id, err
                        );
                        panic!();
                    });
                self.other_nodes.insert(other_server_id, client);
                self.acked_len.insert(other_server_id, 0);
                self.sent_len.insert(other_server_id, 0);
            }
        }
    }

    fn append_entries(
        &mut self,
        prefix_len: usize,
        leader_commit_index: usize,
        suffix: &[LogEntry],
    ) {
        if !suffix.is_empty() && self.log.len() > prefix_len {
            let index = std::cmp::min(self.log.len(), prefix_len + suffix.len()) - 1;
            if self.log.get(index).to_owned().unwrap().term
                != suffix.get(index - prefix_len).to_owned().unwrap().term
            {
                self.log.truncate(prefix_len);
            }
        }
        if prefix_len + suffix.len() > self.log.len() {
            self.log.extend(
                suffix
                    .get((self.log.len() - prefix_len)..)
                    .unwrap_or_default()
                    .iter()
                    .cloned(),
            );
        }
        if leader_commit_index > self.committed_index {
            trace!(
                "{}: Found out about leader commits. Applying locally up to {}",
                self.id,
                leader_commit_index
            );
            for i in self.committed_index..leader_commit_index {
                self.apply(self.log[i].clone());
            }
            self.committed_index = leader_commit_index;
            self.persist();
        }
    }

    fn apply(&mut self, entry: LogEntry) {
        match entry.command {
            LogCommand::Put => {
                self.kv.insert(entry.key, entry.value);
            }
            LogCommand::Remove => {
                self.kv.remove(&entry.key);
            }
        }
    }

    fn commit(&mut self) {
        let quorum_size = (self.total_nodes as usize + 2) / 2; // +2 to round up
        let ready = (0..=self.log.len())
            .rev()
            .find(|i| self.acked_len.values().filter(|&acked| acked >= i).count() >= quorum_size)
            .unwrap_or(0);
        trace!(
            "{}: Considering commit (ready={}, committed={})",
            self.id,
            ready,
            self.committed_index
        );
        if ready > 0
            && ready > self.committed_index
            && self.log.get(ready - 1).unwrap().term == self.term
        {
            info!("{}: Committing up to {}", self.id, ready);
            for i in self.committed_index..ready {
                self.apply(self.log[i].clone());
            }
            self.committed_index = ready;
            self.persist();
        }
    }

    fn recover(&mut self) {
        match std::fs::read_to_string(self.log_file_path.clone()).ok() {
            Some(json) => {
                let (term, voted_for, log): (i32, NodeId, Vec<LogEntry>) =
                    serde_json::from_str(&json).expect("Unable to parse data from json");
                self.term = term;
                self.voted_for = voted_for;
                self.log = log;
            }
            None => {
                info!("{}: Unable to access log file to recover from.", self.id);
            }
        }
    }

    fn persist(&self) {
        let data = (self.term, self.voted_for, self.log.clone());
        let json = serde_json::to_string_pretty(&data).expect("Unable to convert data to json");
        std::fs::write(self.log_file_path.clone(), json).expect("Unable to write json to file");
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
impl KeyValueStore for RaftersServer {
    async fn get_state(&self, _request: Request<Empty>) -> Result<Response<State>, Status> {
        let state = self.node_state.lock().await;
        Ok(Response::new(State {
            term: state.term,
            is_leader: state.kind == RaftNodeKind::Leader,
        }))
    }

    // TODO: figure out what to do with clientid and requestid
    async fn get(&self, request: Request<GetKey>) -> Result<Response<Reply>, Status> {
        let state = self.node_state.lock().await;
        if state.kind == RaftNodeKind::Leader {
            let req = request.into_inner();
            let key = req.key;
            match state.kv.get(&key) {
                Some(val) => Ok(Response::new(Reply {
                    wrong_leader: false,
                    error: String::from(""),
                    value: val.to_string(),
                })),
                None => Err(Status::invalid_argument(format!(
                    "Key {} not in store!",
                    key
                ))),
            }
        } else {
            let req = request.get_ref();
            info!(
                "{}: Got get request ({}), but am not leader",
                state.id, req.key
            );
            let leader = state.current_leader;
            if leader != 0 {
                Ok(Response::new(Reply {
                    wrong_leader: true,
                    error: String::new(),
                    value: leader.to_string(),
                }))
            } else {
                Err(Status::unavailable("Leader unknown!"))
            }
        }
    }

    async fn put(&self, request: Request<KeyValue>) -> Result<Response<Reply>, Status> {
        let mut state = self.node_state.lock().await;
        if state.kind == RaftNodeKind::Leader {
            let req = request.into_inner();
            info!("{}: Got add request ({}, {})", state.id, req.key, req.value);
            let new_entry = LogEntry {
                term: state.term,
                key: req.key,
                value: req.value,
                command: LogCommand::Put,
            };
            state.log.push(new_entry);
            state.persist();
            let id = state.id;
            let len = state.log.len();
            state.acked_len.insert(id, len);
            replicate_log(&state, self.node_state.clone());
            // TODO: figure out what other fields are needed?
            Ok(Response::new(Reply {
                wrong_leader: false,
                error: String::from(""),
                value: String::from(""),
            }))
        } else {
            let req = request.get_ref();
            info!(
                "{}: Got add request ({}, {}), but am not leader",
                state.id, req.key, req.value
            );
            let leader = state.current_leader;
            if leader != 0 {
                Ok(Response::new(Reply {
                    wrong_leader: true,
                    error: String::new(),
                    value: leader.to_string(),
                }))
            } else {
                Err(Status::unavailable("Leader unknown!"))
            }
        }
    }

    async fn replace(&self, request: Request<KeyValue>) -> Result<Response<Reply>, Status> {
        self.put(request).await
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
            state.persist();
        }
        let last_log_term = if !state.log.is_empty() {
            state.log.last().unwrap().term
        } else {
            0
        };
        // Either have a more recent log, or have log from the same term but candidate has at least
        // as many values in their log
        let log_ok = req.last_log_term > last_log_term
            || (req.last_log_term == last_log_term && req.last_log_index >= state.log.len() as i32);
        // If we're in the same term, and their log is okay, and we haven't already voted for
        // someone else
        let grant_vote = req.term == state.term
            && log_ok
            && (state.voted_for == req.candidate_id || state.voted_for == 0);
        if grant_vote {
            state.voted_for = req.candidate_id;
            state.election_timeout_end = Instant::now() + Duration::from_secs(1);
            state.persist();
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
        if req.term > state.term {
            state.term = req.term;
            state.voted_for = 0;
            state.election_timeout_end = Instant::now() + Duration::from_secs(1);
            state.persist();
        }
        if req.term == state.term {
            state.kind = RaftNodeKind::Follower;
            state.current_leader = req.leader_id;
            state.election_timeout_end = Instant::now() + Duration::from_secs(1);
        }
        let log_ok = state.log.len() as i32 >= req.prev_log_index
            && (req.prev_log_index == 0
                || state.log.last().cloned().unwrap().term == req.prev_log_term);
        if req.term == state.term && log_ok {
            let entries: Vec<LogEntry> = req.entries.into_iter().map(Into::into).collect();
            if !entries.is_empty() {
                info!("{}: Adding new entries", state.id);
            }
            state.append_entries(
                req.prev_log_index as usize,
                req.leader_commit_index as usize,
                &entries,
            );
            let ack = req.prev_log_index + entries.len() as i32;
            let resp = AppendEntriesResponse {
                term: state.term,
                id: state.id,
                ack,
                success: true,
            };
            Ok(Response::new(resp))
        } else {
            // send negative
            let resp = AppendEntriesResponse {
                term: state.term,
                id: state.id,
                ack: 0,
                success: false,
            };
            Ok(Response::new(resp))
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
        replicate_log(&state, node_state.clone());
    }
}

fn replicate_log_to(
    node_state: Arc<Mutex<RaftNodeState>>,
    client_id: NodeId,
    client: KeyValueStoreClient<Channel>,
) -> tokio::task::JoinHandle<()> {
    let mut client = client.clone();
    let this_state = node_state.clone();
    tokio::spawn(async move {
        loop {
            let mut state = this_state.lock().await;
            trace!("{}: Replicating log to {}", state.id, client_id);
            let prefix_len = state.sent_len.get(&client_id).copied().unwrap_or_default();
            let prefix_term = if prefix_len > 0 {
                state.log.get(prefix_len - 1).unwrap().term
            } else {
                0
            };
            let suffix: Vec<_> = state
                .log
                .get(prefix_len..)
                .unwrap_or_default()
                .iter()
                .map(|entry| rafters::LogEntry {
                    term: entry.term,
                    key: entry.key.clone(),
                    value: entry.value.clone(),
                    command: entry.command as i32,
                })
                .collect();
            let timeout_time = if suffix.is_empty() {Timeouts::HEARTBEAT} else {Timeouts::LOG_REPLICATE};
            let to_ack_len = prefix_len + suffix.len();
            let req = AppendEntriesRequest {
                term: state.term,
                leader_id: state.id,
                prev_log_index: prefix_len as i32,
                prev_log_term: prefix_term,
                entries: suffix,
                leader_commit_index: state.committed_index as i32,
            };
            let id = state.id;
            let req_result = client.append_entries(req);
            let done = match timeout(timeout_time, req_result).await {
                Ok(Ok(resp)) => {
                    let resp = resp.into_inner();
                    let mut done = true;
                    if resp.term == state.term && state.kind == RaftNodeKind::Leader {
                        if resp.success {
                            trace!(
                                "{}: Replicated to {}. Now acked {}",
                                state.id,
                                client_id,
                                to_ack_len
                            );
                            state.sent_len.insert(client_id, to_ack_len);
                            state.acked_len.insert(client_id, to_ack_len);
                            state.commit();
                        } else if state.sent_len.get(&client_id).copied().unwrap_or_default() > 0 {
                            let l = state.sent_len.get(&client_id).copied().unwrap_or_default();
                            state.sent_len.insert(client_id, l - 1);
                            done = false;
                        }
                    }
                    done
                }
                _ => {
                    warn!("{}: Unable to replicate log to {}", id, client_id);
                    true
                }
            };
            if done {
                break;
            }
        }
    })
}

fn replicate_log(state: &RaftNodeState, node_state: Arc<Mutex<RaftNodeState>>) {
    for (client_id, client) in state.other_nodes.clone() {
        trace!("{}: Queueing replication to {}", state.id, client_id);
        replicate_log_to(node_state.clone(), client_id, client);
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
        info!(
            "{}: Election timer ran out, starting election for term {}",
            state.id,
            state.term + 1
        );
        state.term += 1;
        state.kind = RaftNodeKind::Candidate;
        state.voted_for = state.id;
        state.persist();
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
                    match timeout(Timeouts::VOTE_RESPONSE, req_result).await {
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

                                    let log_len = state.log.len();
                                    for client_id in state.other_nodes.clone().keys() {
                                        state.sent_len.insert(*client_id, log_len);
                                        state.acked_len.insert(*client_id, 0);
                                    }
                                    replicate_log(&state, this_state.clone());
                                }
                            } else if resp.term > state.term {
                                // our term is behind, step out of the election and wait for
                                // someone to start the next one
                                state.term = resp.term;
                                state.kind = RaftNodeKind::Follower;
                                state.voted_for = 0;
                                state.election_timeout_end += Duration::from_secs(1);
                                state.persist();
                            }
                        }
                        err => {
                            warn!(
                                "{}: Couldn't send VoteRequest to {}. Err: {:?}",
                                id, client_id, err
                            )
                        }
                    }
                })
            })
            .collect();
        state.election_timeout_end = Instant::now() + Duration::from_secs(1);
    }
}

async fn server_startup(node_state: Arc<Mutex<RaftNodeState>>) {
    // wait a second for all other nodes to startup
    tokio::time::sleep(Duration::from_secs(1)).await;
    // connect to all the other nodes
    node_state.lock().await.connect_to_others().await;

    tokio::spawn(leader_loop(node_state.clone()));
    tokio::spawn(follower_candidate_loop(node_state.clone()));
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();

    if args.len() != 3 {
        eprintln!("Usage: {} <node_id> <total_nodes>", args[0]);
        process::exit(1);
    }

    let node_num: NodeId = args[1].parse()?;
    let total_nodes: i64 = args[2].parse()?;

    let env = env_logger::Env::default().default_filter_or("info");
    env_logger::Builder::from_env(env)
        .format_timestamp_millis()
        .init();

    let addr = format!("127.0.0.1:{}", 9000 + node_num).parse().unwrap();
    info!("Starting raft node {}. Listening on {}", node_num, addr);

    let node_log_path = format!("raftlog_{}", node_num);
    let raft_node_state = Arc::new(Mutex::new(RaftNodeState::new(
        node_num,
        total_nodes,
        node_log_path,
    )));
    let raftserver = RaftersServer::new(raft_node_state.clone());

    tokio::spawn(server_startup(raft_node_state.clone()));

    Server::builder()
        .add_service(KeyValueStoreServer::new(raftserver))
        .serve(addr)
        .await?;

    Ok(())
}
