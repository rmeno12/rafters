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
use rafters::{Empty, IntegerArg, KeyValue, State, VoteRequest, VoteResponse};

#[derive(Clone, Copy, PartialEq, Eq)]
enum RaftNodeKind {
    Leader,
    Follower,
    Candidate,
}

struct RaftNodeState {
    id: i32,
    servers: HashMap<i32, RaftClient<Channel>>,
    election_timeout_end: Instant,
    kind: RaftNodeKind,
    term: i32,
    voted_for: i32,
    log: Vec<i32>, // TODO: change this to some other type
    last_log_term: i32,
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
        todo!()
    }

    async fn put(&self, request: Request<KeyValue>) -> Result<Response<Empty>, Status> {
        todo!()
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
        state.election_timeout_end = Instant::now() + Duration::from_secs(100);
        // Vote if request is not for an older term, and the candidate's log is at least as recent
        // as this node's
        let grant_vote = req.term >= state.term
            && req.last_log_index >= state.log.len() as i32
            && req.last_log_term >= state.last_log_term;
        if grant_vote {
            state.voted_for = req.candidate_id;
        }
        Ok(Response::new(VoteResponse {
            term: state.term,
            vote_granted: grant_vote,
        }))
    }
}

async fn periodic_logic(node_state: Arc<Mutex<RaftNodeState>>) {
    loop {
        let election_timeout_end = node_state.lock().await.election_timeout_end;
        tokio::time::sleep_until(election_timeout_end).await;
        // Check again to see if the election timeout was delayed by a heartbeat
        let mut state = node_state.lock().await;
        let election_timeout_end = state.election_timeout_end;
        let kind = state.kind;
        if election_timeout_end > Instant::now() || kind != RaftNodeKind::Follower {
            drop(state); // is this necessary?
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

    tokio::spawn(periodic_logic(raft_node_state.clone()));

    Server::builder()
        .add_service(RaftServer::new(raftserver))
        .serve(addr)
        .await?;

    Ok(())
}
