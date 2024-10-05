use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{transport::Server, Request, Response, Status};

pub mod rafters {
    tonic::include_proto!("rafters");
}

use rafters::frontend_server::{Frontend, FrontendServer};
use rafters::{Empty, IntegerArg, KeyValue, State};

pub struct RaftersFrontend {
    term: i32,
    servers: Arc<Mutex<HashMap<i32, std::process::Child>>>,
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
        let num_servers = request.into_inner().arg;
        let mut servers = self.servers.lock().await;
        for i in 1..=num_servers {
            let child = std::process::Command::new(if cfg!(debug_assertions) {
                "target/debug/raftserver"
            } else {
                "target/release/raftserver"
            })
            .arg(i.to_string())
            .spawn()
            .unwrap_or_else(|_| panic!("couldn't start raft node {}", i));
            println!("Started child raft node {} (pid {})", i, child.id());
            servers.insert(i, child);
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
        todo!()
    }

    async fn put(&self, request: Request<KeyValue>) -> Result<Response<Empty>, Status> {
        todo!()
    }

    async fn replace(&self, request: Request<KeyValue>) -> Result<Response<Empty>, Status> {
        todo!()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:8001".parse().unwrap();
    let frontend = RaftersFrontend::default();

    println!("Starting frontend. Listening on {}", addr);

    Server::builder()
        .add_service(FrontendServer::new(frontend))
        .serve(addr)
        .await?;

    Ok(())
}
