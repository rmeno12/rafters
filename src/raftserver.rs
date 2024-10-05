pub mod rafters {
    tonic::include_proto!("rafters"); // The string specified here must match the proto package name
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting raft node.");

    Ok(())
}
