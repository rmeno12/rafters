mod common;

use common::{kill_cluster, rafters::IntegerArg, start_frontend};
use tonic::{
    Request, Response
};
use tokio::time::Duration;


#[tokio::test]
async fn test_cluster_startup() -> Result<(), Box<dyn std::error::Error>> {
    let (child, mut client) = start_frontend().await;

    let mut startup_request = Request::new(IntegerArg{arg: 5});
    startup_request.set_timeout(Duration::from_millis(1000));
    client.start_raft(startup_request).await?;

    kill_cluster(child)?;
    Ok(())
}
