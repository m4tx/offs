use std::net::SocketAddr;

use tonic::transport::Server;

use offs::proto::filesystem::remote_fs_server::RemoteFsServer;
use offs::store::id_generator::RandomHexIdGenerator;
use offs::store::Store;

use crate::remote_fs::{RemoteFs, RemoteFsServerImpl};

pub async fn run_server(
    store: Store<RandomHexIdGenerator>,
    address: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Server listening on {}", address);

    Server::builder()
        .add_service(RemoteFsServer::new(RemoteFsServerImpl::new(RemoteFs::new(
            store,
        )?)))
        .serve(address)
        .await?;

    Ok(())
}
