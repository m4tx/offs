use std::net::SocketAddr;

use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::{Future, Stream};
use tokio::io::AsyncRead;
use tokio::runtime::current_thread;

use offs::filesystem_capnp::remote_fs_proto;
use offs::store::id_generator::RandomHexIdGenerator;
use offs::store::Store;

use crate::remote_fs::RemoteFs;

pub fn run_server(store: Store<RandomHexIdGenerator>, address: SocketAddr) {
    let socket = ::tokio::net::TcpListener::bind(&address).unwrap();

    let client =
        remote_fs_proto::ToClient::new(RemoteFs::new(store)).into_client::<::capnp_rpc::Server>();

    let done = socket.incoming().for_each(move |socket| {
        socket.set_nodelay(true)?;
        let (reader, writer) = socket.split();

        let network = twoparty::VatNetwork::new(
            reader,
            std::io::BufWriter::new(writer),
            rpc_twoparty_capnp::Side::Server,
            Default::default(),
        );

        let rpc_system = RpcSystem::new(Box::new(network), Some(client.clone().client));
        current_thread::spawn(rpc_system.map_err(|e| println!("error: {:?}", e)));

        Ok(())
    });

    println!("Server listening on {}", address);

    current_thread::block_on_all(done).unwrap();
}
