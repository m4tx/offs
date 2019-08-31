use std::io::Read;
use std::net::SocketAddr;
use std::sync::Arc;
use std::{io, thread};

use futures::sync::oneshot;
use futures::Future;
use grpcio::{Environment, ServerBuilder};

use offs::proto::filesystem_grpc::create_remote_fs;
use offs::store::id_generator::RandomHexIdGenerator;
use offs::store::Store;

use crate::remote_fs::RemoteFs;

pub fn run_server(store: Store<RandomHexIdGenerator>, address: SocketAddr) {
    let env = Arc::new(Environment::new(1));
    let service = create_remote_fs(RemoteFs::new(store));
    let mut server = ServerBuilder::new(env)
        .register_service(service)
        .bind(format!("{}", address.ip()), address.port())
        .build()
        .unwrap();
    server.start();
    for &(ref host, port) in server.bind_addrs() {
        println!("listening on {}:{}", host, port);
    }
    let (tx, rx) = oneshot::channel();
    thread::spawn(move || {
        println!("Press ENTER to exit...");
        let _ = io::stdin().read(&mut [0]).unwrap();
        tx.send(())
    });
    let _ = rx.wait();
    let _ = server.shutdown().wait();
}
