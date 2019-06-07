use std::net::ToSocketAddrs;

use clap::{App, Arg};

use offs::store::Store;

mod remote_fs;
mod server;

fn main() {
    let matches = App::new("offs server")
        .version("0.1")
        .author("Mateusz Maćkowski <m4tx@m4tx.pl>")
        .about("offs filesystem server module")
        .arg(
            Arg::with_name("store")
                .short("s")
                .long("store")
                .value_name("FILE")
                .help("Sets a custom store database path")
                .default_value("store.db"),
        )
        .arg(
            Arg::with_name("ADDRESS")
                .help("The address to listen on")
                .validator(offs::validators::check_address)
                .default_value("0.0.0.0:10031")
                .index(1),
        )
        .get_matches();

    let store = Store::new_server(matches.value_of("store").unwrap());

    let address_str = matches.value_of("ADDRESS").unwrap();
    let address = address_str.to_socket_addrs().unwrap().next().unwrap();

    server::run_server(store, address);
}
