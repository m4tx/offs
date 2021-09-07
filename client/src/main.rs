use std::net::ToSocketAddrs;
use std::path::Path;

use clap::{App, Arg};
use nix::unistd::{fork, ForkResult};

use offs::store::Store;
use stderrlog::Timestamp;

mod client;
mod dbus_server;
mod remote_fs_client;

fn main() {
    let matches = App::new("offs client")
        .version("0.1")
        .author("Mateusz Maćkowski <m4tx@m4tx.pl>")
        .about("offs filesystem client module")
        .arg(
            Arg::with_name("cache")
                .short("c")
                .long("cache")
                .value_name("FILE")
                .help("Sets a custom cache database path")
                .default_value("cache.db"),
        )
        .arg(
            Arg::with_name("offline")
                .short("n")
                .long("offline")
                .help("Runs the client in the offline mode"),
        )
        .arg(
            Arg::with_name("foreground")
                .short("f")
                .long("foreground")
                .help("Operate in foreground"),
        )
        .arg(
            Arg::with_name("ADDRESS")
                .help("The address of the server to connect to")
                .validator(offs::validators::check_address)
                .required(true)
                .index(1),
        )
        .arg(
            Arg::with_name("MOUNT_POINT")
                .help("The path to mount the filesystem to")
                .validator(offs::validators::check_is_dir)
                .required(true)
                .index(2),
        )
        .arg(
            Arg::with_name("verbosity")
                .short("v")
                .multiple(true)
                .help("Increase message verbosity"),
        )
        .arg(
            Arg::with_name("quiet")
                .short("q")
                .help("Silence all output"),
        )
        .get_matches();

    let verbose = matches.occurrences_of("verbosity") as usize;
    let quiet = matches.is_present("quiet");
    stderrlog::new()
        .module(module_path!())
        .quiet(quiet)
        .verbosity(verbose)
        .timestamp(Timestamp::Millisecond)
        .init()
        .unwrap();

    let store = Store::new_client(matches.value_of("cache").unwrap()).unwrap();

    let address_str = matches.value_of("ADDRESS").unwrap();
    let address = address_str.to_socket_addrs().unwrap().next().unwrap();

    let offline = matches.is_present("offline");

    let mount_point = Path::new(matches.value_of("MOUNT_POINT").unwrap());

    unsafe {
        if !matches.is_present("foreground") {
            match fork() {
                Ok(ForkResult::Parent { .. }) => return,
                Ok(ForkResult::Child) => {}
                Err(_) => panic!("Fork failed"),
            }
        }
    }

    client::run_client(mount_point, address, offline, store);
}
