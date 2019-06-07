use std::net::ToSocketAddrs;
use std::path::Path;

pub fn check_address(address: String) -> Result<(), String> {
    let address = address.to_socket_addrs();

    if address.is_err() {
        Err(address.unwrap_err().to_string())
    } else {
        Ok(())
    }
}

pub fn check_is_dir(path_string: String) -> Result<(), String> {
    let path = Path::new(&path_string);

    if !path.is_dir() {
        Err("the path does not point to a directory".to_owned())
    } else {
        Ok(())
    }
}
