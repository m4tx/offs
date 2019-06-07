# offs [![Build Status](https://travis-ci.com/m4tx/offs.svg?token=7xjUrAWoxmsK6Jr6MJpx&branch=master)](https://travis-ci.com/m4tx/offs)

*offs* (OFfline FileSystem) is a distributed file system with offline work support.

## Features

* **Distributed**: allows any number of clients to be simultaneously connected
  to the same server.
* **File system**: the data is visible as a filesystem, allowing easy access via
  existing system tools.
* **Cached**: the files that were opened at least once can be accessed again
  without internet connection, be worked on, and the changes are synchronized
  once the network access is regained.
* **Content-addressable**: repeating file chunks are compressed away, so there
  is no need to re-download the same big file.

## Dependencies

* D-Bus
* FUSE 2.x
* SQLite

## Build dependencies

* [Cargo](https://doc.rust-lang.org/cargo/getting-started/installation.html)
  (Rust stable, beta, or nightly)
* D-Bus development libraries
* SQLite development libraries
* [capnp tool](https://capnproto.org/capnp-tool.html)

## Building

After installing Cargo, execute:

```bash
cargo build --release
```

The binaries can be found inside `target/release` directory.

## Usage

### Server

```bash
offs-server [-s store.db] [LISTEN ADDRESS]
```

`-s` option may be included to specify the file store database path. The port
that the server is listening at may be specified as the `ADDRESS` parameter
(default: `0.0.0.0:10031`)

### Client

```bash
offs-client [-c cache.db] <ADDRESS> <MOUNTPOINT>
```

The client requires the server address and a path to mount the filesystem in.

### Clientctl

```bash
offs-clientctl [-m mountpoint] offline-mode <on/off>
```

The client can be controlled during operation via `clientctl`. Use the
`-m` option to specify the client to control in case you have multiple instances
running at once.
