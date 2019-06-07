extern crate capnpc;

fn main() {
    ::capnpc::CompilerCommand::new()
        .src_prefix("src/proto")
        .edition(::capnpc::RustEdition::Rust2018)
        .file("src/proto/filesystem.capnp")
        .run()
        .expect("schema compiler command");
}
