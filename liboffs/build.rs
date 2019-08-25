extern crate capnpc;

fn main() {
    ::capnpc::CompilerCommand::new()
        .src_prefix("src/proto")
        .file("src/proto/filesystem.capnp")
        .run()
        .expect("schema compiler command");

    let proto_root = "src/proto";
    protoc_grpcio::compile_grpc_protos(
        &["filesystem.proto"],
        &[proto_root],
        &proto_root,
        None
    ).expect("Failed to compile gRPC definitions");
}
