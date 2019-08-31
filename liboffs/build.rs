fn main() {
    let proto_root = "src/proto";
    protoc_grpcio::compile_grpc_protos(&["filesystem.proto"], &[proto_root], &proto_root, None)
        .expect("Failed to compile gRPC definitions");
}
