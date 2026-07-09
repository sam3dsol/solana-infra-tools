fn main() {
    tonic_build::configure()
        .build_server(false)
        .compile_protos(&["proto/shredstream.proto"], &["proto"])
        .expect("failed to compile shredstream.proto");
}
