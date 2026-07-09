fn main() {
    tonic_build::configure()
        .build_server(false)
        .compile_protos(
            &["proto/auth.proto","proto/searcher.proto","proto/bundle.proto","proto/packet.proto","proto/shared.proto"],
            &["proto"],
        )
        .expect("compile jito protos");
}
