fn main() -> Result<(), Box<dyn std::error::Error>> {
    // SAFETY: This is safe because build scripts are single-threaded and run before
    // any other code. We're setting PROTOC once at the start of the build process.
    unsafe {
        std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    }

    // Only re-run protoc when proto files change.
    println!("cargo:rerun-if-changed=../admin/proto/admin.proto");
    println!("cargo:rerun-if-changed=../auth/proto/auth.proto");
    println!("cargo:rerun-if-changed=../chaos/proto/chaos.proto");
    println!("cargo:rerun-if-changed=../custodian/proto/custodian.proto");
    println!("cargo:rerun-if-changed=../db/proto/db.proto");
    println!("cargo:rerun-if-changed=../honeypot/proto/honeypot.proto");

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &[
                "../admin/proto/admin.proto",
                "../auth/proto/auth.proto",
                "../chaos/proto/chaos.proto",
                "../custodian/proto/custodian.proto",
                "../db/proto/db.proto",
                "../honeypot/proto/honeypot.proto",
            ],
            &[
                "../admin/proto",
                "../auth/proto",
                "../chaos/proto",
                "../custodian/proto",
                "../db/proto",
                "../honeypot/proto",
            ],
        )?;
    Ok(())
}
