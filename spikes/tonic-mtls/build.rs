fn main() -> Result<(), Box<dyn std::error::Error>> {
    // SAFETY: build scripts are single-threaded; PROTOC is set once before
    // any compilation happens. Same pattern as the production proto crate.
    unsafe {
        std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    }

    println!("cargo:rerun-if-changed=proto/echo.proto");

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/echo.proto"], &["proto"])?;

    Ok(())
}
