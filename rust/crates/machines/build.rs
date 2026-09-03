//! Generates the exact public Machines transport from its canonical schema.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    let mut prost = prost_build::Config::new();
    prost.protoc_executable(protoc);
    let output = std::path::PathBuf::from(
        std::env::var_os("OUT_DIR").ok_or("Cargo did not provide OUT_DIR")?,
    );
    prost.file_descriptor_set_path(output.join("acyclic-machines-v1.bin"));
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_with_config(
            prost,
            &["../../../proto/machines/v1/machines.proto"],
            &["../../../proto"],
        )?;
    println!("cargo:rerun-if-changed=../../../proto/machines/v1/machines.proto");
    Ok(())
}
