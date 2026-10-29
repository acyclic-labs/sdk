//! Generate the public machines contract from the crate-owned protocol.

#[path = "build/client.rs"]
mod client;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let grpc = std::env::var_os("CARGO_FEATURE_GRPC").is_some();
    let mut config = acyclic_proto_build::config()?;
    config.type_attribute(
        ".acyclic.harness.v2",
        "#[derive(serde::Serialize, serde::Deserialize)]",
    );
    config.type_attribute(
        ".acyclic.harness.v2",
        "#[cfg_attr(target_arch = \"wasm32\", derive(tsify::Tsify))]",
    );
    config.type_attribute(
        ".acyclic.harness.v2",
        "#[serde(rename_all = \"camelCase\")]",
    );
    let services = acyclic_proto_build::services(grpc)
        .build_transport(std::env::var("CARGO_CFG_TARGET_ARCH")? != "wasm32")
        .server_mod_attribute(".", "#[cfg(not(target_arch = \"wasm32\"))]");
    acyclic_proto_build::compile(
        config,
        services,
        &["proto/machines/v1/machines.proto"],
        "acyclic-machines-v1.bin",
    )?;
    let output = std::env::var_os("OUT_DIR").ok_or("OUT_DIR is unset")?;
    client::generate(std::path::Path::new(&output))
}
