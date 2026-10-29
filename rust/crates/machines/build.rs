//! Generate the public machines contract from the crate-owned protocol.

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
    acyclic_proto_build::compile(
        config,
        acyclic_proto_build::services(grpc),
        &["proto/machines/v1/machines.proto"],
        "acyclic-machines-v1.bin",
    )
}
