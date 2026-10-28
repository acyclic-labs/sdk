//! Rust-owned generator payloads shared by the contract exporter.

mod defaults;
mod git_compat;

/// Exact stdout of each retained filesystem contract exporter, including its newline.
pub fn exports() -> Result<std::collections::BTreeMap<&'static str, String>, serde_json::Error> {
    Ok(std::collections::BTreeMap::from([
        (
            "filesystem-descriptor-digest",
            format!("{}\n", blake3::hash(acyclic_fs_wire::FILE_DESCRIPTOR_SET).to_hex()),
        ),
        (
            "filesystem-typescript-defaults",
            format!("{}\n", defaults::contract_json()?),
        ),
        (
            "filesystem-typescript-hosted-contract",
            format!("{}\n", crate::hosted_contract::contract_json()),
        ),
        (
            "filesystem-git-compat-contract",
            format!("{}\n", git_compat::contract_json()?),
        ),
    ]))
}
