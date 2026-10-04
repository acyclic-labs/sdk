//! Contract-only RPC inventory derived from the Rust registry and immutable history.
use acyclic_sdk_contract_wire::family_registry::FAMILY_VIEWS;
use prost::Message;
use prost_types::FileDescriptorSet;
use serde_json::{Value, json};
use std::fs;
use std::path::PathBuf;

fn descriptor_rows(
    family: &str,
    descriptor: &[u8],
    retired: bool,
) -> Result<Vec<Value>, prost::DecodeError> {
    let view = FAMILY_VIEWS.iter().find(|view| view.name == family);
    let mut rows = Vec::new();
    for file in FileDescriptorSet::decode(descriptor)?.file {
        let package = file.package.as_deref().unwrap_or_default();
        for service in file.service {
            let service_name = service.name.as_deref().unwrap_or_default();
            for method in service.method {
                let method_name = method.name.as_deref().unwrap_or_default();
                let rpc = format!("{package}.{service_name}/{method_name}");
                let route = (!retired)
                    .then(|| {
                        view.and_then(|view| view.routes().iter().find(|route| route.rpc == rpc))
                    })
                    .flatten();
                let policy = (!retired)
                    .then(|| {
                        view.and_then(|view| {
                            view.operation_policies
                                .iter()
                                .find(|policy| policy.rpc == rpc)
                        })
                    })
                    .flatten();
                let mut local_name = method_name.to_owned();
                if let Some(first) = local_name.get_mut(..1) {
                    first.make_ascii_lowercase();
                }
                let kind = match (
                    method.client_streaming.unwrap_or(false),
                    method.server_streaming.unwrap_or(false),
                ) {
                    (false, false) => "unary",
                    (true, false) => "client_streaming",
                    (false, true) => "server_streaming",
                    (true, true) => "bi_di_streaming",
                };
                rows.push(json!({
                    "family": family, "rpc": rpc, "service": format!("{package}.{service_name}"),
                    "method": method_name, "localName": local_name, "kind": kind,
                    "request": method.input_type.as_deref().unwrap_or_default().trim_start_matches('.'),
                    "response": method.output_type.as_deref().unwrap_or_default().trim_start_matches('.'),
                    "contractStatus": if retired { "retired" } else { "target" },
                    "httpOperation": route.map(|route| route.path.trim_start_matches('/')),
                    "httpMethod": route.map(|route| route.method),
                    "capabilities": policy.map(|policy| policy.capabilities),
                    "errors": policy.map(|policy| policy.errors),
                    "validations": policy.map(|policy| policy.validations),
                }));
            }
        }
    }
    Ok(rows)
}

fn inventory() -> Result<Vec<Value>, prost::DecodeError> {
    let mut rows = Vec::new();
    for family in FAMILY_VIEWS {
        rows.extend(descriptor_rows(
            family.name,
            &family.model.descriptor(),
            false,
        )?);
    }
    rows.extend(descriptor_rows(
        "objects",
        include_bytes!("../../../../../compatibility/objects/v1/objects_descriptor.bin"),
        true,
    )?);
    rows.sort_by(|a, b| a["rpc"].as_str().cmp(&b["rpc"].as_str()));
    Ok(rows)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args_os().skip(1);
    let mut source_root = None;
    let mut output = None;
    while let Some(argument) = args.next() {
        match argument.to_string_lossy().as_ref() {
            "--source-root" => source_root = args.next().map(PathBuf::from),
            "--output" => output = args.next().map(PathBuf::from),
            unknown => return Err(format!("unknown sdk-rpc-contracts argument: {unknown}").into()),
        }
    }
    let source_root = source_root.ok_or("--source-root is required")?;
    if !source_root.is_dir() {
        return Err(format!("RPC inventory source root is missing: {}", source_root.display()).into());
    }
    let rendered = format!("{}\n", serde_json::to_string_pretty(&inventory()?)?);
    let output = output.ok_or("--output is required")?;
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&output, rendered.as_bytes())?;
    println!("wrote Rust RPC inventory to {}", output.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn inventory_has_every_registry_rpc_and_exact_routes() {
        let rows = inventory().expect("inventory");
        for view in FAMILY_VIEWS {
            let descriptor =
                FileDescriptorSet::decode(view.model.descriptor().as_slice()).expect("descriptor");
            let expected: usize = descriptor
                .file
                .iter()
                .flat_map(|file| &file.service)
                .map(|service| service.method.len())
                .sum();
            assert_eq!(
                rows.iter()
                    .filter(|row| row["family"] == view.name && row["contractStatus"] == "target")
                    .count(),
                expected
            );
            for route in view.routes() {
                let row = rows
                    .iter()
                    .find(|row| row["rpc"] == route.rpc)
                    .expect("RPC route owner");
                assert_eq!(row["httpOperation"], route.path.trim_start_matches('/'));
                assert_eq!(row["httpMethod"], route.method);
            }
        }
        assert!(rows.iter().any(|row| {
            row["contractStatus"] == "retired"
                && row["rpc"]
                    .as_str()
                    .is_some_and(|rpc| rpc.starts_with("acyclic.objects.v1."))
        }));
    }
    #[test]
    fn retired_contracts_never_inherit_current_http_or_policy_metadata() {
        for row in inventory()
            .expect("inventory")
            .iter()
            .filter(|row| row["contractStatus"] == "retired")
        {
            assert!(row["httpOperation"].is_null());
            assert!(row["capabilities"].is_null());
        }
    }
}
