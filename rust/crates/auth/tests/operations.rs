//! Descriptor coverage and compiler-linked authorization option checks.
use prost_reflect::DescriptorPool;
use std::{path::Path, process::Command};

#[acyclic_contract_derive::message(error = acyclic_auth::Error, file = OptionsFile)]
struct Paths {
    #[wire(tag = 1, string)]
    paths: Vec<String>,
}

#[acyclic_contract_derive::service(file = OptionsFile)]
enum MultiResource {
    #[wire(auth(action = "streams/append", resource = "streams:/{paths[]}"))]
    #[wire(auth(extractor = "streams.transaction"), client_streaming)]
    Commit {
        request: PathsProto,
        response: PathsProto,
    },
}

#[acyclic_contract_derive::file(
    family = "optionstest",
    messages(PathsProto),
    services(MultiResource)
)]
struct OptionsFile;

#[test]
fn derived_options_preserve_repeated_resources_and_streaming()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    OptionsFile::render(root.path())?;
    let status = Command::new(protoc_bin_vendored::protoc_bin_path()?)
        .arg(format!("--proto_path={}", root.path().display()))
        .arg(format!(
            "--proto_path={}",
            protoc_bin_vendored::include_path()?.display()
        ))
        .arg(format!(
            "--descriptor_set_out={}",
            root.path().join("options.bin").display()
        ))
        .arg("--include_imports")
        .arg("optionstest/v1/optionstest.proto")
        .status()?;
    assert!(status.success());
    let pool = DescriptorPool::decode(std::fs::read(root.path().join("options.bin"))?.as_slice())?;
    let extension = pool
        .get_extension_by_name("acyclic.validation.v1.auth")
        .ok_or("missing auth")?;
    let service = pool
        .get_service_by_name("acyclic.optionstest.v1.MultiResource")
        .ok_or("missing service")?;
    let method = service.methods().next().ok_or("missing method")?;
    assert!(method.is_client_streaming());
    assert_eq!(
        method
            .options()
            .get_extension(&extension)
            .as_list()
            .ok_or("rules")?
            .len(),
        2
    );
    assert!(MultiResource::schema().contains("streams:/{paths[]}"));
    Ok(())
}

#[test]
fn every_product_rpc_declares_authorization() -> Result<(), Box<dyn std::error::Error>> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
    let temp = tempfile::tempdir()?;
    let output = temp.path().join("products.bin");
    let sources = [
        "actors/v1/actors.proto",
        "filesystem/v2/filesystem.proto",
        "objects/v1/objects.proto",
        "machines/v1/machines.proto",
        "inference/v1/inference.proto",
        "stream/v1/stream.proto",
        "stream/v1/session.proto",
    ];
    let status = Command::new(protoc_bin_vendored::protoc_bin_path()?)
        .arg(format!("--proto_path={}", root.join("proto").display()))
        .arg(format!(
            "--proto_path={}",
            root.join("rust/crates/stream/proto").display()
        ))
        .arg(format!(
            "--proto_path={}",
            protoc_bin_vendored::include_path()?.display()
        ))
        .arg(format!("--descriptor_set_out={}", output.display()))
        .arg("--include_imports")
        .args(sources)
        .status()?;
    assert!(status.success());
    let pool = DescriptorPool::decode(std::fs::read(output)?.as_slice())?;
    let auth = pool
        .get_extension_by_name("acyclic.validation.v1.auth")
        .ok_or("missing auth option")?;
    let mut count = 0;
    for service in pool.services() {
        for method in service.methods() {
            let options = method.options();
            let value = options.get_extension(&auth);
            let rules = value.as_list().ok_or("auth option must be repeated")?;
            assert!(
                !rules.is_empty(),
                "{} lacks authorization",
                method.full_name()
            );
            for rule in rules {
                let rule = rule.as_message().ok_or("auth rule must be a message")?;
                let extractor = rule
                    .get_field_by_name("extractor")
                    .ok_or("extractor field")?;
                let action = rule.get_field_by_name("action").ok_or("action field")?;
                let resource = rule.get_field_by_name("resource").ok_or("resource field")?;
                let extractor = extractor.as_str().ok_or("extractor string")?;
                let action = action.as_str().ok_or("action string")?;
                let resource = resource.as_str().ok_or("resource string")?;
                assert!(
                    extractor.is_empty()
                        && acyclic_auth::valid_action(action)
                        && resource.contains(":/")
                        || !extractor.is_empty() && action.is_empty() && resource.is_empty(),
                    "{} invalid declaration",
                    method.full_name()
                );
            }
            count += 1;
        }
    }
    assert!(
        count > 80,
        "product service coverage unexpectedly shrank: {count}"
    );
    Ok(())
}
