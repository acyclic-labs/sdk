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
    #[wire(
        auth(
            action = "streams/append",
            action = "streams/create",
            extractor = "streams.transaction"
        ),
        client_streaming
    )]
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
    let mut command = Command::new(protoc_bin_vendored::protoc_bin_path()?);
    for family in [
        "actors",
        "filesystem",
        "objects",
        "machines",
        "inference",
        "stream",
        "harness",
        "contract-derive",
    ] {
        command.arg(format!(
            "--proto_path={}",
            root.join("rust/crates")
                .join(family)
                .join("proto")
                .display()
        ));
    }
    let status = command
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
    let extractor_extension = pool
        .get_extension_by_name("acyclic.validation.v1.extractors")
        .ok_or("registry")?;
    let product_extension = pool
        .get_extension_by_name("acyclic.validation.v1.products")
        .ok_or("products")?;
    let mut registry = std::collections::BTreeMap::new();
    let mut catalog = std::collections::BTreeSet::new();
    for file in pool.files() {
        let options = file.options();
        let products = options.get_extension(&product_extension);
        for product in products.as_list().ok_or("products list")? {
            let product = product.as_message().ok_or("product message")?;
            let actions = product.get_field_by_name("actions").ok_or("actions")?;
            for action in actions.as_list().ok_or("action list")? {
                catalog.insert(action.as_str().ok_or("action")?.to_owned());
            }
        }
        let entries = options.get_extension(&extractor_extension);
        for entry in entries.as_list().ok_or("entries list")? {
            let entry = entry.as_message().ok_or("extractor message")?;
            let name = entry
                .get_field_by_name("name")
                .ok_or("name")?
                .as_str()
                .ok_or("name string")?
                .to_owned();
            let resources_only = entry
                .get_field_by_name("resources_only")
                .ok_or("resources_only")?
                .as_bool()
                .ok_or("bool")?;
            let actions = entry
                .get_field_by_name("possible_actions")
                .ok_or("possible_actions")?;
            let actions = actions
                .as_list()
                .ok_or("possible_actions list")?
                .iter()
                .map(|a| a.as_str().map(str::to_owned).ok_or("action string"))
                .collect::<Result<Vec<_>, _>>()?;
            assert!(!actions.is_empty());
            assert!(
                registry.insert(name, (resources_only, actions)).is_none(),
                "duplicate extractor"
            );
        }
    }
    assert!(
        registry
            .values()
            .all(|(_, actions)| actions.iter().all(|a| catalog.contains(a)))
    );
    for action in acyclic_auth::FS_ACTIONS {
        assert!(catalog.contains(&format!("fs/{action}")));
    }
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
                let extractor = rule.get_field_by_name("extractor").ok_or("extractor")?;
                let resource = rule.get_field_by_name("resource").ok_or("resource")?;
                let actions = rule.get_field_by_name("action").ok_or("action")?;
                let actions = actions.as_list().ok_or("action list")?;
                let extractor = extractor.as_str().ok_or("extractor string")?;
                let resource = resource.as_str().ok_or("resource string")?;
                assert!(
                    extractor.is_empty() != resource.is_empty(),
                    "{} requires one resource resolver",
                    method.full_name()
                );
                assert!(actions.iter().all(|a| {
                    a.as_str()
                        .is_some_and(|a| acyclic_auth::valid_action(a) && catalog.contains(a))
                }));
                if extractor.is_empty() {
                    assert!(!actions.is_empty() && resource.contains(":/"));
                } else {
                    let (resources_only, possible) =
                        registry.get(extractor).ok_or("unregistered extractor")?;
                    assert_eq!(
                        *resources_only,
                        !actions.is_empty(),
                        "{} must declare static actions",
                        method.full_name()
                    );
                    assert!(
                        actions
                            .iter()
                            .all(|a| possible.iter().any(|p| Some(p.as_str()) == a.as_str()))
                    );
                }
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
