//! Contract codec, grammar and descriptor checks.
use acyclic_auth::{contract::*, valid_action, valid_resource};
use prost::Message;

#[test]
fn repeated_scalar_fields_and_required_caveats_round_trip() -> Result<(), Box<dyn std::error::Error>>
{
    let subject = Principal {
        kind: PrincipalKind::Agent,
        id: "agent".into(),
        lineage: vec!["parent".into(), "child".into()],
        groups: vec!["developers".into()],
        roles: vec![],
        attributes: vec![],
    };
    let wire: PrincipalProto = subject.clone().into();
    let decoded = Principal::try_from(PrincipalProto::decode(wire.encode_to_vec().as_slice())?)?;
    assert_eq!(decoded, subject);
    let filter = AttributeMatch {
        name: "author".into(),
        any_of: vec![vec![0, 1], vec![2]],
        subjects: vec![],
    };
    let wire: AttributeMatchProto = filter.clone().into();
    assert_eq!(
        AttributeMatch::try_from(AttributeMatchProto::decode(
            wire.encode_to_vec().as_slice()
        )?)?,
        filter
    );
    assert!(Caveat::try_from(CaveatProto { kind: None }).is_err());
    assert!(PrincipalKind::try_from(77).is_err());
    Ok(())
}

#[test]
fn resources_and_actions_use_the_shared_grammar() {
    for name in [
        "fs:/v/secret",
        "streams:/",
        "objects:/bucket/key",
        "actors:/class/id",
    ] {
        assert!(valid_resource(name), "{name}");
    }
    for name in [
        "fs:/v/../secret",
        "fs:/v/*",
        "fs:/v/secret?",
        "fs:/v\\secret",
        "fs://v",
        "BAD:/x",
    ] {
        assert!(!valid_resource(name), "{name}");
    }
    assert!(valid_action("fs/merge-into"));
    assert!(!valid_action("fs/ApplyTransaction"));
    assert!(valid_action("new-product/invoke"));
}

#[test]
fn rendered_contract_compiles_and_preserves_presence() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    render(root.path())?;
    let checked_in =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("proto/auth/v1/auth.proto");
    assert_eq!(
        std::fs::read(root.path().join("auth/v1/auth.proto"))?,
        std::fs::read(checked_in)?
    );
    let descriptor = root.path().join("auth.bin");
    let status = std::process::Command::new(protoc_bin_vendored::protoc_bin_path()?)
        .arg(format!("--proto_path={}", root.path().display()))
        .arg(format!("--descriptor_set_out={}", descriptor.display()))
        .arg("auth/v1/auth.proto")
        .status()?;
    assert!(status.success());
    let set = prost_types::FileDescriptorSet::decode(std::fs::read(descriptor)?.as_slice())?;
    let file = set.file.first().ok_or("missing descriptor")?;
    assert_eq!(file.package.as_deref(), Some("acyclic.auth.v1"));
    let limits = file
        .message_type
        .iter()
        .find(|message| message.name.as_deref() == Some("Limits"))
        .ok_or("missing limits")?;
    assert!(
        limits
            .field
            .iter()
            .all(|field| field.proto3_optional == Some(true))
    );
    Ok(())
}

#[test]
fn objects_opaque_key_examples() {
    for key in [
        "dir/../secret",
        "report?.txt",
        "a//b",
        "/x",
        "time:12:00",
        "%2F",
    ] {
        let name = acyclic_auth::objects_resource(b"bucket", key.as_bytes());
        assert!(valid_resource(&name));
        assert_eq!(
            acyclic_auth::decode_component(name.rsplit('/').next().unwrap()).unwrap(),
            key.as_bytes()
        );
    }
}
#[test]
fn packaged_options_are_identical() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
    let canonical =
        std::fs::read(root.join("rust/crates/contract-derive/proto/validation/v1/options.proto"))
            .unwrap();
    for copy in [
        "rust/crates/actors/proto/validation/v1/options.proto",
        "rust/crates/filesystem/proto/validation/v1/options.proto",
        "rust/crates/inference/proto/validation/v1/options.proto",
        "rust/crates/machines/proto/validation/v1/options.proto",
        "rust/crates/objects/proto/validation/v1/options.proto",
        "rust/crates/stream/proto/validation/v1/options.proto",
    ] {
        assert_eq!(canonical, std::fs::read(root.join(copy)).unwrap());
    }
}

#[test]
fn public_certificate_fuzz_seeds_are_admitted() {
    for seed in [
        include_bytes!("../fuzz/corpus/capability/public-certificates").as_slice(),
        include_bytes!("../fuzz/corpus/capability/lineage-chain").as_slice(),
    ] {
        assert!(acyclic_auth::authority::decode_capability(seed).is_ok());
    }
}
