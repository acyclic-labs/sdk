//! Audit the Rust crates that materialize public bindings from explicit inputs.
//! This is deliberately explicit: a new build script must declare its
//! authority here before it can silently become a second schema source.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BindingAuthority {
    /// Rust-owned descriptor model is the input to prost/tonic generation.
    RustModelDescriptor,
    /// The crate still invokes protoc over an authored or mirrored .proto.
    AuthoredProto,
    /// The crate packages committed generated Rust and descriptor bytes.
    CommittedGenerated,
}

const EXPECTED_BINDING_AUTHORITY: &[(&str, BindingAuthority)] = &[
    ("actors", BindingAuthority::CommittedGenerated),
    ("objects", BindingAuthority::CommittedGenerated),
    ("workers", BindingAuthority::CommittedGenerated),
    ("machines", BindingAuthority::RustModelDescriptor),
    ("stream", BindingAuthority::RustModelDescriptor),
    ("harness", BindingAuthority::RustModelDescriptor),
    ("filesystem", BindingAuthority::RustModelDescriptor),
    ("inference", BindingAuthority::RustModelDescriptor),
    ("inference-contract", BindingAuthority::RustModelDescriptor),
];

#[test]
fn public_rust_binding_authority_inventory_is_explicit() {
    assert_eq!(EXPECTED_BINDING_AUTHORITY.len(), 9);
    assert_eq!(
        EXPECTED_BINDING_AUTHORITY
            .iter()
            .filter(|(_, authority)| *authority == BindingAuthority::AuthoredProto)
            .count(),
        0,
        "all public binding build scripts consume explicit model or committed inputs"
    );
    assert_eq!(
        EXPECTED_BINDING_AUTHORITY
            .iter()
            .filter(|(_, authority)| *authority == BindingAuthority::RustModelDescriptor)
            .count(),
        6,
        "Stream, Filesystem, Harness, Inference, inference-contract, and Machines consume the Rust model"
    );
}

#[test]
fn build_scripts_have_bounded_inputs_and_explicit_authority() {
    let stream = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../stream/build.rs"));
    assert!(stream.contains("BindingFamily::Stream"));
    assert!(stream.contains("generate_rust_bindings"));
    assert!(!stream.contains("proto/stream/v2/stream.proto"));
    assert!(stream.contains("cargo:rerun-if-changed=build.rs"));
    let stream_lib = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../stream/src/lib.rs"));
    assert!(stream_lib.contains("stream_descriptor.bin"));

    for script in [
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../filesystem/build.rs"
        )),
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../inference/build.rs"
        )),
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../inference-contract/build.rs"
        )),
        include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../machines/build.rs")),
        include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../harness/build.rs")),
    ] {
        assert!(script.contains("compile_fds"));
        assert!(!script.contains("compile_protos"));
        assert!(!script.contains("proto/"));
        assert!(script.contains("MODEL_DESCRIPTOR"));
    }
}
