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
    ("remote-web", BindingAuthority::RustModelDescriptor),
];

#[test]
fn public_rust_binding_authority_inventory_is_explicit() {
    assert_eq!(EXPECTED_BINDING_AUTHORITY.len(), 10);
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
        7,
        "Stream, Filesystem, Harness, Inference, inference-contract, Machines, and remote-web consume the Rust model"
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

    let filesystem = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../filesystem/build.rs"
    ));
    assert_model_input(
        "Filesystem",
        filesystem,
        &[
            "BindingFamily::Filesystem",
            "descriptor_set_with_docs",
            "include_bytes!(\"src/generated/rust-model-filesystem-v2.bin\")",
        ],
    );

    let harness = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../harness/build.rs"));
    assert_model_input(
        "Harness",
        harness,
        &[
            "BindingFamily::Harness",
            "descriptor_set_with_docs",
            "include_bytes!(\"src/generated/rust-model-harness-v2.bin\")",
            "include_bytes!(\"src/generated/harness-archived-v2.bin\")",
        ],
    );

    let inference = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../inference/build.rs"
    ));
    assert_model_input(
        "Inference",
        inference,
        &[
            "const MODEL_DESCRIPTOR: &str = \"inference_model_descriptor.bin\"",
            "const DOC_DESCRIPTOR: &str = \"inference_model_descriptor_docs.bin\"",
            "const MODEL_DESCRIPTOR_ENV: &str = \"ACYCLIC_INFERENCE_MODEL_DESCRIPTOR\"",
            "unwrap_or_else(|| std::path::PathBuf::from(DOC_DESCRIPTOR))",
            "std::fs::read(&descriptor_path)",
        ],
    );

    let inference_contract = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../inference-contract/build.rs"
    ));
    assert_model_input(
        "Inference contract",
        inference_contract,
        &[
            "const MODEL_DESCRIPTOR: &str = \"inference_model_descriptor.bin\"",
            "const MODEL_DESCRIPTOR_ENV: &str = \"ACYCLIC_INFERENCE_CONTRACT_MODEL_DESCRIPTOR\"",
            "unwrap_or_else(|| std::path::PathBuf::from(MODEL_DESCRIPTOR))",
            "std::fs::read(&descriptor_path)",
        ],
    );

    let machines = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../machines/build.rs"));
    assert_model_input(
        "Machines",
        machines,
        &[
            "BindingFamily::Machines",
            "machines_descriptor()",
            "descriptor_set_with_docs",
        ],
    );

    let remote_web = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../sdk-remote-web/build.rs"
    ));
    assert_model_input(
        "remote web",
        remote_web,
        &[
            "BindingFamily::Filesystem",
            "filesystem_family.model_descriptor()",
            "BindingFamily::Harness",
            "harness_family.model_descriptor()",
        ],
    );

    for (name, script) in [
        ("Filesystem", filesystem),
        ("Inference", inference),
        ("Inference contract", inference_contract),
        ("Machines", machines),
        ("Harness", harness),
    ] {
        assert!(script.contains("compile_fds"), "{name} must compile descriptors");
        assert!(
            !script.contains("compile_protos"),
            "{name} must not compile an authored proto tree"
        );
        assert!(!script.contains("proto/"), "{name} must not read a proto tree");
    }
}

fn assert_model_input(name: &str, script: &str, required_tokens: &[&str]) {
    for token in required_tokens {
        assert!(
            script.contains(token),
            "{name} build script is missing its explicit Rust-owned input: {token}"
        );
    }
}
