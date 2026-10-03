use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../..")
        .canonicalize()
        .expect("sdk repository root")
}

fn temporary_root() -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "acyclic-product-parity-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos()
    ));
    fs::create_dir_all(&root).expect("temporary product root");
    root
}

fn run_product_command(command: &str, root: &Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_sdk-contract-wire"))
        .arg(command)
        .arg("--root")
        .arg(root)
        .output()
        .expect("run Rust product exporter")
}

#[test]
fn generated_product_artifacts_are_exact_and_drift_is_rejected() {
    let root = temporary_root();
    let generated = run_product_command("generate-products", &root);
    assert!(
        generated.status.success(),
        "generator failed: {}",
        String::from_utf8_lossy(&generated.stderr)
    );

    let checked = run_product_command("check-products", &root);
    assert!(
        checked.status.success(),
        "fresh product artifacts failed parity: {}",
        String::from_utf8_lossy(&checked.stderr)
    );

    for relative in [
        "rust/crates/filesystem/src/generated/rust-model-filesystem-v2.bin",
        "rust/crates/inference/inference_model_descriptor.bin",
        "rust/crates/inference-contract/inference_model_descriptor.bin",
        "rust/crates/inference/inference_model_descriptor_docs.bin",
        "rust/crates/inference-contract/inference_model_descriptor_docs.bin",
        "rust/crates/inference/inference_descriptor.bin",
        "rust/crates/machines/src/generated/acyclic-machines-v1.model.bin",
        "rust/crates/machines/src/generated/acyclic-machines-v1.model.docs.bin",
        "rust/crates/machines/src/generated/acyclic-machines-v1.bin",
    ] {
        let path = root.join(relative);
        let mut bytes = fs::read(&path).expect("generated product fixture");
        bytes.push(0);
        fs::write(&path, bytes).expect("mutate product fixture");

        let drifted = run_product_command("check-products", &root);
        assert!(
            !drifted.status.success(),
            "drifted product unexpectedly passed: {relative}"
        );
        let output = format!(
            "{}{}",
            String::from_utf8_lossy(&drifted.stdout),
            String::from_utf8_lossy(&drifted.stderr)
        );
        assert!(
            output.contains("product artifact drifted"),
            "drift diagnostic missing for {relative}: {output}"
        );

        let restored = run_product_command("generate-products", &root);
        assert!(
            restored.status.success(),
            "product regeneration failed for {relative}: {}",
            String::from_utf8_lossy(&restored.stderr)
        );
    }

    let _ = fs::remove_dir_all(root);
}

#[test]
fn archived_descriptors_reject_drift_without_rewriting_the_fixture() {
    use acyclic_sdk_contract_wire::bindings::BindingFamily;

    let root = temporary_root();
    for (relative, expected) in [
        (
            "rust/crates/inference/inference_descriptor.bin",
            BindingFamily::Inference.archived_runtime_descriptor(),
        ),
        (
            "rust/crates/machines/src/generated/acyclic-machines-v1.bin",
            BindingFamily::Machines.archived_runtime_descriptor(),
        ),
    ] {
        let path = root.join(relative);
        fs::create_dir_all(path.parent().expect("archive parent")).expect("create archive parent");
        fs::write(&path, expected).expect("write archived descriptor");
        let mut mutated = fs::read(&path).expect("generated archived descriptor");
        mutated.push(0);
        fs::write(&path, &mutated).expect("mutate archived descriptor");

        let checked = fs::read(&path).expect("read archived descriptor for hash check");
        assert_ne!(
            checked, expected,
            "archived drift unexpectedly passed: {relative}"
        );
        assert_eq!(
            checked, mutated,
            "archive validation rewrote immutable fixture: {relative}"
        );
    }

    let _ = fs::remove_dir_all(root);
}

#[test]
fn tonic_generation_preserves_inference_model_options_and_archive_identity() {
    use acyclic_sdk_contract_wire::bindings::{
        BindingFamily, BindingTransport, generate_rust_bindings,
    };
    use acyclic_sdk_contract_wire::inference::inference_descriptor;

    let output = temporary_root().join("inference-tonic");
    let generated = generate_rust_bindings(
        BindingFamily::Inference,
        &output,
        BindingTransport::Tonic {
            client: true,
            server: true,
        },
    )
    .expect("generate Inference tonic bindings");
    assert_eq!(
        generated.model_descriptor,
        inference_descriptor(),
        "tonic generation changed the Rust-model descriptor bytes"
    );
    assert_eq!(
        generated.archived_runtime_descriptor,
        include_bytes!("fixtures/inference-v1.descriptor.bin"),
        "tonic generation changed the archived handshake identity"
    );
    assert!(
        fs::read_dir(&output)
            .expect("generated Inference tonic directory")
            .any(|entry| entry
                .expect("generated Inference tonic entry")
                .path()
                .extension()
                .is_some_and(|extension| extension == "rs")),
        "tonic generation produced no Rust source"
    );

    let _ = fs::remove_dir_all(output.parent().expect("temporary root"));
}

#[test]
fn committed_product_artifacts_match_the_rust_model() {
    let root = repository_root();
    let checked = run_product_command("check-products", &root);
    assert!(
        checked.status.success(),
        "committed product artifacts failed parity: {}{}",
        String::from_utf8_lossy(&checked.stdout),
        String::from_utf8_lossy(&checked.stderr)
    );
}
