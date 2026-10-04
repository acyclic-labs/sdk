use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

fn temporary_root(label: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "acyclic-product-routing-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos()
    ));
    fs::create_dir_all(&root).expect("temporary product root");
    root
}

fn run_product_command(command: &str, root: &Path, output: &Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_sdk-contract-wire"))
        .arg(command)
        .arg("--root")
        .arg(root)
        .arg("--out")
        .arg(output)
        .output()
        .expect("run Rust product exporter")
}

#[test]
fn explicit_output_routing_keeps_frozen_source_checkout_untouched() {
    let source = temporary_root("source");
    let output = temporary_root("output");

    let generated = run_product_command("generate-products", &source, &output);
    assert!(
        generated.status.success(),
        "generator failed with explicit output: {}",
        String::from_utf8_lossy(&generated.stderr)
    );
    assert!(
        fs::read_dir(&source)
            .expect("source root directory")
            .next()
            .is_none(),
        "product generation wrote into the Rust source checkout"
    );
    assert!(
        output
            .join("rust/crates/filesystem/src/generated/rust-model-filesystem-v2.bin")
            .is_file(),
        "explicit product output is missing the filesystem descriptor"
    );

    let policy_paths = [
        "ruby/lib/acyclic_sdk/generated_remote_policy.rb",
        "php/src/Acyclic/Runtime/GeneratedRemotePolicy.php",
        "dart/lib/src/generated_remote_policy.dart",
        "jvm/src/main/java/dev/acyclic/transport/GeneratedRemotePolicy.java",
        "dotnet/GeneratedRemotePolicy.cs",
    ];
    for relative in policy_paths {
        assert!(
            output.join(relative).is_file(),
            "missing external policy: {relative}"
        );
    }

    let checked = run_product_command("check-products", &source, &output);
    assert!(
        checked.status.success(),
        "explicit product output failed parity: {}",
        String::from_utf8_lossy(&checked.stderr)
    );

    assert!(
        fs::read_dir(&source)
            .expect("source after parity check")
            .next()
            .is_none()
    );
    fs::write(output.join(policy_paths[0]), b"tampered generated policy\n")
        .expect("tamper external output");
    let tampered = run_product_command("check-products", &source, &output);
    assert!(
        !tampered.status.success(),
        "parity check accepted an edited external policy"
    );
    assert!(
        fs::read_dir(&source)
            .expect("source after negative check")
            .next()
            .is_none()
    );

    let _ = fs::remove_dir_all(source);
    let _ = fs::remove_dir_all(output);
}
