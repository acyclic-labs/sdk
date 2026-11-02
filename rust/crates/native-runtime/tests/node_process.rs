//! Exercise the published Node entry against the original native executable.

#[test]
fn node_process_bridge_owns_real_input_output_and_descendant_cancellation() {
    let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../typescript/packages/sdk/test/process.test.mjs");
    let status = std::process::Command::new("node")
        .arg(script)
        .env("ACYCLIC_PROCESS_HOST", env!("CARGO_BIN_EXE_acyclic-process-host"))
        .status()
        .expect("step=node-process-control; recovery=install Node.js and rerun cargo test --locked -p acyclic-native-runtime --test node_process");
    assert!(
        status.success(),
        "step=node-process-control; recovery=inspect the original Node control failure and rerun cargo test --locked -p acyclic-native-runtime --test node_process after correction"
    );
}
