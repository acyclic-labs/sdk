//! Exercise the published Node entry against the original native executable.

#[test]
fn node_process_bridge_owns_real_input_output_and_descendant_cancellation() {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::ffi::{OsStrExt as _, OsStringExt as _};
        use std::os::unix::process::ExitStatusExt as _;

        let recovery = "step=native-argument-capture; recovery=cargo test --locked -p acyclic-native-runtime --test node_process -- node_process_bridge_owns_real_input_output_and_descendant_cancellation --exact --nocapture";
        let mut records = Vec::new();
        // Node cannot pass an invalid UTF-8 OS string. Exercise the same
        // executable directly, then retain the original native argv bytes.
        for (arguments, expected) in [
            (
                vec![
                    "--invocation".into(),
                    std::ffi::OsString::from_vec(vec![0xff]),
                ],
                125,
            ),
            (vec!["--inherit-input".into(), "/bin/false".into()], 1),
        ] {
            let mut command = std::process::Command::new(acyclic_test_support::binary_path!(
                "acyclic-process-host"
            ));
            command.args(arguments);
            let mut argv = Vec::new();
            for argument in command.get_args() {
                if !argv.is_empty() {
                    argv.push(0);
                }
                argv.extend_from_slice(argument.as_bytes());
            }
            let output = command.output().expect(recovery);
            assert_eq!(output.status.code(), Some(expected), "{recovery}");
            assert_eq!(output.status.signal(), None, "{recovery}");
            assert!(output.stdout.is_empty(), "{recovery}");
            if expected == 125 {
                assert!(
                    String::from_utf8_lossy(&output.stderr)
                        .contains("step=process-host-arguments;"),
                    "{recovery}"
                );
            } else {
                assert!(output.stderr.is_empty(), "{recovery}");
            }
            records.push(serde_json::json!({"argv": argv, "code": output.status.code(),
                "signal": output.status.signal(), "stdout": output.stdout, "stderr": output.stderr}));
        }
        println!(
            "PROCESS_HOST_NATIVE_ARGUMENTS_JSON={}",
            serde_json::to_string(&records).expect(recovery)
        );
    }
    let script = acyclic_test_support::source_path("typescript/packages/sdk/test/process.test.mjs");
    let status = std::process::Command::new("node")
        .arg(script)
        .env("ACYCLIC_PROCESS_HOST", acyclic_test_support::binary_path!("acyclic-process-host"))
        .status()
        .expect("step=node-process-control; recovery=install Node.js and rerun cargo test --locked -p acyclic-native-runtime --test node_process");
    assert!(
        status.success(),
        "step=node-process-control; recovery=inspect the original Node control failure and rerun cargo test --locked -p acyclic-native-runtime --test node_process after correction"
    );
}
