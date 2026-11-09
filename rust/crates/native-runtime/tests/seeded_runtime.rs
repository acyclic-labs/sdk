//! The published test-support entry point admits only configured replay.
#![cfg(feature = "test-support")]

#[test]
fn seeded_runtime_requires_explicit_executor_configuration() {
    let result = acyclic_native_runtime::testing::seeded_runtime(71);
    if cfg!(tokio_unstable) {
        let runtime = result.expect("configured replay must build its original seeded executor");
        runtime.block_on(async { tokio::task::yield_now().await });
    } else {
        let error = result.expect_err("unconfigured replay admitted an unseeded executor");
        assert_eq!(error.kind(), std::io::ErrorKind::Unsupported);
        assert!(
            error
                .to_string()
                .contains("RUSTFLAGS=\"--cfg tokio_unstable\"")
        );
    }
}
