use super::*;
use proptest::prelude::*;

#[test]
fn capture_output_child() {
    println!("captured");
}

#[test]
fn capture_callbacks_can_reenter_a_single_tokio_blocking_worker()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    acyclic_native_runtime::initialize_native_io_services()?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(1)
        .enable_all()
        .build()?;
    let handle = runtime.handle().clone();
    let observed = Arc::new(AtomicUsize::new(0));
    let completed = Arc::clone(&observed);
    let root = tempfile::tempdir()?;
    let invocation = Invocation {
        executable: std::env::current_exe()?,
        argv: vec![
            "--exact".into(),
            "filesystem::native_process::capture::tests::capture_output_child".into(),
            "--nocapture".into(),
        ],
        cwd: root.path().into(),
        environment: Default::default(),
        inherit_environment: true,
        timeout_ms: 300_000,
        control_timeout_ms: 1000,
        cancellation_poll_ms: 25,
        maximum_output_bytes: 32_768,
        standard_input: None,
        mcp_stdio: None,
    };
    let observer = Arc::new(move |_: acyclic_native_runtime::ProcessOutput<'_>| {
        // The old capture owner occupied this runtime's only blocking worker.
        // Detect that placement before attempting re-entry, so its negative
        // control fails immediately instead of leaving a blocked worker.
        assert!(
            tokio::runtime::Handle::try_current().is_err(),
            "capture occupies the publication executor"
        );
        let completed = Arc::clone(&completed);
        handle.block_on(async move {
            tokio::task::spawn_blocking(move || {
                completed.fetch_add(1, Ordering::SeqCst);
            })
            .await
            .map_err(std::io::Error::other)
        })
    });
    let output = runtime.block_on(capture_checked(invocation, Some(observer), || Ok(())))?;
    let output = output.ok_or_else(|| Error::Storage("capture was interrupted".into()))?;
    assert!(output.success, "{output:?}");
    assert!(String::from_utf8_lossy(&output.stdout).contains("captured"));
    assert!(observed.load(Ordering::SeqCst) > 0);
    Ok(())
}

#[test]
fn capture_owner_check_constructs_its_timer_on_the_runtime()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    acyclic_native_runtime::initialize_native_io_services()?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let handle = runtime.handle().clone();
    let check = acyclic_native_runtime::run_blocking_io(
        move || {
            assert!(tokio::runtime::Handle::try_current().is_err());
            check_on_runtime(&handle, Duration::from_millis(1000), async {
                // Construct and await a real timer on the production owner-check path.
                tokio::time::sleep(Duration::ZERO).await;
                Ok(())
            })
        },
        None,
    )?;
    runtime.block_on(check)??;
    Ok(())
}

proptest! {
    // Every backend/control cause remains recoverable beneath an Interrupted
    // classification, so ownership loss follows the control-stop path.
    #[test]
    fn control_failure_preserves_its_concrete_cause(code in 1i32..4096) {
        let failure = control_error(std::io::Error::from_raw_os_error(code));
        prop_assert_eq!(failure.kind(), std::io::ErrorKind::Interrupted);
        prop_assert_eq!(failure.get_ref()
            .and_then(|cause| cause.downcast_ref::<std::io::Error>())
            .and_then(std::io::Error::raw_os_error), Some(code));
    }
}
