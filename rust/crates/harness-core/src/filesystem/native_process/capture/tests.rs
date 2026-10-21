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

#[cfg(all(feature = "test-support", unix))]
mod simulation {
    use super::*;
    use acyclic_native_runtime::testing::{Schedule, fill_seeded, unix_millis};
    use std::{future::Future, pin::Pin, sync::OnceLock};

    #[derive(Debug, PartialEq, Eq)]
    struct Observed {
        source: (bool, [u8; 16], Option<u64>),
        trace: Vec<String>,
        boundaries: Vec<&'static str>,
        polls: Vec<(u64, &'static str)>,
        stdout: Vec<u8>,
    }

    fn capture_probe(entropy: bool, simulated_process: bool) -> Result<Observed> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|cause| Error::Storage(cause.to_string()))?;
        runtime.block_on(async {
            tokio::time::pause();
            let (schedule, mut boundaries) = if entropy {
                Schedule::seeded(71)
            } else {
                Schedule::without_entropy(71)
            };
            if simulated_process {
                schedule.set_process_provider(|request, check, observe| {
                    use std::os::unix::process::ExitStatusExt;
                    assert!(unix_millis().is_some(), "provider escaped its clock scope");
                    assert_eq!(request.command.get_program(), "/bin/sh");
                    assert_eq!(
                        request.command.get_args().collect::<Vec<_>>(),
                        ["-c", "printf dst"]
                    );
                    assert_eq!(request.timeout, Duration::from_millis(1000));
                    assert_eq!(request.maximum, 3);
                    assert!(request.input.is_none());
                    check().expect("script ownership check");
                    for chunk in [b"s".as_slice(), b"im".as_slice()] {
                        let action = observe(acyclic_native_runtime::ProcessOutput::Stdout(chunk))
                            .expect("script observer");
                        assert!(action.bytes.is_empty());
                        assert!(!action.complete);
                    }
                    check().expect("script terminal ownership check");
                    Ok(std::process::Output {
                        status: std::process::ExitStatus::from_raw(0),
                        stdout: b"sim".to_vec(),
                        stderr: Vec::new(),
                    })
                });
            }
            let source = Arc::new(OnceLock::new());
            let worker_source = Arc::clone(&source);
            let invocation = Invocation {
                executable: "/bin/sh".into(),
                argv: vec!["-c".into(), "printf dst".into()],
                cwd: "/".into(),
                environment: Default::default(),
                inherit_environment: false,
                timeout_ms: 1000,
                control_timeout_ms: 1000,
                cancellation_poll_ms: 25,
                maximum_output_bytes: 3,
                mcp_stdio: None,
                standard_input: None,
            };
            let handle = tokio::runtime::Handle::current();
            let operation = capture_with_custody(
                invocation,
                |custody: &mut Vec<u8>, chunk| {
                    if let acyclic_native_runtime::ProcessOutput::Stdout(bytes) = chunk {
                        custody.extend_from_slice(bytes);
                    }
                    Ok(())
                },
                move || {
                    if worker_source.get().is_none() {
                        let mut bytes = [0; 16];
                        let seeded = fill_seeded(&mut bytes);
                        worker_source.set((seeded, bytes, unix_millis())).unwrap();
                        // Exercise a real Pending ownership check. A worker
                        // retaining its permit here deadlocks the seeded driver.
                        assert!(tokio::runtime::Handle::try_current().is_err());
                        wait_control(&handle, || {
                            assert!(
                                tokio::runtime::Handle::try_current().is_ok(),
                                "control future constructed outside its runtime"
                            );
                            tokio::time::sleep(Duration::from_millis(3))
                        });
                        assert!(tokio::runtime::Handle::try_current().is_err());
                    }
                    Ok(())
                },
                Vec::new(),
            );
            type Capture<'a> =
                Pin<Box<dyn Future<Output = Result<(Option<NativeProcessResult>, Vec<u8>)>> + 'a>>;
            let operation: Capture<'_> = Box::pin(operation);
            let mut operation = Box::pin(schedule.scope(operation));
            let mut phases = Vec::new();
            let (output, custody) = loop {
                tokio::select! {
                    biased;
                    boundary = boundaries.recv() => {
                        let boundary = boundary.expect("lost process boundary");
                        phases.push(boundary.name);
                        boundary.release();
                    },
                    output = &mut operation => break output?,
                }
            };
            schedule.drained().await;
            let output = output.expect("native capture was interrupted");
            assert!(output.success, "{:?}", output.stop);
            assert_eq!(output.exit_code, Some(0));
            assert!(output.stderr.is_empty());
            assert_eq!(
                output.stdout,
                if simulated_process { b"sim" } else { b"dst" }
            );
            assert_eq!(custody, output.stdout, "observer lost worker custody");
            Ok(Observed {
                source: *source.get().expect("ownership was never checked"),
                trace: schedule.trace(),
                boundaries: phases,
                polls: schedule.polls(),
                stdout: output.stdout,
            })
        })
    }

    fn retained_sources(observed: &Observed) -> bool {
        observed.source.0
            && observed.source.2.is_some()
            // Capture uses the native IO service rather than occupying a
            // Tokio publication worker. Require its complete original custody
            // path, including physical completion, in the seeded trace.
            && observed.trace.iter().any(|event| event == "native:dispatch")
            && ["native:submitted", "native:completed"]
                .iter()
                .all(|phase| observed.boundaries.contains(phase))
            && !observed.trace.iter().any(|event| event == "blocking:dispatch")
    }

    #[test]
    fn capture_retains_seeded_worker_sources_and_custody() -> Result<()> {
        let first = capture_probe(true, false)?;
        let second = capture_probe(true, false)?;
        assert!(retained_sources(&first));
        assert_eq!(first, second, "real process handoff did not replay");
        // Erase only the worker's entropy source. Keep its seeded executor,
        // clock and physical terminal, including the same Pending control check.
        assert!(
            !retained_sources(&capture_probe(false, false)?),
            "source-erased process handoff escaped the same oracle"
        );
        Ok(())
    }

    #[test]
    fn scoped_process_provider_replays_at_the_original_worker_boundary() -> Result<()> {
        let first = capture_probe(true, true)?;
        assert_eq!(first, capture_probe(true, true)?);
        let accepted = |observed: &Observed| {
            observed.stdout == b"sim"
                && observed
                    .trace
                    .iter()
                    .any(|event| event.starts_with("process:"))
                && retained_sources(observed)
        };
        assert!(accepted(&first));
        // Removing the provider restores the original OS path. The same
        // output/custody/dispatch oracle must reject that boundary mutation.
        assert!(!accepted(&capture_probe(true, false)?));
        Ok(())
    }
}
