use super::*;
use std::{
    pin::pin,
    task::{Context, Poll, Waker},
};

#[derive(Clone, Default)]
struct Output(Arc<std::sync::Mutex<Vec<u8>>>);

impl io::Write for Output {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn owned_diagnostic_lifecycle() {
    termination_reports_the_live_wait_before_cancellation_with_logging_disabled();
    dropped_producer_exits_with_its_typed_error_and_no_live_wait();
    broken_diagnostic_output_does_not_skip_graceful_owner_cleanup();
    blocked_diagnostic_output_does_not_delay_graceful_owner_cleanup();
}

fn blocked_diagnostic_output_does_not_delay_graceful_owner_cleanup() {
    struct Blocked {
        coordination: std::thread::ThreadId,
        starting: Option<tokio::sync::oneshot::Sender<()>>,
        release: Option<std::sync::mpsc::Receiver<()>>,
    }
    impl io::Write for Blocked {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if std::thread::current().id() == self.coordination {
                return Err(io::ErrorKind::WouldBlock.into());
            }
            if let Some(starting) = self.starting.take() {
                let _ = starting.send(());
            }
            if let Some(release) = self.release.take() {
                release
                    .recv()
                    .map_err(|error| io::Error::new(io::ErrorKind::BrokenPipe, error))?;
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let runtime = acyclic_native_runtime::coordination_runtime().unwrap();
    let report = WaitReport::default();
    let dispatch = tracing::Dispatch::new(subscriber(&report, Some("off".into())));
    let _subscriber = tracing::dispatcher::set_default(&dispatch);
    runtime.block_on(async {
        let (terminate, signal) = tokio::sync::oneshot::channel();
        let shutdown = async {
            signal.await.unwrap();
            Ok(143)
        }
        .boxed()
        .shared();
        let stopped = shutdown.clone();
        let (retire, retiring) = tokio::sync::oneshot::channel();
        let cleaned = std::cell::Cell::new(false);
        let operation = async {
            let retiring =
                acyclic_native_runtime::obs::waiting("channel-close", "cleanup-key", retiring);
            stopped.await.unwrap();
            retiring.await.unwrap();
            cleaned.set(true);
            Ok(0)
        };
        let (starting, started) = tokio::sync::oneshot::channel();
        let (release, held) = std::sync::mpsc::channel();
        let mut output = Blocked {
            coordination: std::thread::current().id(),
            starting: Some(starting),
            release: Some(held),
        };
        // The old synchronous sink fails the same finite boundary oracle.
        use io::Write as _;
        assert_eq!(
            output.write(b"control").unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        let mut observed = pin!(observe(&report, output, shutdown, true, operation));
        assert!(poll(observed.as_mut()).is_pending());
        terminate.send(()).unwrap();
        assert!(poll(observed.as_mut()).is_pending());
        started.await.unwrap();
        retire.send(()).unwrap();
        let Poll::Ready((result, diagnostic)) = poll(observed.as_mut()) else {
            panic!("blocked diagnostic sink prevented graceful cleanup")
        };
        assert_eq!(result.unwrap(), 0);
        assert!(cleaned.get());
        let mut diagnostic = pin!(diagnostic.unwrap());
        assert!(poll(diagnostic.as_mut()).is_pending());
        // Output owns this release producer; losing it ends the worker wait
        // in a typed error after cleanup has already completed.
        drop(release);
        assert_eq!(
            diagnostic.await.unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
    });
}

fn poll<F: Future>(future: std::pin::Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

fn termination_reports_the_live_wait_before_cancellation_with_logging_disabled() {
    let report = WaitReport::default();
    let dispatch = tracing::Dispatch::new(subscriber(&report, Some("off".into())));
    tracing::dispatcher::with_default(&dispatch, || {
        let (producer, completion) = tokio::sync::oneshot::channel::<()>();
        let (terminate, signal) = tokio::sync::oneshot::channel();
        let shutdown = async {
            signal.await.unwrap();
            Ok(143)
        }
        .boxed()
        .shared();
        let output = Output::default();
        let operation = async {
            acyclic_native_runtime::obs::waiting("channel-close", "turn-17", completion).await?;
            Ok(0)
        };
        {
            let mut observed = pin!(observe(&report, output.clone(), shutdown, false, operation));
            assert!(poll(observed.as_mut()).is_pending());
            terminate.send(()).unwrap();
            let Poll::Ready((result, diagnostic)) = poll(observed.as_mut()) else {
                panic!("termination did not wake the process boundary")
            };
            assert_eq!(result.unwrap(), 143);
            acyclic_native_runtime::coordination_runtime()
                .unwrap()
                .block_on(diagnostic.unwrap())
                .unwrap();
        }
        assert!(
            String::from_utf8(output.0.lock().unwrap().clone())
                .unwrap()
                .starts_with("waiting: channel-close turn-17 since ")
        );
        assert!(producer.is_closed());
    });
}

fn dropped_producer_exits_with_its_typed_error_and_no_live_wait() {
    let report = WaitReport::default();
    let dispatch = tracing::Dispatch::new(subscriber(&report, Some("off".into())));
    tracing::dispatcher::with_default(&dispatch, || {
        let (producer, completion) = tokio::sync::oneshot::channel::<()>();
        let shutdown = std::future::pending().boxed().shared();
        let output = Output::default();
        let operation = async {
            acyclic_native_runtime::obs::waiting("channel-close", "turn-18", completion).await?;
            Ok(0)
        };
        {
            let mut observed = pin!(observe(&report, output.clone(), shutdown, false, operation));
            assert!(poll(observed.as_mut()).is_pending());
            drop(producer);
            let Poll::Ready((result, diagnostic)) = poll(observed.as_mut()) else {
                panic!("closed producer left a pending operation")
            };
            assert!(
                result
                    .unwrap_err()
                    .is::<tokio::sync::oneshot::error::RecvError>()
            );
            acyclic_native_runtime::coordination_runtime()
                .unwrap()
                .block_on(diagnostic.unwrap())
                .unwrap();
        }
        assert!(output.0.lock().unwrap().is_empty());
    });
}

fn broken_diagnostic_output_does_not_skip_graceful_owner_cleanup() {
    struct Broken;
    impl io::Write for Broken {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            Err(io::ErrorKind::BrokenPipe.into())
        }
        fn flush(&mut self) -> io::Result<()> {
            Err(io::ErrorKind::BrokenPipe.into())
        }
    }
    let report = WaitReport::default();
    let (terminate, signal) = tokio::sync::oneshot::channel();
    let shutdown = async {
        signal.await.unwrap();
        Ok(143)
    }
    .boxed()
    .shared();
    let stopped = shutdown.clone();
    let (release, retiring) = tokio::sync::oneshot::channel();
    let cleaned = std::cell::Cell::new(false);
    let operation = async {
        stopped.await.unwrap();
        retiring.await.unwrap();
        cleaned.set(true);
        Ok(0)
    };
    let mut observed = pin!(observe(&report, Broken, shutdown, true, operation));
    assert!(poll(observed.as_mut()).is_pending());
    terminate.send(()).unwrap();
    assert!(poll(observed.as_mut()).is_pending());
    assert!(!cleaned.get());
    release.send(()).unwrap();
    let Poll::Ready((result, diagnostic)) = poll(observed.as_mut()) else {
        panic!("shutdown did not reach its owner")
    };
    assert_eq!(result.unwrap(), 0);
    assert!(cleaned.get());
    assert_eq!(
        acyclic_native_runtime::coordination_runtime()
            .unwrap()
            .block_on(diagnostic.unwrap())
            .unwrap_err()
            .kind(),
        io::ErrorKind::BrokenPipe
    );
}

#[cfg(unix)]
#[test]
fn sigterm_reports_from_the_process_boundary() {
    use std::io::{BufRead as _, Write as _};
    const CHILD: &str = "GRAPHCODER_WAIT_REPORT_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let report = WaitReport::default();
        let dispatch = tracing::Dispatch::new(subscriber(&report, Some("off".into())));
        let _subscriber = tracing::dispatcher::set_default(&dispatch);
        let runtime = acyclic_native_runtime::coordination_runtime().unwrap();
        runtime.block_on(async {
            let mut shutdown = shutdown();
            // Arm the actual signal receiver before publishing readiness.
            futures::future::poll_fn(|context| {
                assert!(std::pin::Pin::new(&mut shutdown).poll(context).is_pending());
                Poll::Ready(())
            })
            .await;
            let (_producer, completion) = tokio::sync::oneshot::channel::<()>();
            let operation = async {
                let mut wait = pin!(acyclic_native_runtime::obs::waiting(
                    "channel-close",
                    "signal-proof",
                    completion
                ));
                futures::future::poll_fn(|context| {
                    assert!(wait.as_mut().poll(context).is_pending());
                    Poll::Ready(())
                })
                .await;
                println!("WAIT_REPORT_READY");
                io::stdout().flush().unwrap();
                wait.await?;
                Ok(0)
            };
            let (result, diagnostic) =
                observe(&report, io::stderr(), shutdown, false, operation).await;
            assert_eq!(result.unwrap(), 143);
            diagnostic.unwrap().await.unwrap();
        });
        return;
    }
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "lifecycle::tests::sigterm_reports_from_the_process_boundary",
            "--nocapture",
        ])
        .env(CHILD, "1")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let mut ready = io::BufReader::new(child.stdout.take().unwrap());
    let mut line = String::new();
    loop {
        assert_ne!(
            ready.read_line(&mut line).unwrap(),
            0,
            "child exited before arming termination"
        );
        if line.trim_end().ends_with("WAIT_REPORT_READY") {
            break;
        }
        line.clear();
    }
    assert!(
        std::process::Command::new("kill")
            .args(["-TERM", &child.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .starts_with("waiting: channel-close signal-proof since ")
    );
}
