use super::*;
use std::{
    pin::pin,
    task::{Context, Poll, Waker},
};

fn poll<F: Future>(future: std::pin::Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

#[test]
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
        let mut output = Vec::new();
        let operation = async {
            acyclic_native_runtime::obs::waiting("channel-close", "turn-17", completion).await?;
            Ok(0)
        };
        {
            let mut observed = pin!(observe(&report, &mut output, shutdown, false, operation));
            assert!(poll(observed.as_mut()).is_pending());
            terminate.send(()).unwrap();
            let Poll::Ready((result, diagnostic)) = poll(observed.as_mut()) else {
                panic!("termination did not wake the process boundary")
            };
            assert_eq!(result.unwrap(), 143);
            diagnostic.unwrap();
        }
        assert!(
            String::from_utf8(output)
                .unwrap()
                .starts_with("waiting: channel-close turn-17 since ")
        );
        assert!(producer.is_closed());
    });
}

#[test]
fn dropped_producer_exits_with_its_typed_error_and_no_live_wait() {
    let report = WaitReport::default();
    let dispatch = tracing::Dispatch::new(subscriber(&report, Some("off".into())));
    tracing::dispatcher::with_default(&dispatch, || {
        let (producer, completion) = tokio::sync::oneshot::channel::<()>();
        let shutdown = std::future::pending().boxed().shared();
        let mut output = Vec::new();
        let operation = async {
            acyclic_native_runtime::obs::waiting("channel-close", "turn-18", completion).await?;
            Ok(0)
        };
        {
            let mut observed = pin!(observe(&report, &mut output, shutdown, false, operation));
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
            diagnostic.unwrap();
        }
        assert!(output.is_empty());
    });
}

#[test]
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
    let mut output = Broken;
    let mut observed = pin!(observe(&report, &mut output, shutdown, true, operation));
    assert!(poll(observed.as_mut()).is_pending());
    terminate.send(()).unwrap();
    assert!(poll(observed.as_mut()).is_pending());
    assert!(!cleaned.get());
    release.send(()).unwrap();
    let Poll::Ready((result, diagnostic)) = poll(observed.as_mut()) else {
        panic!("shutdown did not reach its owner")
    };
    assert_eq!(result.unwrap(), 0);
    assert_eq!(diagnostic.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
    assert!(cleaned.get());
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
                observe(&report, &mut io::stderr(), shutdown, false, operation).await;
            assert_eq!(result.unwrap(), 143);
            diagnostic.unwrap();
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
