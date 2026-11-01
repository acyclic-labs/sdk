use super::*;
use crate::obs::CaptureObservation;
use acyclic_observability::{Export, ExportReceiver, ExportRecord, Operations, Outcome};
use std::future::Future;
use std::task::{Context, Poll, Waker};
use tokio::sync::Semaphore;
use tracing_subscriber::prelude::*;

struct Observed {
    observation: CaptureObservation,
    error: Option<io::Error>,
    retired: Option<bool>,
    panic_on_retirement: bool,
}

impl CaptureHandler for Observed {
    fn check(&mut self) -> io::Result<()> {
        self.error.take().map_or(Ok(()), Err)
    }
    fn observe(&mut self, _: ProcessOutput<'_>) -> io::Result<ProcessInput> {
        Ok(ProcessInput::default())
    }
    fn retired(&mut self, completed: bool) {
        assert_eq!(self.retired.replace(completed), None);
        assert!(
            !self.panic_on_retirement,
            "original retirement callback lost"
        );
    }
    fn observation(&mut self) -> Option<&mut CaptureObservation> {
        Some(&mut self.observation)
    }
}

fn record(output: &mut ExportReceiver) -> io::Result<ExportRecord> {
    // The original producer has already completed or been dropped. A missing
    // synchronous terminal is a failure, not a wait on an unrelated producer.
    let mut receive = std::pin::pin!(output.recv());
    match receive
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(Some(record)) => Ok(record),
        _ => Err(io::Error::other("original capture observation absent")),
    }
}

fn child_tree() -> io::Result<ProcessTree> {
    let mut command = ProcessCommand::new(std::env::current_exe()?);
    command
        .args([
            "--exact",
            "process_tree::stream::tests::child",
            "--nocapture",
        ])
        .env("ACYCLIC_TEST_STREAM_CAPTURE", "exit")
        .stdin(ProcessStdio::piped())
        .stdout(ProcessStdio::piped())
        .stderr(ProcessStdio::piped());
    let mut tree = ProcessTree::spawn(&mut command)?;
    // Release the same real child fixture on its original pipe, without a
    // timer, extra launcher or fabricated provider fixture.
    tree.child
        .as_mut()
        .unwrap()
        .stdin
        .take()
        .unwrap()
        .write_all(b"!")?;
    Ok(tree)
}

#[tokio::test]
async fn original_capture_retirement_preserves_failure_loss_and_credit() -> io::Result<()> {
    let credit = Arc::new(Semaphore::new(8));
    let (export, mut output) = Export::channel(Arc::clone(&credit), true);
    let (minimal, mut minimal_output) = Export::channel(Arc::clone(&credit), false);
    let dispatch = tracing::Dispatch::new(
        tracing_subscriber::registry().with(Operations((export.clone(), minimal))),
    );
    let directory = tempfile::tempdir()?;
    for mode in [
        "success",
        "capture-error",
        "before-capture",
        "callback-loss",
    ] {
        let error = if mode == "capture-error" {
            Some(std::fs::File::open(directory.path().join("missing")).unwrap_err())
        } else {
            None
        };
        let native_error = error.as_ref().and_then(io::Error::raw_os_error);
        let (handler, retained) = tracing::dispatcher::with_default(&dispatch, || {
            let observation = CaptureObservation::new();
            let retained = observation.context().clone();
            (
                Observed {
                    observation,
                    error,
                    retired: None,
                    panic_on_retirement: mode == "callback-loss",
                },
                retained,
            )
        });
        let capture = child_tree()?.capture(
            if mode == "before-capture" {
                CaptureSchedule::Overflow
            } else {
                CaptureSchedule::Unbounded
            },
            usize::MAX,
            None,
            crate::ProcessControl::unbounded()?.bind(handler),
        );
        // The original callback runs on the native worker. Its panic must not
        // emit successful cleanup telemetry, even after physical cleanup.
        if mode == "callback-loss" {
            match tokio::spawn(capture).await {
                Err(error) => assert!(error.is_panic()),
                Ok(_) => panic!("lost original callback returned normally"),
            }
        } else {
            let outcome = capture.await?;
            let handler = outcome.handler.into_inner();
            assert_eq!(handler.retired, Some(true));
            match outcome.result {
                Ok(output) => {
                    assert_eq!(mode, "success");
                    assert_eq!(output.status.code(), Some(7));
                    assert_eq!(output.stderr, [0, 255, 7]);
                }
                Err(failure) => {
                    assert_ne!(mode, "success");
                    assert!(failure.cleanup_completed);
                    if mode == "capture-error" {
                        assert_eq!(failure.error.raw_os_error(), native_error);
                    } else {
                        assert_eq!(failure.error.kind(), io::ErrorKind::InvalidInput);
                    }
                }
            }
        }
        let evidence = record(&mut output)?;
        let sample = evidence.sample();
        assert_eq!(sample.span_id, &retained.id().unwrap());
        assert!(
            sample.binding.is_none(),
            "no admitted identity was supplied"
        );
        let cleanup = sample.capture_retirement.unwrap();
        assert_eq!(cleanup.os_error, native_error);
        assert_eq!(
            cleanup.cleanup_completed,
            match mode {
                "callback-loss" => None,
                _ => Some(true),
            }
        );
        assert_eq!(
            sample.outcome,
            match mode {
                "success" => Outcome::Ok,
                "callback-loss" => Outcome::Incomplete,
                _ => Outcome::Err,
            }
        );
        let minimal_evidence = record(&mut minimal_output)?;
        assert!(minimal_evidence.sample().capture_retirement.is_none());
        assert_eq!(minimal_evidence.sample().outcome, sample.outcome);
        assert_eq!(export.stats().retained, 1);
        assert_eq!(credit.available_permits(), 6);
        // A late diagnostic update cannot rewrite the original terminal.
        retained.record("capture.cleanup_completed", true);
        retained.record("outcome", "ok");
        assert_eq!(export.stats().retained, 1);
        drop(retained);
        drop(evidence);
        drop(minimal_evidence);
        assert_eq!(export.stats().retained, 0);
        assert_eq!(credit.available_permits(), 8);
    }
    // Producer loss before capture starts still emits incomplete while another
    // span handle survives. It cannot manufacture a cleanup observation.
    let observation = tracing::dispatcher::with_default(&dispatch, CaptureObservation::new);
    let retained = observation.context().clone();
    drop(observation);
    let incomplete = record(&mut output)?;
    assert_eq!(incomplete.sample().outcome, Outcome::Incomplete);
    assert_eq!(
        incomplete
            .sample()
            .capture_retirement
            .unwrap()
            .cleanup_completed,
        None
    );
    drop((incomplete, record(&mut minimal_output)?, retained));
    assert_eq!(credit.available_permits(), 8);
    tracing::dispatcher::with_default(&tracing::Dispatch::none(), || {
        drop(CaptureObservation::new());
        let disabled = acyclic_test_support::measure(|| {
            let mut observation = CaptureObservation::new();
            observation.retired(true, None);
        });
        let extra = acyclic_test_support::measure(|| {
            let mut observation = CaptureObservation::new();
            observation.retired(true, None);
            drop(std::hint::black_box(Box::new(1_u64)));
        });
        assert_eq!(disabled.count_total, 0);
        assert_eq!(extra.count_total, 1);
    });
    Ok(())
}
