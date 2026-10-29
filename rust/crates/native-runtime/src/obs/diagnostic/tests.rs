use super::*;
#[cfg(unix)]
use std::io::Read as _;
use std::task::Waker;
use tracing_subscriber::layer::SubscriberExt as _;

fn poll(completion: &mut DiagnosticCompletion) -> Poll<io::Result<()>> {
    Pin::new(completion).poll(&mut Context::from_waker(Waker::noop()))
}

#[test]
fn a_lost_completion_producer_is_typed() {
    let (producer, receiver) = tokio::sync::oneshot::channel();
    let mut completion = DiagnosticCompletion(receiver);
    assert!(poll(&mut completion).is_pending());
    drop(producer);
    let Poll::Ready(Err(error)) = poll(&mut completion) else {
        panic!("producer loss left diagnostic completion pending")
    };
    assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
    assert_eq!(
        error.get_ref().unwrap().downcast_ref(),
        Some(&DiagnosticFailure::ProducerDropped)
    );
}

#[test]
fn snapshot_survives_settlement_and_writes_without_the_registry() {
    let report = super::super::WaitReport::default();
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(report.clone()));
    let (producer, receiver) = tokio::sync::oneshot::channel::<()>();
    let operation = tracing::dispatcher::with_default(&dispatch, || {
        crate::obs::waiting("channel-close", "owned-key", receiver)
    });
    let captured = report.snapshot();
    drop(operation);
    assert!(producer.is_closed());
    assert!(report.snapshot().is_empty());
    let mut output = Vec::new();
    captured.write_to(&mut output).unwrap();
    assert!(
        String::from_utf8(output)
            .unwrap()
            .starts_with("waiting: channel-close owned-key since ")
    );
}

#[cfg(unix)]
#[test]
fn full_output_does_not_block_cancellation_or_other_producers() -> io::Result<()> {
    use std::os::unix::net::UnixStream;

    struct Output {
        stream: UnixStream,
        coordination: std::thread::ThreadId,
        starting: Option<tokio::sync::oneshot::Sender<()>>,
        retired: Arc<AtomicBool>,
    }
    impl Write for Output {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            // The synchronous negative control refuses before it could
            // block the sibling that owns cancellation and the pipe reader.
            if std::thread::current().id() == self.coordination {
                return Err(io::ErrorKind::WouldBlock.into());
            }
            if let Some(starting) = self.starting.take() {
                let _ = starting.send(());
            }
            self.stream.write(bytes)
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    impl Drop for Output {
        fn drop(&mut self) {
            self.retired.store(true, Ordering::Release);
        }
    }

    initialize_diagnostic_service()?;
    crate::coordination()?.block_on(async {
        let report = super::super::WaitReport::default();
        let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(report.clone()));
        let (producer, receiver) = tokio::sync::oneshot::channel::<()>();
        let operation = tracing::dispatcher::with_default(&dispatch, || {
            crate::obs::waiting("channel-close", "cancel-key", receiver)
        });
        let (mut stream, reader) = UnixStream::pair()?;
        stream.set_nonblocking(true)?;
        let mut filled = 0;
        loop {
            match stream.write(&[0; 4096]) {
                Ok(count) => {
                    assert_ne!(count, 0);
                    filled += count;
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) => return Err(error),
            }
        }
        assert!(filled > 0);
        assert_eq!(
            stream.write(b"x").unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        stream.set_nonblocking(false)?;
        let (starting, started) = tokio::sync::oneshot::channel();
        let retired = Arc::new(AtomicBool::new(false));
        let mut output = Output {
            stream,
            coordination: std::thread::current().id(),
            starting: Some(starting),
            retired: Arc::clone(&retired),
        };
        assert_eq!(
            report.report(&mut output).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        let mut completion = report.snapshot().try_write_to(output)?;
        started.await.map_err(io::Error::other)?;
        assert!(poll(&mut completion).is_pending());
        drop(operation);
        assert!(producer.is_closed());
        assert!(report.snapshot().is_empty());
        assert!(!retired.load(Ordering::Acquire));
        // Both unrelated shared services retain a producer while output is full.
        assert_eq!(crate::spawn_persistence(|| 17).await?, 17);
        assert_eq!(crate::run_blocking_io(|| 23, None)?.await?, 23);
        for count in [1, 100_000] {
            let allocation = acyclic_test_support::measure(|| {
                for _ in 0..count {
                    let snapshot = report.snapshot();
                    assert_eq!(
                        snapshot.try_write_to(io::sink()).err().unwrap().kind(),
                        io::ErrorKind::WouldBlock
                    );
                }
            });
            assert_eq!(allocation.count_total, 0);
        }
        // Reader ownership is the physical write's producer. Losing it ends
        // that write with the OS error, then retires custody before completion.
        drop(reader);
        assert!(matches!(
            completion.await.unwrap_err().kind(),
            io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset
        ));
        assert!(retired.load(Ordering::Acquire));

        writer_failures_and_detached_reports(&report, &dispatch).await
    })
}

#[cfg(unix)]
async fn writer_failures_and_detached_reports(
    report: &super::super::WaitReport,
    dispatch: &tracing::Dispatch,
) -> io::Result<()> {
    use std::os::unix::net::UnixStream;

    struct PanicOutput;
    impl Write for PanicOutput {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            panic!("diagnostic output control")
        }
        fn flush(&mut self) -> io::Result<()> {
            panic!("diagnostic output control")
        }
    }
    let failure = report
        .snapshot()
        .try_write_to(PanicOutput)?
        .await
        .unwrap_err();
    assert_eq!(
        failure.get_ref().unwrap().downcast_ref(),
        Some(&DiagnosticFailure::WriterPanicked)
    );
    report.snapshot().try_write_to(io::sink())?.await?;

    struct Counted(Arc<std::sync::atomic::AtomicUsize>);
    impl Write for Counted {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let (producer, receiver) = tokio::sync::oneshot::channel::<()>();
    let operation = tracing::dispatcher::with_default(dispatch, || {
        crate::obs::waiting("channel-close", "batched-key", receiver)
    });
    let writes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    report
        .snapshot()
        .write_to(&mut Counted(Arc::clone(&writes)))?;
    // The original unbuffered formatter fails the one-chunk write oracle.
    assert!(writes.swap(0, Ordering::Relaxed) > 1);
    report
        .snapshot()
        .try_write_to(Counted(Arc::clone(&writes)))?
        .await?;
    assert_eq!(writes.load(Ordering::Relaxed), 1);
    drop(operation);
    drop(producer);

    // Dropping the observer does not abandon an accepted physical report.
    // EOF proves writer retirement, not that admission has been released;
    // no subsequent submission relies on the detached job's completion.
    let (output, mut input) = UnixStream::pair()?;
    let (producer, receiver) = tokio::sync::oneshot::channel::<()>();
    let operation = tracing::dispatcher::with_default(dispatch, || {
        crate::obs::waiting("channel-close", "detached-key", receiver)
    });
    drop(report.snapshot().try_write_to(output)?);
    drop(operation);
    drop(producer);
    let mut bytes = String::new();
    input.read_to_string(&mut bytes)?;
    assert!(bytes.starts_with("waiting: channel-close detached-key since "));
    Ok(())
}
