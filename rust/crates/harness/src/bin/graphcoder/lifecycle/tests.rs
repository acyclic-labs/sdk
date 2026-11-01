use super::*;
use acyclic_native_runtime::obs::{ReportAck, ReportOutcome, ReportRequest};
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};

const GENERATION: &str = "0123456789abcdef0123456789abcdef";

fn request() -> ReportRequest {
    ReportRequest {
        request_id: "abcdef0123456789abcdef0123456789".into(),
        generation: GENERATION.into(),
        deadline_unix_ms: u64::MAX,
    }
}

#[test]
fn acknowledged_snapshot_precedes_cancellation_and_keeps_the_original_producer() {
    let runtime = acyclic_native_runtime::coordination_runtime().unwrap();
    let report = WaitReport::default();
    let dispatch = tracing::Dispatch::new(
        subscriber(&report, Some("off".into()), || Ok(None))
            .unwrap()
            .0,
    );
    let _subscriber = tracing::dispatcher::set_default(&dispatch);
    runtime.block_on(async {
        let (owner, child) = tokio::io::duplex(4096);
        let port = ReportPort::new(GENERATION.into(), child).unwrap();
        let (producer, completion) = tokio::sync::oneshot::channel::<()>();
        let (terminate, signal) = tokio::sync::oneshot::channel();
        let shutdown = async {
            signal.await.unwrap();
            Ok(143)
        }
        .boxed()
        .shared();
        let operation = async {
            acyclic_native_runtime::obs::waiting("channel-close", "turn-17", completion).await?;
            Ok(0)
        };
        let host = async {
            let mut owner = BufReader::new(owner);
            let mut line = String::new();
            owner.read_line(&mut line).await.unwrap();
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&line).unwrap()["generation"],
                GENERATION
            );
            let mut bytes = serde_json::to_vec(&request()).unwrap();
            bytes.push(b'\n');
            owner.get_mut().write_all(&bytes).await.unwrap();
            line.clear();
            owner.read_line(&mut line).await.unwrap();
            let ack: ReportAck = serde_json::from_str(&line).unwrap();
            assert_eq!(ack.request_id, request().request_id);
            assert_eq!(ack.generation, GENERATION);
            assert_eq!(ack.outcome, ReportOutcome::Report);
            assert_eq!(ack.waits.len(), 1);
            assert_eq!(ack.waits[0].primitive, "channel-close");
            assert_eq!(ack.waits[0].key, "turn-17");
            assert!(
                !producer.is_closed(),
                "reporting must not cancel the observed operation"
            );
            terminate.send(()).unwrap();
            producer
        };
        let ((result, diagnostic), producer) = tokio::join!(
            observe(&report, Ok(Some(port)), shutdown, false, operation),
            host
        );
        assert_eq!(result.unwrap(), 143);
        diagnostic.unwrap();
        assert!(producer.is_closed());
        assert!(report.snapshot().is_empty());
    });
}

#[test]
fn dropped_producer_exits_with_its_typed_error_and_no_live_wait() {
    let runtime = acyclic_native_runtime::coordination_runtime().unwrap();
    let report = WaitReport::default();
    let dispatch = tracing::Dispatch::new(
        subscriber(&report, Some("off".into()), || Ok(None))
            .unwrap()
            .0,
    );
    let _subscriber = tracing::dispatcher::set_default(&dispatch);
    runtime.block_on(async {
        let (producer, completion) = tokio::sync::oneshot::channel::<()>();
        drop(producer);
        let operation = async {
            acyclic_native_runtime::obs::waiting("channel-close", "turn-18", completion).await?;
            Ok(0)
        };
        let (result, _) = observe(
            &report,
            Ok(None),
            std::future::pending().boxed().shared(),
            false,
            operation,
        )
        .await;
        assert!(
            result
                .unwrap_err()
                .is::<tokio::sync::oneshot::error::RecvError>()
        );
        assert!(report.snapshot().is_empty());
    });
}

#[cfg(target_os = "linux")]
#[test]
fn diagnostic_terminal_never_waits_for_an_unread_final_message() -> io::Result<()> {
    use std::{future::poll_fn, os::fd::AsFd as _, pin::Pin, task::Poll};
    use tokio::io::AsyncWrite as _;

    struct FailedFormatter;
    impl fmt::Debug for FailedFormatter {
        fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
            Err(fmt::Error)
        }
    }

    let runtime = acyclic_native_runtime::coordination_runtime()?;
    runtime.block_on(async {
        for field in ["record", "capacity", "formatter"] {
            for operation_error in [false, true] {
                let (reader, original) = io::pipe()?;
                let mut output = InheritedOutput::from_file(
                    original.as_fd().try_clone_to_owned()?.into(),
                )?;
                let chunk = [b'x'; 8192];
                output.write_all(&chunk).await?;
                loop {
                    let written = poll_fn(|cx| {
                        Poll::Ready(match Pin::new(&mut output).poll_write(cx, &chunk) {
                            Poll::Ready(result) => Some(result),
                            Poll::Pending => None,
                        })
                    })
                    .await;
                    let Some(written) = written else { break };
                    assert_ne!(written?, 0);
                }
                // The old post-terminal summary would wait on this actual
                // full pipe, even though the original result is already ready.
                let old_writer = InheritedOutput::from_file(
                    original.as_fd().try_clone_to_owned()?.into(),
                )?;
                let mut old_summary = Box::pin(old_writer.write_owned(b"final error\n"));
                poll_fn(|cx| {
                    assert!(old_summary.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                drop(old_summary);

                let report = WaitReport::default();
                let (subscriber, receiver) = subscriber(&report, Some("info".into()), || {
                    Ok(Some(vec![vec![0; 64].into_boxed_slice()]))
                })
                .unwrap();
                let dispatch = tracing::Dispatch::new(subscriber);
                let _subscriber = tracing::dispatcher::set_default(&dispatch);
                let operation = async {
                    match field {
                        "capacity" => tracing::info!(field = "a field exceeding the original admitted sixty-four-byte record storage without any native write"),
                        "formatter" => tracing::info!(field = ?FailedFormatter),
                        _ => tracing::info!("terminal"),
                    }
                    if operation_error {
                        Err(Box::new(io::Error::from(io::ErrorKind::PermissionDenied)) as Failure)
                    } else {
                        Ok(19)
                    }
                };
                let original = observe(
                    &report,
                    Ok(None),
                    std::future::pending().boxed().shared(),
                    false,
                    operation,
                );
                let mut host = Box::pin(diagnostics(
                    original,
                    Some((receiver.unwrap(), output)),
                ));
                let ((result, report_result), failure) = poll_fn(|cx| {
                    let Poll::Ready(result) = host.as_mut().poll(cx) else {
                        panic!("diagnostic output held the completed original result")
                    };
                    Poll::Ready(result)
                })
                .await;
                report_result.unwrap();
                if operation_error {
                    assert_eq!(
                        result.unwrap_err().downcast::<io::Error>().unwrap().kind(),
                        io::ErrorKind::PermissionDenied
                    );
                } else {
                    assert_eq!(result.unwrap(), 19);
                }
                assert_eq!(
                    matches!(failure, Some(OutputFailure::Formatting)),
                    field == "formatter"
                );
                drop(reader);
            }
        }
        Ok(())
    })
}

#[test]
fn blocked_or_closed_report_sink_cannot_hold_graceful_owner_cleanup() {
    for closed in [false, true] {
        let runtime = acyclic_native_runtime::coordination_runtime().unwrap();
        runtime.block_on(async {
            let report = WaitReport::default();
            let (owner, child) = tokio::io::duplex(1);
            let port = ReportPort::new(GENERATION.into(), child).unwrap();
            let owner = if closed {
                drop(owner);
                None
            } else {
                Some(owner)
            };
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
            let controller = async {
                terminate.send(()).unwrap();
                release.send(()).unwrap();
            };
            let ((result, diagnostic), ()) = tokio::join!(
                observe(&report, Ok(Some(port)), shutdown, true, operation),
                controller
            );
            assert_eq!(result.unwrap(), 0);
            assert!(cleaned.get());
            if let Err(error) = diagnostic {
                assert!(matches!(
                    error.kind(),
                    io::ErrorKind::BrokenPipe | io::ErrorKind::Interrupted
                ));
            }
            drop(owner);
        });
    }
}

#[test]
fn a_backpressured_acknowledgement_cannot_hold_the_original_persistence_producer() {
    let runtime = acyclic_native_runtime::coordination_runtime().unwrap();
    let report = WaitReport::default();
    let dispatch = tracing::Dispatch::new(
        subscriber(&report, Some("off".into()), || Ok(None))
            .unwrap()
            .0,
    );
    let _subscriber = tracing::dispatcher::set_default(&dispatch);
    runtime.block_on(async {
        let (owner, child) = tokio::io::duplex(256);
        let port = ReportPort::new(GENERATION.into(), child).unwrap();
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, held) = std::sync::mpsc::channel::<()>();
        let completion = acyclic_native_runtime::spawn_persistence(
            "report",
            "held",
            acyclic_native_runtime::PersistenceKey::new(std::ptr::from_ref(&report) as usize, 0),
            move || {
                let _ = entered.send(());
                held.recv().map_err(io::Error::other)
            },
        );
        tokio::pin!(completion);
        let (terminate, signal) = tokio::sync::oneshot::channel();
        let shutdown = async {
            signal.await.unwrap();
            Ok(143)
        }
        .boxed()
        .shared();
        let operation = async {
            completion.as_mut().await??;
            Ok(0)
        };
        // Retain an additional live primitive so the real acknowledgement
        // exceeds the channel credit. Reading its prefix leaves it blocked.
        let (_sender, receiver) = tokio::sync::oneshot::channel::<()>();
        let _held =
            acyclic_native_runtime::obs::waiting("channel-close", "k".repeat(1024), receiver);
        let host = async {
            let mut owner = BufReader::new(owner);
            let mut line = String::new();
            owner.read_line(&mut line).await.unwrap();
            started.await.unwrap();
            let mut bytes = serde_json::to_vec(&request()).unwrap();
            bytes.push(b'\n');
            owner.get_mut().write_all(&bytes).await.unwrap();
            let prefix = owner.fill_buf().await.unwrap();
            assert!(!prefix.is_empty() && !prefix.contains(&b'\n'));
            assert!(
                report
                    .snapshot()
                    .iter()
                    .any(|wait| wait.primitive == "persistence-completion")
            );
            terminate.send(()).unwrap();
            owner
        };
        let ((result, diagnostic), owner) = tokio::join!(
            observe(&report, Ok(Some(port)), shutdown, false, operation),
            host
        );
        assert_eq!(result.unwrap(), 143);
        assert_eq!(diagnostic.unwrap_err().kind(), io::ErrorKind::Interrupted);
        release.send(()).unwrap();
        completion.await.unwrap().unwrap();
        assert!(
            !report
                .snapshot()
                .iter()
                .any(|wait| wait.primitive == "persistence-completion")
        );
        drop(owner);
    });
}
