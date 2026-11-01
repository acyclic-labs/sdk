//! Original storage, real pipe credit, and formatter/consumer terminal controls.
#![cfg(all(target_os = "linux", not(loom)))]

use acyclic_observability::{OutputFailure, OutputRefusal, TextOutput};
use std::{
    fmt,
    future::poll_fn,
    io::{self, Read as _},
    os::fd::AsFd as _,
    pin::Pin,
    task::Poll,
};
use tokio::io::{AsyncWrite as _, AsyncWriteExt as _};
use tracing_subscriber::{Layer as _, layer::SubscriberExt as _};

acyclic_test_support::counting_allocator!();

fn disabled_event() {
    tracing::info!("disabled {}", 19);
}

#[test]
fn text_output_retains_storage_through_native_backpressure_and_terminal_loss() -> io::Result<()> {
    let runtime = acyclic_native_runtime::coordination_runtime()?;
    runtime.block_on(async {
        for terminal in ["drain", "cancel", "reader-loss"] {
            let (mut reader, original) = io::pipe()?;
            let mut writer = acyclic_native_runtime::InheritedOutput::from_file(original.as_fd().try_clone_to_owned()?.into())?;
            let prefix = [b'x'; 8192];
            let mut filled = writer.write(&prefix).await?;
            loop {
                let written = poll_fn(|cx| Poll::Ready(match Pin::new(&mut writer).poll_write(cx, &prefix) { Poll::Ready(result) => Some(result), Poll::Pending => None })).await;
                let Some(written) = written else { break };
                let written = written?;
                assert_ne!(written, 0);
                filled += written;
            }
            let storage = vec![0; 128].into_boxed_slice();
            let pointer = storage.as_ptr();
            let (output, mut receiver) = TextOutput::channel(vec![storage])?;
            output.record(|buffer| write!(buffer, "original record\n")).unwrap();
            let mut record = receiver.recv().await.unwrap();
            assert_eq!(record.bytes().as_ptr(), pointer);
            assert_eq!(output.stats().retained, 1);
            assert!(matches!(output.record(|_| panic!("full admission must not format")), Err(OutputRefusal::Full)));
            let mut delivery = Box::pin(async {
                record.write_to(&mut writer).await
            });
            poll_fn(|cx| { assert!(delivery.as_mut().poll(cx).is_pending()); Poll::Ready(()) }).await;
            // This channel event is independent of the original full pipe;
            // no sleeping, probe loop, worker or deadline advances delivery.
            let (send, receive) = tokio::sync::oneshot::channel();
            send.send(19).unwrap();
            assert_eq!(receive.await.unwrap(), 19);
            match terminal {
                "drain" => {
                    let mut remaining = filled;
                    while remaining != 0 {
                        let count = remaining.min(prefix.len());
                        let mut bytes = [0; 8192];
                        reader.read_exact(&mut bytes[..count])?;
                        assert_eq!(&bytes[..count], &prefix[..count]);
                        remaining -= count;
                    }
                    delivery.as_mut().await?;
                }
                "cancel" => receiver.close(),
                "reader-loss" => {
                    drop(reader);
                    let error = delivery.as_mut().await.unwrap_err();
                    assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
                    assert!(matches!(receiver.fail(error), OutputFailure::Write(_)));
                    assert!(matches!(output.failed().await, OutputFailure::Write(error) if error.kind() == io::ErrorKind::BrokenPipe));
                    drop(delivery);
                    drop(record);
                    assert_eq!(output.stats().retained, 0);
                    let terminal = output.terminal_stats();
                    assert_eq!(terminal.published, Some(1));
                    assert_eq!(terminal.writer_accepted, Some(0));
                    assert_eq!(terminal.incomplete, Some(1));
                    assert_eq!(terminal.pending, 0);
                    continue;
                }
                _ => unreachable!(),
            }
            drop(delivery);
            assert_eq!(output.stats().retained, 1, "dequeue/write completion cannot release the retained record");
            assert_eq!(output.terminal_stats().pending, 1);
            assert_eq!(output.terminal_stats().writer_accepted, Some(0));
            if terminal == "cancel" {
                let error = record.write_to(&mut writer).await.unwrap_err();
                assert_eq!(error.kind(), io::ErrorKind::InvalidInput, "a partial attempt cannot be resubmitted");
            }
            drop(record);
            assert_eq!(output.stats().retained, 0);
            let disposed = output.terminal_stats();
            assert_eq!(disposed.published, Some(1));
            assert_eq!(disposed.pending, 0);
            assert_eq!(disposed.writer_accepted, Some(u64::from(terminal == "drain")));
            assert_eq!(disposed.incomplete, Some(u64::from(terminal == "cancel")));
            assert_eq!(disposed.discarded, Some(0));
            writer.shutdown().await?;
            drop(original);
            let mut received = Vec::new();
            reader.read_to_end(&mut received)?;
            if terminal == "drain" { assert_eq!(received, b"original record\n"); }
            else { assert_eq!(received, prefix.repeat(filled / prefix.len()).into_iter().chain(prefix[..filled % prefix.len()].iter().copied()).collect::<Vec<_>>()); }
            if terminal == "drain" {
                output.record(|buffer| write!(buffer, "reuse")).unwrap();
                assert_eq!(receiver.recv().await.unwrap().bytes().as_ptr(), pointer);
                assert_eq!(output.stats().high_water, 1);
            }
        }

        let (output, mut receiver) = TextOutput::channel(vec![vec![0; 32].into_boxed_slice()])?;
        let overflow = output.record(|buffer| buffer.write_str("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"));
        assert!(matches!(overflow, Err(OutputRefusal::Full)));
        assert_eq!(output.stats().retained, 0);
        assert_eq!(output.stats().full, Some(1));
        assert_eq!(output.terminal_stats().published, Some(0), "format refusal is not accepted delivery");
        let failure = std::pin::pin!(output.failed());
        assert!(matches!(output.record(|buffer| { buffer.write_str("partial")?; Err(fmt::Error) }), Err(OutputRefusal::Failed(OutputFailure::Formatting))));
        assert!(matches!(failure.await, OutputFailure::Formatting));
        assert!(matches!(receiver.fail(io::ErrorKind::BrokenPipe.into()), OutputFailure::Formatting));
        drop(receiver);

        // The operation itself publishes Formatting in its completing poll.
        // Selecting that original result first must not erase the first cause.
        let (output, receiver) = TextOutput::channel(vec![vec![0; 32].into_boxed_slice()])?;
        let original = async {
            assert!(matches!(output.record(|_| Err(fmt::Error)), Err(OutputRefusal::Failed(OutputFailure::Formatting))));
            19
        };
        let (result, cause) = receiver.observe(original, &mut tokio::io::sink()).await;
        assert_eq!(result, 19);
        assert!(matches!(cause, Some(OutputFailure::Formatting)));
        assert_eq!(output.terminal_stats().published, Some(0));

        // One original record remains queued and another is held by the caller.
        // Neither consumer loss nor record drop can manufacture write completion.
        let (output, mut receiver) = TextOutput::channel(vec![vec![0; 32].into_boxed_slice(), vec![0; 32].into_boxed_slice()])?;
        output.record(|buffer| buffer.write_str("held")).unwrap();
        output.record(|buffer| buffer.write_str("queued")).unwrap();
        let record = receiver.recv().await.unwrap();
        receiver.close();
        assert_eq!(output.terminal_stats().pending, 2);
        drop(receiver);
        assert_eq!(output.terminal_stats().pending, 1);
        assert_eq!(output.terminal_stats().discarded, Some(1));
        assert_eq!(output.terminal_stats().writer_accepted, Some(0));
        drop(record);
        let terminal = output.terminal_stats();
        assert_eq!(terminal.published, Some(2));
        assert_eq!(terminal.pending, 0);
        assert_eq!(terminal.discarded, Some(1));
        assert_eq!(terminal.incomplete, Some(1));
        assert_eq!(terminal.writer_accepted, Some(0));

        let (output, receiver) = TextOutput::channel(vec![vec![0; 32].into_boxed_slice()])?;
        let closed = output.failed();
        drop(receiver);
        assert!(matches!(closed.await, OutputFailure::ConsumerClosed));
        assert!(matches!(output.record(|_| panic!("closed consumer must not format")), Err(OutputRefusal::Closed)));
        assert_eq!(output.stats().closed, Some(1));

        let (output, mut receiver) = TextOutput::channel(vec![vec![0; 128].into_boxed_slice()])?;
        let layer = output.clone().with_filter(tracing_subscriber::filter::LevelFilter::OFF);
        let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(layer));
        tracing::dispatcher::with_default(&dispatch, || {
            disabled_event();
            let physical = acyclic_test_support::measure(disabled_event);
            assert_eq!(physical.count_total, 0);
        });
        // Warm the original channel allocation, then distinguish direct funded
        // formatting from a real extra allocation, not from an invented count.
        output.record(|buffer| write!(buffer, "warm")).unwrap();
        drop(receiver.recv().await.unwrap());
        let direct = acyclic_test_support::measure(|| output.record(|buffer| write!(buffer, "original {}", 19)).unwrap());
        drop(receiver.recv().await.unwrap());
        let copied = acyclic_test_support::measure(|| output.record(|buffer| { let extra = std::hint::black_box(format!("original {}", 19)); buffer.write_str(&extra) }).unwrap());
        assert!(copied.count_total > direct.count_total, "extra formatting allocation must be observable");
        let record = receiver.recv().await.unwrap();
        drop(record);
        let enabled = tracing::Dispatch::new(tracing_subscriber::registry().with(output.clone().with_filter(tracing_subscriber::filter::LevelFilter::INFO)));
        tracing::dispatcher::with_default(&enabled, || tracing::info!(answer = 19, "original event"));
        let record = receiver.recv().await.unwrap();
        let text = std::str::from_utf8(record.bytes()).unwrap();
        assert!(text.contains("original event") && text.contains("answer=19"));
        drop(dispatch);
        drop(enabled);
        drop(output);
        assert!(receiver.recv().await.is_none(), "retained record must not retain producer");
        drop(record);
        Ok(())
    })
}
