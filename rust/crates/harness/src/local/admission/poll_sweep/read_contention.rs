use super::*;
use acyclic_native_runtime::testing;
use acyclic_stream::{AppendRequest, OwnedBytes, StreamPath, StreamProvider};
use std::{future::Future, pin::Pin, task::Poll};

async fn read_cut(after_completion: bool, omit_hook: bool) -> Result<bool> {
    let directory = tempfile::tempdir().map_err(storage)?;
    let path = StreamPath::new("admission").map_err(storage)?;
    let stream = LocalStream::open(directory.path(), Default::default())
        .await
        .map_err(storage)?;
    let (io, mut events) = testing::Schedule::new();
    let request = AppendRequest {
        path: path.clone(),
        records: vec![OwnedBytes::from_static(b"committed")],
        if_tail: Some(0),
        idempotency_key: None,
    };
    let mut writing = Box::pin(io.scope(stream.append(request)));
    let writer_gate = loop {
        tokio::select! {
            result = &mut writing => panic!("writer escaped its persist gate: {result:?}"),
            event = events.recv() => {
                let event = event.unwrap();
                if event.name == "blocking:submitted" { break event; }
                event.release();
            }
        }
    };
    // Persist has acquired the real LocalStream visibility write lock. Its
    // independently owned task survives dropping the submitting caller.
    drop(writing);
    let read = stream.tail(path.clone());
    let mut reading: Pin<
        Box<dyn Future<Output = std::result::Result<u64, acyclic_stream::StreamError>> + '_>,
    > = if omit_hook {
        Box::pin(testing::untracked(read))
    } else {
        Box::pin(io.scope(tokio::task::unconstrained(read)))
    };
    let initial = std::future::poll_fn(|cx| Poll::Ready(reading.as_mut().poll(cx))).await;
    assert!(
        initial.is_pending(),
        "read bypassed a real pending publication"
    );
    let read_gate = events.try_recv().ok();
    let detected = read_gate
        .as_ref()
        .is_some_and(|event| event.name == "stream:read:blocked");
    let final_gate = if after_completion && detected {
        read_gate.unwrap().release();
        writer_gate.release();
        loop {
            tokio::select! {
                result = &mut reading => panic!("read escaped its completion gate: {result:?}"),
                event = events.recv() => {
                    let event = event.unwrap();
                    if event.name == "stream:read:completed" { break Some(event); }
                    event.release();
                }
            }
        }
    } else {
        if let Some(gate) = read_gate {
            gate.release();
        }
        Some(writer_gate)
    };
    io.cancel();
    drop(reading);
    final_gate.unwrap().release();
    loop {
        tokio::select! {
            _ = io.drained() => break,
            event = events.recv() => event.unwrap().release(),
        }
    }
    assert_eq!(stream.tail(path.clone()).await.map_err(storage)?, 1);
    drop(stream);
    let reopened = LocalStream::open(directory.path(), Default::default())
        .await
        .map_err(storage)?;
    assert_eq!(reopened.tail(path).await.map_err(storage)?, 1);
    Ok(detected)
}

#[tokio::test]
async fn actual_blocked_read_and_completion_are_cancellation_points() -> Result<()> {
    for after_completion in [false, true] {
        assert!(read_cut(after_completion, false).await?);
    }
    Ok(())
}

#[tokio::test]
async fn omitting_the_actual_read_hook_is_detected() -> Result<()> {
    assert!(!read_cut(false, true).await?);
    Ok(())
}
