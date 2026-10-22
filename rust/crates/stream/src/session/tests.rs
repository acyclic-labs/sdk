mod properties;
mod queue_scaling;

use super::*;
use crate::{AppendRequest, MemoryStream, OwnedBytes};
use futures::channel::mpsc;
use std::time::Duration;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

fn hello(bytes: u64, frames: u32) -> wire::SessionFrame {
    frame(
        0,
        0,
        Body::Hello(wire::SessionHello {
            credentials: vec![crate::session::testing::credential()],
            version: 1,
            client_id: Bytes::from_static(b"client"),
            destination_node: "node".into(),
            receive_bytes: bytes,
            receive_frames: frames,
        }),
    )
}
fn subscribe(id: u64, path: &str, from: u64, latest: bool) -> wire::SessionSubscription {
    wire::SessionSubscription {
        credential: 0,
        filter: None,
        channel: Some(wire::SessionChannel {
            channel_id: id,
            generation: 1,
        }),
        resume: Some(cursor(from)),
        kind: Some(Kind::Follow(wire::SessionFollow {
            path: path.into(),
            delivery: if latest {
                wire::SessionDelivery::Latest
            } else {
                wire::SessionDelivery::All
            } as i32,
            ..Default::default()
        })),
    }
}
fn interest(revision: u64, upsert: Vec<wire::SessionSubscription>) -> wire::SessionFrame {
    frame(
        0,
        0,
        Body::Interest(wire::SessionInterest {
            revision,
            upsert,
            remove: Vec::new(),
        }),
    )
}
fn window(channel: u64) -> wire::SessionFrame {
    frame(
        channel,
        u64::from(channel != 0),
        Body::Window(wire::SessionWindow {
            bytes: 1_000_000,
            frames: 100,
        }),
    )
}
async fn append(provider: &MemoryStream, path: &str, value: Bytes) -> Result<(), StreamError> {
    provider
        .append(AppendRequest {
            path: StreamPath::new(path)?,
            records: vec![(OwnedBytes::from(value)).into()],
            if_tail: None,
            idempotency_key: None,
        })
        .await?;
    Ok(())
}
async fn next(
    output: &mut BoxStream<'static, Result<wire::SessionFrame, StreamError>>,
) -> Result<wire::SessionFrame, Box<dyn std::error::Error + Send + Sync>> {
    Ok(tokio::time::timeout(Duration::from_secs(1), output.next())
        .await?
        .ok_or("session ended")??)
}

#[tokio::test]
async fn follows_share_one_session_and_credit_is_per_channel() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    for path in ["a", "b"] {
        append(&provider, path, Bytes::from_static(b"seed")).await?;
    }
    let (sender, receiver) = mpsc::unbounded();
    sender.unbounded_send(Ok(hello(1_000_000, 100)))?;
    sender.unbounded_send(Ok(interest(
        1,
        vec![subscribe(1, "a", 1, false), subscribe(3, "b", 1, false)],
    )))?;
    let mut output = serve(
        Arc::clone(&provider),
        crate::session::testing::authority(),
        "node".into(),
        Limits::default(),
        receiver.boxed(),
    )?;
    assert!(matches!(
        next(&mut output).await?.body,
        Some(Body::Welcome(_))
    ));
    let mut opened = 0;
    let mut applied = false;
    for _ in 0..3 {
        match next(&mut output).await?.body {
            Some(Body::Opened(_)) => opened += 1,
            Some(Body::InterestApplied(_)) => applied = true,
            _ => return Err("unexpected control".into()),
        }
    }
    assert_eq!(opened, 2);
    assert!(applied);
    append(&provider, "a", Bytes::from_static(b"one")).await?;
    append(&provider, "b", Bytes::from_static(b"two")).await?;
    assert!(futures::poll!(output.next()).is_pending());
    sender.unbounded_send(Ok(window(3)))?;
    assert_eq!(next(&mut output).await?.channel_id, 3);
    sender.unbounded_send(Ok(window(1)))?;
    assert_eq!(next(&mut output).await?.channel_id, 1);
    assert!(futures::poll!(output.next()).is_pending());
    Ok(())
}

#[tokio::test]
async fn checked_follow_confirms_the_provider_guard_and_refuses_only_a_bad_channel() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    append(&provider, "events", Bytes::from_static(b"seed")).await?;
    let mut read = provider
        .read(crate::ReadRequest {
            path: StreamPath::new("events")?,
            from: 0,
            limit: 1,
        })
        .await?;
    let anchor = read.next().await.ok_or("missing anchor")??.commit_id;
    let mut guarded = subscribe(1, "events", 1, false);
    let mut refused = subscribe(3, "events", 1, false);
    for (subscription, id) in [
        (&mut guarded, anchor),
        (&mut refused, crate::CommitId::from_bytes([42; 32])),
    ] {
        let Some(Kind::Follow(follow)) = subscription.kind.as_mut() else {
            return Err("missing follow".into());
        };
        follow.preceding_commit_id = Bytes::copy_from_slice(id.as_bytes());
    }
    let (sender, receiver) = mpsc::unbounded();
    sender.unbounded_send(Ok(hello(1_000_000, 100)))?;
    sender.unbounded_send(Ok(interest(
        1,
        vec![guarded, refused, subscribe(5, "events", 1, false)],
    )))?;
    let mut output = serve(
        Arc::clone(&provider),
        crate::session::testing::authority(),
        "node".into(),
        Limits::default(),
        receiver.boxed(),
    )?;
    assert!(matches!(
        next(&mut output).await?.body,
        Some(Body::Welcome(_))
    ));
    let mut opened = BTreeSet::new();
    let mut closed = false;
    for _ in 0..4 {
        let frame = next(&mut output).await?;
        match frame.body {
            Some(Body::InterestApplied(_)) => {}
            Some(Body::Opened(value)) => {
                assert_eq!(
                    value.preceding_commit_id.as_ref(),
                    if frame.channel_id == 1 {
                        anchor.as_bytes().as_slice()
                    } else {
                        &[]
                    }
                );
                opened.insert(frame.channel_id);
            }
            Some(Body::Closed(value)) => {
                assert_eq!(frame.channel_id, 3);
                assert_eq!(value.code, "prefix_not_retained");
                closed = true;
            }
            _ => return Err("unexpected pre-credit data".into()),
        }
    }
    assert_eq!(opened, BTreeSet::from([1, 5]));
    assert!(closed);
    append(&provider, "events", Bytes::from_static(b"delta")).await?;
    assert!(futures::poll!(output.next()).is_pending());
    for channel in [1, 5] {
        sender.unbounded_send(Ok(window(channel)))?;
        let frame = next(&mut output).await?;
        assert_eq!(frame.channel_id, channel);
        assert!(
            matches!(frame.body, Some(Body::Data(data)) if data.after.as_ref().is_some_and(|after| after.next == 2))
        );
    }
    Ok(())
}

#[tokio::test]
async fn backpressure_keeps_one_value_per_key_and_preserves_attention() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let status = |value: &'static [u8], attention| {
        Bytes::from(
            wire::SessionLatestValue {
                key: Bytes::from_static(b"agent"),
                value: Bytes::from_static(value),
                attention,
            }
            .encode_to_vec(),
        )
    };
    append(&provider, "status", status(b"seed", false)).await?;
    let (sender, receiver) = mpsc::unbounded();
    sender.unbounded_send(Ok(hello(0, 0)))?;
    sender.unbounded_send(Ok(interest(1, vec![subscribe(1, "status", 1, true)])))?;
    sender.unbounded_send(Ok(window(1)))?;
    let mut output = serve(
        Arc::clone(&provider),
        crate::session::testing::authority(),
        "node".into(),
        Limits::default(),
        receiver.boxed(),
    )?;
    for _ in 0..3 {
        next(&mut output).await?;
    }
    for (value, attention) in [
        (b"old".as_slice(), false),
        (b"new", false),
        (b"approval", true),
        (b"latest", false),
    ] {
        append(&provider, "status", status(value, attention)).await?;
        assert!(futures::poll!(output.next()).is_pending());
    }
    sender.unbounded_send(Ok(window(0)))?;
    for expected in [b"approval".as_slice(), b"latest"] {
        let received = next(&mut output).await?;
        let Some(Body::Data(wire::SessionData {
            item: Some(Item::LatestValue(value)),
            ..
        })) = received.body
        else {
            return Err("expected latest value".into());
        };
        assert_eq!(value.value.as_ref(), expected);
    }
    assert!(futures::poll!(output.next()).is_pending());
    Ok(())
}

#[tokio::test]
async fn replacement_resumes_from_cursor_and_ignores_stale_source() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    append(&provider, "a", Bytes::from_static(b"seed")).await?;
    let (sender, receiver) = mpsc::unbounded();
    sender.unbounded_send(Ok(hello(1_000_000, 100)))?;
    sender.unbounded_send(Ok(interest(1, vec![subscribe(1, "a", 0, false)])))?;
    sender.unbounded_send(Ok(window(1)))?;
    let mut output = serve(
        Arc::clone(&provider),
        crate::session::testing::authority(),
        "node".into(),
        Limits::default(),
        receiver.boxed(),
    )?;
    for _ in 0..3 {
        next(&mut output).await?;
    }
    let delivered = next(&mut output).await?;
    assert!(matches!(delivered.body, Some(Body::Data(_))));
    let mut replacement = subscribe(1, "a", 1, false);
    replacement.channel = Some(wire::SessionChannel {
        channel_id: 1,
        generation: 2,
    });
    sender.unbounded_send(Ok(interest(2, vec![replacement])))?;
    sender.unbounded_send(Ok(frame(
        1,
        2,
        Body::Window(wire::SessionWindow {
            bytes: 1000,
            frames: 1,
        }),
    )))?;
    next(&mut output).await?;
    next(&mut output).await?;
    append(&provider, "a", Bytes::from_static(b"new")).await?;
    let delivered = next(&mut output).await?;
    assert_eq!(delivered.generation, 2);
    let Some(Body::Data(data)) = delivered.body else {
        return Err("data missing".into());
    };
    assert_eq!(data.after.map(|cursor| cursor.next), Some(2));
    Ok(())
}

#[tokio::test]
async fn malformed_latest_value_closes_only_its_own_channel() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    append(&provider, "status", Bytes::from_static(b"not protobuf")).await?;
    append(&provider, "events", Bytes::from_static(b"seed")).await?;
    let (sender, receiver) = mpsc::unbounded();
    sender.unbounded_send(Ok(hello(1_000_000, 100)))?;
    sender.unbounded_send(Ok(interest(
        1,
        vec![
            subscribe(1, "status", 0, true),
            subscribe(3, "events", 0, false),
        ],
    )))?;
    sender.unbounded_send(Ok(window(1)))?;
    sender.unbounded_send(Ok(window(3)))?;
    let mut output = serve(
        Arc::clone(&provider),
        crate::session::testing::authority(),
        "node".into(),
        Limits::default(),
        receiver.boxed(),
    )?;
    let mut closed = false;
    let mut delivered = false;
    while !closed || !delivered {
        let received = next(&mut output).await?;
        match received.body {
            Some(Body::Closed(error)) => {
                assert_eq!(received.channel_id, 1);
                assert_eq!(error.code, StreamError::InvalidArgument.code());
                closed = true;
            }
            Some(Body::Data(_)) => {
                assert_eq!(received.channel_id, 3);
                delivered = true;
            }
            _ => {}
        }
    }
    append(&provider, "events", Bytes::from_static(b"healthy")).await?;
    let received = next(&mut output).await?;
    assert_eq!(received.channel_id, 3);
    assert!(matches!(received.body, Some(Body::Data(_))));
    Ok(())
}
