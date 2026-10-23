use super::*;
use crate::{CommitId, session::runtime::PlatformExecutor};
use proptest::prelude::*;

#[derive(Default)]
struct Received {
    // One fixture lock, never nested or held across an await/wakeup.
    last: Mutex<Option<(u64, u64, CommitId)>>,
    changed: tokio::sync::Notify,
}
struct Observed {
    inner: transport::Endpoint,
    received: std::sync::Arc<Received>,
    received_anchor_mutant: bool,
}
impl transport::Transport for Observed {
    fn session(
        &self,
        source: transport::Requests,
    ) -> crate::BoxProviderFuture<'_, Result<Response, StreamError>> {
        Box::pin(async move {
            let received = self.received.clone();
            let broken = self.received_anchor_mutant;
            let requests = source.map(move |mut frame| {
                // Observable mutant: send the last received commit rather
                // than the last yielded commit on recovery. The actual Rust
                // client, queue, credit and recovery run in both variants.
                if broken && let Some(Body::Interest(interest)) = &mut frame.body {
                    let last = *received.last.lock().unwrap();
                    for upsert in &mut interest.upsert {
                        if upsert.channel.is_some_and(|channel| channel.generation > 1)
                            && let Some(Kind::Follow(follow)) = &mut upsert.kind
                            && let Some((_, _, commit)) = last
                        {
                            follow.preceding_commit_id = Bytes::copy_from_slice(commit.as_bytes());
                        }
                    }
                }
                frame
            });
            let source = self.inner.session(Box::pin(requests)).await?;
            let received = self.received.clone();
            Ok(Box::pin(source.map(move |frame| {
                if let Ok(wire::SessionFrame {
                    generation,
                    body: Some(Body::Data(data)),
                    ..
                }) = &frame
                    && let (Some(after), Some(Item::Record(record))) = (&data.after, &data.item)
                {
                    let commit =
                        CommitId::from_bytes(record.commit_id.as_ref().try_into().unwrap());
                    *received.last.lock().unwrap() = Some((*generation, after.next, commit));
                    received.changed.notify_one();
                }
                frame
            })) as Response)
        })
    }
}
fn endpoint(
    channel: TransportChannel,
    received: &std::sync::Arc<Received>,
    broken: bool,
) -> transport::Endpoint {
    std::sync::Arc::new(Observed {
        inner: native::endpoint(channel, Some("Bearer fixture".parse().unwrap()), None),
        received: received.clone(),
        received_anchor_mutant: broken,
    })
}
async fn append_one(provider: &MemoryStream, ledger: &mut Vec<CommitId>) -> TestResult {
    let sequence = ledger.len();
    provider
        .append(AppendRequest {
            path: StreamPath::new("events")?,
            records: vec![(OwnedBytes::from(vec![sequence.to_le_bytes()[0]])).into()],
            if_tail: Some(sequence as u64),
            idempotency_key: None,
        })
        .await?;
    let commit = checked::anchor(provider, "events", sequence as u64).await?;
    assert!(
        !ledger.contains(&commit),
        "each record must have a distinct commit"
    );
    ledger.push(commit);
    Ok(())
}
async fn buffered(received: &Received, generation: u64, next: u64, tail: u64) -> TestResult {
    let expected = tail.min(next + 2);
    loop {
        let changed = received.changed.notified();
        if received
            .last
            .lock()
            .unwrap()
            .is_some_and(|(current, after, _)| current == generation && after >= expected)
        {
            return Ok(());
        }
        tokio::time::timeout(Duration::from_secs(1), changed).await?;
    }
}
/// Wait for `target` to resume follow 1 at `generation`, from the last yielded commit.
async fn assert_resumed(
    target: &Fixture,
    generation: u64,
    next: u64,
    ledger: &[CommitId],
) -> TestResult {
    loop {
        let changed = target.counts.changed.notified();
        let resume = target
            .counts
            .resumes
            .lock()
            .unwrap()
            .iter()
            .find(|(id, current, _)| *id == 1 && *current == generation)
            .copied();
        let guard = target
            .counts
            .guards
            .lock()
            .unwrap()
            .iter()
            .find(|(id, current, _)| *id == 1 && *current == generation)
            .cloned();
        if let (Some((_, _, resume)), Some((_, _, guard))) = (resume, guard) {
            assert_eq!(resume, next, "checked ALL must resume the last yielded cut");
            let yielded = ledger
                .get(usize::try_from(next - 1)?)
                .ok_or("unyielded cut")?;
            assert_eq!(
                guard.as_ref(),
                yielded.as_bytes(),
                "checked ALL must resume the last yielded commit, never the last received commit"
            );
            return Ok(());
        }
        tokio::time::timeout(Duration::from_secs(1), changed).await?;
    }
}
async fn run(actions: &[bool], boundary: usize, moved: bool, broken: bool) -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let mut ledger = Vec::new();
    for _ in 0..6 {
        append_one(&provider, &mut ledger).await?;
    }
    let old = Fixture::open(provider.clone(), "home").await?;
    let new = Fixture::open(provider.clone(), "owner").await?;
    let received = std::sync::Arc::new(Received::default());
    let home = endpoint(old._channel.clone(), &received, broken);
    let destination = endpoint(new._channel.clone(), &received, broken);
    let connect = std::sync::Arc::new(move |_| {
        let destination = destination.clone();
        Box::pin(async move { Ok(destination) })
            as crate::BoxProviderFuture<'static, Result<transport::Endpoint, StreamError>>
    });
    let client = Client::with_transport(home, connect, std::sync::Arc::new(PlatformExecutor));
    client.register_credential(0, crate::session::testing::credential())?;
    let mut data = client
        .follow_data(
            wire::SessionFollow {
                resource: crate::wire_codec::session::stream_resource("events"),
                delivery: wire::SessionDelivery::All as i32,
                preceding_commit_id: Bytes::copy_from_slice(
                    ledger.first().ok_or("empty ledger")?.as_bytes(),
                ),
            },
            1,
            crate::FollowFilter::default(),
        )
        .await?;
    // Independent state: a ledger supplied by accepted appends and a cursor
    // advanced only after an application yield. Received frames never update it.
    let mut next = 1_u64;
    let mut generation = 1;
    let mut can_buffer = true;
    for index in 0..=actions.len() {
        if can_buffer {
            buffered(&received, generation, next, ledger.len() as u64).await?;
        }
        if index == boundary {
            if moved {
                old.controls.send(Ok(frame(
                    1,
                    1,
                    Body::Moved(wire::SessionMoved {
                        node: Some(wire::SessionNode {
                            node_id: "owner".into(),
                            endpoint: "https://owner.invalid".into(),
                            placement_epoch: 1,
                        }),
                        resume: Some(cursor(999)),
                    }),
                )))?;
            } else {
                old.controls
                    .send(Err(Status::unavailable("property recovery")))?;
            }
            generation += 1;
            // An unpolled cursor still owns its old-generation deliveries.
            // Recovery cannot refill their credit until next() discards them.
            can_buffer = false;
            assert_resumed(if moved { &new } else { &old }, generation, next, &ledger).await?;
        }
        if let Some(append) = actions.get(index) {
            if *append {
                append_one(&provider, &mut ledger).await?;
            } else if next < ledger.len() as u64 {
                let frame = tokio::time::timeout(Duration::from_secs(1), data.next())
                    .await?
                    .ok_or("ALL ended")??;
                let Some(Item::Record(record)) = frame.item else {
                    return Err("ALL returned another item".into());
                };
                assert_eq!(record.sequence, next);
                let expected = ledger
                    .get(usize::try_from(next)?)
                    .ok_or("unappended record")?;
                assert_eq!(record.commit_id.as_ref(), expected.as_bytes());
                assert_eq!(frame.after.as_ref().map(|after| after.next), Some(next + 1));
                next += 1;
                can_buffer = true;
            }
        }
    }
    drop(data);
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn checked_all_matches_the_yielded_ledger_at_every_recovery_boundary(
        actions in prop::collection::vec(any::<bool>(), 0..7),
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        for boundary in 0..=actions.len() {
            for moved in [false, true] {
                runtime.block_on(run(&actions, boundary, moved, false)).unwrap_or_else(|error| {
                    panic!("actions={actions:?} boundary={boundary} moved={moved}: {error}")
                });
            }
        }
    }
}

#[test]
#[should_panic(
    expected = "checked ALL must resume the last yielded commit, never the last received commit"
)]
fn negative_control_advances_the_anchor_from_received_data() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime
        .block_on(run(&[false, true, false], 0, true, true))
        .unwrap();
}

#[test]
fn recovery_preserves_unpolled_credit_through_later_appends() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for moved in [false, true] {
        runtime
            .block_on(run(&[true, false, true, false], 0, moved, false))
            .unwrap();
    }
}
