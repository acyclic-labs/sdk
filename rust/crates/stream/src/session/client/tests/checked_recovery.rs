use super::*;
use crate::{CommitId, runtime::PlatformExecutor};
use proptest::prelude::*;

#[derive(Default)]
struct Received {
    // One fixture lock, never nested or held across an await/wakeup.
    last: Mutex<Option<(u64, u64, CommitId)>>,
    changed: tokio::sync::Notify,
}
impl Received {
    fn observe(&self, generation: u64, next: u64, commit: CommitId) {
        let mut last = self.last.lock().unwrap();
        // A moved route can leave an old physical session readable. Its late
        // frames must not erase evidence already observed from the successor.
        if last
            .as_ref()
            .is_none_or(|(current, after, _)| (generation, next) > (*current, *after))
        {
            *last = Some((generation, next, commit));
        }
        drop(last);
        self.changed.notify_one();
    }
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
                }) = frame.as_ref().map(Frame::wire)
                    && let (Some(after), Some(Item::Record(record))) = (&data.after, &data.item)
                {
                    let commit =
                        CommitId::from_bytes(record.commit_id.as_ref().try_into().unwrap());
                    received.observe(*generation, after.next, commit);
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
            idempotency_key: crate::IdempotencyKey::unique(),
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
        tokio::time::timeout(Duration::from_secs(1), changed)
            .await
            .map_err(|error| {
                format!(
                    "buffered generation={generation} next={next} expected={expected} last={:?}: {error}",
                    *received.last.lock().unwrap()
                )
            })?;
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
        changed.await;
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
                key: None,
                read_limit: 0,
                bootstrap_head: false,
                when_absent: Default::default(),
                resource: crate::wire_codec::session::stream_resource("events"),
                delivery: wire::SessionDelivery::All as i32,
                preceding_commit_id: Bytes::copy_from_slice(
                    ledger.first().ok_or("empty ledger")?.as_bytes(),
                ),
                until: 0,
                at_least: Default::default(),
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
            assert_resumed(if moved { &new } else { &old }, generation, next, &ledger)
                .await
                .map_err(|error| format!("resume generation={generation} next={next}: {error}"))?;
        }
        if let Some(append) = actions.get(index) {
            if *append {
                append_one(&provider, &mut ledger).await?;
            } else if next < ledger.len() as u64 {
                let frame = tokio::time::timeout(Duration::from_secs(1), data.next())
                    .await
                    .map_err(|error| format!("yield generation={generation} next={next}: {error}"))?
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

#[test]
fn recovery_after_consuming_the_original_tail() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime
        .block_on(run(
            &[false, false, false, false, false, true],
            4,
            true,
            false,
        ))
        .unwrap();
}

fn check_observation_orders(observe: impl Fn(&Received, u64, u64, CommitId)) {
    // All 24 permutations, including every ordering within each generation.
    // This overapproximates the two ordered physical response streams.
    // The first permutation is itself a legal pair of ordered streams: the
    // successor's two frames arrive before the old endpoint's two late frames.
    let frames = [(2, 2), (2, 3), (1, 2), (1, 3)];
    for a in frames {
        for b in frames {
            for c in frames {
                for d in frames {
                    let order = [a, b, c, d];
                    if order
                        .into_iter()
                        .collect::<std::collections::BTreeSet<_>>()
                        .len()
                        != order.len()
                    {
                        continue;
                    }
                    let received = Received::default();
                    let mut observed = Vec::new();
                    for (generation, next) in order {
                        observed.push((generation, next));
                        observe(&received, generation, next, CommitId::from_bytes([0; 32]));
                        let last = received.last.lock().unwrap();
                        assert!(
                            last.as_ref().is_some_and(|(generation, next, _)| {
                                observed.iter().all(|seen| seen <= &(*generation, *next))
                            }),
                            "received frontier must survive every arrival order: {order:?}"
                        );
                    }
                    let last = received.last.lock().unwrap();
                    assert_eq!(
                        last.as_ref()
                            .map(|(generation, next, _)| (*generation, *next)),
                        Some((2, 3)),
                        "received frontier must survive every arrival order: {order:?}"
                    );
                }
            }
        }
    }
}

#[test]
fn received_frontier_survives_every_cross_generation_arrival_order() {
    check_observation_orders(Received::observe);
}

#[test]
#[should_panic(expected = "received frontier must survive every arrival order")]
fn negative_control_last_arrival_erases_the_successor_frontier() {
    check_observation_orders(|received, generation, next, commit| {
        *received.last.lock().unwrap() = Some((generation, next, commit));
    });
}
