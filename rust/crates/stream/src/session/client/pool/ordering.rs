//! Independent state machine at the real Cursor drop/Open dispatch boundary.
use super::*;
use futures::FutureExt;
use proptest::{
    prelude::*,
    test_runner::{TestCaseError, TestCaseResult},
};
use std::task::{Context, Poll};

type Action = (u8, u8, u8);

/// A production failure inside the model is a failed case.
fn fail(error: impl std::fmt::Debug) -> TestCaseError {
    TestCaseError::fail(format!("{error:?}"))
}

fn missing(what: &'static str) -> TestCaseError {
    TestCaseError::fail(format!("missing {what}"))
}

fn link(pool: &Pool) -> Result<&Link, TestCaseError> {
    pool.links.get("good").ok_or_else(|| missing("good link"))
}
#[derive(Default)]
struct Model {
    drops: VecDeque<u64>,
    live: BTreeSet<u64>,
    expected: VecDeque<(bool, u64)>,
    credits: BTreeMap<u64, u32>,
    buffered: BTreeMap<u64, u32>,
    received: BTreeMap<u64, u64>,
    applied: BTreeMap<u64, u64>,
    global_credit: u32,
}
impl Model {
    fn retire(&mut self) {
        while let Some(id) = self.drops.pop_front() {
            if self.live.remove(&id) {
                self.expected.push_back((false, id));
            }
        }
    }
    fn open(&mut self, id: u64) {
        self.retire();
        self.live.insert(id);
        self.expected.push_back((true, id));
    }
    fn wire(&mut self, value: wire::SessionFrame) -> TestCaseResult {
        match value.body {
            Some(Body::Interest(interest)) => {
                for id in interest.remove.into_iter().map(|c| c.channel_id) {
                    prop_assert_eq!(
                        self.expected.pop_front(),
                        Some((false, id)),
                        "drop must precede the replacement Open"
                    );
                }
                for id in interest
                    .upsert
                    .into_iter()
                    .map(|s| s.channel.unwrap().channel_id)
                {
                    prop_assert_eq!(
                        self.expected.pop_front(),
                        Some((true, id)),
                        "Open overtook a cursor drop"
                    );
                }
            }
            Some(Body::Window(window)) if value.channel_id != 0 => {
                let credit = self.credits.entry(value.channel_id).or_default();
                *credit += window.frames;
                let buffered = self.buffered.get(&value.channel_id).copied().unwrap_or(0);
                prop_assert!(
                    *credit + buffered <= 2,
                    "delivery credit exceeded the independent two-slot model"
                );
            }
            Some(Body::Window(window)) => {
                self.global_credit += window.frames;
            }
            _ => return Err(TestCaseError::fail("unexpected outbound frame")),
        }
        Ok(())
    }
}

fn output(pool: &Pool, model: &mut Model) -> TestCaseResult {
    while let Some(frame) = link(pool)?.sender.next().now_or_never() {
        model.wire(frame.map_err(fail)?)?;
    }
    Ok(())
}

// Every boundary can leave a wake registered, or dispatch the ready removal
// first. Open itself must preserve owner order under either scheduler choice.
fn boundary(pool: &mut Pool, model: &mut Model, dispatch: bool) -> TestCaseResult {
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    if dispatch {
        model.retire();
        while let Poll::Ready(id) = pool.removed.poll(&mut cx) {
            pool.remove(id.map_err(fail)?).map_err(fail)?;
            output(pool, model)?;
        }
        while let Poll::Ready(release) = pool.released.poll(&mut cx) {
            pool.released(release.map_err(fail)?).map_err(fail)?;
            output(pool, model)?;
        }
        output(pool, model)?;
        prop_assert!(
            model.expected.is_empty(),
            "a dispatch omitted an interest change"
        );
    } else {
        // Empty queues register an actual production waiter; queued signals
        // remain queued when the command branch wins the select.
        if model.drops.is_empty() {
            prop_assert!(pool.removed.poll(&mut cx).is_pending());
        }
    }
    Ok(())
}

fn open(
    pool: &mut Pool,
    client: &Client,
    model: &mut Model,
    id: u64,
    wakes: u8,
    broken: bool,
) -> Result<Cursor, TestCaseError> {
    boundary(pool, model, wakes & 1 != 0)?;
    let progress = Arc::new(Progress::new(0));
    let (records, receiver) = mpsc::channel(2);
    let cursor = client
        .cursor(id, Arc::clone(&progress), receiver)
        .map_err(fail)?;
    boundary(pool, model, wakes & 2 != 0)?;
    let (ready, _opened) = oneshot::channel();
    let command = Command::Open {
        id,
        attempt_epoch: 0,
        acknowledge: false,
        raw: false,
        progress,
        kind: Kind::Follow(wire::SessionFollow {
            path: format!("view/{id}"),
            delivery: wire::SessionDelivery::All as i32,
            ..Default::default()
        }),
        records,
        ready,
        observation: cursor.observation.operation.clone(),
    };
    boundary(pool, model, wakes & 4 != 0)?;
    model.open(id);
    if broken {
        pool.apply_command(command).map_err(fail)?;
    } else {
        pool.command(command).map_err(fail)?;
    }
    output(pool, model)?;
    prop_assert!(
        model.expected.is_empty(),
        "a dispatch omitted an interest change"
    );
    boundary(pool, model, wakes & 8 != 0)?;
    pool.receive(
        "good",
        frame(
            id,
            1,
            Body::Opened(wire::SessionOpened {
                resume: Some(super::super::super::cursor(0)),
                preceding_commit_id: Bytes::new(),
            }),
        ),
    )
    .map_err(fail)?;
    output(pool, model)?;
    boundary(pool, model, wakes & 16 != 0)?;
    Ok(cursor)
}

fn exercise(actions: &[Action], broken: bool) -> TestCaseResult {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(fail)?;
    let _entered = runtime.enter();
    let endpoint =
        tonic::transport::Endpoint::from_static("http://ordering.invalid").connect_lazy();
    let (mut pool, receiver) = fixture::fixture(&endpoint);
    let client = fixture::client(&pool);
    // An independent overview follow keeps the physical connection in use.
    let overview = pool.routes.get(&3).ok_or_else(|| missing("overview"))?;
    let _overview = client
        .cursor(3, Arc::clone(&overview.progress), receiver)
        .map_err(fail)?;
    let mut model = Model {
        global_credit: 2,
        ..Default::default()
    };
    let mut next = 5;
    let mut slots = [
        Some(open(&mut pool, &client, &mut model, next, 0, broken)?),
        None,
    ];
    next += 2;
    for &(kind, slot, wakes) in actions {
        let [first, second] = &mut slots;
        let entry = if slot % 2 == 0 { first } else { second };
        match kind % 8 {
            0 | 2 => {
                if kind % 8 == 2
                    && let Some(old) = entry.take()
                {
                    model.drops.push_back(old.id);
                    model.buffered.remove(&old.id);
                    drop(old);
                }
                if entry.is_none() {
                    *entry = Some(open(&mut pool, &client, &mut model, next, wakes, broken)?);
                    next += 2;
                }
            }
            1 => {
                if let Some(old) = entry.take() {
                    model.drops.push_back(old.id);
                    model.buffered.remove(&old.id);
                    drop(old);
                }
            }
            3 | 4 => {
                if let Some(cursor) = entry.as_ref() {
                    deliver(&mut pool, &mut model, cursor.id)?;
                }
            }
            5 => {
                if let Some(cursor) = entry.as_mut() {
                    consume(cursor, &mut model)?;
                }
            }
            _ => boundary(&mut pool, &mut model, true)?,
        }
        boundary(&mut pool, &mut model, wakes & 32 != 0)?;
        output(&pool, &mut model)?;
        for cursor in slots.iter().flatten() {
            prop_assert_eq!(
                cursor.progress.next().map_err(fail)?,
                model.applied.get(&cursor.id).copied().unwrap_or(0)
            );
        }
    }
    boundary(&mut pool, &mut model, true)?;
    let actual: BTreeSet<_> = pool
        .routes
        .keys()
        .copied()
        .filter(|id| *id != 1 && *id != 3)
        .collect();
    prop_assert_eq!(actual, model.live);
    Ok(())
}

/// Deliver the next record to `id` when the model holds credit for it.
fn deliver(pool: &mut Pool, model: &mut Model, id: u64) -> TestCaseResult {
    if model.credits.get(&id).copied().unwrap_or(0) > 0 && model.global_credit > 0 {
        let sequence = *model.received.entry(id).or_default();
        pool.receive(
            "good",
            frame(
                id,
                1,
                Body::Data(wire::SessionData {
                    after: Some(super::super::super::cursor(sequence + 1)),
                    accepted: sequence + 1,
                    durable: sequence + 1,
                    item: Some(Item::Record(wire_codec::record_wire(crate::Record {
                        sequence,
                        value: vec![sequence.to_le_bytes()[0]].into(),
                        commit_id: crate::CommitId::from_bytes([1; 32]),
                        committed_at_micros: 0,
                    }))),
                }),
            ),
        )
        .map_err(fail)?;
        *model.received.entry(id).or_default() += 1;
        *model.credits.entry(id).or_default() -= 1;
        model.global_credit -= 1;
        *model.buffered.entry(id).or_default() += 1;
    } else {
        // A stalled follow must have no remaining delivery grant.
        let route = pool.routes.get(&id).ok_or_else(|| missing("route"))?;
        prop_assert!(route.credit.frames == 0 || link(pool)?.credit.frames == 0);
    }
    Ok(())
}

/// Apply one buffered delivery in sequence order.
fn consume(cursor: &mut Cursor, model: &mut Model) -> TestCaseResult {
    if let Ok(delivery) = cursor.receiver.try_recv() {
        let delivery = delivery.map_err(fail)?;
        let expected = model.applied.entry(cursor.id).or_default();
        prop_assert_eq!(delivery.sequence, *expected);
        prop_assert!(
            cursor
                .progress
                .consume_record(delivery.generation, delivery.sequence, [1; 32])
                .map_err(fail)?
        );
        *expected += 1;
        *model.buffered.entry(cursor.id).or_default() -= 1;
        drop(delivery);
    }
    Ok(())
}

fn actions() -> impl Strategy<Value = Vec<Action>> {
    prop::collection::vec((0_u8..8, 0_u8..2, any::<u8>()), 0..40)
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    #[test]
    fn cursor_drop_open_and_credit_match_the_stateful_oracle(actions in actions()) {
        exercise(&actions, false)?;
    }
}
#[test]
fn separate_queue_mutant_fails_the_same_oracle_and_shrinks() {
    use proptest::test_runner::{Config, TestError, TestRunner};
    let mut runner = TestRunner::new(Config {
        cases: 32,
        failure_persistence: None,
        ..Config::default()
    });
    // The mutant has a known one-replacement minimum. Shrink arbitrary
    // prehistory directly to that minimum rather than spending the shrink
    // budget simplifying irrelevant fields in every preceding command.
    let strategy = prop_oneof![
        Just((Vec::<Action>::new(), 0_u8)),
        (actions(), 0_u8..8).no_shrink(),
    ];
    let result = runner.run(&strategy, |(mut actions, wakes)| {
        // The final replacement selects Open before the independent drop queue.
        actions.push((2, 0, wakes << 3));
        exercise(&actions, true)
    });
    let Err(TestError::Fail(_, (actions, wakes))) = result else {
        panic!("separate-queue mutant escaped the ordering oracle");
    };
    assert!(
        actions.is_empty(),
        "counterexample did not shrink to one replacement: {actions:?}"
    );
    assert_eq!(wakes, 0);
    exercise(&[(2, 0, 0)], false).unwrap();
}

#[test]
fn exhausted_credit_and_replacement_cover_every_dispatch_boundary() {
    for wakes in 0..64 {
        exercise(
            &[
                (3, 0, wakes),
                (4, 0, wakes),
                (3, 0, wakes),
                (5, 0, wakes),
                (6, 0, wakes),
                (3, 0, wakes),
                (2, 0, wakes),
                (1, 0, wakes),
                (0, 0, wakes),
            ],
            false,
        )
        .unwrap();
    }
}
