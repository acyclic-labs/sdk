//! Queues partition locally runnable sources; global credit gates every poll.
use super::*;
use proptest::prelude::*;
use std::collections::BTreeSet;

fn source(identity: wire::SessionChannel) -> Source {
    futures::stream::iter(
        [Ok(Event::Opened(None))]
            .into_iter()
            .chain((0..2).map(|sequence| {
                Ok(Event::Record(crate::Record {
                    sequence,
                    value: vec![sequence.to_le_bytes()[0]].into(),
                    commit_id: crate::CommitId::from_bytes([1; 32]),
                    committed_at_micros: 0,
                }))
            }))
            .map(move |event| (identity, event)),
    )
    .boxed()
}
fn partition(sources: &Sources) -> Result<(), TestCaseError> {
    let mut seen = BTreeSet::new();
    for (class, queue) in sources.queues.iter().enumerate() {
        let mut before = None;
        let mut next = queue.first;
        while let Some(id) = next {
            prop_assert!(seen.insert(id), "duplicate or cyclic source {id}");
            let Some(entry) = sources.entries.get(&id) else {
                return Err(TestCaseError::fail("queued source has no entry"));
            };
            let Some(links) = entry.links else {
                return Err(TestCaseError::fail("linked source has no links"));
            };
            prop_assert_eq!(links.class, class);
            prop_assert_eq!(links.before, before);
            prop_assert!(entry.ready);
            prop_assert_eq!(
                Some(class),
                if entry.opening {
                    Some(OPENING)
                } else {
                    entry.mode
                }
            );
            before = Some(id);
            next = links.after;
        }
        prop_assert_eq!(queue.last, before);
    }
    for (id, entry) in &sources.entries {
        let runnable = entry.ready && (entry.opening || entry.mode.is_some());
        prop_assert_eq!(seen.contains(id), runnable);
        prop_assert_eq!(entry.links.is_some(), runnable);
    }
    Ok(())
}
fn mode(value: u8) -> Option<usize> {
    match value % 4 {
        0 => None,
        1 => Some(ALL),
        2 => Some(LATEST),
        _ => Some(EITHER),
    }
}
/// The channel `selected` names, or a failed case.
fn slot<T>(values: &mut [T], selected: usize) -> Result<&mut T, TestCaseError> {
    values
        .get_mut(selected)
        .ok_or_else(|| TestCaseError::fail(format!("no channel {selected}")))
}
fn exercise(operations: &[(u8, u8, u8)], broken: bool) -> Result<(), TestCaseError> {
    let mut sources = Sources::new();
    let mut generations = [1; 3];
    let mut delivered = [0; 3];
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    // A ready opened source makes a changed admission class observable immediately.
    let first = wire::SessionChannel {
        channel_id: 1,
        generation: 1,
    };
    sources.insert(first, source(first), Some(ALL));
    assert!(matches!(
        sources.poll((true, true, true), &mut cx),
        Poll::Ready(Ok(Some(_)))
    ));
    for &(action, selected, value) in operations {
        let id = u64::from(selected % 3) + 1;
        let selected = usize::from(selected % 3);
        match action % 5 {
            0 => {
                let generation = slot(&mut generations, selected)?;
                *generation += 1;
                let identity = wire::SessionChannel {
                    channel_id: id,
                    generation: *generation,
                };
                *slot(&mut delivered, selected)? = 0;
                sources.insert(identity, source(identity), mode(value));
            }
            1 => {
                if broken {
                    if let Some(entry) = sources.entries.get_mut(&id) {
                        entry.mode = mode(value);
                    }
                } else {
                    sources.mode(id, mode(value));
                }
            }
            2 => sources.remove(id),
            3 => {
                if let Some(entry) = sources.entries.get(&id) {
                    Wake::wake_by_ref(&entry.wake);
                }
            }
            _ => {
                let room = (value & 1 != 0, value & 2 != 0, value & 4 != 0);
                if let Poll::Ready(Ok(Some((identity, Ok(Event::Record(record)))))) =
                    sources.poll(room, &mut cx)
                {
                    let selected = usize::try_from(identity.channel_id - 1)?;
                    let delivered = slot(&mut delivered, selected)?;
                    prop_assert_eq!(record.sequence, *delivered);
                    *delivered += 1;
                    let class = sources
                        .entries
                        .get(&identity.channel_id)
                        .and_then(|entry| entry.mode);
                    let permitted = match class {
                        Some(ALL) => room.1 && room.2,
                        Some(LATEST) => room.0,
                        Some(EITHER) => room.0 || (room.1 && room.2),
                        _ => false,
                    };
                    prop_assert!(permitted);
                }
            }
        }
        partition(&sources)?;
        if let Some(entry) = sources.entries.get(&id) {
            let mode = entry.mode;
            let queues = sources
                .queues
                .iter()
                .map(|queue| (queue.first, queue.last))
                .collect::<Vec<_>>();
            sources.mode(id, mode);
            prop_assert_eq!(
                queues,
                sources
                    .queues
                    .iter()
                    .map(|queue| (queue.first, queue.last))
                    .collect::<Vec<_>>()
            );
            partition(&sources)?;
        }
    }
    Ok(())
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn queues_preserve_exact_membership_order_and_credit(
        operations in prop::collection::vec((any::<u8>(), any::<u8>(), any::<u8>()), 0..40)
    ) {
        exercise(&operations, false)?;
    }
}
#[test]
fn negative_control_changes_class_without_moving_the_source() {
    use proptest::test_runner::{Config, TestError, TestRunner};
    let mut runner = TestRunner::new(Config {
        cases: 32,
        failure_persistence: None,
        ..Config::default()
    });
    let result = runner.run(&any::<u8>(), |value| {
        exercise(&[(1, 0, 2 + (value & 1))], true)
    });
    let Err(TestError::Fail(_, value)) = result else {
        panic!("broken promotion escaped");
    };
    assert_eq!(value, 0);
    exercise(&[(1, 0, 2)], false).unwrap();
}
#[test]
fn every_local_and_global_credit_combination_matches_the_admission_contract() {
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    for latest in [false, true] {
        for pending in 0..5 {
            for frames in 0..2 {
                for bytes in 0..2 {
                    for ending in [false, true] {
                        for room in 0..8 {
                            let channel = Channel {
                                generation: 1,
                                latest,
                                opaque_cursor: false,
                                pending: (0..pending)
                                    .map(|_| Pending {
                                        frame: Default::default(),
                                        key: None,
                                    })
                                    .collect(),
                                indexed: None,
                                credit: wire::SessionWindow { frames, bytes },
                                sent: Default::default(),
                                applied: Default::default(),
                                ending: ending.then_some(StreamError::Unavailable),
                            };
                            let room = (room & 1 != 0, room & 2 != 0, room & 4 != 0);
                            let expected = !ending
                                && ((latest && pending < 3 && room.0)
                                    || (pending == 0
                                        && frames > 0
                                        && bytes > 0
                                        && room.1
                                        && room.2));
                            let mut sources = Sources::new();
                            let identity = wire::SessionChannel {
                                channel_id: 1,
                                generation: 1,
                            };
                            sources.insert(identity, source(identity), channel.source_mode());
                            assert!(matches!(
                                sources.poll(room, &mut cx),
                                Poll::Ready(Ok(Some((_, Ok(Event::Opened(_))))))
                            ));
                            let polled = sources.poll(room, &mut cx);
                            assert_eq!(
                                matches!(polled, Poll::Ready(Ok(Some((_, Ok(Event::Record(_))))))),
                                expected
                            );
                            partition(&sources).unwrap();
                        }
                    }
                }
            }
        }
    }
}
#[test]
fn replaced_generation_ignores_a_late_provider_wakeup() {
    let mut sources = Sources::new();
    let old = wire::SessionChannel {
        channel_id: 1,
        generation: 1,
    };
    sources.insert(old, source(old), Some(ALL));
    let stale = sources.entries.get(&1).map(|entry| entry.wake.clone());
    let new = wire::SessionChannel {
        generation: 2,
        ..old
    };
    let stream = futures::stream::once(async move { (new, Ok(Event::Opened(None))) })
        .chain(futures::stream::pending())
        .boxed();
    sources.insert(new, stream, Some(ALL));
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(sources.poll((true, true, true), &mut cx).is_ready());
    assert!(sources.poll((true, true, true), &mut cx).is_pending());
    let Some(stale) = stale else {
        panic!("the replaced source had an entry");
    };
    Wake::wake_by_ref(&stale);
    assert!(sources.poll((true, true, true), &mut cx).is_pending());
    assert_eq!(
        sources.entries.get(&1).map(|entry| entry.ready),
        Some(false)
    );
    partition(&sources).unwrap();
}

fn old_mode(sources: &mut Sources, id: u64, mode: Option<usize>) {
    sources.unlink(id);
    if let Some(entry) = sources.entries.get_mut(&id) {
        entry.mode = mode;
    }
    sources.enqueue(id);
}

#[test]
fn unchanged_admission_does_no_queue_edits_and_preserves_turn_order() {
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    for legacy in [true, false] {
        let mut sources = Sources::new();
        for channel_id in 1..=10_000 {
            let identity = wire::SessionChannel {
                channel_id,
                generation: 1,
            };
            sources.insert(identity, source(identity), Some(ALL));
        }
        for channel_id in 1..=10_000 {
            assert!(matches!(sources.poll((false, false, false), &mut cx),
                Poll::Ready(Ok(Some((identity, Ok(Event::Opened(_)))))) if identity.channel_id == channel_id));
        }
        sources.queue_edits = 0;
        let started = std::time::Instant::now();
        for _ in 0..10_000 {
            if legacy {
                old_mode(&mut sources, 1, Some(ALL));
            } else {
                sources.mode(1, Some(ALL));
            }
        }
        let elapsed = started.elapsed();
        let bound = |edits| edits == 0;
        assert_eq!(
            bound(sources.queue_edits),
            !legacy,
            "unchanged admission must not relink a runnable source; legacy must fail the same bound"
        );
        assert_eq!(sources.queue_edits, if legacy { 20_000 } else { 0 });
        assert_eq!(sources.queues[ALL].first, Some(if legacy { 2 } else { 1 }));
        partition(&sources).unwrap();
        eprintln!(
            "source_refresh legacy={legacy} follows=10000 refreshes=10000 queue_edits={} elapsed={elapsed:?}",
            sources.queue_edits
        );
    }
}
