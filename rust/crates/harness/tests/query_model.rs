#![cfg(all(acyclic_loom, acyclic_harness_loom))]
use acyclic_harness::task_volume::model_checking::{Fault, assert_index_matches, set_fault};

use acyclic_harness::task_volume::{
    Component, Error, FileKey, Query, Snapshot, TaskFile, TaskVolume, Write,
};
use acyclic_stream::verification::{
    assert_negative_control, block_on, check as model, check_with_bound,
};
use acyclic_stream::{
    AppendRequest, CommitCondition, MemoryLimits, MemoryStream, OwnedBytes, StreamError,
    StreamPath, StreamProvider,
};
use futures::StreamExt as _;
use loom::thread;
use std::future::Future as _;
use std::mem::ManuallyDrop;
use std::sync::Arc;
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn active_command_cut_control(broken: bool) {
    model(move || {
        let provider = Arc::new(MemoryStream::default());
        let path = StreamPath::new("command-cut")?;
        let volume = Arc::new(block_on(TaskVolume::open(provider.clone(), path.clone()))?);
        let session = Component::new("session")?;
        let request = Write::new(
            FileKey::Session {
                session: session.clone(),
                file: TaskFile::Request(Component::new("00000000-0000-0000-0000-000000000002")?),
            },
            OwnedBytes::from_static(b"request"),
        );
        let base = block_on(volume.snapshot(&Query::Root, 0))?.generation;
        let base = block_on(volume.create(base, &request))?;
        // Independent owners have independent indexes. Seed both before racing
        // one publication against one projection; shared-index serialization
        // and the provider's atomic commit are covered by their lock models.
        let reader = block_on(TaskVolume::open(provider, path))?;
        let worker = thread::spawn({
            let volume = volume.clone();
            let session = session.clone();
            move || {
                block_on(volume.create(
                    base,
                    &Write::new(
                        FileKey::Session {
                            session,
                            file: TaskFile::ExecutionResult(Component::new(
                                "00000000-0000-0000-0000-000000000002",
                            )?),
                        },
                        OwnedBytes::from_static(b"done"),
                    ),
                ))
            }
        });
        let query = if broken {
            Query::Root
        } else {
            Query::CommandSession { session, recent: 0 }
        };
        let snapshot = block_on(reader.snapshot(&query, 2))?;
        if snapshot.generation.position == base.position {
            assert_eq!(snapshot.files, vec![request]);
        } else {
            assert_eq!(snapshot.generation.position, base.position + 1);
            assert!(snapshot.files.is_empty(), "settled request stayed active");
        }
        worker.join().map_err(|_| "publisher panicked")??;
        let settled = block_on(reader.snapshot(&query, 2))?;
        assert_eq!(settled.generation.position, base.position + 1);
        assert!(settled.files.is_empty(), "settled request stayed active");
        Ok(())
    });
}

#[test]
fn active_command_snapshot_and_completion_share_one_cut() {
    active_command_cut_control(false);
}

#[test]
#[should_panic(expected = "settled request stayed active")]
fn negative_control_detects_retaining_settled_commands() {
    active_command_cut_control(true);
}

fn create_claim_control(broken: bool, session: bool) {
    model(move || {
        // competing_writers_cannot_tear_the_shared_projection models the real
        // atomic publication fence. At that cut, an identical claim contender
        // either captured the old generation or observed the winning claim.
        // Cover both classes without re-exploring provider commit internals.
        for observed_winner in [false, true] {
            let provider = Arc::new(MemoryStream::default());
            let path = StreamPath::new("claim")?;
            let first = block_on(TaskVolume::open(provider.clone(), path.clone()))?;
            let second = block_on(TaskVolume::open(provider, path))?;
            let cut = block_on(first.snapshot(&Query::Root, 1))?.generation;
            let body = TaskFile::ExecutionClaim(Component::new(
                "command-00000000-0000-0000-0000-000000000001.00000000-0000-0000-0000-000000000002",
            )?);
            let key = if session {
                FileKey::Session {
                    session: Component::new("session")?,
                    file: body,
                }
            } else {
                FileKey::Task {
                    task: Component::new("task")?,
                    file: body,
                }
            };
            let file = Write::new(key, OwnedBytes::from_static(b"same request"));
            block_on(first.create(cut, &file))?;
            let contender = if observed_winner {
                block_on(second.snapshot(&Query::Root, 1))?.generation
            } else {
                cut
            };
            // An idempotent append may return success for an identical claim.
            // Creation must never authorize the contender's second effect.
            let accepted = if broken {
                block_on(second.append(contender, &file)).is_ok()
            } else {
                block_on(second.create(contender, &file)).is_ok()
            };
            assert!(!accepted, "duplicate request dispatch");
            assert_eq!(
                block_on(second.snapshot(&Query::Root, 1))?.files,
                vec![file]
            );
        }
        Ok(())
    });
}

#[test]
fn command_claim_create_authorizes_one_dispatch() {
    for session in [false, true] {
        create_claim_control(false, session);
    }
}

#[test]
#[should_panic(expected = "duplicate request dispatch")]
fn negative_control_detects_idempotent_command_claim() {
    create_claim_control(true, true);
}

// Control property: one locked increment preserves both updates. Splitting the
// same operations into separate critical sections must expose a lost update.
fn increment_control(split: bool) {
    model(move || {
        use acyclic_stream::query_sync::Mutex;
        let value = Arc::new(Mutex::new(0_u8));
        let increment = move |value: Arc<Mutex<u8>>| {
            block_on(async {
                if split {
                    let before = *value.lock().await;
                    *value.lock().await = before + 1;
                } else {
                    *value.lock().await += 1;
                }
            });
        };
        let worker = thread::spawn({
            let value = value.clone();
            move || increment(value)
        });
        increment(value.clone());
        worker.join().map_err(|_| "increment worker panicked")?;
        assert_eq!(*block_on(value.lock()), 2, "split increment lost update");
        Ok(())
    });
}

#[test]
fn modeled_mutex_preserves_atomic_increment() {
    increment_control(false);
}

#[test]
#[should_panic(expected = "split increment lost update")]
fn negative_control_detects_split_increment() {
    increment_control(true);
}

fn rwlock_control(split: bool) {
    model(move || {
        use acyclic_stream::query_sync::RwLock;
        let value = Arc::new(RwLock::new(0_u8));
        let increment = move |value: Arc<RwLock<u8>>| {
            block_on(async {
                if split {
                    let before = *value.read().await;
                    *value.write().await = before + 1;
                } else {
                    *value.write().await += 1;
                }
            });
        };
        let worker = thread::spawn({
            let value = value.clone();
            move || increment(value)
        });
        increment(value.clone());
        worker.join().map_err(|_| "increment worker panicked")?;
        assert_eq!(*block_on(value.read()), 2, "split rwlock lost update");
        Ok(())
    });
}

#[test]
fn modeled_rwlock_preserves_atomic_increment() {
    rwlock_control(false);
}

#[test]
#[should_panic(expected = "split rwlock lost update")]
fn negative_control_detects_split_rwlock_increment() {
    rwlock_control(true);
}

fn watch_control(discard_unseen: bool) {
    model(move || {
        use acyclic_stream::query_sync::watch;
        let (sender, mut receiver) = watch::channel(0_u8);
        let worker = thread::spawn(move || {
            sender.send_replace(1);
        });
        if *receiver.borrow_and_update() == 0 {
            if discard_unseen {
                // Deliberately broken: consume a publication between the check
                // and parking. No explicit yield may manufacture the race.
                receiver.borrow_and_update();
            }
            assert!(
                block_on(receiver.changed()).is_ok(),
                "discarded watch publication"
            );
        }
        assert_eq!(*receiver.borrow_and_update(), 1);
        worker.join().map_err(|_| "watch publisher panicked")?;
        Ok(())
    });
}

#[test]
fn modeled_watch_preserves_unseen_publication_and_close() {
    watch_control(false);
}

#[test]
#[should_panic(expected = "discarded watch publication")]
fn negative_control_detects_discarded_watch_publication() {
    watch_control(true);
}

fn initialized(
    provider: Arc<MemoryStream>,
    bytes: &'static [u8],
) -> Result<(Arc<TaskVolume<MemoryStream>>, Snapshot)> {
    let volume = Arc::new(block_on(TaskVolume::open(
        provider,
        StreamPath::new("model")?,
    ))?);
    let generation = block_on(volume.snapshot(&Query::Root, 0))?.generation;
    block_on(volume.append(
        generation,
        &Write::new(FileKey::Goal, OwnedBytes::from_static(bytes)),
    ))?;
    let snapshot = block_on(volume.snapshot(&Query::Root, 1))?;
    Ok((volume, snapshot))
}

fn snapshot_follow_and_publish(fault: Fault, preemptions: Option<usize>) {
    check_with_bound(preemptions, move || {
        let volume = Arc::new(block_on(TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("model")?,
        ))?);
        let cut = block_on(volume.snapshot(&Query::Root, 1))?;
        let keeper = ManuallyDrop::new(volume.clone());
        let writer = thread::spawn({
            let volume = volume.clone();
            move || {
                block_on(volume.append(
                    cut.generation,
                    &Write::new(FileKey::Goal, OwnedBytes::from_static(b"a")),
                ))
            }
        });
        set_fault(fault);
        block_on(async {
            let observation = volume.observe(Query::Root, None, 1).await?;
            let snapshot = observation.snapshot.ok_or("missing snapshot")?;
            writer.join().map_err(|_| "writer panicked")??;
            let files = if snapshot.files.is_empty() {
                let mut stream = observation.changes;
                let mut next = Box::pin(stream.next());
                let mut context = std::task::Context::from_waker(std::task::Waker::noop());
                match next.as_mut().poll(&mut context) {
                    std::task::Poll::Ready(Some(Ok(change))) => {
                        vec![change.file.ok_or("missing event")?]
                    }
                    other => panic!("snapshot follow skipped publication: {other:?}"),
                }
            } else {
                snapshot.files
            };
            assert_eq!(
                files,
                vec![Write::new(FileKey::Goal, OwnedBytes::from_static(b"a"))]
            );
            Ok(()) as Result
        })?;
        drop(ManuallyDrop::into_inner(keeper));
        Ok(())
    });
}

fn competing_writers(fault: Fault, preemptions: Option<usize>) {
    check_with_bound(preemptions, move || {
        let volume = Arc::new(block_on(TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("model")?,
        ))?);
        let expected = block_on(volume.snapshot(&Query::Root, 1))?.generation;
        let worker = thread::spawn({
            let volume = volume.clone();
            move || {
                set_fault(fault);
                block_on(volume.append(
                    expected,
                    &Write::new(FileKey::Goal, OwnedBytes::from_static(b"a")),
                ))
            }
        });
        set_fault(fault);
        let second = block_on(volume.append(
            expected,
            &Write::new(FileKey::Goal, OwnedBytes::from_static(b"b")),
        ));
        let results = [worker.join().map_err(|_| "writer panicked")?, second];
        assert!(
            results
                .iter()
                .all(|result| matches!(result, Ok(_) | Err(Error::Conflict(_)))),
            "unexpected publication error: {results:?}"
        );
        assert_eq!(
            results.iter().filter(|result| result.is_ok()).count(),
            1,
            "competing publication escaped its fence"
        );
        let snapshot = block_on(volume.snapshot(&Query::Root, 1))?;
        assert_eq!(snapshot.generation.position, expected.position + 1);
        assert_eq!(snapshot.files.len(), 1);
        Ok(())
    });
}

#[test]
fn platform_follow_push() {
    acyclic_stream::verification::subscription_and_append_have_no_lost_wakeup();
}

#[test]
fn local_accepted_query_push() {
    acyclic_stream::verification::local_accepted_queries_do_not_wait_for_durability();
}

#[test]
fn local_persist_push_reaches_an_already_parked_follower() {
    acyclic_stream::verification::local_persist_wakes_parked_followers(false);
}

#[test]
#[should_panic(expected = "persist omitted publication wake")]
fn negative_control_detects_omitted_persist_wake() {
    acyclic_stream::verification::local_persist_wakes_parked_followers(true);
}

#[test]
fn local_failure_push() {
    acyclic_stream::verification::local_poison_wakes_followers();
}
#[test]
fn platform_follow_recovery() {
    acyclic_stream::verification::subscription_and_recovery_never_wait_for_a_missing_anchor();
}
#[test]
fn platform_head_recovery() {
    acyclic_stream::verification::head_fence_and_recovery_cannot_publish_an_obsolete_generation();
}

fn recovery_and_refresh(fault: Fault, preemptions: Option<usize>) {
    check_with_bound(preemptions, move || {
        let provider = Arc::new(MemoryStream::default());
        let (volume, old) = initialized(provider.clone(), b"a")?;
        let replacement = MemoryStream::default();
        let (_, new) = initialized(Arc::new(replacement.clone()), b"b")?;
        let recovery = thread::spawn({
            let provider = provider.clone();
            move || {
                block_on(acyclic_stream::verification::recover(
                    &provider,
                    &replacement,
                ))
            }
        });
        set_fault(fault);
        let observed = block_on(volume.observe(Query::Root, None, 2))
            .expect("obsolete projection was not rebuilt")
            .snapshot
            .ok_or("missing snapshot")?;
        if observed.generation == old.generation {
            assert_eq!(observed.files, old.files);
        } else {
            assert_eq!(observed.generation, new.generation);
            assert_eq!(observed.files, new.files);
        }
        recovery.join().map_err(|_| "recovery panicked")??;
        let current = block_on(volume.observe(Query::Root, None, 2))
            .expect("obsolete projection was not rebuilt")
            .snapshot
            .ok_or("missing current snapshot")?;
        assert_eq!(current.generation, new.generation);
        assert_eq!(current.files, new.files);
        Ok(())
    });
}

fn recovery_and_acknowledgement(fault: Fault, preemptions: Option<usize>) {
    check_with_bound(preemptions, move || {
        let provider = Arc::new(MemoryStream::default());
        let (volume, old) = initialized(provider.clone(), b"a")?;
        let expected = old.generation;
        let replacement = MemoryStream::default();
        let (_, new) = initialized(Arc::new(replacement.clone()), b"b")?;
        assert_eq!(expected.position, new.generation.position);
        let recovery = thread::spawn({
            let provider = provider.clone();
            let volume = volume.clone();
            move || {
                block_on(async {
                    acyclic_stream::verification::recover(&provider, &replacement).await?;
                    volume.observe(Query::Root, None, 2).await?;
                    Ok(()) as Result
                })
            }
        });
        set_fault(fault);
        let result = block_on(volume.append(
            expected,
            &Write::new(FileKey::Goal, OwnedBytes::from_static(b"c")),
        ));
        assert!(matches!(
            result,
            Ok(_) | Err(Error::ResetRequired | Error::Conflict(_))
        ));
        recovery.join().map_err(|_| "recovery panicked")??;
        block_on(assert_index_matches(&volume, &new));
        let current = block_on(volume.observe(Query::Root, None, 2))?
            .snapshot
            .ok_or("missing current snapshot")?;
        assert_eq!(current.generation, new.generation);
        assert_eq!(current.files, new.files);
        Ok(())
    });
}

loom::thread_local! {
    static ADVANCE_DEADLINE: std::cell::Cell<bool> = std::cell::Cell::new(false);
}

fn fenced_publication(expiry: bool, broken: bool, preemptions: Option<usize>) {
    use loom::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    struct Clock {
        now: AtomicU64,
        tick: bool,
    }
    impl acyclic_stream::UnixMillisClock for Clock {
        fn now_unix_millis(&self) -> u64 {
            if ADVANCE_DEADLINE.with(std::cell::Cell::get) {
                self.now.store(3, Ordering::SeqCst);
            }
            if self.tick {
                self.now.fetch_add(1, Ordering::SeqCst)
            } else {
                self.now.load(Ordering::SeqCst)
            }
        }
    }
    check_with_bound(preemptions, move || {
        let clock = Arc::new(Clock {
            now: AtomicU64::new(1),
            tick: !expiry,
        });
        let provider = Arc::new(MemoryStream::new_with_clock(
            MemoryLimits::default(),
            clock.clone(),
        ));
        let volume = block_on(TaskVolume::open(
            provider.clone(),
            StreamPath::new("model")?,
        ))?;
        let expected = block_on(volume.snapshot(&Query::Root, 0))?.generation;
        let actor = StreamPath::new("actor")?;
        block_on(provider.append(AppendRequest {
            path: actor.clone(),
            records: vec![OwnedBytes::from_static(b"owner")],
            if_tail: Some(0),
            idempotency_key: None,
        }))?;
        let lost = Arc::new(AtomicBool::new(false));
        let worker = thread::spawn({
            let lost = lost.clone();
            let provider = provider.clone();
            move || {
                block_on(async {
                    let invalidated_at = if expiry {
                        // The unrelated append advances time under the real
                        // provider's lock, without changing the owner condition.
                        // Its clock sample cannot land between a successful
                        // publication's final deadline check and timestamp.
                        ADVANCE_DEADLINE.with(|advance| advance.set(true));
                        provider
                            .append(AppendRequest {
                                path: StreamPath::new("clock")?,
                                records: vec![OwnedBytes::from_static(b"expire")],
                                if_tail: Some(0),
                                idempotency_key: None,
                            })
                            .await?;
                        None
                    } else {
                        provider
                            .append(AppendRequest {
                                path: actor.clone(),
                                records: vec![OwnedBytes::from_static(b"new owner")],
                                if_tail: Some(1),
                                idempotency_key: None,
                            })
                            .await?;
                        let mut records = provider
                            .read(acyclic_stream::ReadRequest {
                                path: actor,
                                from: 1,
                                limit: 1,
                            })
                            .await?;
                        Some(
                            records
                                .next()
                                .await
                                .ok_or("missing revocation")??
                                .committed_at_micros,
                        )
                    };
                    lost.store(true, Ordering::SeqCst);
                    Ok(invalidated_at) as Result<Option<u64>>
                })
            }
        });
        let lost_at_entry = if expiry {
            clock.now.load(Ordering::SeqCst) >= 2
        } else {
            lost.load(Ordering::SeqCst)
        };
        // The expiry control retains admission-time rejection. It must expose
        // a missing recheck after preparation, not merely an already-expired call.
        if broken && (!expiry || !lost_at_entry) {
            set_fault(if expiry {
                Fault::IgnoreDeadline
            } else {
                Fault::OmitAuthorityConditions
            });
        }
        let file = Write::new(FileKey::Goal, OwnedBytes::from_static(b"p"));
        let result = block_on(volume.append_if(
            expected,
            &file,
            vec![CommitCondition::Tail {
                path: StreamPath::new("actor")?,
                expected: 1,
            }],
            expiry.then_some(2),
            None,
        ));
        let invalidated_at = worker.join().map_err(|_| "owner panicked")??;
        let committed = result.is_ok();
        assert!(
            committed
                || matches!(
                    result,
                    Err(Error::FenceConflict | Error::Storage(StreamError::DeadlineElapsed))
                )
        );
        if lost_at_entry {
            assert!(!committed, "stale publisher committed");
        }
        if committed {
            let mut records = block_on(provider.read(acyclic_stream::ReadRequest {
                path: StreamPath::new("model")?,
                from: expected.position,
                limit: 1,
            }))?;
            let record = block_on(records.next()).ok_or("missing publication")??;
            // The tick clock is sampled under the provider's state lock.
            // These distinct commit times order publication and revocation.
            assert!(
                record.committed_at_micros
                    < if expiry {
                        2_000
                    } else {
                        invalidated_at.unwrap()
                    },
                "stale publisher committed"
            );
        }
        let snapshot = block_on(volume.snapshot(&Query::Root, 1))?;
        assert_eq!(
            snapshot.generation.position,
            expected.position + u64::from(committed)
        );
        assert_eq!(
            snapshot.files,
            if committed { vec![file] } else { Vec::new() }
        );
        Ok(())
    });
}

#[test]
fn snapshot_follow_and_publish_are_one_coherent_cut_without_deadlock() {
    snapshot_follow_and_publish(Fault::None, Some(4));
    assert_negative_control(1, 4, "snapshot follow skipped publication", |bound| {
        snapshot_follow_and_publish(Fault::ResampleFollow, bound)
    });
}

#[test]
fn competing_writers_cannot_tear_the_shared_projection() {
    competing_writers(Fault::None, Some(4));
    assert_negative_control(2, 4, "competing publication escaped its fence", |bound| {
        competing_writers(Fault::ResamplePublicationFence, bound)
    });
}

#[test]
fn recovery_and_refresh_cannot_retain_files_from_the_obsolete_epoch() {
    recovery_and_refresh(Fault::None, Some(4));
    assert_negative_control(0, 4, "obsolete projection was not rebuilt", |bound| {
        recovery_and_refresh(Fault::RetainObsoleteIndex, bound)
    });
}

#[test]
fn recovery_and_append_acknowledgement_cannot_corrupt_the_rebuilt_index() {
    recovery_and_acknowledgement(Fault::None, Some(4));
    assert_negative_control(1, 4, "acknowledgement overwrote rebuilt", |bound| {
        recovery_and_acknowledgement(Fault::OverwriteRebuiltIndex, bound)
    });
}

#[test]
fn lease_expiry_and_authority_changes_fence_the_atomic_publication() {
    for expiry in [false, true] {
        fenced_publication(expiry, false, Some(4));
        assert_negative_control(1, 4, "stale publisher committed", |bound| {
            fenced_publication(expiry, true, bound)
        });
    }
}
