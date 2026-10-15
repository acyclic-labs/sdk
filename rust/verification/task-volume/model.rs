use acyclic_query_verification::{
    Component, Error, FileKey, Query, Snapshot, TaskFile, TaskVolume, Write,
};
use acyclic_stream::verification::{block_on, check as model};
use acyclic_stream::{
    AppendRequest, CommitCondition, MemoryLimits, MemoryStream, OwnedBytes, StreamError,
    StreamPath, StreamProvider,
};
use futures::StreamExt as _;
use loom::thread;
use std::sync::Arc;
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

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
    let mut generation = block_on(volume.snapshot(&Query::Root, 0))?.generation;
    for key in [
        FileKey::Goal,
        FileKey::Task {
            task: Component::new("t")?,
            file: TaskFile::Status,
        },
    ] {
        generation =
            block_on(volume.append(generation, &Write::new(key, OwnedBytes::from_static(bytes))))?;
    }
    let snapshot = block_on(volume.snapshot(&Query::Root, 2))?;
    Ok((volume, snapshot))
}

#[test]
fn snapshot_follow_and_publish_are_one_coherent_cut_without_deadlock() {
    model(|| {
        let volume = Arc::new(block_on(TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("model")?,
        ))?);
        let cut = block_on(volume.snapshot(&Query::Root, 1))?;
        let writer = thread::spawn({
            let volume = volume.clone();
            move || {
                block_on(volume.append(
                    cut.generation,
                    &Write::new(FileKey::Goal, OwnedBytes::from_static(b"accepted")),
                ))
            }
        });
        block_on(async {
            let observation = volume.observe(Query::Root, None, 1).await?;
            let snapshot = observation.snapshot.ok_or("missing snapshot")?;
            let files = if snapshot.files.is_empty() {
                let mut stream = observation.changes;
                vec![
                    stream
                        .next()
                        .await
                        .ok_or("follow ended")??
                        .file
                        .ok_or("missing event")?,
                ]
            } else {
                snapshot.files
            };
            assert_eq!(
                files,
                vec![Write::new(
                    FileKey::Goal,
                    OwnedBytes::from_static(b"accepted")
                )]
            );
            Ok(()) as Result
        })?;
        writer.join().map_err(|_| "writer panicked")??;
        Ok(())
    });
}

#[test]
fn competing_writers_cannot_tear_the_shared_projection() {
    model(|| {
        let volume = Arc::new(block_on(TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("model")?,
        ))?);
        let expected = block_on(volume.snapshot(&Query::Root, 1))?.generation;
        let worker = thread::spawn({
            let volume = volume.clone();
            move || {
                block_on(volume.append(
                    expected,
                    &Write::new(FileKey::Goal, OwnedBytes::from_static(b"one")),
                ))
            }
        });
        let second = block_on(volume.append(
            expected,
            &Write::new(FileKey::Goal, OwnedBytes::from_static(b"two")),
        ));
        let results = [worker.join().map_err(|_| "writer panicked")?, second];
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert!(
            results
                .iter()
                .all(|result| matches!(result, Ok(_) | Err(Error::Conflict(_))))
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

#[test]
fn recovery_and_refresh_cannot_retain_files_from_the_obsolete_epoch() {
    model(|| {
        let provider = Arc::new(MemoryStream::default());
        let (volume, old) = initialized(provider.clone(), b"old")?;
        let replacement = MemoryStream::default();
        let (_, new) = initialized(Arc::new(replacement.clone()), b"new")?;
        let recovery = thread::spawn({
            let provider = provider.clone();
            move || {
                block_on(acyclic_stream::verification::recover(
                    &provider,
                    &replacement,
                ))
            }
        });
        let observed = block_on(volume.observe(Query::Root, None, 2))?
            .snapshot
            .ok_or("missing snapshot")?;
        if observed.generation == old.generation {
            assert_eq!(observed.files, old.files);
        } else {
            assert_eq!(observed.generation, new.generation);
            assert_eq!(observed.files, new.files);
        }
        recovery.join().map_err(|_| "recovery panicked")??;
        let current = block_on(volume.observe(Query::Root, None, 2))?
            .snapshot
            .ok_or("missing current snapshot")?;
        assert_eq!(current.generation, new.generation);
        assert_eq!(current.files, new.files);
        Ok(())
    });
}

#[test]
fn recovery_and_append_acknowledgement_cannot_corrupt_the_rebuilt_index() {
    model(|| {
        let provider = Arc::new(MemoryStream::default());
        let (volume, old) = initialized(provider.clone(), b"old")?;
        let expected = old.generation;
        let replacement = MemoryStream::default();
        let (_, new) = initialized(Arc::new(replacement.clone()), b"new")?;
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
        let result = block_on(volume.append(
            expected,
            &Write::new(FileKey::Goal, OwnedBytes::from_static(b"obsolete")),
        ));
        assert!(matches!(
            result,
            Ok(_) | Err(Error::ResetRequired | Error::Conflict(_))
        ));
        recovery.join().map_err(|_| "recovery panicked")??;
        let current = block_on(volume.observe(Query::Root, None, 2))?
            .snapshot
            .ok_or("missing current snapshot")?;
        assert_eq!(current.generation, new.generation);
        assert_eq!(current.files, new.files);
        Ok(())
    });
}

#[test]
fn lease_expiry_and_authority_changes_fence_the_atomic_publication() {
    use loom::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    for expiry in [false, true] {
        model(move || {
            let clock = Arc::new(AtomicU64::new(1));
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
                let actor = actor.clone();
                move || {
                    block_on(async {
                        if expiry {
                            clock.store(3, Ordering::SeqCst);
                        } else {
                            provider
                                .append(AppendRequest {
                                    path: actor,
                                    records: vec![OwnedBytes::from_static(b"new owner")],
                                    if_tail: Some(1),
                                    idempotency_key: None,
                                })
                                .await?;
                        }
                        lost.store(true, Ordering::SeqCst);
                        Ok(()) as Result
                    })
                }
            });
            let lost_at_entry = lost.load(Ordering::SeqCst);
            let file = Write::new(FileKey::Goal, OwnedBytes::from_static(b"publish"));
            let result = block_on(volume.append_if(
                expected,
                &file,
                vec![CommitCondition::Tail {
                    path: actor,
                    expected: 1,
                }],
                expiry.then_some(2),
            ));
            worker.join().map_err(|_| "owner panicked")??;
            let committed = result.is_ok();
            assert!(
                committed
                    || matches!(
                        result,
                        Err(Error::FenceConflict | Error::Storage(StreamError::DeadlineElapsed))
                    )
            );
            if lost_at_entry {
                assert!(!committed);
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
}
