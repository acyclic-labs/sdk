use acyclic_query_verification::{
    Component, Error, FileKey, Query, Snapshot, TaskFile, TaskVolume, Write,
};
use acyclic_stream::verification::{block_on, check as model};
use acyclic_stream::{MemoryStream, OwnedBytes, StreamPath};
use futures::StreamExt as _;
use loom::thread;
use std::sync::Arc;
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

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
