use super::*;
use crate::conversation::{FileDescriptor, VolumeClass, VolumeOwner, VolumeRef};
use crate::resources::ProviderRef;
use crate::test_support::crash_cuts::{Cut, enumerate};
use acyclic_stream::{LocalStream, StreamPath};
use std::cell::Cell;

const MAXIMUM: u64 = 4096;

fn path() -> StreamPath {
    StreamPath::new("pinned-manifest").unwrap()
}

fn task() -> TaskId {
    TaskId::from_bytes([1; 16])
}

fn manifest(bytes: &[u8]) -> FileRef {
    FileRef::new(
        VolumeRef::new(
            ProviderRef::new("test".into(), "filesystem".into(), "1".into(), &mut None).unwrap(),
            "journal".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([2; 16])),
            &mut None,
        )
        .unwrap(),
        "inputs/manifest.json".into(),
        "1".into(),
        FileDescriptor::from_bytes(bytes, "application/json", &mut None).unwrap(),
        "manifest.json".into(),
        &mut None,
    )
    .unwrap()
}

#[derive(Debug)]
struct Recovered {
    pinned_before: bool,
    authorized: bool,
    admission_reads: usize,
    exact_pointer: bool,
    replay_stable: bool,
    replacement_denied: bool,
    other_task_denied: bool,
}

async fn recover(stream: Arc<LocalStream>, rediscover: bool) -> Recovered {
    let volume = TaskVolume::open(stream, path()).await.unwrap();
    let original = manifest(b"original admitted configuration");
    let replacement = manifest(b"edited live configuration");
    let query = Query::File(key(task()).unwrap());
    let pinned_before = !volume.snapshot(&query, 1).await.unwrap().files.is_empty();
    let reads = Cell::new(0);
    let authorized = authorize(
        &volume,
        task(),
        original.descriptor().sha256(),
        MAXIMUM,
        || {
            reads.set(reads.get() + 1);
            // The control replaces the retained admission with rediscovery of
            // mutable settings at the real recovery callback boundary.
            std::future::ready(Ok(if rediscover {
                replacement.clone()
            } else {
                original.clone()
            }))
        },
    )
    .await
    .is_ok();
    let admission_reads = reads.get();
    let files = volume.snapshot(&query, 1).await.unwrap().files;
    let exact_pointer =
        files.len() == 1 && files[0].bytes.as_ref() == serde_json::to_vec(&original).unwrap();
    let before = volume.head().await.unwrap();
    let absent =
        || std::future::ready(Err(Error::Unauthorized("no admitted model command".into())));
    let replay = authorize(
        &volume,
        task(),
        original.descriptor().sha256(),
        MAXIMUM,
        absent,
    )
    .await;
    let replacement_denied = matches!(
        authorize(
            &volume,
            task(),
            replacement.descriptor().sha256(),
            MAXIMUM,
            absent,
        )
        .await,
        Err(Error::Unauthorized(_))
    );
    let other_task_denied = matches!(
        authorize(
            &volume,
            TaskId::from_bytes([3; 16]),
            original.descriptor().sha256(),
            MAXIMUM,
            absent,
        )
        .await,
        Err(Error::Unauthorized(_))
    );
    Recovered {
        pinned_before,
        authorized,
        admission_reads,
        exact_pointer,
        replay_stable: replay.is_ok() && volume.head().await.unwrap() == before,
        replacement_denied,
        other_task_denied,
    }
}

fn violation(observed: &Recovered) -> Option<&'static str> {
    if !observed.authorized {
        return Some("recovery lost the admitted configuration");
    }
    if observed.admission_reads != usize::from(!observed.pinned_before) {
        return Some("recovery did not use exactly the missing admission");
    }
    if !observed.exact_pointer {
        return Some("recovery did not reconstruct the exact manifest pointer");
    }
    if !observed.replay_stable {
        return Some("replay changed durable state");
    }
    if !observed.replacement_denied || !observed.other_task_denied {
        return Some("configuration authority escaped its task and digest");
    }
    None
}

async fn cuts(rediscover: bool) -> Vec<(Option<Cut>, Recovered)> {
    let found = enumerate(
        |stream| async move {
            TaskVolume::open(stream, path()).await.unwrap();
        },
        |stream| async move {
            let volume = TaskVolume::open(stream, path()).await.unwrap();
            let _ = retain(
                &volume,
                task(),
                &manifest(b"original admitted configuration"),
                MAXIMUM,
            )
            .await;
        },
        |stream| recover(stream, rediscover),
    )
    .await
    .expect("every cut models power loss faithfully");
    assert_eq!(found.writes, 1, "manifest pointer publication changed");
    std::iter::once((None, found.clean))
        .chain(found.cuts.into_iter().map(|(cut, seen)| (Some(cut), seen)))
        .collect()
}

#[tokio::test]
async fn every_durable_cut_recovers_the_exact_admitted_manifest() {
    let cuts = cuts(false).await;
    for (cut, seen) in &cuts {
        assert_eq!(violation(seen), None, "{cut:?}: {seen:?}");
        assert_eq!(
            seen.pinned_before,
            *cut != Some(Cut::BeforeWrite(0)),
            "{cut:?}: the oracle must observe absence without creating a pointer"
        );
    }
}

#[tokio::test]
async fn negative_control_rediscovering_live_settings_loses_the_admitted_manifest() {
    let cuts = cuts(true).await;
    let mut caught = Vec::new();
    for (cut, seen) in &cuts {
        if let Some(reason) = violation(seen) {
            assert_eq!(reason, "recovery lost the admitted configuration");
            caught.push(*cut);
        }
    }
    assert_eq!(caught, [Some(Cut::BeforeWrite(0))]);
}
