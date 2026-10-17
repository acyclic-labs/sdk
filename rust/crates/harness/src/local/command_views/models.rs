//! The new no-op branch races one real publisher across its snapshot handoff.
use super::*;
use acyclic_stream::{MemoryStream, StreamPath};

const _: fn(acyclic_stream::query_sync::SyncMutex<()>) -> loom::sync::Mutex<()> = |lock| lock;

fn scenario(broken: bool, publish_first: bool) {
    let volume = Arc::new(
        loom::future::block_on(Box::pin(TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("projection-model").unwrap(),
        )))
        .unwrap(),
    );
    let key = FileKey::parse(".ui/config/attention.json").unwrap();
    let query = Query::File(key.clone());
    let old = FileValue::new(key.clone(), b"old".to_vec());
    let empty = loom::future::block_on(Box::pin(volume.snapshot(&query, 1))).unwrap();
    loom::future::block_on(Box::pin(publish_changed(&volume, &empty, &old))).unwrap();
    let cut = loom::future::block_on(Box::pin(volume.snapshot(&query, 1))).unwrap();
    let next = FileValue::new(key, b"new".to_vec());
    let peer = Arc::clone(&volume);
    let file = next.clone();
    let generation = cut.generation;
    let writer = loom::thread::spawn(move || {
        loom::future::block_on(Box::pin(peer.append(generation, &file)))
    });
    // Cover both orders of the publisher and the entire no-op branch, which
    // performs no synchronization or I/O. Changed-file CAS is the existing
    // TaskVolume path, modeled separately in task_volume/publication/models.rs.
    let mut writer = Some(writer);
    if publish_first {
        writer.take().unwrap().join().unwrap().unwrap();
    }
    let result = if broken {
        // Wrong: rebase the old projection onto a later publication cut.
        let fresh = loom::future::block_on(Box::pin(volume.snapshot(&query, 1))).unwrap();
        loom::future::block_on(Box::pin(publish_changed(&volume, &fresh, &old)))
    } else {
        loom::future::block_on(Box::pin(publish_changed(&volume, &cut, &old)))
    };
    if let Some(writer) = writer {
        writer.join().unwrap().unwrap();
    }
    result.unwrap();
    let after = loom::future::block_on(Box::pin(volume.snapshot(&query, 1))).unwrap();
    assert_eq!(
        after.files.as_slice(),
        std::slice::from_ref(&next),
        "unchanged projection overwrote a concurrent change"
    );
    assert_eq!(after.generation.position, cut.generation.position + 1);
    assert!(matches!(
        loom::future::block_on(Box::pin(publish_changed(
            &volume,
            &cut,
            &FileValue::new(old.key, b"stale".to_vec())
        ))),
        Err(crate::task_volume::Error::Conflict(_))
    ));
}

#[test]
fn unchanged_projections_cannot_overwrite_a_concurrent_change() {
    for publish_first in [false, true] {
        loom::model(move || scenario(false, publish_first));
    }
    let error = std::panic::catch_unwind(|| loom::model(|| scenario(true, true)))
        .expect_err("stale-state retry was accepted");
    let message = error
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| error.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        message.contains("unchanged projection overwrote a concurrent change"),
        "{message}"
    );
}
