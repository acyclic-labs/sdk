use crate::{
    Blobs, ByteRange, Delete, Digest, Medium, MediumError, MemoryMedium, Put, RootId, Store,
    conformance, wire::ErrorCode,
};
use acyclic_stream::OwnedBytes;
use std::ops::Range;
use std::sync::atomic::{AtomicU8, Ordering};

#[tokio::test]
async fn memory_store_passes_the_shared_suite() -> Result<(), Box<dyn std::error::Error>> {
    let store = Store::new(MemoryMedium::default());
    conformance::verify(&store.account(b"first")?, &store.account(b"second")?, "memory").await?;
    Ok(())
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
#[tokio::test]
async fn local_store_passes_the_shared_suite_and_reopens() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let store = Store::new(crate::LocalMedium::open(root.path(), Default::default(), None).await?);
    conformance::verify(&store.account(b"first")?, &store.account(b"second")?, "local").await?;
    let body = OwnedBytes::from(vec![9; 128]);
    let digest = Digest::of(&body);
    store.account(b"first")?.put(body.clone(), Vec::new()).await?;
    assert!(matches!(
        crate::LocalMedium::open(root.path(), Default::default(), None).await,
        Err(crate::local::LocalOpenError::AlreadyOwned)
    ));
    drop(store);
    let reopened = Store::new(crate::LocalMedium::open(root.path(), Default::default(), None).await?);
    let object = reopened.account(b"first")?.get(digest, ByteRange::FULL, 128).await?;
    assert_eq!(object.body(), &body);
    Ok(())
}

/// Fails selected operations: bit 0 makes creates ambiguous, bit 1 fails reads of root batches.
#[derive(Default)]
struct Faulty {
    inner: MemoryMedium,
    faults: AtomicU8,
}

#[async_trait::async_trait]
impl Medium for Faulty {
    async fn create(&self, key: &str, bytes: OwnedBytes) -> Result<bool, MediumError> {
        if self.faults.load(Ordering::SeqCst) & 1 != 0 {
            return Err(MediumError::Ambiguous(std::io::ErrorKind::TimedOut.into()));
        }
        self.inner.create(key, bytes).await
    }
    async fn read(&self, key: &str, range: Option<Range<u64>>) -> Result<Option<OwnedBytes>, MediumError> {
        if self.faults.load(Ordering::SeqCst) & 2 != 0 && key.contains("/r/") {
            return Err(MediumError::Failed(std::io::ErrorKind::TimedOut.into()));
        }
        self.inner.read(key, range).await
    }
    async fn remove(&self, key: &str) -> Result<bool, MediumError> {
        self.inner.remove(key).await
    }
    async fn list(&self, prefix: &str, after: Option<&str>, limit: usize) -> Result<Vec<(String, u64)>, MediumError> {
        self.inner.list(prefix, after, limit).await
    }
}

#[tokio::test]
async fn ambiguous_publication_is_unknown_and_retry_safe() -> Result<(), Box<dyn std::error::Error>> {
    let store = Store::new(Faulty::default());
    let blobs = store.account(b"account")?;
    let body = OwnedBytes::from(vec![1; 64]);
    store.medium().faults.store(1, Ordering::SeqCst);
    assert_eq!(blobs.put(body.clone(), Vec::new()).await?, Put::Unknown);
    store.medium().faults.store(0, Ordering::SeqCst);
    assert!(matches!(blobs.put(body, Vec::new()).await?, Put::Created { .. }));
    Ok(())
}

#[tokio::test]
async fn incomplete_root_scan_suspends_collection() -> Result<(), Box<dyn std::error::Error>> {
    let store = Store::new(Faulty::default());
    let blobs = store.account(b"account")?;
    let Put::Created { verified } = blobs.put(vec![2; 64].into(), Vec::new()).await? else {
        return Err("not created".into());
    };
    blobs.retain(&RootId::new(*b"root")?, vec![verified.digest]).await?;
    store.medium().faults.store(2, Ordering::SeqCst);
    for _ in 0..3 {
        assert_eq!(blobs.collect().await.map_err(|error| error.code), Err(ErrorCode::Suspended));
    }
    // Delete consults the inbound index, never the root scan.
    assert_eq!(blobs.delete(verified.digest).await?, Delete::Referenced);
    store.medium().faults.store(0, Ordering::SeqCst);
    blobs.collect().await?;
    blobs.collect().await?;
    assert_eq!(blobs.head(verified.digest).await?.length, 64);
    assert_eq!(blobs.delete(verified.digest).await?, Delete::Referenced);
    Ok(())
}

#[tokio::test]
async fn unrooted_blobs_survive_exactly_one_collection() -> Result<(), Box<dyn std::error::Error>> {
    let blobs = crate::MemoryObjects::memory();
    let Put::Created { verified } = blobs.put(vec![3; 8].into(), Vec::new()).await? else {
        return Err("not created".into());
    };
    assert_eq!(blobs.collect().await?.swept, 0);
    assert_eq!(blobs.head(verified.digest).await?.length, 8);
    assert_eq!(blobs.collect().await?.swept, 1);
    assert_eq!(blobs.head(verified.digest).await.map_err(|error| error.code), Err(ErrorCode::NotFound));
    Ok(())
}

#[test]
fn manifest_and_envelope_encodings_round_trip() -> Result<(), crate::Error> {
    let manifest = crate::Manifest {
        parts: vec![crate::Part {
            digest: Digest::of(b"a"),
            length: 1,
        }],
        edges: vec![Digest::of(b"b")],
    };
    assert_eq!(crate::Manifest::decode(&manifest.encode())?, manifest);
    let mut trailing = manifest.encode();
    trailing.push(0);
    assert!(crate::Manifest::decode(&trailing).is_err());
    assert_eq!(ByteRange::FULL.resolve(0)?, 0..0);
    assert!(ByteRange::new(1, 1).resolve(4).is_err());
    Ok(())
}

/// One shared medium that two [`Store`]s, standing for two processes, open.
/// It parks the first operation matching `pause` until released, to force one
/// interleaving, and with `hide_tombstones` lists no tombstone: the negative
/// control, a store without the delete protocol.
#[derive(Clone, Default)]
struct Shared {
    inner: MemoryMedium,
    pause: std::sync::Arc<std::sync::Mutex<Option<Pause>>>,
    hide_tombstones: bool,
}

struct Pause {
    operation: &'static str,
    fragment: String,
    reached: std::sync::Arc<tokio::sync::Notify>,
    release: std::sync::Arc<tokio::sync::Notify>,
}

impl Shared {
    /// Parks the first `operation` on a key containing `fragment`, after it applies.
    fn park(&self, operation: &'static str, fragment: impl Into<String>) -> (std::sync::Arc<tokio::sync::Notify>, std::sync::Arc<tokio::sync::Notify>) {
        let reached = std::sync::Arc::new(tokio::sync::Notify::new());
        let release = std::sync::Arc::new(tokio::sync::Notify::new());
        *self.pause.lock().unwrap() = Some(Pause {
            operation,
            fragment: fragment.into(),
            reached: reached.clone(),
            release: release.clone(),
        });
        (reached, release)
    }

    async fn after(&self, operation: &'static str, key: &str) {
        let parked = {
            let mut pause = self.pause.lock().unwrap();
            match &*pause {
                Some(p) if p.operation == operation && key.contains(&p.fragment) => pause.take(),
                _ => None,
            }
        };
        if let Some(pause) = parked {
            pause.reached.notify_one();
            pause.release.notified().await;
        }
    }
}

#[async_trait::async_trait]
impl Medium for Shared {
    async fn create(&self, key: &str, bytes: OwnedBytes) -> Result<bool, MediumError> {
        let created = self.inner.create(key, bytes).await;
        self.after("create", key).await;
        created
    }
    async fn read(&self, key: &str, range: Option<Range<u64>>) -> Result<Option<OwnedBytes>, MediumError> {
        self.inner.read(key, range).await
    }
    async fn remove(&self, key: &str) -> Result<bool, MediumError> {
        let removed = self.inner.remove(key).await;
        self.after("remove", key).await;
        removed
    }
    async fn list(&self, prefix: &str, after: Option<&str>, limit: usize) -> Result<Vec<(String, u64)>, MediumError> {
        if self.hide_tombstones && prefix.contains("/d/") {
            return Ok(Vec::new());
        }
        let listed = self.inner.list(prefix, after, limit).await;
        self.after("list", prefix).await;
        listed
    }
}

/// Which referencing write races the delete.
#[derive(Clone, Copy)]
enum Writer {
    Put,
    Retain,
}

/// The deleter passes its index check, then another process references the
/// blob before the deleter removes it. Returns whether the writer succeeded
/// and whether the blob it referenced survived.
async fn delete_then_reference(writer: Writer, hide_tombstones: bool) -> Result<(bool, bool), Box<dyn std::error::Error>> {
    let medium = Shared {
        hide_tombstones,
        ..Shared::default()
    };
    let (deleting, writing) = (Store::new(medium.clone()), Store::new(medium.clone()));
    let (deleting, writing) = (deleting.account(b"acct")?, writing.account(b"acct")?);
    let child = writing.put(vec![7; 32].into(), Vec::new()).await?.verified().ok_or("unknown")?.digest;
    let (reached, release) = medium.park("list", "/i/");
    let (deleted, written) = tokio::join!(deleting.delete(child), async {
        reached.notified().await;
        let written = match writer {
            Writer::Put => writing.put(vec![8; 32].into(), vec![child]).await.map(|put| put.verified().is_some()),
            Writer::Retain => writing.retain(&RootId::new(*b"root")?, vec![child]).await.map(|_| true),
        };
        release.notify_one();
        written
    });
    let written = match written {
        Ok(written) => written,
        Err(error) if error.code == ErrorCode::MissingReference => false,
        Err(error) => return Err(error.into()),
    };
    let deleted = deleted?;
    let survived = writing.head(child).await.is_ok();
    assert_eq!(deleted == Delete::Deleted, !survived);
    Ok((written, survived))
}

#[tokio::test]
async fn delete_never_breaks_a_reference_written_by_another_process() -> Result<(), Box<dyn std::error::Error>> {
    for writer in [Writer::Put, Writer::Retain] {
        // The writer sees the tombstone and refuses; the delete completes.
        assert_eq!(delete_then_reference(writer, false).await?, (false, false));
        // Negative control: without tombstones the same schedule commits a
        // reference to a deleted blob.
        assert_eq!(delete_then_reference(writer, true).await?, (true, false));
    }
    Ok(())
}

#[tokio::test]
async fn a_reference_indexed_first_refuses_the_delete() -> Result<(), Box<dyn std::error::Error>> {
    let medium = Shared::default();
    let (deleting, writing) = (Store::new(medium.clone()).account(b"acct")?, Store::new(medium.clone()).account(b"acct")?);
    let child = writing.put(vec![7; 32].into(), Vec::new()).await?.verified().ok_or("unknown")?.digest;
    let (reached, release) = medium.park("create", "/i/");
    let (written, deleted) = tokio::join!(writing.put(vec![8; 32].into(), vec![child]), async {
        reached.notified().await;
        let deleted = deleting.delete(child).await;
        release.notify_one();
        deleted
    });
    assert!(matches!(written?, Put::Created { .. }));
    assert_eq!(deleted?, Delete::Referenced);
    assert_eq!(writing.head(child).await?.length, 32);
    Ok(())
}

/// Another process puts the same bytes after the deleter removed the
/// envelope, before it removed the payload. Returns that put and whether the
/// bytes are readable once both finish.
async fn delete_then_same_bytes(hide_tombstones: bool) -> Result<(Put, bool), Box<dyn std::error::Error>> {
    let medium = Shared {
        hide_tombstones,
        ..Shared::default()
    };
    let (deleting, writing) = (Store::new(medium.clone()).account(b"acct")?, Store::new(medium.clone()).account(b"acct")?);
    let body = OwnedBytes::from(vec![9; 32]);
    let digest = writing.put(body.clone(), Vec::new()).await?.verified().ok_or("unknown")?.digest;
    let (reached, release) = medium.park("remove", "/e/");
    let (deleted, put) = tokio::join!(deleting.delete(digest), async {
        reached.notified().await;
        let put = writing.put(body.clone(), Vec::new()).await;
        release.notify_one();
        put
    });
    assert_eq!(deleted?, Delete::Deleted);
    Ok((put?, writing.get(digest, ByteRange::FULL, 32).await.is_ok()))
}

#[tokio::test]
async fn bytes_being_deleted_are_never_reported_durable() -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(delete_then_same_bytes(false).await?, (Put::Unknown, false));
    // Negative control: without tombstones the put reports bytes the delete then removes.
    let (put, readable) = delete_then_same_bytes(true).await?;
    assert!(put.verified().is_some() && !readable);
    Ok(())
}

#[tokio::test]
async fn delete_reads_only_its_inbound_index() -> Result<(), Box<dyn std::error::Error>> {
    let blobs = crate::MemoryObjects::memory();
    let child = blobs.put(vec![1; 8].into(), Vec::new()).await?.verified().ok_or("unknown")?.digest;
    let parent = blobs.put(vec![2; 8].into(), vec![child]).await?.verified().ok_or("unknown")?.digest;
    let root = RootId::new(*b"root")?;
    blobs.retain(&root, vec![child]).await?;
    assert_eq!(blobs.delete(child).await?, Delete::Referenced);
    assert_eq!(blobs.delete(parent).await?, Delete::Deleted);
    assert_eq!(blobs.delete(child).await?, Delete::Referenced);
    assert!(blobs.release(&root).await?);
    assert_eq!(blobs.delete(child).await?, Delete::Deleted);
    Ok(())
}

#[tokio::test]
async fn an_interrupted_holders_index_key_pins_its_child() -> Result<(), Box<dyn std::error::Error>> {
    let store = Store::new(MemoryMedium::default());
    let blobs = store.account(b"acct")?;
    let child = blobs.put(vec![3; 8].into(), Vec::new()).await?.verified().ok_or("unknown")?.digest;
    let stale = format!(
        "{}/i/{}/e{}",
        Digest::of(b"acct").to_hex(),
        child.to_hex(),
        Digest::of(b"interrupted put").to_hex()
    );
    store.medium().create(&stale, OwnedBytes::new()).await?;
    // The holder may yet commit on retry, so the key is honoured: it leaks, never breaks.
    for _ in 0..3 {
        blobs.collect().await?;
    }
    assert_eq!(blobs.delete(child).await?, Delete::Referenced);
    assert_eq!(blobs.head(child).await?.length, 8);
    Ok(())
}

#[tokio::test]
async fn another_processs_collection_keeps_a_fresh_blob_for_one_full_pass() -> Result<(), Box<dyn std::error::Error>> {
    let medium = MemoryMedium::default();
    let (writing, collecting) = (Store::new(medium.clone()).account(b"acct")?, Store::new(medium).account(b"acct")?);
    let pinned = writing.put(vec![4; 8].into(), Vec::new()).await?.verified().ok_or("unknown")?.digest;
    let loose = writing.put(vec![5; 8].into(), Vec::new()).await?.verified().ok_or("unknown")?.digest;
    // The writer's admission is durable: the other process's first pass keeps both.
    assert_eq!(collecting.collect().await?.swept, 0);
    writing.retain(&RootId::new(*b"root")?, vec![pinned]).await?;
    assert_eq!(collecting.collect().await?.swept, 1);
    assert_eq!(collecting.head(pinned).await?.length, 8);
    assert_eq!(collecting.head(loose).await.map_err(|error| error.code), Err(ErrorCode::NotFound));
    // A re-put refreshes the grace of bytes still present.
    let refreshed = writing.put(vec![4; 8].into(), Vec::new()).await?;
    assert!(matches!(refreshed, Put::AlreadyExists { .. }));
    Ok(())
}

/// One process releases a root while another retains an equal batch under
/// it. Returns the delete of the retained child once both finish.
async fn release_then_retain() -> Result<Delete, Box<dyn std::error::Error>> {
    let medium = Shared::default();
    let (releasing, retaining) = (Store::new(medium.clone()).account(b"acct")?, Store::new(medium.clone()).account(b"acct")?);
    let child = retaining.put(vec![6; 32].into(), Vec::new()).await?.verified().ok_or("unknown")?.digest;
    let root = RootId::new(*b"root")?;
    retaining.retain(&root, vec![child]).await?;
    let (reached, release) = medium.park("remove", "/r/");
    let (released, retained) = tokio::join!(releasing.release(&root), async {
        reached.notified().await;
        let retained = retaining.retain(&root, vec![child]).await;
        release.notify_one();
        retained
    });
    assert!(released?);
    retained?;
    Ok(retaining.delete(child).await?)
}

#[tokio::test]
async fn a_release_never_unpins_a_concurrent_equal_retain() -> Result<(), Box<dyn std::error::Error>> {
    // The second retain owns its batch keys; the release removes only its own.
    // Negative control: with one shared key per batch, the release removed the
    // second retain's key and this delete was `Deleted`.
    assert_eq!(release_then_retain().await?, Delete::Referenced);
    Ok(())
}

#[tokio::test]
async fn a_delete_never_removes_a_writers_pending_reference() -> Result<(), Box<dyn std::error::Error>> {
    // The deleter passes its index check; a parent writer creates its child key
    // and pauses; the delete completes; a third process re-puts the child; the
    // writer resumes and commits. Its key must still pin the child.
    // Negative control: a deleter that cleared inbound keys left the committed
    // parent unpinning its child, and this delete was `Deleted`.
    let medium = Shared::default();
    let stores = (
        Store::new(medium.clone()).account(b"acct")?,
        Store::new(medium.clone()).account(b"acct")?,
        Store::new(medium.clone()).account(b"acct")?,
    );
    let (deleting, writing, reputting) = stores;
    let body = OwnedBytes::from(vec![7; 32]);
    let child = writing.put(body.clone(), Vec::new()).await?.verified().ok_or("unknown")?.digest;
    let (deleter_reached, deleter_release) = medium.park("list", format!("/i/{}/", child.to_hex()));
    let deleted = tokio::spawn({
        let deleting = deleting.clone();
        async move { deleting.delete(child).await }
    });
    deleter_reached.notified().await;
    let (writer_reached, writer_release) = medium.park("create", format!("/i/{}/e", child.to_hex()));
    let written = tokio::spawn({
        let writing = writing.clone();
        async move { writing.put(vec![8; 32].into(), vec![child]).await }
    });
    writer_reached.notified().await;
    deleter_release.notify_one();
    assert_eq!(deleted.await??, Delete::Deleted);
    assert!(reputting.put(body, Vec::new()).await?.verified().is_some());
    writer_release.notify_one();
    assert!(written.await??.verified().is_some());
    assert_eq!(deleting.delete(child).await?, Delete::Referenced);
    assert_eq!(writing.head(child).await?.length, 32);
    Ok(())
}

#[tokio::test]
async fn one_delete_never_clears_another_deleters_tombstone() -> Result<(), Box<dyn std::error::Error>> {
    // Two deletes pass their checks; the first pauses before removing; the
    // second completes. A same-bytes put and a referencing put must then
    // refuse, because the first is still removing.
    // Negative control: a deleter that cleared every tombstone let both
    // commit, and the first then removed bytes a live parent referenced.
    let medium = Shared::default();
    let (first, second, writing) = (
        Store::new(medium.clone()).account(b"acct")?,
        Store::new(medium.clone()).account(b"acct")?,
        Store::new(medium.clone()).account(b"acct")?,
    );
    let body = OwnedBytes::from(vec![9; 32]);
    let digest = writing.put(body.clone(), Vec::new()).await?.verified().ok_or("unknown")?.digest;
    let (reached, release) = medium.park("list", format!("/i/{}/", digest.to_hex()));
    let paused = tokio::spawn({
        let first = first.clone();
        async move { first.delete(digest).await }
    });
    reached.notified().await;
    assert_eq!(second.delete(digest).await?, Delete::Deleted);
    let reput = writing.put(body, Vec::new()).await?;
    let parent = writing.put(vec![10; 32].into(), vec![digest]).await;
    release.notify_one();
    paused.await??;
    assert_eq!(reput, Put::Unknown);
    assert_eq!(parent.map_err(|error| error.code).err(), Some(ErrorCode::MissingReference));
    Ok(())
}

#[tokio::test]
async fn collection_clears_an_abandoned_tombstone_after_its_fence() -> Result<(), Box<dyn std::error::Error>> {
    // A deleter crashed holding its tombstone on bytes now stored nowhere.
    // Negative control: before collection cleared flags, every later put of
    // these bytes stayed `Unknown`.
    let store = Store::new(MemoryMedium::default());
    let blobs = store.account(b"acct")?;
    let body = OwnedBytes::from(vec![11; 32]);
    let digest = Digest::of(&body);
    let abandoned = format!("{}/d/{}/{:020}.crashed", Digest::of(b"acct").to_hex(), digest.to_hex(), 0);
    store.medium().create(&abandoned, OwnedBytes::new()).await?;
    assert_eq!(blobs.put(body.clone(), Vec::new()).await?, Put::Unknown);
    // The first pass is fenced at the tombstone's own revision: a deleter may
    // still be live, so it stays.
    blobs.collect().await?;
    assert_eq!(blobs.put(body.clone(), Vec::new()).await?, Put::Unknown);
    blobs.collect().await?;
    assert!(matches!(blobs.put(body, Vec::new()).await?, Put::Created { .. }));
    Ok(())
}
