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
    conformance::verify(
        &store.account(b"first")?,
        &store.account(b"second")?,
        "memory",
    )
    .await?;
    conformance::verify_medium(store.medium().as_ref(), "memory").await?;
    Ok(())
}

#[tokio::test]
async fn a_bounded_memory_medium_holds_its_budget_exactly() -> Result<(), MediumError> {
    let medium = MemoryMedium::bounded(4);
    assert!(medium.create("a", OwnedBytes::from(vec![1; 3])).await?);
    assert!(
        medium
            .create("b", OwnedBytes::from(vec![2; 2]))
            .await
            .is_err()
    );
    assert!(
        medium
            .replace("a", &[1; 3], OwnedBytes::from(vec![3; 5]))
            .await
            .is_err()
    );
    assert!(
        medium
            .replace("a", &[1; 3], OwnedBytes::from(vec![3; 4]))
            .await?
    );
    assert!(medium.remove_if("a", &[3; 4]).await?);
    assert!(medium.create("b", OwnedBytes::from(vec![2; 4])).await?);
    assert!(medium.remove("b").await?);
    assert!(medium.create("c", OwnedBytes::from(vec![4; 4])).await?);
    Ok(())
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
#[tokio::test]
async fn local_store_passes_the_shared_suite_and_reopens() -> Result<(), Box<dyn std::error::Error>>
{
    let root = tempfile::tempdir()?;
    let store = Store::new(crate::LocalMedium::open(root.path(), Default::default(), None).await?);
    conformance::verify(
        &store.account(b"first")?,
        &store.account(b"second")?,
        "local",
    )
    .await?;
    conformance::verify_medium(store.medium().as_ref(), "local").await?;
    let body = OwnedBytes::from(vec![9; 128]);
    let digest = Digest::of(&body);
    store
        .account(b"first")?
        .put(body.clone(), Vec::new())
        .await?;
    assert!(matches!(
        crate::LocalMedium::open(root.path(), Default::default(), None).await,
        Err(crate::local::LocalOpenError::AlreadyOwned)
    ));
    drop(store);
    let reopened =
        Store::new(crate::LocalMedium::open(root.path(), Default::default(), None).await?);
    let object = reopened
        .account(b"first")?
        .get(digest, ByteRange::FULL, 128)
        .await?;
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
    async fn read(
        &self,
        key: &str,
        range: Option<Range<u64>>,
    ) -> Result<Option<OwnedBytes>, MediumError> {
        if self.faults.load(Ordering::SeqCst) & 2 != 0 && key.contains("/r/") {
            return Err(MediumError::Failed(std::io::ErrorKind::TimedOut.into()));
        }
        self.inner.read(key, range).await
    }
    async fn remove(&self, key: &str) -> Result<bool, MediumError> {
        self.inner.remove(key).await
    }
    async fn replace(
        &self,
        key: &str,
        expected: &[u8],
        bytes: OwnedBytes,
    ) -> Result<bool, MediumError> {
        self.inner.replace(key, expected, bytes).await
    }
    async fn remove_if(&self, key: &str, expected: &[u8]) -> Result<bool, MediumError> {
        self.inner.remove_if(key, expected).await
    }
    async fn list(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, u64)>, MediumError> {
        self.inner.list(prefix, after, limit).await
    }
}

#[tokio::test]
async fn ambiguous_publication_is_unknown_and_retry_safe() -> Result<(), Box<dyn std::error::Error>>
{
    let store = Store::new(Faulty::default());
    let blobs = store.account(b"account")?;
    let body = OwnedBytes::from(vec![1; 64]);
    store.medium().faults.store(1, Ordering::SeqCst);
    assert_eq!(blobs.put(body.clone(), Vec::new()).await?, Put::Unknown);
    store.medium().faults.store(0, Ordering::SeqCst);
    assert!(matches!(
        blobs.put(body, Vec::new()).await?,
        Put::Created { .. }
    ));
    Ok(())
}

#[tokio::test]
async fn incomplete_root_scan_suspends_collection() -> Result<(), Box<dyn std::error::Error>> {
    let store = Store::new(Faulty::default());
    let blobs = store.account(b"account")?;
    let Put::Created { verified } = blobs.put(vec![2; 64].into(), Vec::new()).await? else {
        return Err("not created".into());
    };
    blobs
        .retain(&RootId::new(*b"root")?, vec![verified.digest])
        .await?;
    store.medium().faults.store(2, Ordering::SeqCst);
    for _ in 0..3 {
        assert_eq!(
            blobs.collect().await.map_err(|error| error.code),
            Err(ErrorCode::Suspended)
        );
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
    assert_eq!(
        blobs
            .head(verified.digest)
            .await
            .map_err(|error| error.code),
        Err(ErrorCode::NotFound)
    );
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

/// One shared medium that several [`Store`]s, standing for processes, open.
/// It parks one operation matching `pause` until released, to force one
/// interleaving. With `forgetful` its conditional writes ignore the bytes
/// they were given: a deleter's mark is never stored and its removal matches
/// whatever is there, the negative control for the delete protocol.
#[derive(Clone)]
struct Shared {
    inner: std::sync::Arc<dyn Medium>,
    pause: std::sync::Arc<std::sync::Mutex<Option<Pause>>>,
    forgetful: bool,
}

impl Default for Shared {
    fn default() -> Self {
        Self::over(MemoryMedium::default(), false)
    }
}

struct Pause {
    operation: &'static str,
    fragment: String,
    skip: usize,
    reached: std::sync::Arc<tokio::sync::Notify>,
    release: std::sync::Arc<tokio::sync::Notify>,
}

impl Shared {
    fn over(inner: impl Medium, forgetful: bool) -> Self {
        Self {
            inner: std::sync::Arc::new(inner),
            pause: Default::default(),
            forgetful,
        }
    }

    /// Parks the first `operation` on a key containing `fragment`, after it applies.
    fn park(
        &self,
        operation: &'static str,
        fragment: impl Into<String>,
    ) -> (
        std::sync::Arc<tokio::sync::Notify>,
        std::sync::Arc<tokio::sync::Notify>,
    ) {
        self.park_after(operation, fragment, 0)
    }

    /// Parks the match after `skip` earlier matches.
    fn park_after(
        &self,
        operation: &'static str,
        fragment: impl Into<String>,
        skip: usize,
    ) -> (
        std::sync::Arc<tokio::sync::Notify>,
        std::sync::Arc<tokio::sync::Notify>,
    ) {
        let reached = std::sync::Arc::new(tokio::sync::Notify::new());
        let release = std::sync::Arc::new(tokio::sync::Notify::new());
        *self.pause.lock().unwrap() = Some(Pause {
            operation,
            fragment: fragment.into(),
            skip,
            reached: reached.clone(),
            release: release.clone(),
        });
        (reached, release)
    }

    async fn after(&self, operation: &'static str, key: &str) {
        let parked = {
            let mut pause = self.pause.lock().unwrap();
            match &mut *pause {
                Some(p) if p.operation == operation && key.contains(&p.fragment) => {
                    if p.skip == 0 {
                        pause.take()
                    } else {
                        p.skip -= 1;
                        None
                    }
                }
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
    async fn read(
        &self,
        key: &str,
        range: Option<Range<u64>>,
    ) -> Result<Option<OwnedBytes>, MediumError> {
        self.inner.read(key, range).await
    }
    async fn remove(&self, key: &str) -> Result<bool, MediumError> {
        let removed = self.inner.remove(key).await;
        self.after("remove", key).await;
        removed
    }
    async fn replace(
        &self,
        key: &str,
        expected: &[u8],
        bytes: OwnedBytes,
    ) -> Result<bool, MediumError> {
        let replaced = if self.forgetful {
            Ok(self.inner.read(key, Some(0..0)).await?.is_some())
        } else {
            self.inner.replace(key, expected, bytes).await
        };
        self.after("replace", key).await;
        replaced
    }
    async fn remove_if(&self, key: &str, expected: &[u8]) -> Result<bool, MediumError> {
        let removed = if self.forgetful {
            self.inner.remove(key).await
        } else {
            self.inner.remove_if(key, expected).await
        };
        self.after("remove", key).await;
        removed
    }
    async fn list(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, u64)>, MediumError> {
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

/// The deleter marks the blob and passes its index check, then another
/// process references the blob before the deleter removes it. Returns whether
/// the writer succeeded and whether the blob it referenced survived.
async fn delete_then_reference(
    writer: Writer,
    forgetful: bool,
) -> Result<(bool, bool), Box<dyn std::error::Error>> {
    let medium = Shared::over(MemoryMedium::default(), forgetful);
    let (deleting, writing) = (Store::new(medium.clone()), Store::new(medium.clone()));
    let (deleting, writing) = (deleting.account(b"acct")?, writing.account(b"acct")?);
    let child = writing
        .put(vec![7; 32].into(), Vec::new())
        .await?
        .verified()
        .ok_or("unknown")?
        .digest;
    let (reached, release) = medium.park_after("list", "/i/", 1);
    let (deleted, written) = tokio::join!(deleting.delete(child), async {
        reached.notified().await;
        let written = match writer {
            Writer::Put => writing
                .put(vec![8; 32].into(), vec![child])
                .await
                .map(|put| put.verified().is_some()),
            Writer::Retain => writing
                .retain(&RootId::new(*b"root")?, vec![child])
                .await
                .map(|_| true),
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
async fn delete_never_breaks_a_reference_written_by_another_process()
-> Result<(), Box<dyn std::error::Error>> {
    for writer in [Writer::Put, Writer::Retain] {
        // The writer sees the deleter's mark and refuses; the delete completes.
        assert_eq!(delete_then_reference(writer, false).await?, (false, false));
        // Negative control: without a stored mark the same schedule commits a
        // reference to a deleted blob.
        assert_eq!(delete_then_reference(writer, true).await?, (true, false));
    }
    Ok(())
}

#[tokio::test]
async fn a_reference_indexed_first_refuses_the_delete() -> Result<(), Box<dyn std::error::Error>> {
    let medium = Shared::default();
    let (deleting, writing) = (
        Store::new(medium.clone()).account(b"acct")?,
        Store::new(medium.clone()).account(b"acct")?,
    );
    let child = writing
        .put(vec![7; 32].into(), Vec::new())
        .await?
        .verified()
        .ok_or("unknown")?
        .digest;
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
/// envelope, before it removed the payload. The re-put is a new version with
/// its own payload, so the deleter's remaining work never touches it.
#[tokio::test]
async fn a_reput_between_envelope_and_payload_removal_survives()
-> Result<(), Box<dyn std::error::Error>> {
    let medium = Shared::default();
    let (deleting, writing) = (
        Store::new(medium.clone()).account(b"acct")?,
        Store::new(medium.clone()).account(b"acct")?,
    );
    let body = OwnedBytes::from(vec![9; 32]);
    let digest = writing
        .put(body.clone(), Vec::new())
        .await?
        .verified()
        .ok_or("unknown")?
        .digest;
    let (reached, release) = medium.park("remove", "/e/");
    let (deleted, put) = tokio::join!(deleting.delete(digest), async {
        reached.notified().await;
        let put = writing.put(body.clone(), Vec::new()).await;
        release.notify_one();
        put
    });
    assert_eq!(deleted?, Delete::Deleted);
    assert!(matches!(put?, Put::Created { .. }));
    assert_eq!(
        writing.get(digest, ByteRange::FULL, 32).await?.body(),
        &body
    );
    Ok(())
}

#[tokio::test]
async fn delete_reads_only_its_inbound_index() -> Result<(), Box<dyn std::error::Error>> {
    let blobs = crate::MemoryObjects::memory();
    let child = blobs
        .put(vec![1; 8].into(), Vec::new())
        .await?
        .verified()
        .ok_or("unknown")?
        .digest;
    let parent = blobs
        .put(vec![2; 8].into(), vec![child])
        .await?
        .verified()
        .ok_or("unknown")?
        .digest;
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
async fn an_interrupted_holders_index_key_pins_its_child() -> Result<(), Box<dyn std::error::Error>>
{
    let store = Store::new(MemoryMedium::default());
    let blobs = store.account(b"acct")?;
    let child = blobs
        .put(vec![3; 8].into(), Vec::new())
        .await?
        .verified()
        .ok_or("unknown")?
        .digest;
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
async fn another_processs_collection_keeps_a_fresh_blob_for_one_full_pass()
-> Result<(), Box<dyn std::error::Error>> {
    let medium = MemoryMedium::default();
    let (writing, collecting) = (
        Store::new(medium.clone()).account(b"acct")?,
        Store::new(medium).account(b"acct")?,
    );
    let pinned = writing
        .put(vec![4; 8].into(), Vec::new())
        .await?
        .verified()
        .ok_or("unknown")?
        .digest;
    let loose = writing
        .put(vec![5; 8].into(), Vec::new())
        .await?
        .verified()
        .ok_or("unknown")?
        .digest;
    // The writer's admission is durable: the other process's first pass keeps both.
    assert_eq!(collecting.collect().await?.swept, 0);
    writing
        .retain(&RootId::new(*b"root")?, vec![pinned])
        .await?;
    assert_eq!(collecting.collect().await?.swept, 1);
    assert_eq!(collecting.head(pinned).await?.length, 8);
    assert_eq!(
        collecting.head(loose).await.map_err(|error| error.code),
        Err(ErrorCode::NotFound)
    );
    // A re-put refreshes the grace of bytes still present.
    let refreshed = writing.put(vec![4; 8].into(), Vec::new()).await?;
    assert!(matches!(refreshed, Put::AlreadyExists { .. }));
    Ok(())
}

/// One process releases a root while another retains an equal batch under
/// it. Returns the delete of the retained child once both finish.
async fn release_then_retain() -> Result<Delete, Box<dyn std::error::Error>> {
    let medium = Shared::default();
    let (releasing, retaining) = (
        Store::new(medium.clone()).account(b"acct")?,
        Store::new(medium.clone()).account(b"acct")?,
    );
    let child = retaining
        .put(vec![6; 32].into(), Vec::new())
        .await?
        .verified()
        .ok_or("unknown")?
        .digest;
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
async fn a_release_never_unpins_a_concurrent_equal_retain() -> Result<(), Box<dyn std::error::Error>>
{
    // The second retain owns its batch keys; the release removes only its own.
    // Negative control: with one shared key per batch, the release removed the
    // second retain's key and this delete was `Deleted`.
    assert_eq!(release_then_retain().await?, Delete::Referenced);
    Ok(())
}

#[tokio::test]
async fn a_delete_never_removes_a_writers_pending_reference()
-> Result<(), Box<dyn std::error::Error>> {
    // The deleter marks and passes its index check; a parent writer creates its
    // child key and pauses; the delete completes; a third process re-puts the child; the
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
    let child = writing
        .put(body.clone(), Vec::new())
        .await?
        .verified()
        .ok_or("unknown")?
        .digest;
    let (deleter_reached, deleter_release) =
        medium.park_after("list", format!("/i/{}/", child.to_hex()), 1);
    let deleted = tokio::spawn({
        let deleting = deleting.clone();
        async move { deleting.delete(child).await }
    });
    deleter_reached.notified().await;
    let (writer_reached, writer_release) =
        medium.park("create", format!("/i/{}/e", child.to_hex()));
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

/// A deleter marks and passes its checks, then stalls before removing. Two
/// collections run meanwhile, the second removing the blob; a fresh put of
/// the same bytes is retained. Returns the stalled delete and whether the
/// retained blob is still readable once it resumes.
async fn stalled_delete_then_reput(
    medium: impl Medium,
    forgetful: bool,
) -> Result<(Result<Delete, ErrorCode>, bool), Box<dyn std::error::Error>> {
    let medium = Shared::over(medium, forgetful);
    let (deleting, others) = (
        Store::new(medium.clone()).account(b"acct")?,
        Store::new(medium.clone()).account(b"acct")?,
    );
    let body = OwnedBytes::from(vec![12; 32]);
    let digest = others
        .put(body.clone(), Vec::new())
        .await?
        .verified()
        .ok_or("unknown")?
        .digest;
    let (reached, release) = medium.park_after("list", format!("/i/{}/", digest.to_hex()), 1);
    let stalled = tokio::spawn({
        let deleting = deleting.clone();
        async move { deleting.delete(digest).await }
    });
    reached.notified().await;
    others.collect().await?;
    others.collect().await?;
    assert_eq!(
        others.head(digest).await.map_err(|error| error.code).err(),
        Some(ErrorCode::NotFound)
    );
    assert!(matches!(
        others.put(body.clone(), Vec::new()).await?,
        Put::Created { .. }
    ));
    others.retain(&RootId::new(*b"root")?, vec![digest]).await?;
    release.notify_one();
    let deleted = stalled.await?.map_err(|error| error.code);
    let readable = others.get(digest, ByteRange::FULL, 32).await.is_ok();
    Ok((deleted, readable))
}

#[tokio::test]
async fn a_stalled_deleter_never_removes_a_reput_retained_blob()
-> Result<(), Box<dyn std::error::Error>> {
    // The deleter's removal matches only the envelope bearing its own mark,
    // which collection revived and then removed itself.
    assert_eq!(
        stalled_delete_then_reput(MemoryMedium::default(), false).await?,
        (Err(ErrorCode::OutcomeUnknown), true)
    );
    // Negative control: an unconditional removal erases the fresh blob.
    assert_eq!(
        stalled_delete_then_reput(MemoryMedium::default(), true).await?,
        (Ok(Delete::Deleted), false)
    );
    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    for forgetful in [false, true] {
        let root = tempfile::tempdir()?;
        let local = crate::LocalMedium::open(root.path(), Default::default(), None).await?;
        let (_, readable) = stalled_delete_then_reput(local, forgetful).await?;
        assert_eq!(readable, !forgetful);
    }
    Ok(())
}

#[tokio::test]
async fn collection_revives_a_kept_blob_whose_deleter_crashed()
-> Result<(), Box<dyn std::error::Error>> {
    // A deleter crashed after marking a blob that a fresh put still protects.
    let store = Store::new(MemoryMedium::default());
    let blobs = store.account(b"acct")?;
    let child = blobs
        .put(vec![11; 32].into(), Vec::new())
        .await?
        .verified()
        .ok_or("unknown")?
        .digest;
    let hex = child.to_hex();
    let key = format!("{}/e/{}/{hex}", Digest::of(b"acct").to_hex(), &hex[..2]);
    let live = store.medium().read(&key, None).await?.ok_or("absent")?;
    let mut marked = crate::blob::Envelope::decode(&live)?;
    marked.dying = Some([7; 16]);
    assert!(
        store
            .medium()
            .replace(&key, &live, marked.encode().into())
            .await?
    );
    // Marked, it refuses new holders, as a live deleter needs.
    let parent = blobs.put(vec![12; 32].into(), vec![child]).await;
    assert_eq!(
        parent.map_err(|error| error.code).err(),
        Some(ErrorCode::MissingReference)
    );
    assert_eq!(blobs.collect().await?.swept, 0);
    assert!(
        blobs
            .put(vec![12; 32].into(), vec![child])
            .await?
            .verified()
            .is_some()
    );
    Ok(())
}
