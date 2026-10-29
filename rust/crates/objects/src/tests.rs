use crate::{
    Blobs, ByteRange, Delete, Digest, Medium, MediumError, MemoryMedium, Put, RootId, Store,
    conformance, wire::ErrorCode,
};
use acyclic_stream::OwnedBytes;
use std::ops::Range;
use std::sync::atomic::{AtomicU8, Ordering};

pub(crate) mod availability;

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
mod read_admission;

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

#[tokio::test]
async fn a_full_bounded_account_can_still_delete() -> Result<(), Box<dyn std::error::Error>> {
    async fn stored(medium: &MemoryMedium) -> Result<u64, MediumError> {
        Ok(medium
            .list("", None, usize::MAX)
            .await?
            .iter()
            .map(|(_, bytes)| bytes)
            .sum())
    }
    let probe = Store::new(MemoryMedium::default());
    probe
        .account(b"acct")?
        .put(vec![5; 8].into(), Vec::new())
        .await?;
    let full = stored(probe.medium()).await?;
    // Exactly full after one put: the delete mark must not need more room.
    let store = Store::new(MemoryMedium::bounded(full));
    let blobs = store.account(b"acct")?;
    let digest = blobs
        .put(vec![5; 8].into(), Vec::new())
        .await?
        .verified()
        .ok_or("unknown")?
        .digest;
    assert_eq!(stored(store.medium()).await?, full);
    assert_eq!(blobs.delete(digest).await?, Delete::Deleted);
    assert!(stored(store.medium()).await? < full);
    Ok(())
}

#[tokio::test]
async fn releases_and_sweeps_commit_revisions() -> Result<(), Box<dyn std::error::Error>> {
    let blobs = crate::MemoryObjects::memory();
    let put = blobs.put(OwnedBytes::from(vec![3; 8]), Vec::new()).await?;
    let digest = put.verified().ok_or("a put verifies")?.digest;
    let root = RootId::new(b"root".to_vec())?;
    let retained = blobs.retain(&root, vec![digest]).await?;
    let retained = retained.revision.ok_or("a retain commits")?;
    let released = blobs.release(&root).await?;
    assert!(released.existed);
    let released = released.revision.ok_or("a release commits")?;
    assert!(released.index > retained.index);
    // Negative control: releasing nothing commits nothing.
    let again = blobs.release(&root).await?;
    assert!(!again.existed);
    assert_eq!(again.revision.as_ref(), Some(&released));

    let mut passes = Vec::new();
    for _ in 0..4 {
        passes.push(blobs.collect().await?);
    }
    let sweep = passes
        .iter()
        .position(|pass| pass.swept > 0)
        .filter(|sweep| *sweep < 3)
        .ok_or("an unretained blob is swept")?;
    let swept = passes[sweep].revision.clone().ok_or("a sweep commits")?;
    assert!(swept.index > released.index);
    // Negative control: a pass that sweeps nothing commits nothing.
    assert_eq!(passes[3].swept, 0);
    assert_eq!(passes[3].revision.as_ref(), Some(&swept));
    Ok(())
}

#[tokio::test]
async fn stores_sharing_a_medium_serve_each_others_revisions()
-> Result<(), Box<dyn std::error::Error>> {
    use acyclic_stream::{RevisionCut, StreamError};
    let medium = MemoryMedium::default();
    let first = Store::new(medium.clone()).account(b"shared")?;
    let second = Store::new(medium).account(b"shared")?;
    first.flush(None).await?;
    let applied = first.revisions().await?.ok_or("stores issue revisions")?;
    let put = second.put(OwnedBytes::from(vec![4; 8]), Vec::new()).await?;
    let written = put
        .verified()
        .and_then(|verified| verified.revision.clone())
        .ok_or("a put commits a revision")?;
    // Negative control: the first store's own authority cannot wait for it.
    assert!(matches!(
        applied.wait(&RevisionCut::of(written.clone())).await,
        Err(StreamError::RevisionForeign)
    ));
    assert!(
        first
            .admitted(RevisionCut::of(written.clone()))
            .await?
            .is_empty()
    );
    // A run no store claimed is never admitted.
    let unclaimed = acyclic_stream::Revision {
        scope: acyclic_stream::RevisionScope::new(
            [&written.scope.as_bytes()[..32], &9_u64.to_be_bytes()].concat(),
        ),
        ..written.clone()
    };
    assert!(!first.admitted(RevisionCut::of(unclaimed)).await?.is_empty());
    // This run's own unissued index still waits on its exact applied index.
    let own = applied.at(applied.current().index + 1);
    let kept = first.admitted(RevisionCut::of(own)).await?;
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), applied.wait(&kept))
            .await
            .is_err()
    );
    Ok(())
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
#[tokio::test]
async fn a_reopened_store_serves_tokens_from_before_the_restart()
-> Result<(), Box<dyn std::error::Error>> {
    use acyclic_stream::RevisionCut;
    let root = tempfile::tempdir()?;
    let open = async |root: &std::path::Path| -> Result<_, Box<dyn std::error::Error>> {
        let store = Store::new(crate::LocalMedium::open(root, Default::default(), None).await?);
        Ok(store.account(b"restart")?)
    };
    let committed = |put: Put| {
        put.verified()
            .and_then(|verified| verified.revision.clone())
    };

    let before = open(root.path()).await?;
    let written = committed(before.put(OwnedBytes::from(vec![1; 8]), Vec::new()).await?)
        .ok_or("a put commits a revision")?;
    drop(before);

    let reopened = open(root.path()).await?;
    reopened.flush(None).await?;
    assert!(
        reopened
            .admitted(RevisionCut::of(written.clone()))
            .await?
            .is_empty()
    );
    // The restarted run keeps the incarnation under its own run's scope.
    let later = committed(
        reopened
            .put(OwnedBytes::from(vec![2; 8]), Vec::new())
            .await?,
    )
    .ok_or("a put commits a revision")?;
    assert_eq!(later.incarnation, written.incarnation);
    assert_ne!(later.scope, written.scope);
    drop(reopened);

    // Negative control: a wiped store is a new incarnation.
    std::fs::remove_dir_all(root.path())?;
    std::fs::create_dir(root.path())?;
    let wiped = open(root.path()).await?;
    wiped.flush(None).await?;
    assert_eq!(
        wiped
            .admitted(RevisionCut::of(written))
            .await
            .map_err(|error| error.code),
        Err(ErrorCode::RevisionForeign)
    );
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
    async fn flush(&self, keys: &[String]) -> Result<(), MediumError> {
        self.inner.flush(keys).await
    }
    fn volatile(&self) -> bool {
        self.inner.volatile()
    }
    fn recovered(&self) -> bool {
        self.inner.recovered()
    }
    fn settle(&self) {
        self.inner.settle();
    }
}

#[tokio::test]
async fn ambiguous_publication_is_unknown_and_retry_safe() -> Result<(), Box<dyn std::error::Error>>
{
    let store = Store::new(Faulty::default());
    let blobs = store.account(b"account")?;
    let body = OwnedBytes::from(vec![1; 64]);
    // The fault lands on the publication, not on opening the revisions.
    blobs.flush(None).await?;
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
async fn an_unrooted_blob_survives_the_pass_after_the_one_finding_it()
-> Result<(), Box<dyn std::error::Error>> {
    let blobs = crate::MemoryObjects::memory();
    let Put::Created { verified } = blobs.put(vec![3; 8].into(), Vec::new()).await? else {
        return Err("not created".into());
    };
    for _ in 0..2 {
        assert_eq!(blobs.collect().await?.swept, 0);
        assert_eq!(blobs.head(verified.digest).await?.length, 8);
    }
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
/// interleaving. With `forgetful` the delete protocol's conditional writes
/// ignore the bytes they were given: a deleter's mark is never stored and its
/// envelope removal matches whatever is there, the negative control for the
/// delete protocol. Creates of keys containing `ambiguous` are lost with an
/// unknown outcome.
#[derive(Clone)]
struct Shared {
    inner: std::sync::Arc<dyn Medium>,
    pause: std::sync::Arc<std::sync::Mutex<Option<Pause>>>,
    forgetful: bool,
    ambiguous: std::sync::Arc<std::sync::Mutex<Option<&'static str>>>,
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
            ambiguous: Default::default(),
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

    /// Whether `forgetful` drops this replace: a deleter's mark.
    fn forgets_mark(&self, bytes: &[u8]) -> bool {
        self.forgetful
            && crate::blob::Envelope::decode(bytes).is_ok_and(|envelope| envelope.dying.is_some())
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
        let lost = self
            .ambiguous
            .lock()
            .unwrap()
            .is_some_and(|fragment| key.contains(fragment));
        let created = if lost {
            Err(MediumError::Ambiguous(std::io::ErrorKind::TimedOut.into()))
        } else {
            self.inner.create(key, bytes).await
        };
        self.after("create", key).await;
        created
    }
    async fn read(
        &self,
        key: &str,
        range: Option<Range<u64>>,
    ) -> Result<Option<OwnedBytes>, MediumError> {
        let read = self.inner.read(key, range).await;
        self.after("read", key).await;
        read
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
        let replaced = if self.forgets_mark(&bytes) {
            Ok(self.inner.read(key, Some(0..0)).await?.is_some())
        } else {
            self.inner.replace(key, expected, bytes).await
        };
        self.after("replace", key).await;
        replaced
    }
    async fn remove_if(&self, key: &str, expected: &[u8]) -> Result<bool, MediumError> {
        let removed = if self.forgetful && key.contains("/e/") {
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
    async fn flush(&self, keys: &[String]) -> Result<(), MediumError> {
        self.inner.flush(keys).await
    }
    fn volatile(&self) -> bool {
        self.inner.volatile()
    }
    fn recovered(&self) -> bool {
        self.inner.recovered()
    }
    fn settle(&self) {
        self.inner.settle();
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
    // The writer sees the deleter's mark and refuses, and the delete
    // completes: its last check precedes the writer's index key.
    for writer in [Writer::Put, Writer::Retain] {
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
    assert!(blobs.release(&root).await?.existed);
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
    // The other process's first pass finds both unreferenced and its next
    // keeps them, so the writer has one whole pass to commit.
    assert_eq!(collecting.collect().await?.swept, 0);
    writing
        .retain(&RootId::new(*b"root")?, vec![pinned])
        .await?;
    assert_eq!(collecting.collect().await?.swept, 0);
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
    assert!(released?.existed);
    retained?;
    Ok(retaining.delete(child).await?.outcome)
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
    for _ in 0..3 {
        others.collect().await?;
    }
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
    let deleted = stalled
        .await?
        .map(|deletion| deletion.outcome)
        .map_err(|error| error.code);
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
    // Marked, it refuses new holders, as a live deleter needs; the refused
    // parent's index key still holds it.
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

/// Whether any key of `digest` under `fragment` is stored.
async fn stored_under(
    medium: &Shared,
    digest: Digest,
    fragment: &str,
) -> Result<bool, MediumError> {
    let hex = digest.to_hex();
    Ok(Medium::list(
        medium,
        &format!("{}/", Digest::of(b"account").to_hex()),
        None,
        100_000,
    )
    .await?
    .iter()
    .any(|(key, _)| key.contains(fragment) && key.contains(&hex)))
}

/// Where a put stalls: after creating its envelope, and after writing its
/// payload.
const PUT_STALLS: [(&str, &str, usize); 2] = [("create", "/e/", 0), ("create", "/p/", 0)];

/// A put paused at `stall` while an independent store collects `passes`
/// times, then committed by a retain. The commit either references a blob
/// GET verifies or fails; it never commits a lost blob. Whether it succeeded.
async fn stalled_put(
    medium: Shared,
    stall: (&'static str, &'static str, usize),
    passes: usize,
    fill: u8,
) -> Result<bool, Box<dyn std::error::Error>> {
    let writer = Store::new(medium.clone()).account(b"account")?;
    let collector = Store::new(medium.clone()).account(b"account")?;
    let body = OwnedBytes::from(vec![fill; 64]);
    let digest = Digest::of(&body);
    let (operation, fragment, skip) = stall;
    let (reached, release) = medium.park_after(operation, fragment, skip);
    let put = tokio::spawn({
        let writer = writer.clone();
        let body = body.clone();
        async move { writer.put(body, Vec::new()).await }
    });
    reached.notified().await;
    for _ in 0..passes {
        collector.collect().await?;
    }
    let fenced = !stored_under(&medium, digest, "/e/").await?;
    release.notify_one();
    // A put reads nothing back, so it reports what it wrote.
    let outcome = put.await??;
    assert!(
        matches!(outcome, Put::Created { .. }),
        "{stall:?}: {outcome:?}"
    );
    // A stream entry's flush refuses a removed blob as a retain does.
    let flushed = writer.flush(Some(&[digest])).await;
    assert_eq!(
        flushed.map_err(|error| error.code),
        if fenced {
            Err(ErrorCode::MissingReference)
        } else {
            Ok(())
        },
        "{stall:?}"
    );
    let root = RootId::new(*b"root")?;
    match writer.retain(&root, vec![digest]).await {
        Ok(_) => {
            assert!(
                !fenced,
                "{stall:?}: a commit never references a removed blob"
            );
            let read = writer.get(digest, ByteRange::FULL, 64).await?;
            assert_eq!(read.body().as_ref(), body.as_ref());
            Ok(true)
        }
        Err(error) => {
            assert_eq!(error.code, ErrorCode::MissingReference, "{stall:?}");
            // Negative control: the collector really removed the envelope,
            // so a commit that skipped its check would reference a lost blob.
            assert!(fenced, "{stall:?}");
            let retried = writer.put(body.clone(), Vec::new()).await?;
            assert!(matches!(retried, Put::Created { .. }));
            writer.retain(&root, vec![digest]).await?;
            let read = writer.get(digest, ByteRange::FULL, 64).await?;
            assert_eq!(read.body().as_ref(), body.as_ref());
            Ok(false)
        }
    }
}

#[tokio::test]
async fn a_put_stalled_past_its_grace_never_commits_a_lost_blob()
-> Result<(), Box<dyn std::error::Error>> {
    for stall in PUT_STALLS {
        // The third pass removes what the first found.
        assert!(
            !stalled_put(Shared::default(), stall, 3, 5).await?,
            "{stall:?}"
        );
        // Control: two passes keep the blob, and the commit holds it.
        assert!(
            stalled_put(Shared::default(), stall, 2, 6).await?,
            "{stall:?}"
        );
    }
    Ok(())
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
#[tokio::test]
async fn a_local_put_stalled_past_its_grace_never_commits_a_lost_blob()
-> Result<(), Box<dyn std::error::Error>> {
    for stall in PUT_STALLS {
        for (passes, committed) in [(3, false), (2, true)] {
            let root = tempfile::tempdir()?;
            let local = crate::LocalMedium::open(root.path(), Default::default(), None).await?;
            let outcome = stalled_put(Shared::over(local, false), stall, passes, 7).await?;
            assert_eq!(outcome, committed, "{stall:?}");
        }
    }
    Ok(())
}

#[tokio::test]
async fn a_delayed_collector_never_removes_a_later_version()
-> Result<(), Box<dyn std::error::Error>> {
    let medium = Shared::default();
    let writer = Store::new(medium.clone()).account(b"account")?;
    let collector = Store::new(medium.clone()).account(b"account")?;
    let body = OwnedBytes::from(vec![8; 64]);
    let digest = Digest::of(&body);
    let (reached, release) = medium.park("create", "/p/");
    let put = tokio::spawn({
        let writer = writer.clone();
        let body = body.clone();
        async move { writer.put(body, Vec::new()).await }
    });
    reached.notified().await;
    collector.collect().await?;
    collector.collect().await?;
    // The third pass parks right after removing the envelope, before its payload.
    let (removed, resume) = medium.park("remove", "/e/");
    let collecting = tokio::spawn({
        let collector = collector.clone();
        async move { collector.collect().await }
    });
    removed.notified().await;
    release.notify_one();
    // Its version is gone; a commit of it fails its check.
    assert!(matches!(put.await??, Put::Created { .. }));
    // A later put of the same digest publishes a fresh version meanwhile.
    assert!(matches!(
        writer.put(body.clone(), Vec::new()).await?,
        Put::Created { .. }
    ));
    resume.notify_one();
    collecting.await??;
    let read = writer.get(digest, ByteRange::FULL, 64).await?;
    assert_eq!(read.body().as_ref(), body.as_ref());
    // Negative control: the removed version's payload really is gone.
    let hex = digest.to_hex();
    let payloads = Medium::list(
        &medium,
        &format!("{}/", Digest::of(b"account").to_hex()),
        None,
        100_000,
    )
    .await?
    .into_iter()
    .filter(|(key, _)| key.contains("/p/") && key.contains(&hex))
    .count();
    assert_eq!(payloads, 1);
    Ok(())
}

#[test]
fn envelopes_round_trip_and_decode_earlier_encodings() -> Result<(), Box<dyn std::error::Error>> {
    let envelope = crate::blob::Envelope {
        kind: crate::Kind::Blob,
        length: 3,
        stored: 3,
        references: vec![Digest::of(b"child")],
        version: [4; 16],
        dying: Some([5; 16]),
        condemned: 9,
    };
    let encoded = envelope.encode();
    assert_eq!(crate::blob::Envelope::decode(&encoded)?, envelope);
    // Stamping and marking never change the size.
    let restamped = crate::blob::Envelope {
        dying: None,
        condemned: u64::MAX,
        ..envelope.clone()
    };
    assert_eq!(restamped.encode().len(), encoded.len());
    // An envelope from before stamps is unstamped.
    let earlier = crate::blob::Envelope::decode(&encoded[..encoded.len() - 8])?;
    assert_eq!((earlier.dying, earlier.condemned), (Some([5; 16]), 0));
    // A pending one, which this protocol no longer writes, marks no deleter.
    let mut pending = encoded[..encoded.len() - 24].to_vec();
    *pending.last_mut().ok_or("empty")? = 2;
    let decoded = crate::blob::Envelope::decode(&pending)?;
    assert_eq!((decoded.dying, decoded.condemned), (None, 0));
    Ok(())
}

/// Where the first of two equal puts parks while the second runs to
/// completion: after each of its writes; it reads nothing.
const EQUAL_PUT_STALLS: [(&str, &str, usize); 2] = [("create", "/e/", 0), ("create", "/p/", 0)];

/// Two processes put the same bytes, the first parked at `stall`. With
/// `lossy`, the second's payload write is lost with an unknown outcome.
async fn equal_puts(
    stall: (&'static str, &'static str, usize),
    lossy: bool,
) -> Result<[Put; 2], Box<dyn std::error::Error>> {
    let medium = Shared::default();
    let first = Store::new(medium.clone()).account(b"account")?;
    let second = Store::new(medium.clone()).account(b"account")?;
    first.flush(None).await?;
    second.flush(None).await?;
    let body = OwnedBytes::from(vec![9; 64]);
    let digest = Digest::of(&body);
    let (operation, fragment, skip) = stall;
    let (reached, release) = medium.park_after(operation, fragment, skip);
    let stalled = tokio::spawn({
        let first = first.clone();
        let body = body.clone();
        async move { first.put(body, Vec::new()).await }
    });
    reached.notified().await;
    *medium.ambiguous.lock().unwrap() = lossy.then_some("/p/");
    let other = second.put(body.clone(), Vec::new()).await?;
    *medium.ambiguous.lock().unwrap() = None;
    release.notify_one();
    let outcomes = [stalled.await??, other];
    if !lossy {
        let read = second.get(digest, ByteRange::FULL, 64).await?;
        assert_eq!(read.body().as_ref(), body.as_ref());
        let hex = digest.to_hex();
        let payloads = Medium::list(
            &medium,
            &format!("{}/", Digest::of(b"account").to_hex()),
            None,
            100_000,
        )
        .await?
        .into_iter()
        .filter(|(key, _)| key.contains("/p/") && key.contains(&hex))
        .count();
        assert_eq!(payloads, 1, "{stall:?}: equal puts share one version");
    }
    Ok(outcomes)
}

#[tokio::test]
async fn equal_puts_resolve_to_one_created_at_every_interleaving()
-> Result<(), Box<dyn std::error::Error>> {
    let mut detected = false;
    for stall in EQUAL_PUT_STALLS {
        let outcomes = equal_puts(stall, false).await?;
        assert!(
            matches!(
                outcomes,
                [Put::Created { .. }, Put::AlreadyExists { .. }]
                    | [Put::AlreadyExists { .. }, Put::Created { .. }]
            ),
            "{stall:?}: {outcomes:?}"
        );
        // Negative control: a helper whose write is lost reports unknown, so
        // the check above would catch a put that failed to complete its peer.
        detected |= equal_puts(stall, true).await?.contains(&Put::Unknown);
    }
    assert!(detected);
    Ok(())
}

#[tokio::test]
async fn lists_page_past_pending_puts() -> Result<(), Box<dyn std::error::Error>> {
    let medium = Shared::default();
    let blobs = Store::new(medium.clone()).account(b"account")?;
    let mut published = Vec::new();
    for fill in 0..4_u8 {
        let put = blobs.put(vec![fill; 16].into(), Vec::new()).await?;
        published.push(put.verified().ok_or("a put verifies")?.digest);
    }
    // Its envelope is created; its payload is still to come.
    let (reached, release) = medium.park("create", "/e/");
    let stalled = tokio::spawn({
        let blobs = blobs.clone();
        async move { blobs.put(vec![99; 16].into(), Vec::new()).await }
    });
    reached.notified().await;
    let mut listed = Vec::new();
    let mut after = None;
    loop {
        let page = blobs.list(after, 1).await?;
        // The cursor contract every wire client checks.
        assert!(page.next.is_none() || page.next == page.digests.last().copied());
        listed.extend(page.digests.iter().copied());
        match page.next {
            Some(next) => after = Some(next),
            None => break,
        }
    }
    published.sort();
    assert_eq!(listed, published, "the pending put is not listed");
    release.notify_one();
    assert!(matches!(stalled.await??, Put::Created { .. }));
    // Control: once published, it lists.
    assert_eq!(blobs.list(None, 100).await?.digests.len(), 5);
    Ok(())
}

/// Counts each operation of a volatile medium, with the keys of each flush.
#[derive(Default)]
pub(crate) struct Counting {
    inner: MemoryMedium,
    counts: std::sync::Mutex<std::collections::BTreeMap<&'static str, usize>>,
    flushes: std::sync::Mutex<Vec<usize>>,
}

impl Counting {
    fn count(&self, operation: &'static str) {
        *self.counts.lock().unwrap().entry(operation).or_default() += 1;
    }

    fn take(&self) -> std::collections::BTreeMap<&'static str, usize> {
        std::mem::take(&mut *self.counts.lock().unwrap())
    }

    fn flushed(&self) -> Vec<usize> {
        std::mem::take(&mut *self.flushes.lock().unwrap())
    }
}

#[async_trait::async_trait]
impl Medium for Counting {
    async fn create(&self, key: &str, bytes: OwnedBytes) -> Result<bool, MediumError> {
        self.count("create");
        self.inner.create(key, bytes).await
    }
    async fn read(
        &self,
        key: &str,
        range: Option<Range<u64>>,
    ) -> Result<Option<OwnedBytes>, MediumError> {
        self.count("read");
        self.inner.read(key, range).await
    }
    async fn remove(&self, key: &str) -> Result<bool, MediumError> {
        self.count("remove");
        self.inner.remove(key).await
    }
    async fn replace(
        &self,
        key: &str,
        expected: &[u8],
        bytes: OwnedBytes,
    ) -> Result<bool, MediumError> {
        self.count("replace");
        self.inner.replace(key, expected, bytes).await
    }
    async fn remove_if(&self, key: &str, expected: &[u8]) -> Result<bool, MediumError> {
        self.count("remove_if");
        self.inner.remove_if(key, expected).await
    }
    async fn list(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, u64)>, MediumError> {
        self.count("list");
        self.inner.list(prefix, after, limit).await
    }
    async fn flush(&self, keys: &[String]) -> Result<(), MediumError> {
        self.count("flush");
        self.flushes.lock().unwrap().push(keys.len());
        Ok(())
    }
    fn volatile(&self) -> bool {
        true
    }
    fn recovered(&self) -> bool {
        false
    }
    fn settle(&self) {}
}

/// The operations of one fresh put, and of one usage scan, in an account
/// already holding `existing` blobs.
async fn operations(
    existing: u32,
) -> Result<[std::collections::BTreeMap<&'static str, usize>; 2], Box<dyn std::error::Error>> {
    let store = Store::new(Counting::default());
    let blobs = store.account(b"account")?;
    for blob in 0..existing {
        blobs
            .put(blob.to_le_bytes().to_vec().into(), Vec::new())
            .await?;
    }
    store.medium().take();
    assert!(matches!(
        blobs.put(vec![0xff; 64].into(), Vec::new()).await?,
        Put::Created { .. }
    ));
    let put = store.medium().take();
    blobs.usage().await?;
    Ok([put, store.medium().take()])
}

#[tokio::test]
async fn a_fresh_put_writes_twice_reads_nothing_and_flushes_nothing_at_any_size()
-> Result<(), Box<dyn std::error::Error>> {
    let [small, small_scan] = operations(1).await?;
    let [large, large_scan] = operations(2_000).await?;
    assert_eq!(small.get("create"), Some(&2), "{small:?}");
    for operation in ["read", "list", "replace", "remove", "remove_if", "flush"] {
        assert_eq!(small.get(operation), None, "{small:?}");
    }
    assert_eq!(small, large);
    // Negative control: the same count sees a scan grow with the account.
    assert_ne!(small_scan, large_scan);
    Ok(())
}

#[tokio::test]
async fn a_commit_flushes_only_its_unflushed_closure_in_one_batch()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Store::new(Counting::default());
    let blobs = store.account(b"account")?;
    let digest = |put: Put| {
        put.verified()
            .map(|verified| verified.digest)
            .ok_or("unknown")
    };
    let child = digest(blobs.put(vec![1; 8].into(), Vec::new()).await?)?;
    let parent = digest(blobs.put(vec![2; 8].into(), vec![child]).await?)?;
    let root = RootId::new(*b"root")?;
    store.medium().flushed();
    blobs.retain(&root, vec![parent]).await?;
    // One barrier: both envelopes and payloads, the parent's index key on
    // the child, the batch's own and the batch.
    assert_eq!(store.medium().flushed(), [7]);
    // Durable now: the same closure costs only the new batch's keys.
    blobs.retain(&root, vec![parent]).await?;
    assert_eq!(store.medium().flushed(), [2]);
    // A caller's own commit of a durable closure flushes nothing.
    blobs.flush(Some(&[parent])).await?;
    assert_eq!(store.medium().flushed(), Vec::<usize>::new());
    // Negative control: a new blob in the closure is flushed with it.
    let fresh = digest(blobs.put(vec![3; 8].into(), vec![parent]).await?)?;
    blobs.flush(Some(&[fresh])).await?;
    assert_eq!(store.medium().flushed(), [3]);
    Ok(())
}

/// A volatile medium standing for one process over a disk: a write is
/// visible at once and survives a crash only once flushed. After `budget`
/// mutating operations the process dies, failing every later one. Unless
/// `flushes`, a flush makes nothing durable: the negative control for a
/// commit without its barrier. Unless `swaps`, a crash tears an unflushed
/// replace like any write: the control for a rewrite without its ordering.
///
/// With `short_reads`, a range past a key's end reads what there is instead
/// of refusing: the control for a presence probe that trusts a torn key.
/// `recovered` is what a restart after a crash reports until the store
/// settles it; unset on one, it is the control for a store that never
/// repairs what the crash left. A flush
/// never makes the removal of a key matching `lost_removals` durable: the
/// control for a removal reaching the disk after removals ordered behind it.
#[derive(Clone)]
struct Crashing {
    disk: std::sync::Arc<std::sync::Mutex<Disk>>,
    flushes: bool,
    swaps: bool,
    short_reads: bool,
    recovered: bool,
    lost_removals: Option<&'static str>,
}

#[derive(Default)]
struct Disk {
    visible: std::collections::BTreeMap<String, OwnedBytes>,
    durable: std::collections::BTreeMap<String, OwnedBytes>,
    /// Blobs committed by an appended stream entry, durable at once.
    commits: Vec<Digest>,
    /// Keys last written by a replace, which a crash never tears.
    swapped: std::collections::BTreeSet<String>,
    /// Whether the store settled the repair of a restart.
    settled: bool,
    budget: Option<usize>,
    operations: usize,
}

/// Which unflushed writes a crash keeps: whole, or torn to half their bytes.
#[derive(Clone, Copy, Debug)]
enum Survivors {
    None,
    All,
    Torn,
    /// All, with those matching torn.
    TornOnly(&'static str),
    /// All, with those matching at full length but zeroed.
    Damaged(&'static str),
    Only(&'static str),
    AllBut(&'static str),
}

impl Survivors {
    fn tears(self, key: &str) -> bool {
        match self {
            Self::Torn => true,
            Self::TornOnly(fragment) => key.contains(fragment),
            _ => false,
        }
    }

    fn keep(self, key: &str) -> bool {
        match self {
            Self::None => false,
            Self::All | Self::Torn | Self::TornOnly(_) | Self::Damaged(_) => true,
            Self::Only(fragment) => key.contains(fragment),
            Self::AllBut(fragment) => !key.contains(fragment),
        }
    }
}

const SURVIVORS: [Survivors; 10] = [
    Survivors::None,
    Survivors::All,
    Survivors::Torn,
    Survivors::TornOnly("/p/"),
    Survivors::Damaged("/p/"),
    // A flush syncs directories concurrently with their files' data, so a
    // crash inside it may keep any key's name with none of its bytes.
    Survivors::Damaged("/"),
    Survivors::Only("/e/"),
    Survivors::Only("/p/"),
    Survivors::Only("/i/"),
    Survivors::AllBut("/i/"),
];

fn dead() -> MediumError {
    MediumError::Failed(std::io::ErrorKind::BrokenPipe.into())
}

impl Crashing {
    fn new(flushes: bool, swaps: bool, budget: Option<usize>) -> Self {
        Self::over(
            Disk {
                budget,
                ..Disk::default()
            },
            flushes,
            swaps,
        )
    }

    fn over(disk: Disk, flushes: bool, swaps: bool) -> Self {
        Self {
            disk: std::sync::Arc::new(std::sync::Mutex::new(disk)),
            flushes,
            swaps,
            short_reads: false,
            recovered: false,
            lost_removals: None,
        }
    }

    /// A restart over what a crash left.
    fn restart(disk: Disk) -> Self {
        Self {
            recovered: true,
            ..Self::over(disk, true, true)
        }
    }

    fn alive(&self) -> Result<std::sync::MutexGuard<'_, Disk>, MediumError> {
        let disk = self.disk.lock().unwrap();
        if disk.budget == Some(0) {
            return Err(dead());
        }
        Ok(disk)
    }

    /// One mutating operation, unless the process died.
    fn step(&self) -> Result<std::sync::MutexGuard<'_, Disk>, MediumError> {
        let mut disk = self.alive()?;
        if let Some(budget) = &mut disk.budget {
            *budget -= 1;
        }
        disk.operations += 1;
        Ok(disk)
    }

    /// Appends a stream entry committing `digest`.
    fn append(&self, digest: Digest) -> Result<(), MediumError> {
        self.step()?.commits.push(digest);
        Ok(())
    }

    /// What a restart finds: the durable state, with `survivors` of the
    /// unflushed writes.
    fn recover(&self, survivors: Survivors) -> Disk {
        let disk = self.disk.lock().unwrap();
        let mut found = disk.durable.clone();
        let keys: std::collections::BTreeSet<_> =
            disk.visible.keys().chain(disk.durable.keys()).collect();
        for key in keys {
            if disk.visible.get(key) == disk.durable.get(key) || !survivors.keep(key) {
                continue;
            }
            match disk.visible.get(key) {
                Some(bytes) if survivors.tears(key) && !disk.swapped.contains(key) => {
                    found.insert(key.clone(), bytes.slice(..bytes.len() / 2))
                }
                Some(bytes)
                    if matches!(survivors, Survivors::Damaged(fragment) if key.contains(fragment))
                        && !disk.swapped.contains(key) =>
                {
                    found.insert(key.clone(), vec![0; bytes.len()].into())
                }
                Some(bytes) => found.insert(key.clone(), bytes.clone()),
                None => found.remove(key),
            };
        }
        Disk {
            visible: found.clone(),
            durable: found,
            commits: disk.commits.clone(),
            swapped: std::collections::BTreeSet::new(),
            settled: false,
            budget: None,
            operations: 0,
        }
    }
}

#[async_trait::async_trait]
impl Medium for Crashing {
    async fn create(&self, key: &str, bytes: OwnedBytes) -> Result<bool, MediumError> {
        let mut disk = self.step()?;
        if disk.visible.contains_key(key) {
            return Ok(false);
        }
        disk.swapped.remove(key);
        disk.visible.insert(key.to_owned(), bytes);
        Ok(true)
    }
    async fn read(
        &self,
        key: &str,
        range: Option<Range<u64>>,
    ) -> Result<Option<OwnedBytes>, MediumError> {
        let disk = self.alive()?;
        let Some(bytes) = disk.visible.get(key) else {
            return Ok(None);
        };
        match range {
            None => Ok(Some(bytes.clone())),
            Some(range) if range.start <= range.end && range.end <= bytes.len() as u64 => {
                Ok(Some(bytes.slice(range.start as usize..range.end as usize)))
            }
            Some(range) if self.short_reads => {
                let end = bytes.len();
                Ok(Some(bytes.slice(
                    (range.start as usize).min(end)..(range.end as usize).min(end),
                )))
            }
            Some(_) => Err(MediumError::Failed(std::io::ErrorKind::InvalidInput.into())),
        }
    }
    async fn remove(&self, key: &str) -> Result<bool, MediumError> {
        Ok(self.step()?.visible.remove(key).is_some())
    }
    async fn replace(
        &self,
        key: &str,
        expected: &[u8],
        bytes: OwnedBytes,
    ) -> Result<bool, MediumError> {
        let mut disk = self.step()?;
        if disk
            .visible
            .get(key)
            .is_none_or(|stored| stored.as_ref() != expected)
        {
            return Ok(false);
        }
        if self.swaps {
            disk.swapped.insert(key.to_owned());
        }
        disk.visible.insert(key.to_owned(), bytes);
        Ok(true)
    }
    async fn remove_if(&self, key: &str, expected: &[u8]) -> Result<bool, MediumError> {
        let mut disk = self.step()?;
        if disk
            .visible
            .get(key)
            .is_none_or(|stored| stored.as_ref() != expected)
        {
            return Ok(false);
        }
        disk.visible.remove(key);
        Ok(true)
    }
    async fn list(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, u64)>, MediumError> {
        let disk = self.alive()?;
        Ok(disk
            .visible
            .range(prefix.to_owned()..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .filter(|(key, _)| after.is_none_or(|after| key.as_str() > after))
            .take(limit)
            .map(|(key, bytes)| (key.clone(), bytes.len() as u64))
            .collect())
    }
    async fn flush(&self, keys: &[String]) -> Result<(), MediumError> {
        let mut disk = self.step()?;
        if self.flushes {
            for key in keys {
                match disk.visible.get(key).cloned() {
                    Some(bytes) => disk.durable.insert(key.clone(), bytes),
                    None if self.lost_removals.is_some_and(|lost| key.contains(lost)) => None,
                    None => disk.durable.remove(key),
                };
            }
        }
        Ok(())
    }
    fn volatile(&self) -> bool {
        true
    }
    fn recovered(&self) -> bool {
        self.recovered && !self.disk.lock().unwrap().settled
    }
    fn settle(&self) {
        self.disk.lock().unwrap().settled = true;
    }
}

/// Puts a child and a parent referencing it and retains the parent; puts
/// another blob over the child and commits it as a stream entry, flushing
/// first unless `append_first`; then collects.
async fn commit_scenario(
    medium: &Crashing,
    append_first: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let blobs = Store::new(medium.clone()).account(b"account")?;
    let digest = |put: Put| {
        put.verified()
            .map(|verified| verified.digest)
            .ok_or("unknown")
    };
    let child = digest(blobs.put(vec![1; 48].into(), Vec::new()).await?)?;
    let parent = digest(blobs.put(vec![2; 48].into(), vec![child]).await?)?;
    blobs.retain(&RootId::new(*b"root")?, vec![parent]).await?;
    let gone = RootId::new(*b"gone")?;
    blobs.retain(&gone, vec![child]).await?;
    blobs.release(&gone).await?;
    let entry = digest(blobs.put(vec![3; 48].into(), vec![child]).await?)?;
    if append_first {
        medium.append(entry)?;
        blobs.flush(Some(&[entry])).await?;
    } else {
        blobs.flush(Some(&[entry])).await?;
        medium.append(entry)?;
    }
    blobs.collect().await?;
    Ok(())
}

/// Every committed blob on `disk`, and all it references, is whole: those
/// of each root batch and of each appended stream entry.
fn committed_blobs_are_whole(disk: &Disk) -> Result<(), String> {
    let prefix = format!("{}/", Digest::of(b"account").to_hex());
    let mut pending = disk.commits.clone();
    for (key, batch) in &disk.durable {
        // A torn batch was never acknowledged.
        if let Some(name) = key.strip_prefix(&format!("{prefix}r/"))
            && let Ok(batch) = crate::blob::decode_batch(batch)
        {
            // A batch a crash kept still pins its members against a delete.
            let tag = format!("r{}", name.replacen('/', ".", 1));
            for member in &batch {
                if !disk
                    .durable
                    .contains_key(&format!("{prefix}i/{}/{tag}", member.to_hex()))
                {
                    return Err(format!("{key}: {} unindexed", member.to_hex()));
                }
            }
            pending.extend(batch);
        }
    }
    let mut seen = std::collections::HashSet::new();
    while let Some(digest) = pending.pop() {
        if !seen.insert(digest) {
            continue;
        }
        let hex = digest.to_hex();
        let envelope = disk
            .durable
            .get(&format!("{prefix}e/{}/{hex}", &hex[..2]))
            .ok_or(format!("{hex}: no envelope"))?;
        let envelope =
            crate::blob::Envelope::decode(envelope).map_err(|_| format!("{hex}: torn envelope"))?;
        // Each child is pinned against a delete by this blob's index key.
        let tag = format!("e{hex}.{}", hex::encode(envelope.version));
        for child in &envelope.references {
            if !disk
                .durable
                .contains_key(&format!("{prefix}i/{}/{tag}", child.to_hex()))
            {
                return Err(format!("{hex}: child {} unindexed", child.to_hex()));
            }
        }
        let payload = disk
            .durable
            .get(&format!(
                "{prefix}p/{}/{hex}.{}",
                &hex[..2],
                hex::encode(envelope.version)
            ))
            .ok_or(format!("{hex}: no payload"))?;
        if Digest::of(payload) != digest {
            return Err(format!("{hex}: torn payload"));
        }
        pending.extend(envelope.references);
    }
    Ok(())
}

#[tokio::test]
async fn a_crash_at_any_step_never_leaves_a_commit_on_a_missing_or_torn_blob()
-> Result<(), Box<dyn std::error::Error>> {
    // The protocol, then its negative controls: a barrier that makes
    // nothing durable, a replace a crash can tear, and a stream entry
    // appended before its barrier.
    // A commit flushes its closure, index keys and batch in one round, so
    // what a crash left is judged once repair has run, as the store serves
    // it. A released batch whose removal reaches the disk after its index
    // keys' is repaired like any other.
    for (flushes, swaps, lost_removals, append_first) in [
        (true, true, None, false),
        (true, true, Some("/r/"), false),
        (false, true, None, false),
        (true, false, None, false),
        (true, true, None, true),
    ] {
        let correct = flushes && swaps && !append_first;
        let configured = |budget| Crashing {
            lost_removals,
            ..Crashing::new(flushes, swaps, budget)
        };
        let whole = configured(None);
        commit_scenario(&whole, append_first).await?;
        let steps = whole.disk.lock().unwrap().operations;
        let mut violations = Vec::new();
        for budget in 0..=steps {
            for survivors in SURVIVORS {
                let crashed = configured(Some(budget));
                let _ = commit_scenario(&crashed, append_first).await;
                let restarted = Crashing::restart(crashed.recover(survivors));
                let repaired = Store::new(restarted.clone())
                    .account(b"account")?
                    .revisions()
                    .await
                    .map(drop);
                repaired.map_err(|error| format!("crash at {budget}, {survivors:?}: {error}"))?;
                let disk = restarted.recover(Survivors::None);
                if let Err(broken) = committed_blobs_are_whole(&disk) {
                    violations.push(format!("crash at {budget}, {survivors:?}: {broken}"));
                    continue;
                }
                if correct {
                    // The repaired store commits again.
                    let restarted = Crashing::restart(disk);
                    commit_scenario(&restarted, false)
                        .await
                        .map_err(|error| format!("crash at {budget}, {survivors:?}: {error}"))?;
                    committed_blobs_are_whole(&restarted.recover(Survivors::None))?;
                }
            }
        }
        if correct {
            assert!(violations.is_empty(), "{violations:#?}");
        } else {
            assert!(
                !violations.is_empty(),
                "control flushes={flushes} swaps={swaps} append_first={append_first} went undetected"
            );
        }
    }
    Ok(())
}

/// What a restart after a crash inside a put of `body` observes, or how the
/// store betrayed it: a blob that reads whole or not at all, a reput that
/// completes it, and a collection that leaves none of its keys behind.
async fn after_a_crashed_put(
    medium: &Crashing,
    body: &OwnedBytes,
) -> Result<(), Box<dyn std::error::Error>> {
    let digest = Digest::of(body);
    let blobs = Store::new(medium.clone()).account(b"account")?;
    let whole = |object: Result<crate::Object, crate::Error>| {
        object.is_ok_and(|object| object.body() == body)
    };
    let present = blobs.head(digest).await.is_ok();
    let listed = blobs.list(None, 16).await?.digests.contains(&digest);
    if present != listed || (present && !whole(blobs.get(digest, ByteRange::FULL, 64).await)) {
        return Err(format!("present {present}, listed {listed}, but not whole").into());
    }
    let put = blobs.put(body.clone(), Vec::new()).await?;
    if put.verified().is_none() || !whole(blobs.get(digest, ByteRange::FULL, 64).await) {
        return Err(format!("reput {put:?} left it unreadable").into());
    }
    for _ in 0..3 {
        blobs.collect().await?;
    }
    let left: Vec<_> = medium
        .disk
        .lock()
        .unwrap()
        .visible
        .keys()
        .filter(|key| key.contains(&digest.to_hex()))
        .cloned()
        .collect();
    if !left.is_empty() {
        return Err(format!("collection left {left:?}").into());
    }
    Ok(())
}

#[tokio::test]
async fn a_crash_inside_a_put_leaves_no_blob_until_a_reput_completes_it()
-> Result<(), Box<dyn std::error::Error>> {
    // The envelope precedes its payload: an envelope without a whole payload
    // is no blob to any reader, a reput writes the payload and collection
    // removes the orphan. Negative controls, one layer each: the length
    // probe trusting a torn payload without the restart's repair, and a
    // store that never repairs what the crash damaged.
    let body = OwnedBytes::from(vec![4; 48]);
    for (short_reads, recovered) in [(false, true), (true, false), (false, false)] {
        let whole = Crashing::new(true, true, None);
        Store::new(whole.clone())
            .account(b"account")?
            .put(body.clone(), Vec::new())
            .await?;
        let steps = whole.disk.lock().unwrap().operations;
        let mut violations = Vec::new();
        for budget in 0..=steps {
            for survivors in SURVIVORS {
                if short_reads && matches!(survivors, Survivors::Damaged(_)) {
                    // That control injects a torn payload only.
                    continue;
                }
                let crashed = Crashing::new(true, true, Some(budget));
                let _ = Store::new(crashed.clone())
                    .account(b"account")?
                    .put(body.clone(), Vec::new())
                    .await;
                let restarted = Crashing {
                    short_reads,
                    recovered,
                    ..Crashing::restart(crashed.recover(survivors))
                };
                if let Err(broken) = after_a_crashed_put(&restarted, &body).await {
                    violations.push(format!("crash at {budget}, {survivors:?}: {broken}"));
                }
            }
        }
        if short_reads || !recovered {
            assert!(
                !violations.is_empty(),
                "control short_reads={short_reads} recovered={recovered} went undetected"
            );
        } else {
            assert!(violations.is_empty(), "{violations:#?}");
        }
    }
    Ok(())
}

/// Puts a child and a parent referencing it, committing neither; the parent.
async fn uncommitted_parent(medium: &Crashing) -> Result<Digest, Box<dyn std::error::Error>> {
    let blobs = Store::new(medium.clone()).account(b"account")?;
    let child = Digest::of(&[1; 48]);
    blobs.put(vec![1; 48].into(), Vec::new()).await?;
    blobs.put(vec![2; 48].into(), vec![child]).await?;
    Ok(Digest::of(&[2; 48]))
}

#[tokio::test]
async fn a_recovered_parent_commits_only_with_its_closure_whole_and_indexed()
-> Result<(), Box<dyn std::error::Error>> {
    // A crash can keep a parent while losing its child or the index key
    // pinning it; the restart's repair leaves it a blob only with its
    // closure whole and indexed, before a retain or a flush of every blob
    // commits it. Negative control: a store not told the medium recovered.
    for (recovered, retain) in [(true, true), (true, false), (false, true), (false, false)] {
        let whole = Crashing::new(true, true, None);
        let parent = uncommitted_parent(&whole).await?;
        let steps = whole.disk.lock().unwrap().operations;
        let mut violations = Vec::new();
        for budget in 0..=steps {
            for survivors in SURVIVORS {
                let crashed = Crashing::new(true, true, Some(budget));
                let _ = uncommitted_parent(&crashed).await;
                let restarted = Crashing {
                    recovered,
                    ..Crashing::restart(crashed.recover(survivors))
                };
                let blobs = Store::new(restarted.clone()).account(b"account")?;
                if retain {
                    match blobs.retain(&RootId::new(*b"root")?, vec![parent]).await {
                        Ok(_) => {}
                        Err(error) if error.code == ErrorCode::MissingReference => {}
                        Err(error) => return Err(format!("crash at {budget}: {error}").into()),
                    }
                } else if blobs.head(parent).await.is_ok() {
                    // A caller that finds the parent commits it in a stream
                    // entry after a flush of every blob.
                    blobs.flush(None).await?;
                    restarted.append(parent)?;
                }
                assert!(!recovered || !restarted.recovered(), "repair never settled");
                if let Err(broken) = committed_blobs_are_whole(&restarted.recover(Survivors::None))
                {
                    violations.push(format!("crash at {budget}, {survivors:?}: {broken}"));
                }
            }
        }
        if recovered {
            assert!(violations.is_empty(), "{violations:#?}");
        } else {
            assert!(
                !violations.is_empty(),
                "an unchecked closure went undetected, retain={retain}"
            );
        }
    }
    Ok(())
}

/// The durable syncs `work` issues, by operation.
#[cfg(all(
    feature = "local",
    feature = "test-support",
    not(target_arch = "wasm32")
))]
async fn syncs<T>(work: impl Future<Output = T>) -> (T, std::collections::BTreeMap<String, usize>) {
    use acyclic_native_runtime::testing::Schedule;
    let (schedule, mut events) = Schedule::new();
    let driver = tokio::spawn(async move {
        while let Some(boundary) = events.recv().await {
            boundary.release();
        }
    });
    let value = schedule.scope(work).await;
    schedule.drained().await;
    driver.abort();
    let _ = driver.await;
    let mut syncs = std::collections::BTreeMap::new();
    for call in schedule.durable_writes() {
        if call.operation.contains("sync") {
            *syncs.entry(call.operation.to_string()).or_default() += 1;
        }
    }
    (value, syncs)
}

#[cfg(all(
    feature = "local",
    feature = "test-support",
    not(target_arch = "wasm32")
))]
#[tokio::test]
async fn a_local_put_syncs_nothing_and_its_commit_syncs_its_closure_once()
-> Result<(), Box<dyn std::error::Error>> {
    acyclic_native_runtime::initialize_native_io_services()?;
    let root = tempfile::tempdir()?;
    let store = Store::new(crate::LocalMedium::open(root.path(), Default::default(), None).await?);
    let blobs = store.account(b"account")?;
    blobs.flush(None).await?;
    let digest = |put: Put| {
        put.verified()
            .map(|verified| verified.digest)
            .ok_or("unknown")
    };
    let (child, child_syncs) = syncs(blobs.put(vec![7; 64].into(), Vec::new())).await;
    let child = digest(child?)?;
    let (parent, parent_syncs) = syncs(blobs.put(vec![8; 64].into(), vec![child])).await;
    let parent = digest(parent?)?;
    assert!(child_syncs.is_empty(), "{child_syncs:?}");
    assert!(parent_syncs.is_empty(), "{parent_syncs:?}");
    let root_id = RootId::new(*b"root")?;
    let (retained, commit) = syncs(blobs.retain(&root_id, vec![parent])).await;
    retained?;
    eprintln!("syncs of one put: {child_syncs:?}; of its commit: {commit:?}");
    // Negative control: the same trace sees the commit's barrier.
    assert!(!commit.is_empty());
    // Durable now: a second commit of the closure syncs only its own batch.
    let (retained, again) = syncs(blobs.retain(&root_id, vec![parent])).await;
    retained?;
    eprintln!("syncs of a commit of a durable closure: {again:?}");
    assert!(again.values().sum::<usize>() < commit.values().sum::<usize>());
    Ok(())
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
#[tokio::test]
async fn a_local_root_left_open_is_flushed_whole_on_reopen()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let open = async || crate::LocalMedium::open(root.path(), Default::default(), None).await;
    let body = OwnedBytes::from(vec![4; 32]);
    let store = Store::new(open().await?);
    store
        .account(b"account")?
        .put(body.clone(), Vec::new())
        .await?;
    drop(store);
    // A clean close flushed everything and cleared the marker.
    let marker = root.path().join("OPEN");
    assert!(!marker.exists());
    // As a close that never ran leaves it.
    std::fs::write(&marker, b"")?;
    // A clean close before the store repaired the root keeps it unclean.
    let unrepaired = open().await?;
    assert!(unrepaired.recovered());
    drop(unrepaired);
    assert!(marker.exists());
    let reopened = Store::new(open().await?);
    assert!(reopened.medium().recovered());
    let object = reopened
        .account(b"account")?
        .get(Digest::of(&body), ByteRange::FULL, 32)
        .await?;
    assert_eq!(object.body(), &body);
    // Its repair settled the root, so this close clears the marker.
    assert!(!reopened.medium().recovered());
    drop(reopened);
    assert!(!marker.exists());
    Ok(())
}

#[cfg(all(
    feature = "local",
    feature = "test-support",
    not(target_arch = "wasm32")
))]
#[tokio::test]
async fn a_local_root_closes_only_after_its_last_accepted_write()
-> Result<(), Box<dyn std::error::Error>> {
    use acyclic_native_runtime::testing::Schedule;
    use std::sync::atomic::AtomicBool;
    let root = tempfile::tempdir()?;
    let path = root.path().to_owned();
    let open = || crate::LocalMedium::open(path.clone(), Default::default(), None);
    let (schedule, mut events) = Schedule::new();
    // Every boundary runs, except the first submission once `holding`.
    let holding = std::sync::Arc::new(AtomicBool::new(false));
    let (hold, held) = tokio::sync::oneshot::channel();
    let driver = tokio::spawn({
        let holding = holding.clone();
        async move {
            let mut hold = Some(hold);
            while let Some(boundary) = events.recv().await {
                if boundary.name.ends_with(":submitted") && holding.load(Ordering::SeqCst) {
                    if let Some(hold) = hold.take() {
                        let _ = hold.send(boundary);
                        continue;
                    }
                }
                boundary.release();
            }
        }
    });
    let boundary = schedule
        .scope(async {
            // Control: a root with nothing in flight reopens at once.
            drop(open().await?);
            let medium = open().await?;
            holding.store(true, Ordering::SeqCst);
            let mut write = Box::pin(medium.create("k/key", OwnedBytes::from(vec![1; 8])));
            let boundary = tokio::select! {
                boundary = held => boundary?,
                _ = &mut write => return Err::<_, Box<dyn std::error::Error>>("unheld".into()),
            };
            // The put is cancelled with its write accepted, and its owner closes.
            drop(write);
            drop(medium);
            assert!(matches!(
                open().await,
                Err(crate::local::LocalOpenError::AlreadyOwned)
            ));
            Ok(boundary)
        })
        .await?;
    boundary.release();
    schedule.drained().await;
    // The write done, the root closed clean, the write flushed with it.
    let reopened = schedule.scope(open()).await?;
    assert!(!reopened.recovered());
    assert!(reopened.read("k/key", None).await?.is_some());
    drop(reopened);
    driver.abort();
    let _ = driver.await;
    Ok(())
}

/// Counts how many puts and reads overlap; each parks once before serving.
struct Overlap {
    inner: crate::MemoryObjects,
    live: std::sync::atomic::AtomicUsize,
    peak: std::sync::atomic::AtomicUsize,
}

impl Overlap {
    async fn enter<T>(&self, work: impl Future<Output = T>) -> T {
        let live = self.live.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(live, Ordering::SeqCst);
        let mut parked = false;
        std::future::poll_fn(|context| {
            if std::mem::replace(&mut parked, true) {
                std::task::Poll::Ready(())
            } else {
                context.waker().wake_by_ref();
                std::task::Poll::Pending
            }
        })
        .await;
        let value = work.await;
        self.live.fetch_sub(1, Ordering::SeqCst);
        value
    }
}

#[async_trait::async_trait]
impl Blobs for Overlap {
    async fn put(&self, body: OwnedBytes, references: Vec<Digest>) -> Result<Put, crate::Error> {
        self.enter(self.inner.put(body, references)).await
    }
    async fn put_manifest(&self, manifest: crate::Manifest) -> Result<Put, crate::Error> {
        self.inner.put_manifest(manifest).await
    }
    async fn get(
        &self,
        digest: Digest,
        range: ByteRange,
        maximum: u64,
    ) -> Result<crate::Object, crate::Error> {
        self.enter(self.inner.get(digest, range, maximum)).await
    }
    async fn head(&self, digest: Digest) -> Result<crate::Head, crate::Error> {
        self.inner.head(digest).await
    }
    async fn delete(&self, digest: Digest) -> Result<crate::Deletion, crate::Error> {
        self.inner.delete(digest).await
    }
    async fn list(&self, after: Option<Digest>, limit: u32) -> Result<crate::Page, crate::Error> {
        self.inner.list(after, limit).await
    }
    async fn flush(&self, only: Option<&[Digest]>) -> Result<(), crate::Error> {
        self.inner.flush(only).await
    }
    async fn retain(
        &self,
        root: &RootId,
        references: Vec<Digest>,
    ) -> Result<crate::Retained, crate::Error> {
        self.inner.retain(root, references).await
    }
    async fn release(&self, root: &RootId) -> Result<crate::Released, crate::Error> {
        self.inner.release(root).await
    }
    async fn collect(&self) -> Result<crate::Collection, crate::Error> {
        self.inner.collect().await
    }
    async fn usage(&self) -> Result<crate::Usage, crate::Error> {
        self.inner.usage().await
    }
}

#[tokio::test]
async fn default_batches_overlap_one_window_and_keep_request_order()
-> Result<(), Box<dyn std::error::Error>> {
    let blobs = Overlap {
        inner: crate::MemoryObjects::memory(),
        live: 0.into(),
        peak: 0.into(),
    };
    let count = crate::BATCH_WINDOW * 2 + 3;
    let body = |index: usize| OwnedBytes::from(index.to_le_bytes().to_vec());
    // Negative control: one-at-a-time puts never overlap under the probe.
    blobs.put(body(count), Vec::new()).await?;
    blobs.put(body(count + 1), Vec::new()).await?;
    assert_eq!(blobs.peak.load(Ordering::SeqCst), 1);

    let puts = blobs
        .put_batch((0..count).map(|index| (body(index), Vec::new())).collect())
        .await;
    assert_eq!(blobs.peak.swap(0, Ordering::SeqCst), crate::BATCH_WINDOW);
    let mut reads = Vec::with_capacity(count);
    for (index, put) in puts.into_iter().enumerate() {
        let digest = put?.verified().ok_or("a fresh put verifies")?.digest;
        assert_eq!(digest, Digest::of(&index.to_le_bytes()));
        reads.push((digest, ByteRange::FULL, 8));
    }
    let objects = blobs.get_batch(reads).await;
    assert_eq!(blobs.peak.swap(0, Ordering::SeqCst), crate::BATCH_WINDOW);
    for (index, object) in objects.into_iter().enumerate() {
        assert_eq!(object?.body().as_ref(), index.to_le_bytes());
    }

    // A put that references or repeats an earlier one waits for it.
    let parent = OwnedBytes::from_static(b"dependent parent");
    let digest = Digest::of(&parent);
    let puts = blobs
        .put_batch(vec![
            (parent.clone(), Vec::new()),
            (OwnedBytes::from_static(b"dependent child"), vec![digest]),
            (parent, Vec::new()),
        ])
        .await;
    assert!(matches!(puts[0], Ok(Put::Created { .. })));
    assert!(matches!(puts[1], Ok(Put::Created { .. })));
    assert!(matches!(puts[2], Ok(Put::AlreadyExists { .. })));
    // The child waits for its parent; the repeat, whose original already
    // completed, overlaps the child.
    assert_eq!(blobs.peak.swap(0, Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn a_collection_pages_through_more_blobs_than_one_listing()
-> Result<(), Box<dyn std::error::Error>> {
    let blobs = crate::MemoryObjects::memory();
    let count: u32 = 2 * 1000 + 1;
    let mut kept = None;
    for index in 0..count {
        let put = blobs
            .put(OwnedBytes::from(index.to_le_bytes().to_vec()), Vec::new())
            .await?;
        kept.get_or_insert(put.verified().ok_or("a fresh put verifies")?.digest);
    }
    let kept = kept.ok_or("one blob")?;
    blobs
        .retain(&RootId::new(b"kept".to_vec())?, vec![kept])
        .await?;
    // The first pass finds them, the second keeps them, the third sweeps
    // every unrooted page.
    let mut swept = 0;
    for _ in 0..3 {
        swept += blobs.collect().await?.swept;
    }
    assert_eq!(swept, count as u64 - 1);
    // Negative control: the rooted blob on the first page survives.
    assert_eq!(blobs.usage().await?.blobs, 1);
    assert_eq!(blobs.head(kept).await?.digest, kept);
    Ok(())
}

/// Seals each body of a batch.
#[cfg(not(target_arch = "wasm32"))]
fn sealed(requests: Vec<(OwnedBytes, Vec<Digest>)>) -> Vec<(crate::Sealed, Vec<Digest>)> {
    requests
        .into_iter()
        .map(|(body, references)| (crate::Sealed::new(body), references))
        .collect()
}

/// A meter that admits all but live allocation past `limit`, recording the
/// peak it held, for driving the native entry points.
#[cfg(not(target_arch = "wasm32"))]
struct Budget {
    limit: u64,
    held: std::sync::Mutex<(u64, u64)>,
    cancelled: std::sync::atomic::AtomicBool,
    /// Writes left, unbounded when `None`.
    writes: std::sync::Mutex<Option<u64>>,
    /// Whether a reservation withholds its writes; a hollow one charges
    /// each reserved write as it happens, as if nothing was reserved.
    withholds: bool,
}

#[cfg(not(target_arch = "wasm32"))]
impl Budget {
    fn new(limit: u64) -> Self {
        Self {
            limit,
            held: std::sync::Mutex::new((0, 0)),
            cancelled: std::sync::atomic::AtomicBool::new(false),
            writes: std::sync::Mutex::new(None),
            withholds: true,
        }
    }

    /// Spends `count` writes, refused past the budget.
    fn spend(&self, count: u64) -> std::io::Result<()> {
        match &mut *self.writes.lock().unwrap() {
            Some(left) if *left < count => Err(std::io::ErrorKind::QuotaExceeded.into()),
            Some(left) => {
                *left -= count;
                Ok(())
            }
            None => Ok(()),
        }
    }

    fn hold(&self, bytes: u64) -> std::io::Result<()> {
        let mut held = self.held.lock().unwrap();
        if held.0 + bytes > self.limit {
            return Err(std::io::ErrorKind::OutOfMemory.into());
        }
        held.0 += bytes;
        held.1 = held.1.max(held.0);
        Ok(())
    }

    /// The peak live allocation since the last call.
    fn peak(&self) -> u64 {
        let mut held = self.held.lock().unwrap();
        let live = held.0;
        std::mem::replace(&mut held.1, live)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl acyclic_native_runtime::NativeReadMeter for Budget {
    fn check(&self) -> std::io::Result<()> {
        if self.cancelled.load(Ordering::SeqCst) {
            return Err(std::io::ErrorKind::Interrupted.into());
        }
        Ok(())
    }
    fn reserve_operations(
        &self,
        _: acyclic_native_runtime::NativeOperationKind,
        count: u64,
    ) -> std::io::Result<()> {
        if self.withholds {
            self.spend(count)
        } else {
            Ok(())
        }
    }
    fn cancel_reserved_operations(
        &self,
        _: acyclic_native_runtime::NativeOperationKind,
        count: u64,
    ) {
        if self.withholds
            && let Some(left) = &mut *self.writes.lock().unwrap()
        {
            *left += count;
        }
    }
    fn record_reserved_operation(
        &self,
        _: acyclic_native_runtime::NativeOperationKind,
    ) -> std::io::Result<()> {
        if self.withholds {
            Ok(())
        } else {
            self.spend(1)
        }
    }
    fn admit_write_bytes(&self, _: usize) -> std::io::Result<()> {
        Ok(())
    }
    fn reserve_bytes(
        &self,
        _: acyclic_native_runtime::NativeOperationKind,
        _: u64,
    ) -> std::io::Result<()> {
        Ok(())
    }
    fn cancel_reserved_bytes(&self, _: acyclic_native_runtime::NativeOperationKind, _: u64) {}
    fn record_reserved_bytes(
        &self,
        _: acyclic_native_runtime::NativeOperationKind,
        _: u64,
        _: u64,
    ) -> std::io::Result<()> {
        Ok(())
    }
    fn allocate(&self, bytes: u64) -> std::io::Result<()> {
        self.hold(bytes)
    }
    fn reserve_allocation(&self, bytes: u64) -> std::io::Result<()> {
        self.hold(bytes)
    }
    fn cancel_reserved_allocation(&self, bytes: u64) {
        self.release(bytes);
    }
    fn record_reserved_allocation(&self, _: u64) -> std::io::Result<()> {
        Ok(())
    }
    fn allocation_failed(&self, _: std::collections::TryReserveError) -> std::io::Error {
        std::io::ErrorKind::OutOfMemory.into()
    }
    fn allocate_refcount_header(&self) -> std::io::Result<()> {
        Ok(())
    }
    fn adopt(&self, bytes: u64) -> std::io::Result<()> {
        self.hold(bytes)
    }
    fn record_completed_adoption(&self, bytes: u64) -> (u64, std::io::Result<()>) {
        let mut held = self.held.lock().unwrap();
        held.0 += bytes;
        held.1 = held.1.max(held.0);
        (bytes, Ok(()))
    }
    fn examine(&self, _: u64, _: u64) -> std::io::Result<()> {
        Ok(())
    }
    fn admit_examination(&self, _: u64, _: u64) -> std::io::Result<()> {
        Ok(())
    }
    fn record_examination(&self, _: u64, _: u64) -> std::io::Result<()> {
        Ok(())
    }
    fn release(&self, bytes: u64) {
        self.held.lock().unwrap().0 -= bytes;
    }
    fn admit_bytes(&self, _: usize) -> std::io::Result<()> {
        Ok(())
    }
    fn admit_read(&self, _: usize) -> std::io::Result<()> {
        self.check()
    }
    fn record_read(&self, _: usize) -> std::io::Result<()> {
        Ok(())
    }
    fn admit_write(&self, _: usize) -> std::io::Result<()> {
        self.check()?;
        self.spend(1)
    }
    fn record_write(&self, _: usize) -> std::io::Result<()> {
        Ok(())
    }
    fn admit_durability(&self) -> std::io::Result<()> {
        Ok(())
    }
}

/// A remote-like medium: every operation yields once, as a round trip does,
/// and the peak count of operations in flight together is recorded. A
/// nonempty create of a key naming `slow` takes many round trips.
#[derive(Default)]
struct Latent {
    inner: MemoryMedium,
    flight: std::sync::Mutex<(usize, usize)>,
    slow: std::sync::Mutex<Option<String>>,
    /// Refuses any one engine allocation larger than this.
    allocation: std::sync::Mutex<Option<u64>>,
}

impl Latent {
    /// One round trip.
    async fn trip(&self) {
        {
            let mut flight = self.flight.lock().unwrap();
            flight.0 += 1;
            flight.1 = flight.1.max(flight.0);
        }
        tokio::task::yield_now().await;
        self.flight.lock().unwrap().0 -= 1;
    }

    /// The peak since the last call.
    fn peak(&self) -> usize {
        std::mem::take(&mut self.flight.lock().unwrap().1)
    }
}

#[async_trait::async_trait]
impl Medium for Latent {
    async fn create(&self, key: &str, bytes: OwnedBytes) -> Result<bool, MediumError> {
        let slow = self
            .slow
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|slow| key.contains(slow));
        for _ in 0..if slow && !bytes.is_empty() { 16 } else { 1 } {
            self.trip().await;
        }
        self.inner.create(key, bytes).await
    }
    async fn read(
        &self,
        key: &str,
        range: Option<Range<u64>>,
    ) -> Result<Option<OwnedBytes>, MediumError> {
        self.trip().await;
        self.inner.read(key, range).await
    }
    async fn remove(&self, key: &str) -> Result<bool, MediumError> {
        self.trip().await;
        self.inner.remove(key).await
    }
    async fn replace(
        &self,
        key: &str,
        expected: &[u8],
        bytes: OwnedBytes,
    ) -> Result<bool, MediumError> {
        self.trip().await;
        self.inner.replace(key, expected, bytes).await
    }
    async fn remove_if(&self, key: &str, expected: &[u8]) -> Result<bool, MediumError> {
        self.trip().await;
        self.inner.remove_if(key, expected).await
    }
    async fn list(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, u64)>, MediumError> {
        self.trip().await;
        self.inner.list(prefix, after, limit).await
    }
    async fn flush(&self, keys: &[String]) -> Result<(), MediumError> {
        self.inner.flush(keys).await
    }
    fn volatile(&self) -> bool {
        false
    }
    fn recovered(&self) -> bool {
        false
    }
    fn settle(&self) {}
    fn allocate(&self, bytes: u64) -> Result<(), MediumError> {
        match *self.allocation.lock().unwrap() {
            Some(maximum) if bytes > maximum => {
                Err(MediumError::Failed(std::io::ErrorKind::OutOfMemory.into()))
            }
            _ => Ok(()),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn a_native_batch_has_every_member_in_flight_at_once()
-> Result<(), Box<dyn std::error::Error>> {
    const N: u8 = 16;
    let meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter> =
        std::sync::Arc::new(Budget::new(u64::MAX));
    let bodies = |seed: u8| -> Vec<(OwnedBytes, Vec<Digest>)> {
        (0..N)
            .map(|i| (OwnedBytes::from(vec![seed, i]), Vec::new()))
            .collect()
    };
    let store = Store::new(Latent::default());
    let blobs = store.account(b"account")?;
    // The account's revisions open before any measurement.
    blobs.put(vec![0].into(), Vec::new()).await?;
    store.medium().peak();
    let batch = blobs
        .put_batch_native(
            crate::ObjectsNativeRequest::new(sealed(bodies(1)), (), &meter)?,
            std::sync::Arc::clone(&meter),
        )?
        .await?;
    let mut digests = Vec::new();
    for receipt in batch {
        digests.push(receipt?.put().verified().ok_or("unknown")?.digest);
    }
    assert_eq!(store.medium().peak(), usize::from(N));
    let requests: Vec<_> = digests
        .iter()
        .map(|digest| (*digest, ByteRange::FULL, 8))
        .collect();
    let batch = blobs
        .get_batch_native(
            crate::ObjectsNativeRequest::new(requests, (), &meter)?,
            std::sync::Arc::clone(&meter),
        )?
        .await?;
    for (object, (body, _)) in batch.zip(bodies(1)) {
        assert_eq!(object?.body(), &body);
    }
    assert_eq!(store.medium().peak(), usize::from(N));
    // Negative control: the same work, one member after another.
    for (body, references) in bodies(2) {
        blobs.put(body, references).await?;
    }
    for digest in &digests {
        blobs.get(*digest, ByteRange::FULL, 8).await?;
    }
    assert_eq!(store.medium().peak(), 1);
    Ok(())
}

#[tokio::test]
async fn a_repeat_put_reads_twice_and_writes_nothing() -> Result<(), Box<dyn std::error::Error>> {
    let store = Store::new(Counting::default());
    let blobs = store.account(b"account")?;
    let child = Digest::of(&[1; 8]);
    blobs.put(vec![1; 8].into(), Vec::new()).await?;
    blobs.put(vec![2; 8].into(), vec![child]).await?;
    store.medium().take();
    let again = blobs.put(vec![2; 8].into(), vec![child]).await?;
    assert!(matches!(again, Put::AlreadyExists { .. }));
    let counts = store.medium().take();
    // The failed envelope create, the envelope and the presence probe.
    assert_eq!(counts.get("create"), Some(&1), "{counts:?}");
    assert_eq!(counts.get("read"), Some(&2), "{counts:?}");
    assert_eq!(counts.len(), 2, "{counts:?}");
    // Negative control: a stored envelope whose payload a put still owes is
    // indexed and written.
    let parent = Digest::of(&[2; 8]).to_hex();
    let payload = store
        .medium()
        .inner
        .list(&format!("{}/p/", Digest::of(b"account").to_hex()), None, 8)
        .await?
        .into_iter()
        .find(|(key, _)| key.contains(&parent))
        .ok_or("no payload")?
        .0;
    store.medium().inner.remove(&payload).await?;
    store.medium().take();
    blobs.put(vec![2; 8].into(), vec![child]).await?;
    let counts = store.medium().take();
    assert_eq!(counts.get("create"), Some(&3), "{counts:?}");
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn a_native_batch_member_starts_once_the_members_it_needs_complete()
-> Result<(), Box<dyn std::error::Error>> {
    let meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter> =
        std::sync::Arc::new(Budget::new(u64::MAX));
    let store = Store::new(Latent::default());
    let blobs = store.account(b"account")?;
    blobs.put(vec![0].into(), Vec::new()).await?;
    // The child's writes take many round trips.
    let slow = |seed: u8| {
        *store.medium().slow.lock().unwrap() = Some(Digest::of(&[seed; 8]).to_hex());
    };
    let batch = |seed: u8| -> Vec<(OwnedBytes, Vec<Digest>)> {
        let child = vec![seed; 8];
        let parent = (vec![seed + 1; 8], vec![Digest::of(&child)]);
        vec![
            (child.into(), Vec::new()),
            (parent.0.into(), parent.1),
            (vec![seed + 2; 8].into(), Vec::new()),
        ]
    };
    slow(1);
    let receipts = blobs
        .put_batch_native(
            crate::ObjectsNativeRequest::new(sealed(batch(1)), (), &meter)?,
            std::sync::Arc::clone(&meter),
        )?
        .await?;
    for receipt in receipts {
        assert!(matches!(receipt?.put(), Put::Created { .. }));
    }
    // Negative control: the same members started together, unordered, the
    // parent first.
    let [child, parent, _] = <[_; 3]>::try_from(batch(4)).map_err(|_| "three members")?;
    slow(4);
    let (parent, _) =
        futures::future::join(blobs.put(parent.0, parent.1), blobs.put(child.0, child.1)).await;
    assert_eq!(
        parent.err().map(|error| error.code),
        Some(ErrorCode::MissingReference)
    );
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn a_native_batch_many_times_its_budget_completes_within_it()
-> Result<(), Box<dyn std::error::Error>> {
    const N: usize = 16 * crate::BATCH_WINDOW;
    let bodies = |seed: u8| -> Vec<(OwnedBytes, Vec<Digest>)> {
        (0..N)
            .map(|i| {
                (
                    OwnedBytes::from(vec![seed, i as u8, (i >> 8) as u8]),
                    Vec::new(),
                )
            })
            .collect()
    };
    let store = Store::new(MemoryMedium::default());
    let blobs = store.account(b"account")?;
    blobs.put(vec![0].into(), Vec::new()).await?;
    let put = async |seed: u8, limit: u64, window: usize| {
        let budget = std::sync::Arc::new(Budget::new(limit));
        let meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter> = budget.clone();
        let request = crate::ObjectsNativeRequest::new(sealed(bodies(seed)), (), &meter)?;
        let receipts = crate::native::put_window(&blobs, request, meter, window)?.await?;
        for receipt in receipts {
            assert!(matches!(receipt?.put(), Put::Created { .. }));
        }
        Ok::<_, crate::NativeObjectsError>(budget.peak())
    };
    let budget = put(1, u64::MAX, crate::BATCH_WINDOW).await?;
    // Every member held at once needs many times that budget.
    let whole = put(2, u64::MAX, N).await?;
    assert!(whole > 4 * budget, "{whole} against {budget}");
    let within = put(3, budget, crate::BATCH_WINDOW).await?;
    assert!(within <= budget, "{within} against {budget}");
    // Negative control: the batch holding every member at once is refused.
    let refused = put(4, budget, N)
        .await
        .map(|_| ())
        .err()
        .map(|error| error.into_cause());
    assert!(
        matches!(&refused, Some(crate::NativeObjectsCause::Admission(error)) if error.kind() == std::io::ErrorKind::OutOfMemory),
        "{refused:?}"
    );
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn a_native_retain_runs_inside_the_callers_work() -> Result<(), Box<dyn std::error::Error>> {
    for cancelled in [false, true] {
        let store = Store::new(MemoryMedium::default());
        let blobs = store.account(b"account")?;
        let digest = blobs
            .put(vec![7; 8].into(), Vec::new())
            .await?
            .verified()
            .ok_or("unknown")?
            .digest;
        let budget = std::sync::Arc::new(Budget::new(u64::MAX));
        let meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter> = budget.clone();
        let request = crate::ObjectsNativeRequest::new(
            (RootId::new(b"root".to_vec())?, vec![digest]),
            (),
            &meter,
        )?;
        let future = blobs.retain_native(request, std::sync::Arc::clone(&meter))?;
        budget.cancelled.store(cancelled, Ordering::SeqCst);
        let retained = future.await;
        assert_eq!(retained.is_ok(), !cancelled);
        if !cancelled {
            // The batch body and its index key were charged while held.
            assert!(budget.peak() >= (32 + std::mem::size_of::<String>()) as u64);
        }
        for _ in 0..3 {
            blobs.collect().await?;
        }
        // Negative control: a retain its caller cancelled leaves no root.
        assert_eq!(blobs.head(digest).await.is_ok(), !cancelled);
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn a_retain_short_of_writes_writes_no_index_key() -> Result<(), Box<dyn std::error::Error>> {
    // One reference with one write left: the index key fits, the root batch
    // does not. Negative control: a hollow reservation writes the key, its
    // batch is refused, and the key pins the blob with no batch to release.
    for withholds in [true, false] {
        let store = Store::new(MemoryMedium::default());
        let blobs = store.account(b"account")?;
        let digest = blobs
            .put(vec![7; 8].into(), Vec::new())
            .await?
            .verified()
            .ok_or("unknown")?
            .digest;
        let root = RootId::new(b"root".to_vec())?;
        let budget = std::sync::Arc::new(Budget {
            withholds,
            ..Budget::new(u64::MAX)
        });
        *budget.writes.lock().unwrap() = Some(1);
        let meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter> = budget.clone();
        let request = crate::ObjectsNativeRequest::new((root.clone(), vec![digest]), (), &meter)?;
        let retained = blobs
            .retain_native(request, std::sync::Arc::clone(&meter))?
            .await;
        assert!(retained.is_err());
        blobs.release(&root).await?;
        for _ in 0..3 {
            blobs.collect().await?;
        }
        assert_eq!(blobs.head(digest).await.is_ok(), !withholds);
    }
    Ok(())
}

#[tokio::test]
async fn a_retain_refused_its_memory_writes_no_index_key() -> Result<(), Box<dyn std::error::Error>>
{
    for refused in [false, true] {
        let store = Store::new(Latent::default());
        let blobs = store.account(b"account")?;
        let digest = blobs
            .put(vec![7; 8].into(), Vec::new())
            .await?
            .verified()
            .ok_or("unknown")?
            .digest;
        // The batch body fits; its index-key vector does not.
        if refused {
            *store.medium().allocation.lock().unwrap() = Some(32);
        }
        let retained = blobs
            .retain(&RootId::new(b"root".to_vec())?, vec![digest])
            .await;
        assert_eq!(retained.is_ok(), !refused);
        *store.medium().allocation.lock().unwrap() = None;
        for _ in 0..3 {
            blobs.collect().await?;
        }
        // A refused retain pins nothing: the blob is swept. Negative
        // control: the admitted retain keeps it.
        assert_eq!(blobs.head(digest).await.is_ok(), !refused);
    }
    Ok(())
}

/// Bytes this thread hashed while `work` ran.
#[cfg(feature = "test-support")]
async fn hashed<T>(work: impl Future<Output = T>) -> (T, u64) {
    let before = acyclic_stream::hashed_on_this_thread();
    let value = work.await;
    (value, acyclic_stream::hashed_on_this_thread() - before)
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn a_put_hashes_its_body_once() -> Result<(), Box<dyn std::error::Error>> {
    const LENGTH: u64 = 1 << 20;
    // Storage keys hash a few short names beside the body.
    let once = |hashed: u64, bodies: u64| {
        assert!(
            (bodies * LENGTH..bodies * LENGTH + 4096).contains(&hashed),
            "{hashed} bytes hashed for {bodies} bodies"
        );
    };
    let body = |seed: u8| OwnedBytes::from(vec![seed; LENGTH as usize]);
    let blobs = crate::MemoryObjects::memory();
    blobs.put(vec![0].into(), Vec::new()).await?;
    let (put, bytes) = hashed(blobs.put(body(1), Vec::new())).await;
    put?;
    once(bytes, 1);
    let (puts, bytes) =
        hashed(blobs.put_batch((2..6).map(|seed| (body(seed), Vec::new())).collect())).await;
    for put in puts {
        put?;
    }
    once(bytes, 4);
    // A sealed body is stored without another hash.
    let sealed = crate::Sealed::new(body(6));
    let (put, bytes) = hashed(blobs.put_sealed(sealed, Vec::new())).await;
    put?;
    once(bytes, 0);
    // A received body is hashed once, where it crosses the trust boundary.
    let served = body(8);
    let request = crate::wire::PutBlobRequest {
        digest: Some(Digest::of(&served).into()),
        body: bytes::Bytes::from_owner(served),
        references: Vec::new(),
    };
    let (put, bytes) = hashed(crate::route::put_blob(&blobs, request)).await;
    put?;
    once(bytes, 1);
    // Negative control: a provider that only implements `put` lowers a
    // sealed batch to it, and the body is hashed twice.
    let unsealed = Overlap {
        inner: crate::MemoryObjects::memory(),
        live: Default::default(),
        peak: Default::default(),
    };
    unsealed.put(vec![0].into(), Vec::new()).await?;
    let (puts, bytes) = hashed(unsealed.put_batch(vec![(body(7), Vec::new())])).await;
    for put in puts {
        put?;
    }
    once(bytes, 2);
    Ok(())
}
