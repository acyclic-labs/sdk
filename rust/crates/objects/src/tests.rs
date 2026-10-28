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
    for _ in 0..3 {
        passes.push(blobs.collect().await?);
    }
    let sweep = passes
        .iter()
        .position(|pass| pass.swept > 0)
        .filter(|sweep| *sweep < 2)
        .ok_or("an unretained blob is swept")?;
    let swept = passes[sweep].revision.clone().ok_or("a sweep commits")?;
    assert!(swept.index > released.index);
    // Negative control: a pass that sweeps nothing commits nothing.
    assert_eq!(passes[2].swept, 0);
    assert_eq!(passes[2].revision.as_ref(), Some(&swept));
    Ok(())
}

#[tokio::test]
async fn stores_sharing_a_medium_serve_each_others_revisions()
-> Result<(), Box<dyn std::error::Error>> {
    use acyclic_stream::{RevisionCut, StreamError};
    let medium = MemoryMedium::default();
    let first = Store::new(medium.clone()).account(b"shared")?;
    let second = Store::new(medium).account(b"shared")?;
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
}

#[tokio::test]
async fn ambiguous_publication_is_unknown_and_retry_safe() -> Result<(), Box<dyn std::error::Error>>
{
    let store = Store::new(Faulty::default());
    let blobs = store.account(b"account")?;
    let body = OwnedBytes::from(vec![1; 64]);
    // The fault lands on the publication, not on opening the revisions.
    blobs.revisions().await?;
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
/// interleaving. With `forgetful` the delete protocol's conditional writes,
/// on published envelopes, ignore the bytes they were given: a deleter's mark
/// is never stored and its removal matches whatever is there, the negative
/// control for the delete protocol.
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

    fn forgets(&self, expected: &[u8]) -> bool {
        self.forgetful && crate::blob::Envelope::published_size(expected.len() as u64)
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
        let replaced = if self.forgets(expected) {
            Ok(self.inner.read(key, Some(0..0)).await?.is_some())
        } else {
            self.inner.replace(key, expected, bytes).await
        };
        self.after("replace", key).await;
        replaced
    }
    async fn remove_if(&self, key: &str, expected: &[u8]) -> Result<bool, MediumError> {
        let removed = if self.forgets(expected) {
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

/// Whether any payload of `digest` is stored.
async fn payload_stored(medium: &Shared, digest: Digest) -> Result<bool, MediumError> {
    let hex = digest.to_hex();
    Ok(Medium::list(
        medium,
        &format!("{}/", Digest::of(b"account").to_hex()),
        None,
        100_000,
    )
    .await?
    .iter()
    .any(|(key, _)| key.contains("/p/") && key.contains(&hex)))
}

/// A put paused between its payload and its publication while an independent
/// store collects: the put either publishes a payload GET verifies, or
/// reports its outcome unknown; never a lost payload created.
async fn stalled_put(
    medium: Shared,
    passes: usize,
    fill: u8,
) -> Result<Put, Box<dyn std::error::Error>> {
    let writer = Store::new(medium.clone()).account(b"account")?;
    let collector = Store::new(medium.clone()).account(b"account")?;
    let body = OwnedBytes::from(vec![fill; 64]);
    let digest = Digest::of(&body);
    let (reached, release) = medium.park("create", "/p/");
    let put = tokio::spawn({
        let writer = writer.clone();
        let body = body.clone();
        async move { writer.put(body, Vec::new()).await }
    });
    reached.notified().await;
    for _ in 0..passes {
        collector.collect().await?;
    }
    let fenced = !payload_stored(&medium, digest).await?;
    release.notify_one();
    let outcome = put.await??;
    match &outcome {
        Put::Created { .. } => {
            assert!(!fenced, "a removed payload is never reported created");
            let read = writer.get(digest, ByteRange::FULL, 64).await?;
            assert_eq!(read.body().as_ref(), body.as_ref());
        }
        Put::Unknown => {
            assert_eq!(
                writer.head(digest).await.map_err(|error| error.code),
                Err(ErrorCode::NotFound)
            );
            // Negative control: the collector really removed the payload,
            // and nothing fences an unconditional envelope create, so the
            // unfenced publish would have acknowledged a lost payload.
            assert!(fenced);
            let hex = digest.to_hex();
            assert!(
                !Medium::list(
                    &medium,
                    &format!("{}/", Digest::of(b"account").to_hex()),
                    None,
                    100_000
                )
                .await?
                .iter()
                .any(|(key, _)| key.contains("/e/") && key.contains(&hex))
            );
            let retried = writer.put(body.clone(), Vec::new()).await?;
            assert!(matches!(retried, Put::Created { .. }));
            let read = writer.get(digest, ByteRange::FULL, 64).await?;
            assert_eq!(read.body().as_ref(), body.as_ref());
        }
        Put::AlreadyExists { .. } => return Err("nothing else published this digest".into()),
    }
    Ok(outcome)
}

#[tokio::test]
async fn a_put_stalled_across_collections_never_publishes_a_lost_payload()
-> Result<(), Box<dyn std::error::Error>> {
    // Two passes fence the put's admission and revoke it.
    assert_eq!(stalled_put(Shared::default(), 2, 5).await?, Put::Unknown);
    // Control: one pass keeps the admission, and the put publishes.
    assert!(matches!(
        stalled_put(Shared::default(), 1, 6).await?,
        Put::Created { .. }
    ));
    Ok(())
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
#[tokio::test]
async fn a_local_put_stalled_across_collections_never_publishes_a_lost_payload()
-> Result<(), Box<dyn std::error::Error>> {
    for (passes, created) in [(2, false), (1, true)] {
        let root = tempfile::tempdir()?;
        let local = crate::LocalMedium::open(root.path(), Default::default(), None).await?;
        let outcome = stalled_put(Shared::over(local, false), passes, 7).await?;
        assert_eq!(matches!(outcome, Put::Created { .. }), created);
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
    // The second pass parks right after revoking, before removing anything.
    let (revoked, resume) = medium.park("remove", "/e/");
    let collecting = tokio::spawn({
        let collector = collector.clone();
        async move { collector.collect().await }
    });
    revoked.notified().await;
    release.notify_one();
    assert_eq!(put.await??, Put::Unknown);
    // A later put of the same digest publishes a fresh version meanwhile.
    assert!(matches!(
        writer.put(body.clone(), Vec::new()).await?,
        Put::Created { .. }
    ));
    resume.notify_one();
    collecting.await??;
    let read = writer.get(digest, ByteRange::FULL, 64).await?;
    assert_eq!(read.body().as_ref(), body.as_ref());
    // Negative control: the revoked version's payload really was removed.
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
fn pending_envelopes_round_trip_and_list_by_size() -> Result<(), Box<dyn std::error::Error>> {
    let envelope = crate::blob::Envelope {
        kind: crate::Kind::Blob,
        length: 3,
        stored: 3,
        references: vec![Digest::of(b"child")],
        version: [4; 16],
        dying: None,
        pending: true,
    };
    let pending = envelope.encode();
    assert_eq!(crate::blob::Envelope::decode(&pending)?, envelope);
    assert!(!crate::blob::Envelope::published_size(pending.len() as u64));
    let published = crate::blob::Envelope {
        pending: false,
        ..envelope
    }
    .encode();
    assert!(crate::blob::Envelope::published_size(published.len() as u64));
    assert_eq!(published.len(), pending.len() + 16);
    Ok(())
}

#[tokio::test]
async fn a_retry_takes_over_an_abandoned_pending_put() -> Result<(), Box<dyn std::error::Error>> {
    let medium = Shared::default();
    let first = Store::new(medium.clone()).account(b"account")?;
    let retry = Store::new(medium.clone()).account(b"account")?;
    let body = OwnedBytes::from(vec![9; 64]);
    let digest = Digest::of(&body);
    let (reached, release) = medium.park("create", "/p/");
    let stalled = tokio::spawn({
        let first = first.clone();
        let body = body.clone();
        async move { first.put(body, Vec::new()).await }
    });
    reached.notified().await;
    // The stalled put's pending envelope never blocks a retry of its digest.
    assert!(matches!(
        retry.put(body.clone(), Vec::new()).await?,
        Put::Created { .. }
    ));
    release.notify_one();
    // Negative control: the overtaken put can no longer publish.
    assert_eq!(stalled.await??, Put::Unknown);
    let read = retry.get(digest, ByteRange::FULL, 64).await?;
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
    assert_eq!(payloads, 1);
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
    let (reached, release) = medium.park("create", "/p/");
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

/// A put paused right after publishing while an independent store collects.
async fn stalled_after_publish(passes: usize) -> Result<Put, Box<dyn std::error::Error>> {
    let medium = Shared::default();
    let writer = Store::new(medium.clone()).account(b"account")?;
    let collector = Store::new(medium.clone()).account(b"account")?;
    let body = OwnedBytes::from(vec![10; 64]);
    let digest = Digest::of(&body);
    let (reached, release) = medium.park("replace", "/e/");
    let put = tokio::spawn({
        let writer = writer.clone();
        let body = body.clone();
        async move { writer.put(body, Vec::new()).await }
    });
    reached.notified().await;
    for _ in 0..passes {
        collector.collect().await?;
    }
    release.notify_one();
    let outcome = put.await??;
    if matches!(outcome, Put::Created { .. }) {
        let read = writer.get(digest, ByteRange::FULL, 64).await?;
        assert_eq!(read.body().as_ref(), body.as_ref());
    }
    Ok(outcome)
}

#[tokio::test]
async fn a_put_swept_before_it_returns_is_never_created() -> Result<(), Box<dyn std::error::Error>>
{
    // Two passes fence the admission and sweep the unrooted blob.
    assert_eq!(stalled_after_publish(2).await?, Put::Unknown);
    // Control: one pass keeps it, and the re-admitted put returns created.
    assert!(matches!(
        stalled_after_publish(1).await?,
        Put::Created { .. }
    ));
    Ok(())
}

/// PLATFORM-26/32: a FullFlush create is acknowledged only after the shared
/// native namespace flush of its directory succeeds, on every platform.
#[cfg(all(
    feature = "local",
    feature = "test-support",
    not(target_arch = "wasm32")
))]
#[tokio::test]
async fn a_local_create_never_succeeds_past_a_failed_directory_flush()
-> Result<(), Box<dyn std::error::Error>> {
    use acyclic_native_runtime::testing::{DurableFault, Schedule};
    acyclic_native_runtime::initialize_native_io_services()?;
    let scheduled = async |fault: Option<(usize, DurableFault)>| {
        let root = tempfile::tempdir().unwrap();
        let medium = crate::LocalMedium::open(root.path(), Default::default(), None)
            .await
            .unwrap();
        let (schedule, mut events) = Schedule::new();
        if let Some((ordinal, fault)) = fault {
            schedule.cut_at(ordinal, fault);
        }
        let driver = tokio::spawn(async move {
            while let Some(boundary) = events.recv().await {
                boundary.release();
            }
        });
        let created = schedule
            .scope(medium.create("a/b/key", OwnedBytes::from(vec![7; 16])))
            .await;
        schedule.drained().await;
        driver.abort();
        let _ = driver.await;
        (created, schedule.durable_writes())
    };
    let (created, trace) = scheduled(None).await;
    assert!(created?);
    let published = trace
        .iter()
        .position(|call| call.operation == "namespace:publish_new")
        .ok_or("the create publishes through the shared native link")?;
    // Negative control: a no-op directory adapter records no flush here.
    let flush = trace[published..]
        .iter()
        .find(|call| call.operation == "file:sync_parent")
        .ok_or("the published name's directory is flushed")?
        .ordinal;
    for fault in [DurableFault::Failed, DurableFault::After] {
        let (created, _) = scheduled(Some((flush, fault))).await;
        assert!(
            matches!(created, Err(MediumError::Ambiguous(_))),
            "{fault:?}: a visible name whose directory flush failed is ambiguous, never created"
        );
    }
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
    // Admission protects one cut; the next sweeps every unrooted page.
    let first = blobs.collect().await?.swept;
    let second = blobs.collect().await?.swept;
    assert_eq!(first + second, count as u64 - 1);
    // Negative control: the rooted blob on the first page survives.
    assert_eq!(blobs.usage().await?.blobs, 1);
    assert_eq!(blobs.head(kept).await?.digest, kept);
    Ok(())
}
