use super::*;
use std::cell::Cell;
use std::sync::{atomic::AtomicUsize, mpsc};

thread_local! {
    static OMIT_DEADLINE: Cell<bool> = const { Cell::new(false) };
    static OMIT: Cell<bool> = const { Cell::new(false) };
    static NOW: Cell<Option<u64>> = const { Cell::new(None) };
    static SKIP_RETRY: Cell<bool> = const { Cell::new(false) };
    static OBSERVE_DATA: Cell<bool> = const { Cell::new(false) };
}

pub(in crate::native_mount::adapter) fn override_unix_seconds() -> Option<u64> {
    NOW.get()
}

pub(in crate::native_mount::adapter) fn skip_retry() -> bool {
    SKIP_RETRY.get()
}

struct Retry(bool);
impl Retry {
    fn enter(broken: bool) -> Self {
        Self(SKIP_RETRY.replace(broken))
    }
}
impl Drop for Retry {
    fn drop(&mut self) {
        SKIP_RETRY.set(self.0);
    }
}

pub(in crate::native_mount::adapter) fn omitted() -> bool {
    OMIT.get()
}

struct Omit(bool);
impl Omit {
    fn enter(value: bool) -> Self {
        Self(OMIT.replace(value))
    }
}
impl Drop for Omit {
    fn drop(&mut self) {
        OMIT.set(self.0);
    }
}

pub(in crate::native_mount) fn with_omitted<T>(broken: bool, run: impl FnOnce() -> T) -> T {
    let _old = Omit::enter(broken);
    run()
}

struct Pause {
    remaining: usize,
    entered: mpsc::Sender<()>,
    release: tokio::sync::oneshot::Receiver<()>,
    after: Option<u64>,
}

struct Objects {
    inner: crate::MemoryObjectBackend,
    reads: AtomicUsize,
    data_reads: AtomicUsize,
    // Terminal fixture lock: release before either channel operation or await.
    pause: crate::sync::Mutex<Option<Pause>>,
}

impl Default for Objects {
    fn default() -> Self {
        let (objects, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
        Self {
            inner: crate::MemoryObjectBackend::new(Arc::new(objects), bucket),
            reads: AtomicUsize::new(0),
            data_reads: AtomicUsize::new(0),
            pause: crate::sync::Mutex::new(None),
        }
    }
}

impl Objects {
    fn data_read(&self, id: ObjectId) {
        if id.kind == crate::ObjectKind::BlobChunk && OBSERVE_DATA.get() {
            self.data_reads.fetch_add(1, Ordering::Relaxed);
        }
    }
    async fn before_read(&self) {
        self.reads.fetch_add(1, Ordering::Relaxed);
        let pause = {
            let mut slot = self.pause.lock().unwrap();
            if let Some(pause) = slot.as_mut() {
                pause.remaining -= 1;
                if pause.remaining == 0 {
                    slot.take()
                } else {
                    None
                }
            } else {
                None
            }
        };
        if let Some(pause) = pause {
            pause.entered.send(()).unwrap();
            pause.release.await.unwrap();
            NOW.set(pause.after);
        }
    }
}

impl AsyncObjectStore for Objects {
    fn supports_decoded_cache(&self) -> bool {
        self.inner.supports_decoded_cache()
    }
    async fn put_hashed(
        &self,
        object: crate::storage::HashedObject,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        self.inner.put_hashed(object, work).await
    }
    async fn put(
        &self,
        id: ObjectId,
        bytes: OwnedBytes,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        self.inner.put(id, bytes, work).await
    }
    async fn put_many(
        &self,
        writes: &[crate::storage::ObjectWrite],
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        self.inner.put_many(writes, work).await
    }
    async fn put_many_hashed(
        &self,
        objects: &[crate::storage::HashedObject],
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        self.inner.put_many_hashed(objects, work).await
    }
    async fn flush_before_publish(
        &self,
        scope: crate::PublicationScope<'_>,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::PublicationHold, crate::ObjectStoreError> {
        self.inner.flush_before_publish(scope, work).await
    }
    async fn read(
        &self,
        id: ObjectId,
        maximum: u64,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::ObjectRead, crate::ObjectStoreError> {
        self.data_read(id);
        self.before_read().await;
        self.inner.read(id, maximum, work).await
    }
    async fn read_many(
        &self,
        requests: &[crate::storage::ObjectReadRequest],
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<crate::ObjectRead>, crate::AuthorityAllocation), crate::ObjectStoreError> {
        for request in requests {
            self.data_read(request.object_id);
            self.before_read().await;
        }
        self.inner.read_many(requests, work).await
    }
    async fn contains(
        &self,
        id: ObjectId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<bool, crate::ObjectStoreError> {
        self.inner.contains(id, work).await
    }
}

type Fixture = (Arc<Source>, Arc<Objects>, Arc<dyn MountOpenFile>);
type Source = CheckoutMountSource<crate::MemoryAuthorityBackend, Arc<Objects>>;
const PAYLOAD_BYTES: usize = crate::kernel::MAXIMUM_INLINE_FILE_BYTES + 1;

fn fixture(expires: Option<u64>) -> Result<Fixture, Box<dyn std::error::Error>> {
    let objects = Arc::new(Objects::default());
    let fs = Fs::new(
        crate::StreamAuthorityStore::new(Arc::new(acyclic_stream::MemoryStream::default())),
        objects.clone(),
        crate::EmbeddedCapabilities::MEMORY,
    );
    let profile = if cfg!(unix) {
        FilesystemProfile::Posix
    } else {
        FilesystemProfile::Portable
    };
    let config = VolumeConfig {
        profile,
        ..VolumeConfig::portable(Lifecycle::Ephemeral)
    };
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let cancel = CancellationToken::new();
    let checkout = runtime.block_on(async {
        fs.create_volume(config, WorkBudget::UNBOUNDED, &cancel)
            .await?
            .value
            .checkout(
                GenerationSelector::Head,
                CheckoutMode {
                    access: AccessMode::ReadWrite,
                    consistency: ConsistencyMode::Pinned,
                    mutations: MutationMode::PrivateOverlay,
                },
                WorkBudget::UNBOUNDED,
                &cancel,
            )
            .await
            .map(|r| r.value)
    })?;
    let source = access::bind_test_access(
        CheckoutMountSource::new(
            Arc::new(SharedCheckout::with_publication(
                checkout,
                MountPublication::Manual,
            )),
            config,
        )?,
        vec![],
        None,
    )?;
    let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
    let path = native_test_path("file");
    let file = source.create_file(&path, metadata(), None, &work)?;
    source
        .open_created(&path, &file)?
        .write_range(0, Bytes::from(vec![7; PAYLOAD_BYTES]))?;
    #[cfg(unix)]
    source.write_attribute(
        &path,
        b"user.secret",
        crate::kernel::OwnedSourceChunk::intrinsic(OwnedBytes::from_static(b"attribute"), 0),
        MountAttributeWriteMode::Create,
        &work,
    )?;
    source.sync()?;
    let source = if let Some(expires_at) = expires {
        let capability = crate::access::Capability {
            layers: vec![vec![crate::access::Grant {
                actions: crate::access::Action::ALL.into_iter().collect(),
                resource: crate::access::ResourcePattern {
                    lineage_root: None,
                    volume: None,
                    generations: crate::access::Generations::All,
                    path: "**".into(),
                },
            }]],
            expires_at: Some(expires_at),
        };
        access::bind_test_access(source, vec![], Some(capability))?
    } else {
        source
    };
    let pinned = source.open_file(&path)?;
    Ok((Arc::new(source), objects, pinned))
}

#[derive(Clone, Copy, Debug)]
enum Read {
    Bytes,
    Pinned,
    Metadata,
    Directory,
    Open,
    Detached,
    Seek,
    #[cfg(unix)]
    Attribute,
    #[cfg(unix)]
    Attributes,
}

fn read(source: &Source, pinned: &dyn MountOpenFile, op: Read) -> Result<(), MountSourceError> {
    let path = native_test_path("file");
    let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
    let length = u32::try_from(PAYLOAD_BYTES).map_err(|_| MountSourceError::Stale)?;
    match op {
        Read::Bytes => assert_eq!(
            source.read_range(&path, 0, length)?.as_ref(),
            vec![7; PAYLOAD_BYTES]
        ),
        Read::Pinned => assert_eq!(
            pinned.read_range(0, length)?.as_ref(),
            vec![7; PAYLOAD_BYTES]
        ),
        Read::Metadata => assert_eq!(
            source.lookup(&path)?.unwrap().node.kind,
            MountNodeKind::Regular
        ),
        Read::Directory => assert_eq!(
            source
                .read_directory(&MountPath::root(), None, 8)?
                .entries
                .len(),
            1
        ),
        Read::Open => {
            source.open_file_with_lookup_with_meter(&path, MountOpenAccess::Read, &work)?;
        }
        Read::Detached => {
            source.detach_file_with_meter(&path, &work)?;
        }
        Read::Seek => assert_eq!(source.seek(&path, 0, MountSeekTarget::Data)?, Some(0)),
        #[cfg(unix)]
        Read::Attribute => assert_eq!(
            source
                .read_attribute(&path, b"user.secret", &work)?
                .unwrap()
                .bytes
                .as_ref(),
            b"attribute"
        ),
        #[cfg(unix)]
        Read::Attributes => assert_eq!(
            source.list_attributes(&path, None, 8)?.names,
            vec![b"user.secret".to_vec()]
        ),
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum Change {
    Policy,
    View,
    Expiry,
    Data,
}

fn revoked_return(
    op: Read,
    broken: bool,
    change: Change,
) -> Result<(), Box<dyn std::error::Error>> {
    let expiry = if matches!(change, Change::Expiry) {
        Some(
            super::super::access::access_time(&crate::native_mount::clock::Clock::capture())?
                + 3600,
        )
    } else {
        None
    };
    let (source, objects, pinned) = fixture(expiry)?;
    // Calibrate the last real storage read of this exact operation without the
    // return fence. Pausing that read establishes authorization-before-storage,
    // rather than pausing the policy lookup that precedes authorization.
    objects.reads.store(0, Ordering::Relaxed);
    {
        let _old = Omit::enter(!matches!(change, Change::Data));
        read(&source, pinned.as_ref(), op)?;
    }
    let remaining = objects.reads.load(Ordering::Relaxed);
    assert_ne!(remaining, 0);
    let (entered, pending) = mpsc::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    *objects
        .pause
        .lock()
        .map_err(|_| "fixture pause lock poisoned")? = Some(Pause {
        remaining,
        entered,
        release: released,
        after: expiry,
    });
    let worker = std::thread::spawn({
        let source = source.clone();
        let pinned = pinned.clone();
        move || {
            OBSERVE_DATA.set(true);
            let _old = Omit::enter(broken && !matches!(change, Change::Data));
            let _retry = Retry::enter(broken && matches!(change, Change::Data));
            read(&source, pinned.as_ref(), op)
        }
    });
    pending.recv_timeout(Duration::from_secs(2))?;
    let policy = crate::access::fixtures::denied(crate::access::Action::Read, "**");
    let publication = (|| -> Result<(), Box<dyn std::error::Error>> {
        if matches!(change, Change::Policy) {
            super::policy_refresh::publish(&source, &policy)?;
        } else if matches!(change, Change::View) {
            let view = access::compiled(
                &source,
                2,
                vec![access::deny("revoked", &crate::access::Action::ALL, "**")],
                None,
            )?;
            source
                .access_view
                .as_ref()
                .ok_or("missing fixture access view")?
                .publish(view)?;
        } else if matches!(change, Change::Data) {
            let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
            source.create_file(&native_test_path("unrelated"), metadata(), None, &work)?;
            source.sync()?;
        }
        Ok(())
    })();
    release
        .send(())
        .map_err(|()| "fixture read abandoned its pause")?;
    let result = worker.join().map_err(|_| "native read worker panicked")?;
    publication?;
    if matches!(change, Change::Data) {
        assert!(
            result.is_ok() && objects.data_reads.load(Ordering::Relaxed) == 1,
            "data-only commit invalidated native result: {result:?}"
        );
        return Ok(());
    }
    assert!(
        matches!(result, Err(MountSourceError::AccessDenied)),
        "native result escaped a revoked cut: {op:?} {result:?}"
    );
    Ok(())
}

#[test]
fn every_native_result_rechecks_authority_after_its_storage_read()
-> Result<(), Box<dyn std::error::Error>> {
    for op in [
        Read::Bytes,
        Read::Pinned,
        Read::Metadata,
        Read::Directory,
        Read::Open,
        Read::Detached,
        Read::Seek,
        #[cfg(unix)]
        Read::Attribute,
        #[cfg(unix)]
        Read::Attributes,
    ] {
        revoked_return(op, false, Change::Policy)?;
        revoked_return(op, false, Change::View)?;
    }
    Ok(())
}

#[test]
#[should_panic(expected = "native result escaped a revoked cut")]
fn removed_return_fence_negative_control() {
    revoked_return(Read::Bytes, true, Change::Policy).unwrap();
}

#[test]
#[should_panic(expected = "native result escaped a revoked cut")]
fn removed_pinned_policy_fence_negative_control() {
    revoked_return(Read::Pinned, true, Change::Policy).unwrap();
}

#[test]
fn expiry_during_storage_denies_fresh_and_pinned_returns() -> Result<(), Box<dyn std::error::Error>>
{
    for op in [
        Read::Bytes,
        Read::Pinned,
        Read::Metadata,
        Read::Directory,
        Read::Open,
        Read::Detached,
        Read::Seek,
        #[cfg(unix)]
        Read::Attribute,
        #[cfg(unix)]
        Read::Attributes,
    ] {
        revoked_return(op, false, Change::Expiry)?;
    }
    Ok(())
}

#[test]
#[should_panic(expected = "native result escaped a revoked cut")]
fn removed_pinned_expiry_fence_negative_control() {
    revoked_return(Read::Pinned, true, Change::Expiry).unwrap();
}

#[test]
fn policy_observation_retries_data_only_publication_without_rereading_data()
-> Result<(), Box<dyn std::error::Error>> {
    revoked_return(Read::Bytes, false, Change::Data)?;
    revoked_return(Read::Pinned, false, Change::Data)
}

#[test]
#[should_panic(expected = "data-only commit invalidated native result")]
fn missing_policy_observation_retry_negative_control() {
    revoked_return(Read::Bytes, true, Change::Data).unwrap();
}

pub(in crate::native_mount::adapter) fn omitted_deadline() -> bool {
    OMIT_DEADLINE.get()
}

struct ClockRestore(Option<u64>);
impl Drop for ClockRestore {
    fn drop(&mut self) {
        NOW.set(self.0);
    }
}

fn queued_expiry(kind: usize, omitted: bool) -> Result<(), Box<dyn std::error::Error>> {
    let _restore = ClockRestore(NOW.replace(Some(100)));
    let (source, _, file) = fixture(Some(200))?;
    let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
    match kind {
        0 => file.write_range(0, Bytes::from_static(b"changed"))?,
        1 => file.write_range_with_meter(
            0,
            crate::kernel::OwnedSourceChunk::intrinsic(OwnedBytes::from_static(b"changed"), 0),
            &work,
        )?,
        2 => file.append_range(PAYLOAD_BYTES as u64, Bytes::from_static(b"changed"))?,
        3 => file.append_range_with_meter(
            PAYLOAD_BYTES as u64,
            crate::kernel::OwnedSourceChunk::intrinsic(OwnedBytes::from_static(b"changed"), 0),
            &work,
        )?,
        4 => file.resize(1)?,
        5 => file.resize_with_meter(1, &work)?,
        6 => file.allocate_range(0, 1, MountRangeAllocation::ZeroRange { extend: false })?,
        7 => file.allocate_range_with_meter(
            0,
            1,
            MountRangeAllocation::ZeroRange { extend: false },
            &work,
        )?,
        8 => {
            let metadata = file.lookup()?.metadata;
            file.set_attributes(metadata, Some(1), &work)?;
        }
        9 => source.write_range(&native_test_path("file"), 0, Bytes::from_static(b"changed"))?,
        10 => source.resize(&native_test_path("file"), 1)?,
        11 => {
            let range = file.read_sparse_range(0, 1, &work)?;
            file.write_sparse_range(0, range, &work)?;
        }
        12 => file.clone_range_from(file.as_ref(), 0, 1, 1, &work)?,
        #[cfg(unix)]
        13 => file.write_attribute(
            b"user.secret",
            crate::kernel::OwnedSourceChunk::intrinsic(OwnedBytes::from_static(b"changed"), 0),
            MountAttributeWriteMode::Replace,
            &work,
        )?,
        #[cfg(unix)]
        14 => file.remove_attribute(b"user.secret", &work)?,
        _ => unreachable!(),
    }
    NOW.set(Some(200));
    OMIT_DEADLINE.set(omitted);
    let result = source.sync();
    OMIT_DEADLINE.set(false);
    assert!(
        matches!(result, Err(MountSourceError::AccessDenied)),
        "expired queued mutation published: {result:?}"
    );
    Ok(())
}

#[test]
fn manual_publication_retains_original_write_deadlines() -> Result<(), Box<dyn std::error::Error>> {
    for kind in 0..13 {
        queued_expiry(kind, false)?;
    }
    #[cfg(unix)]
    for kind in 13..15 {
        queued_expiry(kind, false)?;
    }
    Ok(())
}

#[test]
#[should_panic(expected = "expired queued mutation published")]
fn removed_pending_deadline_negative_control() {
    queued_expiry(0, true).unwrap();
}

#[test]
fn unexpired_queued_mutation_can_publish() -> Result<(), Box<dyn std::error::Error>> {
    let _restore = ClockRestore(NOW.replace(Some(100)));
    let (source, _, file) = fixture(Some(200))?;
    file.write_range(0, Bytes::from_static(b"changed"))?;
    source.sync()?;
    assert_eq!(file.read_range(0, 7)?.as_ref(), b"changed");
    Ok(())
}
