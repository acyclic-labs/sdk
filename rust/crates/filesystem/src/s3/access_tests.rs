use super::*;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

thread_local! {
    static OMIT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

pub(super) fn omitted() -> bool {
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

struct Pause {
    remaining: usize,
    entered: tokio::sync::oneshot::Sender<()>,
    release: tokio::sync::oneshot::Receiver<()>,
}

struct Objects {
    inner: crate::MemoryObjectBackend,
    reads: AtomicUsize,
    // Terminal fixture lock; never held while waiting or invoking the backend.
    pause: crate::sync::Mutex<Option<Pause>>,
}

impl Default for Objects {
    fn default() -> Self {
        let (objects, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
        Self {
            inner: crate::MemoryObjectBackend::new(Arc::new(objects), bucket),
            reads: AtomicUsize::new(0),
            pause: crate::sync::Mutex::new(None),
        }
    }
}

impl Objects {
    async fn before_read(&self) -> Result<(), crate::ObjectStoreError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        let pause = {
            let mut slot = self
                .pause
                .lock()
                .map_err(|_| crate::ObjectStoreError::Rejected("fixture lock poisoned".into()))?;
            match slot.as_mut() {
                Some(pause) => {
                    pause.remaining -= 1;
                    if pause.remaining == 0 {
                        slot.take()
                    } else {
                        None
                    }
                }
                None => None,
            }
        };
        if let Some(pause) = pause {
            pause
                .entered
                .send(())
                .map_err(|()| crate::ObjectStoreError::Rejected("reader abandoned pause".into()))?;
            pause.release.await.map_err(|_| {
                crate::ObjectStoreError::Rejected("publication abandoned pause".into())
            })?;
        }
        Ok(())
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
        id: crate::ObjectId,
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
        id: crate::ObjectId,
        maximum: u64,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::ObjectRead, crate::ObjectStoreError> {
        self.before_read().await?;
        self.inner.read(id, maximum, work).await
    }
    async fn read_many(
        &self,
        requests: &[crate::storage::ObjectReadRequest],
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<crate::ObjectRead>, crate::AuthorityAllocation), crate::ObjectStoreError> {
        for _request in requests {
            self.before_read().await?;
        }
        self.inner.read_many(requests, work).await
    }
    async fn contains(
        &self,
        id: crate::ObjectId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<bool, crate::ObjectStoreError> {
        self.inner.contains(id, work).await
    }
}

type Workspace = crate::Workspace<crate::MemoryAuthorityBackend, Arc<Objects>>;

async fn fixture() -> Result<(Workspace, Arc<Objects>), Box<dyn std::error::Error>> {
    let objects = Arc::new(Objects::default());
    let filesystem = crate::Fs::new(
        crate::StreamAuthorityStore::new(Arc::new(acyclic_stream::MemoryStream::default())),
        objects.clone(),
        crate::EmbeddedCapabilities::MEMORY,
    );
    let workspace = filesystem
        .create_workspace_with_operation::<_, (), WorkspaceError>(
            "s3-revocation",
            crate::VolumeInitialization {
                config: crate::VolumeConfig::portable(crate::Lifecycle::Ephemeral),
                access_policy: Some(crate::access::INITIAL_POLICY),
            },
            crate::OperationId::new(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
            None,
        )
        .await?
        .value
        .workspace();
    workspace
        .write_text(
            "/file",
            &"a".repeat(crate::kernel::MAXIMUM_INLINE_FILE_BYTES + 1),
        )
        .await?;
    Ok((workspace, objects))
}

#[derive(Clone, Copy, Debug)]
enum Read {
    Range,
    RangeAt,
    Head,
    HeadAt,
    List,
    ListAt,
}

async fn read(
    view: &S3Workspace<crate::MemoryAuthorityBackend, Arc<Objects>>,
    generation: &crate::Generation<crate::MemoryAuthorityBackend, Arc<Objects>>,
    operation: Read,
) -> Result<(), S3Error> {
    let range = ByteRange {
        offset: 0,
        length: 1,
    };
    match operation {
        Read::Range => {
            view.get_object_range("file", range).await?;
        }
        Read::RangeAt => {
            view.get_object_range_at(generation, "file", range).await?;
        }
        Read::Head => {
            view.head_object("file").await?;
        }
        Read::HeadAt => {
            view.head_object_at(generation, "file").await?;
        }
        Read::List => {
            view.list_objects(S3ListOptions::default()).await?;
        }
        Read::ListAt => {
            view.list_objects_at(generation, S3ListOptions::default())
                .await?;
        }
    }
    Ok(())
}

async fn pending(
    operation: Read,
    broken: bool,
    revoked: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (workspace, objects) = fixture().await?;
    let scoped = workspace
        .clone()
        .bind_coarse_access(&crate::access::Action::ALL, &CancellationToken::new())
        .await?;
    let generation = scoped
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await?
        .value;
    let view = scoped.s3();
    objects.reads.store(0, Ordering::Relaxed);
    {
        // Count storage reads before the return fence, then pause the last one.
        let _omit = Omit::enter(true);
        read(&view, &generation, operation).await?;
    }
    let remaining = objects.reads.load(Ordering::Relaxed);
    assert_ne!(remaining, 0, "no storage read for {operation:?}");
    let (entered, waiting) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    *objects.pause.lock().map_err(|_| "fixture lock poisoned")? = Some(Pause {
        remaining,
        entered,
        release: released,
    });
    let reader = async {
        let _omit = Omit::enter(broken);
        read(&view, &generation, operation).await
    };
    let publisher = async {
        waiting.await?;
        let result = if revoked {
            crate::access::fixtures::publish_policy(
                &workspace,
                &crate::access::fixtures::denied(crate::access::Action::Read, "**"),
            )
            .await
        } else {
            workspace
                .write_text("/unrelated", "publication")
                .await
                .map(|_| ())
                .map_err(Into::into)
        };
        release.send(()).map_err(|()| "reader abandoned pause")?;
        result
    };
    let (result, publication) = tokio::join!(reader, publisher);
    publication?;
    if revoked {
        assert!(
            matches!(
                result,
                Err(S3Error::Workspace(WorkspaceError::AccessDenied))
            ),
            "S3 result escaped revocation: {operation:?} {result:?}"
        );
        // An old generation must also reject every subsequent chunk.
        assert!(matches!(
            view.get_object_range_at(
                &generation,
                "file",
                ByteRange {
                    offset: 1,
                    length: 1
                }
            )
            .await,
            Err(S3Error::Workspace(WorkspaceError::AccessDenied))
        ));
    } else {
        assert!(
            result.is_ok(),
            "data-only publication invalidated S3 read: {result:?}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn every_s3_disclosure_rechecks_pending_storage_and_next_chunk()
-> Result<(), Box<dyn std::error::Error>> {
    for operation in [
        Read::Range,
        Read::RangeAt,
        Read::Head,
        Read::HeadAt,
        Read::List,
        Read::ListAt,
    ] {
        pending(operation, false, true).await?;
    }
    Ok(())
}

#[tokio::test]
async fn data_only_publication_preserves_s3_results() -> Result<(), Box<dyn std::error::Error>> {
    for operation in [
        Read::Range,
        Read::RangeAt,
        Read::Head,
        Read::HeadAt,
        Read::List,
        Read::ListAt,
    ] {
        pending(operation, false, false).await?;
    }
    Ok(())
}

#[tokio::test]
#[should_panic(expected = "S3 result escaped revocation")]
async fn removed_s3_return_fence_negative_control() {
    assert!(pending(Read::RangeAt, true, true).await.is_ok());
}
