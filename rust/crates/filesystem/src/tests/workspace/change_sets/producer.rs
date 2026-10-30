//! Real immutable-store reads interrupted at their original asynchronous boundary.
use super::*;
use crate::{
    AuthorityAllocation, AuthorityWork, ObjectId, ObjectRead, ObjectReadRequest, ObjectStoreError,
};
use std::sync::Mutex;
use tokio::sync::oneshot;

#[derive(Default)]
struct Objects {
    inner: crate::MemoryObjectStore,
    pending: Mutex<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>>,
}

impl Objects {
    fn pause_next_read(&self) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (started, entered) = oneshot::channel();
        let (release, released) = oneshot::channel();
        assert!(
            self.pending
                .lock()
                .unwrap()
                .replace((started, released))
                .is_none()
        );
        (entered, release)
    }

    async fn before_read(&self) -> Result<(), ObjectStoreError> {
        let pending = self.pending.lock().unwrap().take();
        if let Some((started, released)) = pending {
            started
                .send(())
                .map_err(|_| ObjectStoreError::Rejected("read observer lost".into()))?;
            released
                .await
                .map_err(|_| ObjectStoreError::Rejected("read producer lost".into()))?;
        }
        Ok(())
    }
}

impl AsyncObjectStore for Objects {
    fn supports_decoded_cache(&self) -> bool {
        false
    }

    async fn put(
        &self,
        id: ObjectId,
        bytes: OwnedBytes,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        self.inner.put(id, bytes, work).await
    }

    async fn put_many(
        &self,
        writes: &[crate::storage::ObjectWrite],
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        self.inner.put_many(writes, work).await
    }

    async fn put_hashed(
        &self,
        object: crate::HashedObject,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        self.inner.put_hashed(object, work).await
    }

    async fn read(
        &self,
        id: ObjectId,
        maximum: u64,
        work: &mut AuthorityWork<'_>,
    ) -> Result<ObjectRead, ObjectStoreError> {
        self.before_read().await?;
        self.inner.read(id, maximum, work).await
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        work: &mut AuthorityWork<'_>,
    ) -> Result<(Vec<ObjectRead>, AuthorityAllocation), ObjectStoreError> {
        self.before_read().await?;
        self.inner.read_many(requests, work).await
    }

    async fn contains(
        &self,
        id: ObjectId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<bool, ObjectStoreError> {
        self.inner.contains(id, work).await
    }

    async fn flush_before_publish(
        &self,
        scope: crate::async_storage::PublicationScope<'_>,
        work: &mut AuthorityWork<'_>,
    ) -> Result<crate::PublicationHold, ObjectStoreError> {
        self.inner.flush_before_publish(scope, work).await
    }
}

pub(super) async fn withdrawal_and_producer_loss_retire_original_pins() -> Result<(), Box<dyn Error>>
{
    let objects = Arc::new(Objects::default());
    let fs = Fs::new(
        crate::MemoryAuthorityStore::default(),
        Arc::clone(&objects),
        crate::EmbeddedCapabilities::MEMORY,
    );
    let workspace = unbounded!(fs, create_workspace("reader-producer-loss"))?;
    workspace.write_text("/entry", "before").await?;
    let before = unbounded!(workspace, head())?;
    workspace.write_text("/entry", "after").await?;
    let after = unbounded!(workspace, head())?;
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let baseline = original.live_allocation_bytes();
    for withdrawn in [true, false] {
        let mut reader = value!(
            before
                .changed_paths_to(&after, &original, &cancellation)
                .await
        )?;
        let (entered, release) = objects.pause_next_read();
        let mut pending = Box::pin(reader.peek());
        tokio::select! {
            entered = entered => entered?,
            _ = &mut pending => return Err("reader did not reach the original object read".into()),
        }
        if withdrawn {
            drop(pending);
            assert!(
                release.is_closed(),
                "withdrawn read retained its completion receiver"
            );
        } else {
            drop(release);
            let failure = match pending.await {
                Err(failure) => failure,
                Ok(_) => return Err("lost producer returned a row".into()),
            };
            assert!(
                matches!(&failure.error,
                    WorkspaceError::Filesystem(crate::FsError::Object(ObjectStoreError::Rejected(reason)))
                    if reason == "read producer lost"),
                "lost original typed producer error: {:?}",
                failure.error,
            );
        }
        let spent = reader.work();
        assert!(
            matches!(reader.next().await, Err(failure) if matches!(failure.error, WorkspaceError::Filesystem(crate::FsError::InvalidDiff)))
        );
        assert_eq!(
            reader.work(),
            spent,
            "terminal reader dispatched another read"
        );
        drop(reader);
        assert_eq!(original.live_allocation_bytes(), baseline);
    }
    Ok(())
}
