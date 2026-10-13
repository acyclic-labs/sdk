use super::*;
use crate::storage::{ObjectId, ObjectReadRequest, ObjectWrite};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

struct ReenteringObjects {
    inner: crate::memory::MemoryObjectStore,
    entered: Arc<AtomicBool>,
}

impl ReenteringObjects {
    async fn reenter(&self, context: &crate::AuthorityWork<'_>) {
        if let Some(work) = context.original {
            let state = work
                .state
                .0
                .try_lock()
                .expect("capture holds its meter across backend entry");
            state.check().expect("fixture invocation remains active");
            self.entered.store(true, Ordering::Relaxed);
        }
        tokio::task::yield_now().await;
    }
}

impl AsyncObjectStore for ReenteringObjects {
    async fn put(
        &self,
        object_id: ObjectId,
        bytes: acyclic_stream::OwnedBytes,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        self.reenter(context).await;
        self.inner.put(object_id, bytes, context).await
    }

    async fn put_hashed(
        &self,
        object: crate::storage::HashedObject,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        self.reenter(context).await;
        self.inner.put_hashed(object, context).await
    }

    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        self.reenter(context).await;
        self.inner.put_many(writes, context).await
    }

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::ObjectRead, crate::ObjectStoreError> {
        self.reenter(context).await;
        self.inner.read(object_id, maximum_bytes, context).await
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<crate::ObjectRead>, crate::AuthorityAllocation), crate::ObjectStoreError> {
        self.reenter(context).await;
        self.inner.read_many(requests, context).await
    }

    async fn contains(
        &self,
        object_id: ObjectId,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<bool, crate::ObjectStoreError> {
        self.reenter(context).await;
        self.inner.contains(object_id, context).await
    }
}

#[tokio::test]
async fn shared_watch_reenters_original_meter() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    std::fs::write(directory.path().join("file"), vec![b'x'; 256 * 1024])?;

    let entered = Arc::new(AtomicBool::new(false));
    let fs = crate::Fs::new(
        crate::memory::MemoryAuthorityStore::default(),
        ReenteringObjects {
            inner: crate::memory::MemoryObjectStore::default(),
            entered: Arc::clone(&entered),
        },
        crate::EmbeddedCapabilities::MEMORY,
    );
    let workspace = fs
        .create_workspace(
            "shared-watch-reentry",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    workspace.write_text("/file", "prior watch value").await?;
    let mut checkout = workspace
        .checkout(
            crate::model::GenerationSelector::Head,
            crate::model::CheckoutMode::tracking_transaction(),
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    let cancellation = CancellationToken::new();
    let budget = WorkBudget::UNBOUNDED;
    let native =
        crate::NativeMaterializationWork::new(budget, &cancellation).map_err(|(error, _)| error)?;

    let limits = checkout.volume_config().limits;
    let path =
        NamespacePath::from_portable(&crate::path::PortablePath::parse("/file", limits)?, limits)?;
    let options = CaptureOptions {
        source_root: directory.path().to_path_buf(),
        expected_root_identity: capture_root_identity(directory.path())?,
        maximum_paths: 8,
        maximum_extent_spans: 8,
    };
    let captured = capture_watch_batch(
        &mut checkout,
        WatchBatch::Changes {
            original: Default::default(),
            epoch: 9,
            first_sequence: 3,
            next_sequence: 4,
            changes: vec![WatchChange::Created(path)],
        },
        &options,
        &CapturePolicy::default(),
        &native,
        &cancellation,
    )
    .await?;
    assert!(entered.load(Ordering::Relaxed));
    assert_eq!(captured.value.epoch, 9);
    assert_eq!(captured.value.next_sequence, 4);
    Ok(())
}
