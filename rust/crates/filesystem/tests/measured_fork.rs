//! Public measured fork limits, cancellation, recovery and facade observability.

#![cfg(all(
    feature = "memory",
    feature = "distributed",
    not(target_arch = "wasm32")
))]

use acyclic_fs::storage::ObjectWrite;
use acyclic_fs::{
    AsyncObjectStore, CancellationToken, EmbeddedCapabilities, ForkOptions, Fs, IdempotencyKey,
    MemoryAuthorityBackend, MemoryObjectBackend, ObjectId, ObjectRead, ObjectReadRequest,
    ObjectResult, WorkBudget, WorkCounters, WorkspaceError,
};
use acyclic_stream::OwnedBytes;
use std::error::Error;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

fn finite_budget() -> Result<WorkBudget, Box<dyn Error>> {
    let mut value = serde_json::to_value(WorkBudget::UNBOUNDED)?;
    let fields = value
        .as_object_mut()
        .ok_or("work budget is not an object")?;
    for allowance in fields.values_mut() {
        *allowance = serde_json::json!(1_000_000);
    }
    let budget: WorkBudget = serde_json::from_value(value)?;
    assert!(budget.has_finite_allowances());
    Ok(budget)
}

#[tokio::test]
async fn public_measured_fork_bounds_work_and_retries_the_original_publication()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let parent = fs.create_workspace("measured-parent", acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    parent.write_text("/kept.txt", "original").await?;
    let pinned = parent.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let key = IdempotencyKey::new();
    let cancellation = CancellationToken::new();
    let exhausted = parent
        .fork(
            "measured-child",
            ForkOptions::from_generation(pinned.clone(), key), acyclic_fs::PublicationPermit::Unrestricted,
            WorkBudget::default(),
            &cancellation,
        |_| std::future::ready(Ok(acyclic_fs::OperationReceipt { value: (), work: acyclic_fs::WorkCounters::UNCHARGED })))
        .await;
    assert!(exhausted.is_err());
    assert!(fs.open_workspace("measured-child", acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).is_err());
    assert_eq!(parent.head(acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), pinned.id());
    let budget = finite_budget()?;
    let created = parent
        .fork(
            "measured-child",
            ForkOptions::from_generation(pinned.clone(), key), acyclic_fs::PublicationPermit::Unrestricted,
            budget,
            &cancellation,
        |_| std::future::ready(Ok(acyclic_fs::OperationReceipt { value: (), work: acyclic_fs::WorkCounters::UNCHARGED })))
        .await?;
    created.work.verify(budget)?;
    assert!(created.work.backend_write_operations > 0);
    let generation = created.value.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id();
    let retried = parent
        .fork(
            "measured-child",
            ForkOptions::from_generation(pinned.clone(), key), acyclic_fs::PublicationPermit::Unrestricted,
            budget,
            &cancellation,
        |_| std::future::ready(Ok(acyclic_fs::OperationReceipt { value: (), work: acyclic_fs::WorkCounters::UNCHARGED })))
        .await?;
    retried.work.verify(budget)?;
    assert_eq!(retried.value.id(), created.value.id());
    assert_eq!(retried.value.head(acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), generation);
    assert_eq!(
        retried.value.read("/kept.txt", 32, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        OwnedBytes::from_static(b"original")
    );
    assert_eq!(parent.head(acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), pinned.id());
    assert!(
        parent
            .fork(
                "measured-child",
                ForkOptions::from_generation(pinned, key).empty(), acyclic_fs::PublicationPermit::Unrestricted,
                budget,
                &cancellation,
            |_| std::future::ready(Ok(acyclic_fs::OperationReceipt { value: (), work: acyclic_fs::WorkCounters::UNCHARGED })))
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn public_measured_fork_rejects_precancellation_and_foreign_generation()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let parent = fs.create_workspace("cancel-parent", acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let pinned = parent.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let key = IdempotencyKey::new();
    let cancelled = match parent
        .fork(
            "cancel-child",
            ForkOptions::from_generation(pinned.clone(), key), acyclic_fs::PublicationPermit::Unrestricted,
            finite_budget()?,
            &cancellation,
        |_| std::future::ready(Ok(acyclic_fs::OperationReceipt { value: (), work: acyclic_fs::WorkCounters::UNCHARGED }))).await {
        Err(failure) => failure,
        Ok(_) => return Err("precancelled fork unexpectedly succeeded".into()),
    };
    assert!(matches!(cancelled.error, WorkspaceError::Cancelled(_)));
    assert_eq!(*cancelled.work, WorkCounters::default());
    assert!(fs.open_workspace("cancel-child", acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).is_err());
    assert_eq!(parent.head(acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), pinned.id());
    let other = fs.create_workspace("foreign-source", acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let foreign = match parent
        .fork(
            "foreign-child",
            ForkOptions::from_generation(other.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?, IdempotencyKey::new()), acyclic_fs::PublicationPermit::Unrestricted,
            finite_budget()?,
            &CancellationToken::new(),
        |_| std::future::ready(Ok(acyclic_fs::OperationReceipt { value: (), work: acyclic_fs::WorkCounters::UNCHARGED }))).await {
        Err(failure) => failure,
        Ok(_) => return Err("foreign fork unexpectedly succeeded".into()),
    };
    assert!(matches!(foreign.error, WorkspaceError::ForeignGeneration));
    assert_eq!(*foreign.work, WorkCounters::default());
    Ok(())
}

struct CancelAfterPut {
    inner: MemoryObjectBackend,
    armed: Arc<AtomicBool>,
    observed: Arc<AtomicBool>,
}

impl CancelAfterPut {
    fn after_put(&self, cancellation: &CancellationToken) {
        if self.armed.swap(false, Ordering::SeqCst) {
            self.observed.store(true, Ordering::SeqCst);
            cancellation.cancel();
        }
    }
}

impl AsyncObjectStore for CancelAfterPut {
    async fn put(
        &self,
        id: ObjectId,
        bytes: OwnedBytes,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        let receipt = self.inner.put(id, bytes, budget, cancellation).await?;
        self.after_put(cancellation);
        Ok(receipt)
    }

    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        let receipt = self.inner.put_many(writes, budget, cancellation).await?;
        self.after_put(cancellation);
        Ok(receipt)
    }

    async fn read(
        &self,
        id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<ObjectRead> {
        self.inner
            .read(id, maximum_bytes, budget, cancellation)
            .await
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<Vec<ObjectRead>> {
        self.inner.read_many(requests, budget, cancellation).await
    }

    async fn contains(
        &self,
        id: ObjectId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<bool> {
        self.inner.contains(id, budget, cancellation).await
    }
}

#[tokio::test]
async fn public_measured_fork_cancels_after_real_staging_and_retries_exactly()
-> Result<(), Box<dyn Error>> {
    let (objects, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
    let armed = Arc::new(AtomicBool::new(false));
    let observed = Arc::new(AtomicBool::new(false));
    let fs = Fs::new(
        MemoryAuthorityBackend::new(Arc::new(acyclic_stream::MemoryStream::default())),
        CancelAfterPut {
            inner: MemoryObjectBackend::new(Arc::new(objects), bucket),
            armed: armed.clone(),
            observed: observed.clone(),
        },
        EmbeddedCapabilities::MEMORY,
    );
    let parent = fs.create_workspace("inflight-parent", acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    parent.write_text("/kept.txt", "pinned").await?;
    let pinned = parent.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let key = IdempotencyKey::new();
    armed.store(true, Ordering::SeqCst);
    let cancellation = CancellationToken::new();
    assert!(
        parent
            .fork(
                "inflight-child",
                ForkOptions::from_generation(pinned.clone(), key), acyclic_fs::PublicationPermit::Unrestricted,
                finite_budget()?,
                &cancellation,
            |_| std::future::ready(Ok(acyclic_fs::OperationReceipt { value: (), work: acyclic_fs::WorkCounters::UNCHARGED })))
            .await
            .is_err()
    );
    assert!(observed.load(Ordering::SeqCst));
    assert!(cancellation.is_cancelled());
    assert!(fs.open_workspace("inflight-child", acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).is_err());
    assert_eq!(parent.head(acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), pinned.id());
    let created = parent
        .fork(
            "inflight-child",
            ForkOptions::from_generation(pinned.clone(), key), acyclic_fs::PublicationPermit::Unrestricted,
            finite_budget()?,
            &CancellationToken::new(),
        |_| std::future::ready(Ok(acyclic_fs::OperationReceipt { value: (), work: acyclic_fs::WorkCounters::UNCHARGED })))
        .await?;
    let initial = created.value.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id();
    let retried = parent
        .fork(
            "inflight-child",
            ForkOptions::from_generation(pinned, key), acyclic_fs::PublicationPermit::Unrestricted,
            finite_budget()?,
            &CancellationToken::new(),
        |_| std::future::ready(Ok(acyclic_fs::OperationReceipt { value: (), work: acyclic_fs::WorkCounters::UNCHARGED })))
        .await?;
    assert_eq!(retried.value.head(acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), initial);
    assert_eq!(
        retried.value.read("/kept.txt", 32, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        OwnedBytes::from_static(b"pinned")
    );
    Ok(())
}
