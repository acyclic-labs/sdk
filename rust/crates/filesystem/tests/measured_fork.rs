//! Public measured fork limits, cancellation, recovery and facade observability.

#![cfg(all(
    feature = "memory",
    feature = "distributed",
    not(target_arch = "wasm32")
))]

#[macro_use]
#[path = "support/unbounded.rs"]
mod unbounded;

#[macro_use]
#[path = "support/provider_receipt.rs"]
mod provider_receipt;

use acyclic_fs::storage::ObjectWrite;
use acyclic_fs::{
    AsyncObjectStore, CancellationToken, EmbeddedCapabilities, ForkOptions, Fs, IdempotencyKey,
    MemoryAuthorityBackend, MemoryObjectBackend, ObjectId, ObjectReadRequest, WorkBudget,
    WorkCounters, WorkspaceError,
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
    let parent = unbounded!(fs, create_workspace("measured-parent"))?;
    parent.write_text("/kept.txt", "original").await?;
    let pinned = unbounded!(parent, head())?;
    let key = IdempotencyKey::new();
    let cancellation = CancellationToken::new();
    let exhausted = parent
        .fork::<_, _, acyclic_fs::WorkspaceError>(
            "measured-child",
            ForkOptions::from_generation(pinned.clone(), key),
            acyclic_fs::PublicationPermit::Unrestricted,
            WorkBudget::default(),
            &cancellation,
            &mut |_| {
                std::future::ready(Ok(acyclic_fs::OperationReceipt {
                    value: (),
                    work: acyclic_fs::WorkCounters::default(),
                }))
            },
        )
        .await;
    assert!(exhausted.is_err());
    assert!(unbounded!(fs, open_workspace("measured-child")).is_err());
    assert_eq!(unbounded!(parent, head())?.id(), pinned.id());
    let budget = finite_budget()?;
    let created = parent
        .fork::<_, _, acyclic_fs::WorkspaceError>(
            "measured-child",
            ForkOptions::from_generation(pinned.clone(), key),
            acyclic_fs::PublicationPermit::Unrestricted,
            budget,
            &cancellation,
            &mut |_| {
                std::future::ready(Ok(acyclic_fs::OperationReceipt {
                    value: (),
                    work: acyclic_fs::WorkCounters::default(),
                }))
            },
        )
        .await?;
    created.work.verify(budget)?;
    assert!(created.work.backend_write_operations > 0);
    let generation = unbounded!(created.value, head())?.id();
    let retried = parent
        .fork::<_, _, acyclic_fs::WorkspaceError>(
            "measured-child",
            ForkOptions::from_generation(pinned.clone(), key),
            acyclic_fs::PublicationPermit::Unrestricted,
            budget,
            &cancellation,
            &mut |_| {
                std::future::ready(Ok(acyclic_fs::OperationReceipt {
                    value: (),
                    work: acyclic_fs::WorkCounters::default(),
                }))
            },
        )
        .await?;
    retried.work.verify(budget)?;
    assert_eq!(retried.value.id(), created.value.id());
    assert_eq!(unbounded!(retried.value, head())?.id(), generation);
    assert_eq!(
        unbounded!(retried.value, read("/kept.txt", 32))?,
        OwnedBytes::from_static(b"original")
    );
    assert_eq!(unbounded!(parent, head())?.id(), pinned.id());
    assert!(
        parent
            .fork::<_, _, acyclic_fs::WorkspaceError>(
                "measured-child",
                ForkOptions::from_generation(pinned, key).inherit_paths(Vec::new()),
                acyclic_fs::PublicationPermit::Unrestricted,
                budget,
                &cancellation,
                &mut |_| std::future::ready(Ok(acyclic_fs::OperationReceipt {
                    value: (),
                    work: acyclic_fs::WorkCounters::default()
                }))
            )
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn public_measured_fork_rejects_precancellation_and_foreign_generation()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let parent = unbounded!(fs, create_workspace("cancel-parent"))?;
    let pinned = unbounded!(parent, head())?;
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let key = IdempotencyKey::new();
    let cancelled = match parent
        .fork::<_, _, acyclic_fs::WorkspaceError>(
            "cancel-child",
            ForkOptions::from_generation(pinned.clone(), key),
            acyclic_fs::PublicationPermit::Unrestricted,
            finite_budget()?,
            &cancellation,
            &mut |_| {
                std::future::ready(Ok(acyclic_fs::OperationReceipt {
                    value: (),
                    work: acyclic_fs::WorkCounters::default(),
                }))
            },
        )
        .await
    {
        Err(failure) => failure,
        Ok(_) => return Err("precancelled fork unexpectedly succeeded".into()),
    };
    assert!(matches!(cancelled.error, WorkspaceError::Cancelled(_)));
    assert_eq!(*cancelled.work, WorkCounters::default());
    assert!(unbounded!(fs, open_workspace("cancel-child")).is_err());
    assert_eq!(unbounded!(parent, head())?.id(), pinned.id());
    let other = unbounded!(fs, create_workspace("foreign-source"))?;
    let foreign = match parent
        .fork::<_, _, acyclic_fs::WorkspaceError>(
            "foreign-child",
            ForkOptions::from_generation(unbounded!(other, head())?, IdempotencyKey::new()),
            acyclic_fs::PublicationPermit::Unrestricted,
            finite_budget()?,
            &CancellationToken::new(),
            &mut |_| {
                std::future::ready(Ok(acyclic_fs::OperationReceipt {
                    value: (),
                    work: acyclic_fs::WorkCounters::default(),
                }))
            },
        )
        .await
    {
        Err(failure) => failure,
        Ok(_) => return Err("foreign fork unexpectedly succeeded".into()),
    };
    assert!(matches!(foreign.error, WorkspaceError::ForeignGeneration));
    assert_eq!(foreign.work.backend_read_operations, 0);
    assert_eq!(foreign.work.backend_write_operations, 0);
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
        context: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<(), acyclic_fs::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(acyclic_fs::ObjectStoreError::NativeUnavailable);
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
            let receipt = provider_receipt!(self.inner, put(id, bytes), budget, cancellation)?;
            self.after_put(cancellation);
            Ok(receipt)
        }
        .await;
        context.accept(result)
    }

    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        context: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<(), acyclic_fs::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(acyclic_fs::ObjectStoreError::NativeUnavailable);
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
            let receipt = provider_receipt!(self.inner, put_many(writes), budget, cancellation)?;
            self.after_put(cancellation);
            Ok(receipt)
        }
        .await;
        context.accept(result)
    }

    async fn read(
        &self,
        id: ObjectId,
        maximum_bytes: u64,
        context: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<acyclic_fs::ObjectRead, acyclic_fs::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(acyclic_fs::ObjectStoreError::NativeUnavailable);
        }
        let budget = context.read_budget()?;
        let cancellation = context.cancellation;
        let result = provider_receipt!(self.inner, read(id, maximum_bytes), budget, cancellation);
        context.accept(result)
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        context: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<
        (Vec<acyclic_fs::ObjectRead>, acyclic_fs::AuthorityAllocation),
        acyclic_fs::ObjectStoreError,
    > {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(acyclic_fs::ObjectStoreError::NativeUnavailable);
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = provider_receipt!(self.inner, read_many(requests), budget, cancellation);
        context.accept(result).map(|values| (values, None))
    }

    async fn contains(
        &self,
        id: ObjectId,
        context: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<bool, acyclic_fs::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(acyclic_fs::ObjectStoreError::NativeUnavailable);
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = provider_receipt!(self.inner, contains(id), budget, cancellation);
        context.accept(result)
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
    let parent = unbounded!(fs, create_workspace("inflight-parent"))?;
    parent.write_text("/kept.txt", "pinned").await?;
    let pinned = unbounded!(parent, head())?;
    let key = IdempotencyKey::new();
    armed.store(true, Ordering::SeqCst);
    let cancellation = CancellationToken::new();
    assert!(
        parent
            .fork::<_, _, acyclic_fs::WorkspaceError>(
                "inflight-child",
                ForkOptions::from_generation(pinned.clone(), key),
                acyclic_fs::PublicationPermit::Unrestricted,
                finite_budget()?,
                &cancellation,
                &mut |_| std::future::ready(Ok(acyclic_fs::OperationReceipt {
                    value: (),
                    work: acyclic_fs::WorkCounters::default()
                }))
            )
            .await
            .is_err()
    );
    assert!(observed.load(Ordering::SeqCst));
    assert!(cancellation.is_cancelled());
    assert!(unbounded!(fs, open_workspace("inflight-child")).is_err());
    assert_eq!(unbounded!(parent, head())?.id(), pinned.id());
    let created = parent
        .fork::<_, _, acyclic_fs::WorkspaceError>(
            "inflight-child",
            ForkOptions::from_generation(pinned.clone(), key),
            acyclic_fs::PublicationPermit::Unrestricted,
            finite_budget()?,
            &CancellationToken::new(),
            &mut |_| {
                std::future::ready(Ok(acyclic_fs::OperationReceipt {
                    value: (),
                    work: acyclic_fs::WorkCounters::default(),
                }))
            },
        )
        .await?;
    let initial = unbounded!(created.value, head())?.id();
    let retried = parent
        .fork::<_, _, acyclic_fs::WorkspaceError>(
            "inflight-child",
            ForkOptions::from_generation(pinned, key),
            acyclic_fs::PublicationPermit::Unrestricted,
            finite_budget()?,
            &CancellationToken::new(),
            &mut |_| {
                std::future::ready(Ok(acyclic_fs::OperationReceipt {
                    value: (),
                    work: acyclic_fs::WorkCounters::default(),
                }))
            },
        )
        .await?;
    assert_eq!(unbounded!(retried.value, head())?.id(), initial);
    assert_eq!(
        unbounded!(retried.value, read("/kept.txt", 32))?,
        OwnedBytes::from_static(b"pinned")
    );
    Ok(())
}
