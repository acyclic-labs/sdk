use super::*;
use crate::storage::ObjectWrite;
use crate::{AsyncObjectStore, ObjectRead, ObjectReadRequest, ObjectStoreError};
use std::sync::atomic::{AtomicBool, Ordering};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

struct Gate {
    armed: AtomicBool,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    fail: bool,
}

struct Objects {
    fs: crate::LocalFs,
    gate: Arc<Gate>,
}

impl AsyncObjectStore for Objects {
    fn collection(&self) -> Option<&Arc<crate::Collection>> {
        self.fs.objects().collection()
    }

    fn collection_sweeps(&self) -> u64 {
        self.fs.objects().collection_sweeps()
    }

    async fn put_hashed(
        &self,
        object: crate::HashedObject,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        self.fs.objects().put_hashed(object, work).await
    }

    async fn put_many_hashed(
        &self,
        objects: &[crate::HashedObject],
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        self.fs.objects().put_many_hashed(objects, work).await
    }

    async fn read(
        &self,
        id: ObjectId,
        maximum: u64,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<ObjectRead, ObjectStoreError> {
        if self.gate.armed.swap(false, Ordering::SeqCst) {
            self.gate.entered.notify_one();
            self.gate.release.notified().await;
            if self.gate.fail {
                return Err(ObjectStoreError::Missing);
            }
        }
        self.fs.objects().read(id, maximum, work).await
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<ObjectRead>, crate::AuthorityAllocation), ObjectStoreError> {
        self.fs.objects().read_many(requests, work).await
    }

    async fn put(
        &self,
        id: ObjectId,
        bytes: acyclic_stream::OwnedBytes,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        self.fs.objects().put(id, bytes, work).await
    }

    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        self.fs.objects().put_many(writes, work).await
    }

    async fn contains(
        &self,
        id: ObjectId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<bool, ObjectStoreError> {
        self.fs.objects().contains(id, work).await
    }
}

#[derive(Clone)]
struct Owner(Arc<Objects>);

impl PublicationOwner for Owner {
    type Authority = <crate::LocalFs as PublicationOwner>::Authority;
    type Objects = Objects;
    fn authority(&self) -> &Self::Authority {
        self.0.fs.authority()
    }
    fn objects(&self) -> &Objects {
        &self.0
    }
}

async fn core(
    fs: &crate::LocalFs,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> TestResult<(Arc<ProposalCore>, ObjectId)> {
    let config = crate::VolumeConfig::portable(crate::Lifecycle::Durable);
    let volume = fs
        .create_volume(config, WorkBudget::UNBOUNDED, cancellation)
        .await?
        .value;
    let checkout = volume
        .checkout(
            crate::model::GenerationSelector::Head,
            crate::model::CheckoutMode::read_only_pinned(),
            WorkBudget::UNBOUNDED,
            cancellation,
        )
        .await?
        .value;
    let root_id = ObjectId {
        storage: None,
        kind: ObjectKind::GenerationRoot,
        digest: checkout.generation_id().digest(),
    };
    let authority_id = volume_authority_id(volume.id());
    let expected = fs
        .authority()
        .head(
            authority_id,
            &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, cancellation),
        )
        .await?;
    let request = PublishGenerationRequest {
        volume_id: volume.id(),
        authority_id,
        expected,
        epoch: expected.epoch,
        operation_id: OperationId::new(),
        generation_root: root_id,
    };
    Ok((
        proposal_core(fs, request, config, None, accounting, cancellation).await?,
        root_id,
    ))
}

async fn proposal_core(
    fs: &crate::LocalFs,
    mut request: PublishGenerationRequest,
    config: crate::VolumeConfig,
    merge_parent: Option<ObjectId>,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> TestResult<Arc<ProposalCore>> {
    let root_id = request.generation_root;
    let expected = request.expected;
    let (root, allocation) = crate::kernel::read_generation_root_with_limits_and_accounting(
        fs.objects(),
        root_id,
        crate::kernel::DecodeLimits::default(),
        accounting,
        cancellation,
    )
    .await?;
    let bytes = usize_to_u64(root.parents.capacity() * std::mem::size_of::<crate::GenerationId>());
    let base = Retained::from_owned(Owned::new(root, allocation, bytes), accounting)?;
    if let Some(parent) = merge_parent {
        request.generation_root = crate::kernel::build_checkpoint_with_accounting(
            fs.objects(),
            crate::kernel::CheckpointRequest {
                base: root_id,
                file_table: base.value.file_table,
                merge_parent: Some(parent),
            },
            crate::kernel::DecodeLimits::default(),
            accounting,
            cancellation,
        )
        .await?;
    }
    let delta = Retained::from_owned(
        Owned::new(
            GenerationDiff {
                files: Vec::new(),
                bindings: Vec::new(),
                truncated: false,
            },
            accounting.claim_owned(0, 0)?,
            0,
        ),
        accounting,
    )?;
    let (candidate, allocation) = crate::kernel::read_generation_root_with_limits_and_accounting(
        fs.objects(),
        request.generation_root,
        crate::kernel::DecodeLimits::default(),
        accounting,
        cancellation,
    )
    .await?;
    let bytes =
        usize_to_u64(candidate.parents.capacity() * std::mem::size_of::<crate::GenerationId>());
    let candidate = Owned::new(candidate, allocation, bytes);
    let changes = crate::PublicationChanges::between(
        fs.objects(),
        fs.authority(),
        crate::publication_changes::PublicationBasis {
            request,
            predecessor: None,
        },
        &candidate.value,
        accounting,
        cancellation,
    )
    .await;
    candidate.retire(accounting)?;
    let changes = changes?.ok_or("publication changes")?;
    let quoted = super::super::capture::quote(
        request,
        &PublicationIntent::guarded(None, PublicationPermit::Unrestricted),
        changes,
        accounting,
        cancellation,
    )?;
    let core = ProposalCore::new(
        request,
        PublicationPreconditions {
            base: root_id,
            config,
            head: expected,
            rebase: true,
            reads: crate::kernel::CheckoutDependencies::new([], [], 1)?,
        },
        base,
        delta,
        merge_parent,
        quoted,
        accounting,
    )?;
    Ok(core)
}

#[derive(Clone, Copy)]
enum Delivery {
    Complete,
    Drop,
    Cancel,
    Failure,
    Collected,
}

async fn check(native: bool, delivery: Delivery) -> TestResult {
    crate::initialize_test_native_io();
    let directory = tempfile::tempdir()?;
    let fs = crate::LocalFs::local(crate::LocalOptions::new(directory.path())).await?;
    let released = fs
        .local_root_release_barrier()
        .ok_or("missing root ownership")?;
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let baseline = original.live_allocation_bytes();
    let mut accounting = if native {
        KernelAccounting::native(&original, &cancellation)
    } else {
        KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation)
    };
    // Promote the shared ledger once, then measure only task-owned custody.
    drop(crate::kernel::allocation::retained::RetainedAllocation::claim(0, 0, &mut accounting)?);
    let ordinary_baseline = accounting
        .standalone_parts()
        .map(|parts| parts.0.live_bytes());
    let (core, mut root) = core(&fs, &mut accounting, &cancellation).await?;
    let request = core.request;
    let mut collector = None;
    if matches!(delivery, Delivery::Collected) {
        let orphan = crate::kernel::GenerationRoot {
            parents: vec![crate::GenerationId::from_root(root)],
            ..core.base.value.clone()
        };
        let encoded = crate::kernel::encode_generation_root(
            &orphan,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?;
        let hashed = crate::HashedObject::new(ObjectKind::GenerationRoot, encoded.into());
        root = hashed.object_id();
        assert_ne!(root, request.generation_root);
        fs.objects()
            .put_hashed(
                hashed,
                &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation),
            )
            .await?;
        // Local writes stage privately until the canonical publication flush.
        // Make this orphan durable so the real collector can actually sweep it.
        drop(
            fs.objects()
                .flush_before_publish(
                    crate::PublicationScope::Closure {
                        objects: &[root],
                        proven_at: fs.objects().collection_sweeps(),
                    },
                    &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation),
                )
                .await?,
        );
        collector = Some(fs.clone());
    }
    let gate = Arc::new(Gate {
        armed: AtomicBool::new(true),
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
        fail: matches!(delivery, Delivery::Failure),
    });
    let owner = Owner(Arc::new(Objects {
        fs,
        gate: Arc::clone(&gate),
    }));
    let weak = Arc::downgrade(&owner.0);
    let mut preparation = Box::pin(compose(
        owner,
        core,
        root,
        crate::kernel::DecodeLimits::default(),
        &mut accounting,
        &cancellation,
    ));
    tokio::select! {
        () = gate.entered.notified() => {},
        result = &mut preparation => panic!("composition returned before blocked object I/O: {result:?}"),
    }
    match delivery {
        Delivery::Drop | Delivery::Collected => drop(preparation),
        Delivery::Cancel => {
            cancellation.cancel();
            assert!(matches!(
                preparation.await,
                Err(crate::FsError::Cancelled(_))
            ));
        }
        Delivery::Complete | Delivery::Failure => {
            gate.release.notify_one();
            let result = preparation.await;
            if matches!(delivery, Delivery::Failure) {
                assert!(result.is_err());
            } else {
                assert_eq!(result?.kind, ObjectKind::GenerationRoot);
            }
        }
    }
    if matches!(
        delivery,
        Delivery::Drop | Delivery::Cancel | Delivery::Collected
    ) {
        assert!(
            weak.upgrade().is_some(),
            "client withdrawal destroyed the actual composition owner during object I/O"
        );
        assert!(
            !released.is_released(),
            "client withdrawal released the local root"
        );
        if native {
            assert!(original.live_allocation_bytes() > baseline);
        }
        if let Some(collector) = collector.as_ref() {
            let observer = CancellationToken::new();
            let collected = collector.collect_local_garbage(None, &observer).await?;
            assert!(
                collected.removed > 0,
                "durable orphan was not swept: {collected:?}"
            );
            assert!(
                !collector
                    .objects()
                    .contains(
                        root,
                        &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &observer)
                    )
                    .await?,
                "unpublished predecessor was retained after withdrawal"
            );
        }
        gate.release.notify_one();
    }
    if let Some(collector) = collector.take() {
        while weak.strong_count() != 0 {
            tokio::task::yield_now().await;
        }
        let observer = CancellationToken::new();
        assert_eq!(
            collector
                .authority()
                .head(
                    request.authority_id,
                    &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &observer)
                )
                .await?,
            request.expected,
            "abandoned composition published a collected predecessor"
        );
        drop(collector);
    }
    while weak.strong_count() != 0 || !released.is_released() {
        tokio::task::yield_now().await;
    }
    if native {
        assert_eq!(original.live_allocation_bytes(), baseline);
    } else {
        assert_eq!(
            accounting
                .standalone_parts()
                .ok_or("standalone ledger")?
                .0
                .live_bytes(),
            ordinary_baseline.ok_or("standalone baseline")?
        );
    }
    Ok(())
}

#[tokio::test]
async fn ordinary_owned_composition_survives_client_drop_and_retires_local_root_and_original_credit()
-> TestResult {
    check(false, Delivery::Drop).await
}

#[tokio::test]
async fn ordinary_owned_composition_survives_request_cancellation_and_retires_original_credit()
-> TestResult {
    check(false, Delivery::Cancel).await
}

#[tokio::test]
async fn ordinary_owned_composition_completion_returns_the_original_branch() -> TestResult {
    check(false, Delivery::Complete).await
}

#[tokio::test]
async fn ordinary_owned_composition_object_failure_returns_the_original_branch() -> TestResult {
    check(false, Delivery::Failure).await
}

#[tokio::test]
async fn native_owned_composition_survives_client_drop_and_retires_local_root_and_original_credit()
-> TestResult {
    check(true, Delivery::Drop).await
}

#[tokio::test]
async fn native_owned_composition_survives_request_cancellation_and_retires_original_credit()
-> TestResult {
    check(true, Delivery::Cancel).await
}

#[tokio::test]
async fn native_owned_composition_completion_returns_the_original_branch() -> TestResult {
    check(true, Delivery::Complete).await
}

#[tokio::test]
async fn native_owned_composition_object_failure_returns_the_original_branch() -> TestResult {
    check(true, Delivery::Failure).await
}

#[tokio::test]
async fn ordinary_owned_composition_withdrawal_discards_a_collected_predecessor_without_publication()
-> TestResult {
    check(false, Delivery::Collected).await
}

#[tokio::test]
async fn native_owned_composition_withdrawal_discards_a_collected_predecessor_without_publication()
-> TestResult {
    check(true, Delivery::Collected).await
}

mod prefix;
