use super::*;
use crate::facade::FsWork;
use crate::kernel::allocation::retained::Retained;
use crate::{AuthorityStoreError, AuthorityWork, Fs, WorkCounters};
use std::sync::Mutex;

pub(super) struct Prefunded {
    pub(super) budget: WorkBudget,
    pub(super) held: Mutex<Option<(Retained<Vec<u8>>, u64)>>,
}

impl FsWork for &Prefunded {
    type Output<T, E> = (Result<T, E>, WorkCounters, u64, u64);
    type OwnedOutput<T> = Self::Output<(T, KernelOwnedAllocation), FsError>;

    fn finish_owned<T>(
        &self,
        result: Result<(T, KernelOwnedAllocation), FsError>,
        accounting: &KernelAccounting<'_>,
    ) -> Self::OwnedOutput<T> {
        self.finish(result, accounting)
    }

    fn accounting<'a>(&'a self, cancellation: &'a CancellationToken) -> KernelAccounting<'a> {
        let mut accounting = KernelAccounting::standalone(self.budget, cancellation);
        let buffer = crate::kernel::Owned::<Vec<u8>>::allocate(4096, &mut accounting).unwrap();
        let held = Retained::from_owned(buffer, &mut accounting).unwrap();
        accounting.charge_items(1).unwrap();
        let baseline = accounting.standalone_parts().unwrap().0.live_bytes();
        *self.held.lock().unwrap() = Some((held, baseline));
        accounting
    }

    fn finish<T, E>(
        &self,
        result: Result<T, E>,
        accounting: &KernelAccounting<'_>,
    ) -> Self::Output<T, E> {
        let KernelAccounting::Standalone { allocations, .. } = accounting else {
            unreachable!("ordinary fixture")
        };
        let baseline = self.held.lock().unwrap().as_ref().unwrap().1;
        (
            result,
            accounting.spent(),
            allocations.live_bytes(),
            baseline,
        )
    }
}

#[tokio::test]
async fn generation_diff_keeps_original_backing_on_success_refusal_and_withdrawal()
-> Result<(), Box<dyn std::error::Error>> {
    #[cfg(not(target_arch = "wasm32"))]
    crate::initialize_test_native_io();
    let cancellation = CancellationToken::new();
    let budget = crate::performance::BOUNDARY_BUDGET;
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace("original-diff", budget, &cancellation)
        .await?
        .value;
    let before = workspace.head(budget, &cancellation).await?.value;
    let mut transaction = workspace
        .begin_transaction(IdempotencyKey::new(), budget, &cancellation)
        .await?
        .value;
    transaction
        .write(
            "/changed",
            acyclic_stream::OwnedBytes::from_static(b"original bytes"),
            budget,
            &cancellation,
        )
        .await?;
    let committed = transaction
        .commit::<WorkspaceError, _, _>(
            None,
            crate::PublicationPermit::Unrestricted,
            budget,
            &cancellation,
            |_| {
                std::future::ready(Ok(crate::OperationReceipt {
                    value: (),
                    work: WorkCounters::UNCHARGED,
                }))
            },
        )
        .await?
        .value;
    let TransactionCommit::Committed(after) = committed else {
        return Err("original diff fixture did not commit".into());
    };
    for (reads, maximum) in [
        (0, 64),
        (budget.backend_read_operations, 1),
        (budget.backend_read_operations, 64),
    ] {
        let source = Prefunded {
            budget: WorkBudget {
                backend_read_operations: reads,
                ..budget
            },
            held: Mutex::new(None),
        };
        let (result, spent, live, baseline) = before
            .diff_to(&after, maximum, &source, &cancellation)
            .await;
        spent.verify(source.budget)?;
        if reads == 0 {
            assert!(matches!(
                result,
                Err(WorkspaceError::Work(crate::WorkError::BudgetExceeded {
                    counter: "backend_read_operations",
                    ..
                }))
            ));
            assert_eq!(spent.items_examined, 1);
            assert_eq!(spent.backend_read_operations, 0);
            assert_eq!(live, baseline);
        } else if maximum == 1 {
            assert!(matches!(result, Err(WorkspaceError::JoinLimit)));
            assert!(spent.items_examined > 1);
            assert!(spent.backend_read_operations > 0);
            assert!(spent.peak_allocation_bytes > baseline);
            assert_eq!(live, baseline, "discarded diff backing remained charged");
        } else {
            let (changes, _allocation) = result?;
            assert_eq!(changes.work(), spent);
            assert!(!changes.changes().files.is_empty());
            assert!(!changes.changes().bindings.is_empty());
            assert!(live > baseline, "diff backing left the original ledger");
            assert!(spent.peak_allocation_bytes > baseline);
            let paths = changes
                .changed_paths(64, budget, &cancellation)
                .await?
                .value
                .0;
            assert_eq!(paths.len(), 1);
            assert_eq!(namespace_path_text(&paths[0].path)?, "/changed");
        }
    }

    // Reuse one original ledger after repeated partial diffs. Its earlier
    // ordinary scratch and independently retained Vec must both survive.
    let source = Prefunded {
        budget,
        held: Mutex::new(None),
    };
    let funded = &source;
    let mut accounting = funded.accounting(&cancellation);
    let mut earlier = crate::kernel::Owned::<Vec<u8>>::allocate(128, &mut accounting)?;
    earlier.value.push(7);
    let baseline = accounting.standalone_parts().unwrap().0.live_bytes();
    let mut completed = accounting.spent();
    for _ in 0..2 {
        let result = fs
            .workspace_join_changes_with_accounting(
                before.id,
                &after,
                1,
                &mut accounting,
                &cancellation,
            )
            .await;
        assert!(matches!(result, Err(WorkspaceError::JoinLimit)));
        assert_eq!(
            accounting.standalone_parts().unwrap().0.live_bytes(),
            baseline
        );
        assert!(accounting.spent().backend_read_operations > completed.backend_read_operations);
        completed = accounting.spent();
    }

    // Withdraw a suspended operation with real temporary backing. Its owner
    // drops before scope cleanup; completed work and earlier storage remain.
    let mut operation = Box::pin(async {
        let mut scope = accounting.allocation_scope()?;
        let mut temporary = crate::kernel::Owned::<Vec<u8>>::allocate(512, &mut scope)?;
        temporary.value.push(9);
        scope.charge_items(3)?;
        std::future::pending::<()>().await;
        temporary.retire(&mut scope)
    });
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(std::future::Future::poll(operation.as_mut(), &mut context).is_pending());
    cancellation.cancel();
    drop(operation);
    assert_eq!(
        accounting.standalone_parts().unwrap().0.live_bytes(),
        baseline
    );
    assert_eq!(
        accounting.spent().items_examined,
        completed.items_examined + 3
    );
    assert_eq!(
        accounting.spent().backend_read_operations,
        completed.backend_read_operations
    );
    assert_eq!(earlier.value, [7]);
    earlier.retire(&mut accounting)?;
    Ok(())
}

#[tokio::test]
async fn generation_parents_keep_original_work_and_result_custody()
-> Result<(), Box<dyn std::error::Error>> {
    #[cfg(not(target_arch = "wasm32"))]
    crate::initialize_test_native_io();
    let cancellation = CancellationToken::new();
    let budget = crate::performance::BOUNDARY_BUDGET;
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace("parent-custody", budget, &cancellation)
        .await?
        .value;
    let before = workspace.head(budget, &cancellation).await?.value;
    workspace.write_text("/file", "first").await?;
    let selected = workspace.head(budget, &cancellation).await?.value;
    // Later publications cannot replace the selected generation's ancestry.
    workspace.write_text("/file", "second").await?;
    for generation in [&before, &selected] {
        for reads in [0, budget.backend_read_operations] {
            let source = Prefunded {
                budget: WorkBudget {
                    backend_read_operations: reads,
                    ..budget
                },
                held: Mutex::new(None),
            };
            let (result, spent, live, baseline) = generation.parents(&source, &cancellation).await;
            spent.verify(source.budget)?;
            if reads == 0 {
                assert!(matches!(
                    result,
                    Err(WorkspaceError::Work(crate::WorkError::BudgetExceeded {
                        counter: "backend_read_operations",
                        ..
                    }))
                ));
                assert_eq!(spent.backend_read_operations, 0);
                assert_eq!(live, baseline);
            } else {
                let (parents, allocation) = result?;
                assert!(allocation.is_none());
                assert_eq!(
                    parents.as_slice(),
                    if generation.id() == before.id() {
                        &[][..]
                    } else {
                        std::slice::from_ref(&before.id)
                    }
                );
                assert_eq!(spent.backend_read_operations, 1);
                assert_eq!(spent.items_examined, 1 + parents.len() as u64);
                assert_eq!(
                    live,
                    baseline + (parents.capacity() * size_of::<GenerationId>()) as u64
                );
            }
        }
    }
    let foreign_workspace = fs
        .create_workspace("foreign-parent-custody", budget, &cancellation)
        .await?
        .value;
    let foreign = Generation {
        workspace: foreign_workspace,
        id: selected.id(),
        publication_closure: None,
    };
    let source = Prefunded {
        budget,
        held: Mutex::new(None),
    };
    let (result, spent, live, baseline) = foreign.parents(&source, &cancellation).await;
    assert!(matches!(result, Err(WorkspaceError::ForeignGeneration)));
    assert_eq!(spent.backend_read_operations, 1);
    assert_eq!(live, baseline);
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let failure = selected.parents(budget, &cancelled).await.unwrap_err();
    assert!(matches!(failure.error, WorkspaceError::Cancelled(_)));
    assert_eq!(failure.work.backend_read_operations, 0);

    // A parent digest is scalar: both decoded roots and its encoding must
    // retire before the public operation returns, preserving earlier backing.
    let mut completed = None;
    for reads in [2, 1, 0] {
        let source = Prefunded {
            budget: WorkBudget {
                backend_read_operations: reads,
                ..budget
            },
            held: Mutex::new(None),
        };
        let (result, spent, live, baseline) = selected
            .normalized_join_parent_for(&selected, &source, &cancellation)
            .await;
        if reads == 2 {
            assert_eq!(result?, selected.id().object_id().digest);
            assert!(spent.peak_allocation_bytes > baseline);
            completed = Some(spent);
        } else {
            assert!(matches!(
                result,
                Err(WorkspaceError::Work(crate::WorkError::BudgetExceeded {
                    counter: "backend_read_operations",
                    ..
                }))
            ));
        }
        assert_eq!(spent.backend_read_operations, reads);
        assert_eq!(spent.backend_write_operations, 0);
        assert_eq!(live, baseline, "parent digest retained temporary backing");
    }
    let source = Prefunded {
        budget,
        held: Mutex::new(None),
    };
    let (result, spent, live, baseline) = foreign
        .normalized_join_parent_for(&selected, &source, &cancellation)
        .await;
    assert!(matches!(result, Err(WorkspaceError::IncompatibleWorkspace)));
    assert_eq!(spent.backend_read_operations, 2);
    assert_eq!(
        live, baseline,
        "incompatible parent retained decoded backing"
    );
    let source = Prefunded {
        budget: WorkBudget {
            bytes_hashed: completed
                .ok_or("missing completed digest work")?
                .bytes_hashed
                - 1,
            ..budget
        },
        held: Mutex::new(None),
    };
    let (result, spent, live, baseline) = selected
        .normalized_join_parent_for(&selected, &source, &cancellation)
        .await;
    assert!(matches!(
        result,
        Err(WorkspaceError::Work(crate::WorkError::BudgetExceeded {
            counter: "bytes_hashed",
            ..
        }))
    ));
    assert_eq!(spent.backend_read_operations, 2);
    assert_eq!(live, baseline, "denied digest retained temporary backing");
    let failure = selected
        .normalized_join_parent_for(&selected, budget, &cancelled)
        .await
        .unwrap_err();
    assert!(matches!(failure.error, WorkspaceError::Cancelled(_)));
    assert_eq!(*failure.work, WorkCounters::UNCHARGED);

    #[cfg(not(target_arch = "wasm32"))]
    {
        let original = crate::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let (parents, allocation) = selected.parents(&original, &cancellation).await?;
        let allocation = allocation.ok_or("parent result lost original native backing")?;
        assert!(allocation.work().shares_ledger(&original));
        let bytes = (parents.capacity() * size_of::<GenerationId>()) as u64;
        assert_eq!(parents, [before.id()]);
        assert_eq!(allocation.bytes(), bytes);
        assert_eq!(original.live_allocation_bytes(), baseline + bytes);
        drop((parents, allocation));
        assert_eq!(original.live_allocation_bytes(), baseline);
        assert!(matches!(
            foreign.parents(&original, &cancellation).await,
            Err(WorkspaceError::ForeignGeneration)
        ));
        assert_eq!(original.live_allocation_bytes(), baseline);

        let mut accounting = KernelAccounting::native(&original, &cancellation);
        let held = Retained::<Vec<u8>>::allocate(4096, &mut accounting)?;
        let held_baseline = original.live_allocation_bytes();
        let prior = original.receipt();
        assert_eq!(
            selected
                .normalized_join_parent_for(&selected, &original, &cancellation)
                .await?,
            selected.id().object_id().digest
        );
        assert_eq!(original.live_allocation_bytes(), held_baseline);
        assert_eq!(original.receipt().object_probes, prior.object_probes + 2);
        assert!(original.receipt().backend_read_operations > prior.backend_read_operations);
        let prior = original.receipt();
        assert!(matches!(
            selected
                .normalized_join_parent_for(&selected, &original, &cancelled)
                .await,
            Err(WorkspaceError::Cancelled(_))
        ));
        assert_eq!(original.receipt(), prior);
        assert_eq!(original.live_allocation_bytes(), held_baseline);
        drop(held);
        assert_eq!(original.live_allocation_bytes(), baseline);
    }
    Ok(())
}

#[tokio::test]
async fn extent_plans_keep_original_work_and_output_custody()
-> Result<(), Box<dyn std::error::Error>> {
    #[cfg(not(target_arch = "wasm32"))]
    crate::initialize_test_native_io();
    let cancellation = CancellationToken::new();
    let budget = crate::performance::BOUNDARY_BUDGET;
    let workspace = Fs::memory()
        .create_workspace("extent-custody", budget, &cancellation)
        .await?
        .value;
    workspace.write_text("/inline", "small").await?;
    workspace.write_text("/regular", &"x".repeat(300)).await?;
    let mut checkout = workspace
        .checkout(
            GenerationSelector::Head,
            CheckoutMode::read_only_pinned(),
            budget,
            &cancellation,
        )
        .await?
        .value;
    let reader = checkout.pinned_reader()?;
    for (name, length, inline) in [("inline", 5, true), ("regular", 300, false)] {
        let path = paths::customer_path(&format!("/{name}"), checkout.volume_config())?;
        let file = reader
            .resolve_files(std::slice::from_ref(&path), budget, &cancellation)
            .await?
            .value
            .pop()
            .flatten()
            .ok_or("missing extent file")?;
        let range = crate::ByteRange { offset: 0, length };
        for surface in 0..3 {
            for refuse in [false, true] {
                let source = Prefunded {
                    budget: WorkBudget {
                        items_examined: if refuse { 1 } else { budget.items_examined },
                        ..budget
                    },
                    held: Mutex::new(None),
                };
                let (result, spent, live, baseline) = match surface {
                    0 => {
                        checkout
                            .plan_file_extents(&path, range, 8, &source, &cancellation)
                            .await
                    }
                    1 => {
                        reader
                            .plan_file_extents(&path, range, 8, &source, &cancellation)
                            .await
                    }
                    _ => file.plan_extents(range, 8, &source, &cancellation).await,
                };
                spent.verify(source.budget)?;
                // Inline resolved files require no traversal or decoded items.
                if refuse && !(inline && surface == 2) {
                    assert!(matches!(
                        result,
                        Err(FsError::Work(crate::WorkError::BudgetExceeded { .. }))
                    ));
                    assert_eq!(live, baseline);
                } else {
                    let (plan, _) = result?;
                    assert_eq!(plan.is_none(), inline);
                    let bytes = plan
                        .as_ref()
                        .map_or(0, |plan| plan.retained_allocation_bytes);
                    if let Some(plan) = plan {
                        assert_eq!(plan.work, spent);
                        assert_eq!(plan.spans.len(), 1);
                        assert_eq!((plan.spans[0].offset, plan.spans[0].length), (0, length));
                    }
                    assert_eq!(live, baseline + bytes);
                }
            }
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        let original = crate::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(error, _)| error)?;
        let foreign = crate::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(error, _)| error)?;
        let original_baseline = original.live_allocation_bytes();
        let foreign_baseline = foreign.live_allocation_bytes();
        let original_state = std::sync::Arc::downgrade(&original.state);
        let foreign_state = std::sync::Arc::downgrade(&foreign.state);
        let mut checkout = workspace
            .checkout(
                GenerationSelector::Head,
                CheckoutMode::read_only_pinned(),
                &original,
                &cancellation,
            )
            .await?;
        for name in ["inline", "regular"] {
            let path = paths::customer_path(&format!("/{name}"), checkout.volume_config())?;
            let range = crate::ByteRange {
                offset: 0,
                length: 1,
            };
            let baseline = original.live_allocation_bytes();
            let (plan, allocation) = checkout
                .plan_file_extents(&path, range, 8, &original, &cancellation)
                .await?;
            assert!(allocation.work().shares_ledger(&original));
            assert_eq!(
                allocation.bytes(),
                plan.as_ref()
                    .map_or(0, |plan| plan.retained_allocation_bytes)
            );
            assert_eq!(
                original.live_allocation_bytes(),
                baseline + allocation.bytes()
            );
            drop((plan, allocation));
            assert_eq!(original.live_allocation_bytes(), baseline);
            let foreign_before = foreign.receipt();
            for target in [crate::ExtentSeekTarget::Data, crate::ExtentSeekTarget::Hole] {
                let boundary = checkout
                    .seek_file_extent(&path, 0, target, &original, &cancellation)
                    .await?;
                assert_eq!(
                    boundary,
                    Some(match target {
                        crate::ExtentSeekTarget::Data => 0,
                        crate::ExtentSeekTarget::Hole if name == "inline" => 5,
                        crate::ExtentSeekTarget::Hole => 300,
                    })
                );
                assert_eq!(original.live_allocation_bytes(), baseline);
                assert!(matches!(
                    checkout
                        .seek_file_extent(&path, 0, target, &foreign, &cancellation)
                        .await,
                    Err(FsError::NativeKernel(
                        crate::NativeTreeMaterializationError::InvalidLayout
                    ))
                ));
            }
            assert!(matches!(
                checkout
                    .read_file_range(&path, range, &foreign, &cancellation)
                    .await,
                Err(FsError::NativeKernel(
                    crate::NativeTreeMaterializationError::InvalidLayout
                ))
            ));
            assert!(matches!(
                checkout
                    .read_file_up_to(&path, 0, 1, &foreign, &cancellation)
                    .await,
                Err(FsError::NativeKernel(
                    crate::NativeTreeMaterializationError::InvalidLayout
                ))
            ));
            assert!(matches!(
                checkout
                    .plan_file_extents(&path, range, 8, &foreign, &cancellation)
                    .await,
                Err(FsError::NativeKernel(
                    crate::NativeTreeMaterializationError::InvalidLayout
                ))
            ));
            assert_eq!(foreign.receipt(), foreign_before);
        }
        let reader = checkout.into_pinned_reader_with_native_work(&original, &cancellation)?;
        for name in ["inline", "regular"] {
            let path = paths::customer_path(&format!("/{name}"), reader.volume_config())?;
            let range = crate::ByteRange {
                offset: 0,
                length: 1,
            };
            let (files, handles) = reader
                .resolve_files(std::slice::from_ref(&path), &original, &cancellation)
                .await?;
            let file = files[0].as_ref().ok_or("missing native extent file")?;
            for resolved in [false, true] {
                let baseline = original.live_allocation_bytes();
                let (plan, allocation) = if resolved {
                    file.plan_extents(range, 8, &original, &cancellation)
                        .await?
                } else {
                    reader
                        .plan_file_extents(&path, range, 8, &original, &cancellation)
                        .await?
                };
                assert!(allocation.work().shares_ledger(&original));
                assert_eq!(
                    allocation.bytes(),
                    plan.as_ref()
                        .map_or(0, |plan| plan.retained_allocation_bytes)
                );
                assert_eq!(
                    original.live_allocation_bytes(),
                    baseline + allocation.bytes()
                );
                drop((plan, allocation));
                assert_eq!(original.live_allocation_bytes(), baseline);
            }
            drop((files, handles));
            assert!(matches!(
                reader
                    .plan_file_extents(&path, range, 8, &foreign, &cancellation)
                    .await,
                Err(FsError::NativeKernel(
                    crate::NativeTreeMaterializationError::InvalidLayout
                ))
            ));
        }
        drop(reader);
        // The original receipt retires the shared proof-accounting Arc after
        // its final reader leaves; observing live bytes alone does not retire it.
        original.receipt().verify(budget)?;
        assert_eq!(original.live_allocation_bytes(), original_baseline);
        assert_eq!(foreign.live_allocation_bytes(), foreign_baseline);
        drop((original, foreign));
        assert!(original_state.upgrade().is_none());
        assert!(foreign_state.upgrade().is_none());
    }
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let path = paths::customer_path("/regular", checkout.volume_config())?;
    let range = crate::ByteRange {
        offset: 0,
        length: 1,
    };
    for pinned in [false, true] {
        let failure = if pinned {
            reader
                .plan_file_extents(&path, range, 8, budget, &cancelled)
                .await
        } else {
            checkout
                .plan_file_extents(&path, range, 8, budget, &cancelled)
                .await
        }
        .unwrap_err();
        assert!(matches!(failure.error, FsError::Cancelled(_)));
        assert_eq!(failure.work.backend_read_operations, 0);
    }
    Ok(())
}

pub(super) struct Observation {
    pub(super) skip_heads: usize,
    pub(super) minimum: u64,
    pub(super) completion: Option<tokio::sync::oneshot::Receiver<()>>,
}

pub(super) enum ReplayCustody {
    Standalone(Arc<Prefunded>),
    #[cfg(not(target_arch = "wasm32"))]
    Native(crate::NativeMaterializationWork),
}

#[derive(Default)]
pub(super) struct ObservedAuthority {
    inner: crate::MemoryAuthorityStore,
    pub(super) observation: Mutex<Option<Observation>>,
    pub(super) replay_minimum: Mutex<Option<u64>>,
    pub(super) replay_custody: Mutex<Option<ReplayCustody>>,
}

impl AsyncAuthorityStore for ObservedAuthority {
    fn operation_claims(&self) -> &acyclic_stream::OperationClaims {
        self.inner.operation_claims()
    }

    fn actor_scope(&self) -> [u8; 16] {
        self.inner.actor_scope()
    }

    fn policy_owner(&self) -> crate::PolicyOwner {
        self.inner.policy_owner()
    }

    async fn create_authority<G: crate::AuthorityGuard>(
        &self,
        authority: crate::AuthorityId,
        epoch: crate::Epoch,
        work: &mut AuthorityWork<'_>,
        guard: Option<&mut G>,
    ) -> Result<crate::CreateAuthorityOutcome, crate::AuthorityError<G::Error>> {
        self.inner
            .create_authority(authority, epoch, work, guard)
            .await
    }

    async fn create_first_record<G: crate::AuthorityGuard>(
        &self,
        authority: crate::AuthorityId,
        commit: crate::ProposedCommit,
        work: &mut AuthorityWork<'_>,
        guard: Option<&mut G>,
    ) -> Result<bool, crate::AuthorityError<G::Error>> {
        self.inner
            .create_first_record(authority, commit, work, guard)
            .await
    }

    async fn compare_and_append<G: crate::AuthorityGuard>(
        &self,
        request: crate::GuardedAppend,
        work: &mut AuthorityWork<'_>,
        guard: Option<&mut G>,
    ) -> Result<crate::AppendOutcome, crate::AuthorityError<G::Error>> {
        self.inner.compare_and_append(request, work, guard).await
    }

    async fn head(
        &self,
        authority: crate::AuthorityId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<crate::Head, AuthorityStoreError> {
        let observation = {
            let mut observation = self.observation.lock().unwrap();
            if let Some(pending) = observation
                .as_mut()
                .filter(|pending| pending.skip_heads != 0)
            {
                pending.skip_heads -= 1;
                None
            } else {
                observation.take()
            }
        };
        if let Some(observation) = observation {
            let live = work.allocations.live_bytes();
            #[cfg(not(target_arch = "wasm32"))]
            let live = work
                .original
                .map_or(live, |work| work.live_allocation_bytes());
            assert!(live >= observation.minimum);
            assert!(work.spent().items_examined > 0);
            work.charge(WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            })?;
            if let Some(completion) = observation.completion {
                completion.await.map_err(|_| AuthorityStoreError::Retired)?;
            }
        }
        self.inner.head(authority, work).await
    }

    async fn replay(
        &self,
        authority: crate::AuthorityId,
        after: crate::Sequence,
        limit: crate::ReplayLimit,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(Vec<crate::DurableCommit>, crate::AuthorityAllocation), AuthorityStoreError> {
        if let Some(expected) = self.replay_custody.lock().unwrap().take() {
            let shares = match expected {
                ReplayCustody::Standalone(original) => {
                    let held = original.held.lock().unwrap();
                    let ledger = held
                        .as_ref()
                        .unwrap()
                        .0
                        .allocation
                        .standalone_ledger()
                        .unwrap();
                    work.allocations.shares_retained_ledger(Arc::as_ptr(ledger))
                }
                #[cfg(not(target_arch = "wasm32"))]
                ReplayCustody::Native(original) => work
                    .original
                    .is_some_and(|work| work.shares_ledger(&original)),
            };
            assert!(shares, "fork replaced the original accounting ledger");
        }
        let minimum = self.replay_minimum.lock().unwrap().take();
        if let Some(minimum) = minimum {
            let live = work
                .accounting()
                .standalone_parts()
                .map(|parts| parts.0.live_bytes());
            #[cfg(not(target_arch = "wasm32"))]
            let live = work
                .original
                .map(|work| work.live_allocation_bytes())
                .or(live);
            assert!(
                live.is_some_and(|live| live >= minimum),
                "creation lost original workspace-name backing: {live:?} < {minimum}"
            );
        }
        self.inner.replay(authority, after, limit, work).await
    }

    async fn find_operation(
        &self,
        authority: crate::AuthorityId,
        operation: crate::OperationId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<Option<crate::DurableCommit>, AuthorityStoreError> {
        self.inner.find_operation(authority, operation, work).await
    }
}

#[tokio::test]
async fn workspace_extent_projection_keeps_original_work_and_single_output_backing()
-> Result<(), Box<dyn std::error::Error>> {
    #[cfg(not(target_arch = "wasm32"))]
    crate::initialize_test_native_io();
    let cancellation = CancellationToken::new();
    let budget = crate::performance::BOUNDARY_BUDGET;
    let workspace = Fs::memory()
        .create_workspace("workspace-extent-custody", budget, &cancellation)
        .await?
        .value;
    workspace.write_text("/inline", "small").await?;
    workspace.write_text("/regular", &"x".repeat(300)).await?;
    let generation = workspace.head(budget, &cancellation).await?.value;
    for path in ["/inline", "/regular"] {
        for exact in [false, true] {
            for reads in [0, budget.backend_read_operations] {
                let source = Prefunded {
                    budget: WorkBudget {
                        backend_read_operations: reads,
                        ..budget
                    },
                    held: Mutex::new(None),
                };
                let (result, spent, live, baseline) = if exact {
                    generation
                        .plan_extents(path, 1, 3, 8, &source, &cancellation)
                        .await
                } else {
                    workspace
                        .plan_extents(path, 1, 3, 8, &source, &cancellation)
                        .await
                };
                spent.verify(source.budget)?;
                if reads == 0 {
                    assert!(
                        matches!(
                            result,
                            Err(WorkspaceError::Work(crate::WorkError::BudgetExceeded {
                                counter: "backend_read_operations",
                                ..
                            }))
                        ),
                        "zero-read refusal lost its public cause: {:?}",
                        result.as_ref().err(),
                    );
                    assert_eq!(spent.backend_read_operations, 0);
                    assert_eq!(live, baseline);
                } else {
                    let (plan, allocation) = result?;
                    assert!(allocation.is_none());
                    assert_eq!(plan.spans.len(), 1);
                    assert_eq!((plan.spans[0].offset, plan.spans[0].length), (1, 3));
                    assert_eq!(plan.spans[0].kind, WorkspaceExtentKind::Content);
                    assert!(spent.items_examined > 1);
                    assert_eq!(
                        live,
                        baseline
                            + (plan.spans.capacity() * size_of::<WorkspaceExtentSpan>()) as u64
                    );
                }
            }
        }
    }
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let failure = generation
        .plan_extents("/regular", 0, 1, 8, budget, &cancelled)
        .await
        .unwrap_err();
    assert!(matches!(failure.error, WorkspaceError::Cancelled(_)));
    assert_eq!(failure.work.backend_read_operations, 0);

    #[cfg(not(target_arch = "wasm32"))]
    {
        let original = crate::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let owner = std::sync::Arc::downgrade(&original.state);
        for (path, length) in [("/inline", 5), ("/regular", 300)] {
            let (plan, allocation) = generation
                .plan_extents(path, 0, length, 8, &original, &cancellation)
                .await?;
            let allocation = allocation.ok_or("workspace extent output lost original custody")?;
            assert!(allocation.work().shares_ledger(&original));
            let bytes = (plan.spans.capacity() * size_of::<WorkspaceExtentSpan>()) as u64;
            assert_eq!(allocation.bytes(), bytes);
            assert_eq!(plan.spans.len(), 1);
            assert_eq!((plan.spans[0].offset, plan.spans[0].length), (0, length));
            original.receipt().verify(budget)?;
            assert_eq!(original.live_allocation_bytes(), baseline + bytes);
            drop((plan, allocation));
            original.receipt().verify(budget)?;
            assert_eq!(original.live_allocation_bytes(), baseline);
        }
        let (empty, allocation) = generation
            .plan_extents("/inline", 5, 0, 8, &original, &cancellation)
            .await?;
        assert!(empty.spans.is_empty());
        assert_eq!(
            allocation
                .ok_or("empty plan lost original identity")?
                .bytes(),
            0
        );
        original.receipt().verify(budget)?;
        assert_eq!(original.live_allocation_bytes(), baseline);
        drop(original);
        assert!(owner.upgrade().is_none());
    }
    Ok(())
}

#[tokio::test]
async fn workspace_metadata_and_directory_pages_retire_only_their_own_scratch()
-> Result<(), Box<dyn std::error::Error>> {
    #[cfg(not(target_arch = "wasm32"))]
    crate::initialize_test_native_io();
    let cancellation = CancellationToken::new();
    let budget = crate::performance::BOUNDARY_BUDGET;
    let authority = Arc::new(ObservedAuthority::default());
    let filesystem = Fs::new(
        authority.clone(),
        Arc::new(crate::MemoryObjectStore::default()),
        crate::EmbeddedCapabilities::MEMORY,
    );
    let workspace = filesystem
        .create_workspace("workspace-page-custody", budget, &cancellation)
        .await?
        .value;
    workspace.write_text("/a", "first").await?;
    workspace.write_text("/b", "second").await?;
    let generation = workspace.head(budget, &cancellation).await?.value;
    for exact in [false, true] {
        for reads in [0, budget.backend_read_operations] {
            let source = Prefunded {
                budget: WorkBudget {
                    backend_read_operations: reads,
                    ..budget
                },
                held: Mutex::new(None),
            };
            let (result, spent, live, baseline) = if exact {
                generation.stat("/a", &source, &cancellation).await
            } else {
                workspace.stat("/a", &source, &cancellation).await
            };
            spent.verify(source.budget)?;
            assert_eq!(live, baseline, "metadata retained discarded path storage");
            if reads == 0 {
                assert!(matches!(
                    result,
                    Err(WorkspaceError::Work(crate::WorkError::BudgetExceeded {
                        counter: "backend_read_operations",
                        ..
                    }))
                ));
                assert_eq!(spent.backend_read_operations, 0);
            } else {
                assert_eq!(result?.logical_bytes, Some(5));
                assert!(spent.items_examined > 1);
            }
            let (result, spent, live, baseline) = if exact {
                generation
                    .list_directory("/", None, 1, &source, &cancellation)
                    .await
            } else {
                workspace
                    .list_directory("/", None, 1, &source, &cancellation)
                    .await
            };
            spent.verify(source.budget)?;
            if reads == 0 {
                assert!(matches!(
                    result,
                    Err(WorkspaceError::Work(crate::WorkError::BudgetExceeded {
                        counter: "backend_read_operations",
                        ..
                    }))
                ));
                assert_eq!(spent.backend_read_operations, 0);
                assert_eq!(live, baseline);
            } else {
                let (page, allocation) = result?;
                assert!(allocation.is_none());
                assert_eq!(page.entries.len(), 1);
                assert!(page.has_more);
                assert_eq!(page.work, spent);
                let bytes =
                    workspace_vec_bytes(&page.entries) + page.entries[0].name.logical_owned_bytes();
                assert_eq!(live, baseline + bytes, "page retained namespace scratch");
            }
        }
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        let result = if exact {
            generation
                .list_directory("/", None, 1, budget, &cancelled)
                .await
        } else {
            workspace
                .list_directory("/", None, 1, budget, &cancelled)
                .await
        };
        let failure = result.unwrap_err();
        assert!(matches!(failure.error, WorkspaceError::Cancelled(_)));
        assert_eq!(failure.work.backend_read_operations, 0);
    }
    for exact_payload in [false, true] {
        for reads in [0, budget.backend_read_operations] {
            let source = Prefunded {
                budget: WorkBudget {
                    backend_read_operations: reads,
                    ..budget
                },
                held: Mutex::new(None),
            };
            let (result, spent, live, baseline) = if exact_payload {
                generation
                    .matches_file_payload(&generation, "/a", &source, &cancellation)
                    .await
            } else {
                generation
                    .matches_content(&generation, "/a", &source, &cancellation)
                    .await
            };
            spent.verify(source.budget)?;
            assert_eq!(live, baseline, "comparison retained discarded path storage");
            if reads == 0 {
                assert!(matches!(
                    result,
                    Err(WorkspaceError::Work(crate::WorkError::BudgetExceeded {
                        counter: "backend_read_operations",
                        ..
                    }))
                ));
                assert_eq!(spent.backend_read_operations, 0);
            } else {
                assert!(result?);
                assert!(spent.items_examined > 1);
            }
        }
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        let original = crate::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let owner = std::sync::Arc::downgrade(&original.state);
        assert!(
            generation
                .matches_content(&generation, "/a", &original, &cancellation)
                .await?
        );
        assert!(
            generation
                .matches_optional_file_payload(&generation, "/missing", &original, &cancellation)
                .await?
        );
        original.receipt().verify(budget)?;
        assert_eq!(original.live_allocation_bytes(), baseline);
        assert_eq!(
            generation
                .stat("/a", &original, &cancellation)
                .await?
                .logical_bytes,
            Some(5)
        );
        original.receipt().verify(budget)?;
        assert_eq!(original.live_allocation_bytes(), baseline);
        assert!(matches!(
            generation.stat("/missing", &original, &cancellation).await,
            Err(WorkspaceError::NotFound)
        ));
        original.receipt().verify(budget)?;
        assert_eq!(original.live_allocation_bytes(), baseline);
        let (first, first_allocation) = generation
            .list_directory("/", None, 1, &original, &cancellation)
            .await?;
        let first_allocation = first_allocation.ok_or("first page lost original allocation")?;
        assert!(first_allocation.work().shares_ledger(&original));
        let first_bytes =
            workspace_vec_bytes(&first.entries) + first.entries[0].name.logical_owned_bytes();
        assert_eq!(first_allocation.bytes(), first_bytes);
        let (second, second_allocation) = generation
            .list_directory(
                "/",
                Some(&first.entries[0].name),
                1,
                &original,
                &cancellation,
            )
            .await?;
        let second_allocation = second_allocation.ok_or("second page lost original allocation")?;
        let second_bytes =
            workspace_vec_bytes(&second.entries) + second.entries[0].name.logical_owned_bytes();
        assert_eq!(second_allocation.bytes(), second_bytes);
        assert!(!second.has_more);
        original.receipt().verify(budget)?;
        assert_eq!(
            original.live_allocation_bytes(),
            baseline + first_bytes + second_bytes
        );
        drop((second, second_allocation));
        original.receipt().verify(budget)?;
        assert_eq!(original.live_allocation_bytes(), baseline + first_bytes);
        drop((first, first_allocation));
        original.receipt().verify(budget)?;
        assert_eq!(original.live_allocation_bytes(), baseline);
        drop(original);
        assert!(owner.upgrade().is_none());

        for metadata in [false, true] {
            for withdraw in [false, true] {
                let cancellation = CancellationToken::new();
                let original = crate::NativeMaterializationWork::new(budget, &cancellation)
                    .map_err(|(error, _)| error)?;
                original.charge(WorkCounters {
                    items_examined: 1,
                    ..WorkCounters::UNCHARGED
                })?;
                let baseline = original.live_allocation_bytes();
                let (producer, completion) = tokio::sync::oneshot::channel();
                *authority.observation.lock().unwrap() = Some(Observation {
                    skip_heads: 0,
                    minimum: baseline,
                    completion: Some(completion),
                });
                let mut operation = Box::pin(async {
                    if metadata {
                        workspace
                            .stat("/a", &original, &cancellation)
                            .await
                            .map(|_| ())
                    } else {
                        workspace
                            .list_directory("/", None, 1, &original, &cancellation)
                            .await
                            .map(|_| ())
                    }
                });
                assert!(futures::poll!(operation.as_mut()).is_pending());
                assert!(authority.observation.lock().unwrap().is_none());
                if withdraw {
                    cancellation.cancel();
                    drop(operation);
                    drop(producer);
                } else {
                    drop(producer);
                    assert!(matches!(
                        operation.await,
                        Err(WorkspaceError::Filesystem(FsError::WorkspaceDeleted))
                    ));
                }
                original.receipt().verify(budget)?;
                assert!(original.receipt().items_examined > 1);
                assert_eq!(original.live_allocation_bytes(), baseline);
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn workspace_byte_reads_retire_namespace_scratch_and_keep_original_output()
-> Result<(), Box<dyn std::error::Error>> {
    #[cfg(not(target_arch = "wasm32"))]
    crate::initialize_test_native_io();
    let cancellation = CancellationToken::new();
    let budget = crate::performance::BOUNDARY_BUDGET;
    let filesystem = Fs::memory();
    let workspace = filesystem
        .create_workspace("read-output-custody", budget, &cancellation)
        .await?
        .value;
    workspace.write_text("/inline", "first").await?;
    workspace.write_text("/regular", &"x".repeat(300)).await?;
    let mut transaction = workspace
        .begin_transaction(IdempotencyKey::new(), budget, &cancellation)
        .await?
        .value;
    transaction
        .create_symbolic_link(
            "/link",
            acyclic_stream::OwnedBytes::from_static(b"/inline"),
            budget,
            &cancellation,
        )
        .await?;
    let committed = transaction
        .commit::<WorkspaceError, _, _>(
            None,
            crate::PublicationPermit::Unrestricted,
            budget,
            &cancellation,
            |_| {
                std::future::ready(Ok(crate::OperationReceipt {
                    value: (),
                    work: WorkCounters::UNCHARGED,
                }))
            },
        )
        .await?
        .value;
    let TransactionCommit::Committed(generation) = committed else {
        return Err("byte read fixture did not commit".into());
    };
    for exact in [false, true] {
        for kind in 0..3 {
            for reads in [0, budget.backend_read_operations] {
                let source = Prefunded {
                    budget: WorkBudget {
                        backend_read_operations: reads,
                        ..budget
                    },
                    held: Mutex::new(None),
                };
                let selector = if exact {
                    GenerationSelector::Exact(generation.id())
                } else {
                    GenerationSelector::Head
                };
                let (result, spent, live, baseline) = match kind {
                    0 => {
                        let (result, spent, live, baseline) = read_selected(
                            &workspace,
                            selector,
                            "/inline",
                            5,
                            &source,
                            &cancellation,
                        )
                        .await;
                        (
                            result.map(|bytes| {
                                let capacity = bytes.retained_capacity().unwrap_or(0) as u64;
                                (bytes, capacity)
                            }),
                            spent,
                            live,
                            baseline,
                        )
                    }
                    1 => {
                        let (result, spent, live, baseline) = read_range(
                            &workspace,
                            selector,
                            "/regular",
                            crate::ByteRange {
                                offset: 10,
                                length: 7,
                            },
                            None,
                            &source,
                            &cancellation,
                        )
                        .await;
                        (
                            result.map(|chunk| (chunk.bytes, chunk.logical_owned_bytes)),
                            spent,
                            live,
                            baseline,
                        )
                    }
                    _ => {
                        let (result, spent, live, baseline) =
                            read_link(&workspace, selector, "/link", &source, &cancellation).await;
                        (
                            result.map(|chunk| (chunk.bytes, chunk.logical_owned_bytes)),
                            spent,
                            live,
                            baseline,
                        )
                    }
                };
                spent.verify(source.budget)?;
                if reads == 0 {
                    assert!(matches!(
                        result,
                        Err(WorkspaceError::Work(crate::WorkError::BudgetExceeded {
                            counter: "backend_read_operations",
                            ..
                        }))
                    ));
                    assert_eq!(spent.backend_read_operations, 0);
                    assert_eq!(live, baseline);
                } else {
                    let (bytes, retained) = result?;
                    let expected: &[u8] = match kind {
                        0 => b"first",
                        1 => b"xxxxxxx",
                        _ => b"/inline",
                    };
                    assert_eq!(bytes.as_ref(), expected);
                    assert_eq!(
                        live,
                        baseline + retained,
                        "byte read retained namespace storage"
                    );
                    assert!(spent.items_examined > 1);
                }
            }
        }
    }
    let source = Prefunded {
        budget,
        held: Mutex::new(None),
    };
    let (result, spent, live, baseline) =
        generation.read("/inline", 4, &source, &cancellation).await;
    assert!(matches!(result, Err(WorkspaceError::ReadLimitExceeded)));
    spent.verify(budget)?;
    assert_eq!(live, baseline);
    #[cfg(not(target_arch = "wasm32"))]
    {
        let original = crate::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let owner = Arc::downgrade(&original.state);
        let first = generation
            .read("/inline", 5, &original, &cancellation)
            .await?;
        assert_eq!(first.as_ref(), b"first");
        assert!(first.matches_meter(&original.runtime_meter()));
        original.receipt().verify(budget)?;
        let first_live = original.live_allocation_bytes();
        assert!(first_live > baseline);
        let range = generation
            .read_range("/regular", 10, 7, &original, &cancellation)
            .await?;
        assert_eq!(range.bytes.as_ref(), b"xxxxxxx");
        let link = generation
            .read_symbolic_link("/link", &original, &cancellation)
            .await?;
        assert_eq!(link.bytes.as_ref(), b"/inline");
        drop((range, link));
        original.receipt().verify(budget)?;
        assert_eq!(original.live_allocation_bytes(), first_live);
        drop(first);
        original.receipt().verify(budget)?;
        assert_eq!(original.live_allocation_bytes(), baseline);
        drop(original);
        assert!(owner.upgrade().is_none());
    }
    Ok(())
}

#[tokio::test]
async fn policy_revision_retires_original_scratch_on_confirmation_close_and_withdrawal()
-> Result<(), Box<dyn std::error::Error>> {
    #[cfg(not(target_arch = "wasm32"))]
    crate::initialize_test_native_io();
    let cancellation = CancellationToken::new();
    let budget = crate::performance::BOUNDARY_BUDGET;
    let authority = Arc::new(ObservedAuthority::default());
    let fs = Fs::new(
        authority.clone(),
        Arc::new(crate::MemoryObjectStore::default()),
        crate::EmbeddedCapabilities::MEMORY,
    );
    let workspace = fs
        .create_workspace("policy-revision-retirement", budget, &cancellation)
        .await?
        .value;
    // The selected root owns a real parent vector until its metadata is read.
    workspace.write_text("/entry", "payload").await?;
    let selected = workspace.head(budget, &cancellation).await?.value;
    for reads in [0, budget.backend_read_operations] {
        let source = Prefunded {
            budget: WorkBudget {
                backend_read_operations: reads,
                ..budget
            },
            held: Mutex::new(None),
        };
        let (result, spent, live, baseline) = workspace
            .current_access_policy_revision(&source, &cancellation)
            .await;
        spent.verify(source.budget)?;
        assert_eq!(live, baseline);
        if reads == 0 {
            assert!(
                matches!(
                    &result,
                    Err(FsError::Authority(crate::AuthorityStoreError::Work(
                        crate::WorkError::BudgetExceeded {
                            counter: "backend_read_operations",
                            ..
                        }
                    )))
                ),
                "{result:?}"
            );
            assert_eq!(spent.backend_read_operations, 0);
        } else {
            assert_eq!(
                result?.map(|(generation, _)| generation),
                Some(selected.id())
            );
            assert!(spent.items_examined > 1);
        }
    }
    for outcome in 0..3 {
        let cancellation = CancellationToken::new();
        let source = Prefunded {
            budget,
            held: Mutex::new(None),
        };
        let funded = &source;
        let mut accounting = funded.accounting(&cancellation);
        let mut earlier = crate::kernel::Owned::<Vec<u8>>::allocate(128, &mut accounting)?;
        earlier.value.push(7);
        let baseline = accounting.standalone_parts().unwrap().0.live_bytes();
        let before = accounting.spent();
        let (producer, completion) = tokio::sync::oneshot::channel();
        *authority.observation.lock().unwrap() = Some(Observation {
            skip_heads: 1,
            minimum: baseline + 1,
            completion: Some(completion),
        });
        let mut operation = Box::pin(
            workspace.current_access_policy_revision_accounted(&mut accounting, &cancellation),
        );
        assert!(futures::poll!(operation.as_mut()).is_pending());
        assert!(authority.observation.lock().unwrap().is_none());
        match outcome {
            0 => {
                producer
                    .send(())
                    .map_err(|_| "confirmation receiver lost")?;
                assert_eq!(
                    operation.await?.map(|(generation, _)| generation),
                    Some(selected.id())
                );
            }
            1 => {
                drop(producer);
                assert!(matches!(operation.await, Err(FsError::WorkspaceDeleted)));
            }
            _ => {
                cancellation.cancel();
                drop(operation);
                drop(producer);
            }
        }
        accounting.spent().verify(budget)?;
        assert!(accounting.spent().backend_read_operations > before.backend_read_operations);
        assert!(accounting.spent().items_examined > before.items_examined);
        assert_eq!(
            accounting.standalone_parts().unwrap().0.live_bytes(),
            baseline
        );
        assert_eq!(earlier.value, [7]);
        earlier.retire(&mut accounting)?;
    }
    Ok(())
}

#[tokio::test]
async fn sparse_seek_retires_original_traversal_on_read_close_and_withdrawal()
-> Result<(), Box<dyn std::error::Error>> {
    #[cfg(not(target_arch = "wasm32"))]
    crate::initialize_test_native_io();
    let cancellation = CancellationToken::new();
    let budget = crate::performance::BOUNDARY_BUDGET;
    let objects = Arc::new(crate::test_support::PausedStore {
        inner: Arc::new(crate::MemoryObjectStore::default()),
        release: Mutex::new(None),
    });
    let fs = Fs::new(
        Arc::new(crate::MemoryAuthorityStore::default()),
        objects.clone(),
        crate::EmbeddedCapabilities::MEMORY,
    );
    let workspace = fs
        .create_workspace("seek-retirement", budget, &cancellation)
        .await?
        .value;
    workspace.write_text("/regular", &"x".repeat(300)).await?;
    let mut checkout = workspace
        .checkout(
            GenerationSelector::Head,
            CheckoutMode::read_only_pinned(),
            budget,
            &cancellation,
        )
        .await?
        .value;
    let path = paths::customer_path("/regular", workspace.volume_config())?;
    let mut setup = KernelAccounting::standalone(budget, &cancellation);
    let record = checkout
        .lookup_no_follow_with_accounting(&path, &mut setup, &cancellation, false)
        .await?
        .0
        .record
        .ok_or("seek fixture has no regular file")?;
    for outcome in 0..3 {
        let cancellation = CancellationToken::new();
        let source = Prefunded {
            budget,
            held: Mutex::new(None),
        };
        let funded = &source;
        let mut accounting = funded.accounting(&cancellation);
        let mut earlier = crate::kernel::Owned::<Vec<u8>>::allocate(128, &mut accounting)?;
        earlier.value.push(7);
        let baseline = accounting.standalone_parts().unwrap().0.live_bytes();
        let before = accounting.spent();
        let (producer, completion) = tokio::sync::oneshot::channel();
        *objects.release.lock().unwrap() = Some(completion);
        let mut operation = Box::pin(crate::facade::seek_record_extent_with_accounting(
            &workspace.volume,
            record,
            1,
            crate::ExtentSeekTarget::Data,
            &mut accounting,
            &cancellation,
        ));
        assert!(futures::poll!(operation.as_mut()).is_pending());
        assert!(objects.release.lock().unwrap().is_none());
        match outcome {
            0 => {
                producer.send(()).map_err(|_| "seek read receiver lost")?;
                assert_eq!(operation.await?, Some(1));
            }
            1 => {
                drop(producer);
                assert!(matches!(
                    operation.await,
                    Err(FsError::Object(crate::ObjectStoreError::Rejected(message)))
                        if message == "cache fill producer lost"
                ));
            }
            _ => {
                cancellation.cancel();
                drop(operation);
                drop(producer);
            }
        }
        accounting.spent().verify(budget)?;
        assert_eq!(accounting.spent().page_reads, before.page_reads + 1);
        assert_eq!(
            accounting.standalone_parts().unwrap().0.live_bytes(),
            baseline
        );
        assert_eq!(earlier.value, [7]);
        earlier.retire(&mut accounting)?;
    }
    Ok(())
}

#[tokio::test]
async fn scalar_record_reads_retire_original_traversal_on_close_and_withdrawal()
-> Result<(), Box<dyn std::error::Error>> {
    #[cfg(not(target_arch = "wasm32"))]
    crate::initialize_test_native_io();
    let cancellation = CancellationToken::new();
    let budget = crate::performance::BOUNDARY_BUDGET;
    let objects = Arc::new(crate::test_support::PausedStore {
        inner: Arc::new(crate::MemoryObjectStore::default()),
        release: Mutex::new(None),
    });
    let fs = Fs::new(
        Arc::new(crate::MemoryAuthorityStore::default()),
        objects.clone(),
        crate::EmbeddedCapabilities::MEMORY,
    );
    let workspace = fs
        .create_workspace("scalar-record-retirement", budget, &cancellation)
        .await?
        .value;
    workspace.write_text("/entry", "original bytes").await?;
    let mut checkout = workspace
        .checkout(
            GenerationSelector::Head,
            CheckoutMode::read_only_pinned(),
            budget,
            &cancellation,
        )
        .await?
        .value;
    let path = paths::customer_path("/entry", workspace.volume_config())?;
    let record = checkout
        .lookup_no_follow(&path, budget, &cancellation)
        .await?
        .value
        .record
        .ok_or("scalar fixture has no file record")?;
    let metadata = checkout
        .read_metadata(record.file_id, budget, &cancellation)
        .await?
        .value;
    for reads in [0, budget.backend_read_operations] {
        let source = Prefunded {
            budget: WorkBudget {
                backend_read_operations: reads,
                ..budget
            },
            held: Mutex::new(None),
        };
        let (result, spent, live, baseline) = checkout
            .read_file_record_by_id(record.file_id, &source, &cancellation)
            .await;
        spent.verify(source.budget)?;
        assert_eq!(live, baseline);
        if reads == 0 {
            assert!(matches!(
                result,
                Err(FsError::Work(crate::WorkError::BudgetExceeded {
                    counter: "backend_read_operations",
                    ..
                }))
            ));
            assert_eq!(spent.backend_read_operations, 0);
        } else {
            assert_eq!(result?, record);
            assert!(spent.items_examined > 1);
        }
    }
    for kind in 0..3 {
        for outcome in 0..3 {
            let cancellation = CancellationToken::new();
            let source = Prefunded {
                budget,
                held: Mutex::new(None),
            };
            let funded = &source;
            let mut accounting = funded.accounting(&cancellation);
            let mut earlier = crate::kernel::Owned::<Vec<u8>>::allocate(128, &mut accounting)?;
            earlier.value.push(7);
            let baseline = accounting.standalone_parts().unwrap().0.live_bytes();
            let before = accounting.spent();
            let (producer, completion) = tokio::sync::oneshot::channel();
            *objects.release.lock().unwrap() = Some(completion);
            let mut operation = Box::pin(async {
                match kind {
                    0 => {
                        let found = checkout
                            .lookup_file_record_by_id_with_accounting(
                                record.file_id,
                                &mut accounting,
                                &cancellation,
                            )
                            .await?;
                        assert_eq!(found, Some(record));
                    }
                    1 => {
                        let found = checkout
                            .read_metadata_with_accounting(
                                record.file_id,
                                &mut accounting,
                                &cancellation,
                            )
                            .await?;
                        assert_eq!(found, metadata);
                    }
                    _ => {
                        let found = checkout
                            .read_metadata_with_accounting(&path, &mut accounting, &cancellation)
                            .await?;
                        assert_eq!(found, metadata);
                    }
                }
                Ok::<(), FsError>(())
            });
            assert!(futures::poll!(operation.as_mut()).is_pending());
            assert!(objects.release.lock().unwrap().is_none());
            match outcome {
                0 => {
                    producer.send(()).map_err(|_| "scalar read receiver lost")?;
                    operation.await?;
                }
                1 => {
                    drop(producer);
                    assert!(matches!(
                        operation.await,
                        Err(FsError::Object(crate::ObjectStoreError::Rejected(message)))
                            if message == "cache fill producer lost"
                    ));
                }
                _ => {
                    cancellation.cancel();
                    drop(operation);
                    drop(producer);
                }
            }
            accounting.spent().verify(budget)?;
            assert!(accounting.spent().items_examined > before.items_examined);
            assert_eq!(
                accounting.standalone_parts().unwrap().0.live_bytes(),
                baseline
            );
            assert_eq!(earlier.value, [7]);
            earlier.retire(&mut accounting)?;
        }
    }
    Ok(())
}
