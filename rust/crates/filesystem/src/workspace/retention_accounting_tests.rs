use super::*;
use crate::facade::FsWork;
use crate::workspace::accounting_tests::Prefunded;
use crate::{AuthorityStoreError, AuthorityWork, WorkCounters};
use std::sync::Mutex;

struct Observation {
    minimum: u64,
    completion: Option<tokio::sync::oneshot::Receiver<()>>,
}

#[derive(Default)]
struct ObservedAuthority {
    inner: crate::MemoryAuthorityStore,
    observation: Mutex<Option<Observation>>,
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
        let observation = self.observation.lock().unwrap().take();
        if let Some(observation) = observation {
            assert!(work.allocations.live_bytes() >= observation.minimum);
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
async fn retention_keeps_original_work_on_success_refusal_and_producer_loss()
-> Result<(), Box<dyn std::error::Error>> {
    #[cfg(not(target_arch = "wasm32"))]
    crate::initialize_test_native_io();
    let cancellation = CancellationToken::new();
    let budget = crate::performance::BOUNDARY_BUDGET;
    let authority = Arc::new(ObservedAuthority::default());
    let fs = crate::Fs::new(
        authority.clone(),
        Arc::new(crate::MemoryObjectStore::default()),
        crate::EmbeddedCapabilities::MEMORY,
    );
    let workspace = fs
        .create_workspace("original-retention", budget, &cancellation)
        .await?
        .value;
    let generation = workspace.head(budget, &cancellation).await?.value;
    for reads in [0, budget.backend_read_operations] {
        let source = Prefunded {
            budget: WorkBudget {
                backend_read_operations: reads,
                ..budget
            },
            held: Mutex::new(None),
        };
        *authority.observation.lock().unwrap() = Some(Observation {
            minimum: 4096,
            completion: None,
        });
        let (result, spent, live, baseline) = generation.pin("exact", &source, &cancellation).await;
        spent.verify(source.budget)?;
        assert!(authority.observation.lock().unwrap().is_none());
        if reads == 0 {
            assert!(matches!(
                result,
                Err(WorkspaceError::Work(crate::WorkError::BudgetExceeded {
                    counter: "backend_read_operations",
                    ..
                }))
            ));
            assert_eq!(spent.backend_write_operations, 0);
            assert_eq!(live, baseline);
        } else {
            let (identity, pinned) = result?;
            assert_eq!(identity.as_str(), "exact");
            assert_eq!(pinned.id(), generation.id());
            assert!(live > baseline);
            assert!(spent.peak_allocation_bytes > baseline);
        }
    }

    let source = Prefunded {
        budget,
        held: Mutex::new(None),
    };
    *authority.observation.lock().unwrap() = Some(Observation {
        minimum: 4096,
        completion: None,
    });
    let (result, spent, live, baseline) = generation
        .retain_if_unpublished("exact", &source, &cancellation)
        .await;
    result?;
    spent.verify(budget)?;
    assert!(authority.observation.lock().unwrap().is_none());
    assert!(spent.backend_read_operations > 0);
    assert_eq!(spent.backend_write_operations, 0);
    assert_eq!(live, baseline);

    // Repeated exact replay shares one caller ledger and retires only each
    // discarded result's scratch. Its earlier buffer and retained Vec survive.
    let source = Prefunded {
        budget,
        held: Mutex::new(None),
    };
    let funded = &source;
    let mut accounting = funded.accounting(&cancellation);
    let mut earlier = crate::kernel::Owned::<Vec<u8>>::allocate(128, &mut accounting)?;
    earlier.value.push(7);
    let baseline = accounting.standalone_parts().unwrap().0.live_bytes();
    let scratch = accounting.standalone_parts().unwrap().0.scratch_bytes();
    let identity_bytes = {
        let mut scope = accounting.allocation_scope();
        let identity = WorkspaceName::with_accounting("exact", &mut scope)?;
        let bytes = scope.standalone_parts().unwrap().0.scratch_bytes() - scratch;
        drop(identity);
        bytes
    };
    for _ in 0..2 {
        let mut scope = accounting.allocation_scope();
        let pinned = generation
            .pin_with_accounting("exact", &mut scope, &cancellation)
            .await?;
        assert_eq!(pinned.1.id(), generation.id());
        assert_eq!(
            scope.standalone_parts().unwrap().0.scratch_bytes(),
            scratch + identity_bytes,
            "temporary retention buffers outlived publication"
        );
        drop(pinned);
        drop(scope);
        assert_eq!(
            accounting.standalone_parts().unwrap().0.live_bytes(),
            baseline
        );
        assert_eq!(earlier.value, [7]);
    }
    earlier.retire(&mut accounting)?;

    for withdraw in [false, true] {
        let cancellation = CancellationToken::new();
        let source = Prefunded {
            budget,
            held: Mutex::new(None),
        };
        let funded = &source;
        let mut accounting = funded.accounting(&cancellation);
        let baseline = accounting.standalone_parts().unwrap().0.live_bytes();
        let before = accounting.spent();
        let (producer, completion) = tokio::sync::oneshot::channel();
        *authority.observation.lock().unwrap() = Some(Observation {
            minimum: baseline,
            completion: Some(completion),
        });
        let mut operation =
            Box::pin(generation.pin_with_accounting("exact", &mut accounting, &cancellation));
        assert!(futures::poll!(operation.as_mut()).is_pending());
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
        assert_eq!(
            accounting.standalone_parts().unwrap().0.live_bytes(),
            baseline
        );
        assert!(accounting.spent().bytes_hashed > before.bytes_hashed);
        assert!(accounting.spent().items_examined > before.items_examined);
        assert_eq!(accounting.spent().backend_write_operations, 0);
    }
    Ok(())
}
