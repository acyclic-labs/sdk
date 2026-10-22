//! Sealed admission reads for original-ledger preparation inspection.
use super::*;
use crate::filesystem::PreparationIo;
use crate::store::HistoryIo;
use acyclic_fs::{NativeAllocation, NativeMaterializationWork};

/// An authenticated admission reader that carries no execution fence or writer.
pub struct TaskAdmissionReadonlyWitness<P> {
    host: Arc<CoordinatorTaskHost<P>>,
    admission: TaskAdmissionRecord,
    _scratch: Option<crate::resource_preparation::ChildScratchCreationProof>,
    custody: NativeAllocation,
}

impl<P: StreamProvider> TaskAdmissionReadonlyWitness<P> {
    pub(crate) fn task(&self) -> TaskId {
        self.admission.task_id()
    }
    pub(crate) fn admission_operation(&self) -> OperationId {
        self.admission.operation_id
    }
    #[cfg(feature = "native-execution")]
    pub(crate) fn prepared_stream(&self) -> &Arc<P> {
        &self.host.stream
    }
    #[cfg(feature = "native-execution")]
    pub(crate) fn scratch_proof(
        &self,
    ) -> Option<&crate::resource_preparation::ChildScratchCreationProof> {
        self._scratch.as_ref()
    }

    pub(crate) async fn verify_with_native_work(
        &self,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        if !self.custody.work().shares_allocation_context(work) {
            return Err(denied());
        }
        let io = PreparationIo::from_native_work(work)?;
        let observed = self
            .host
            .admission(self.admission_operation(), &mut Some(&mut io.history_io()))
            .await?;
        let custody = io.take_native_custody(
            std::mem::size_of::<TaskAdmissionRecord>() as u64 + observed.logical_owned_bytes()?,
        )?;
        if observed != self.admission {
            return Err(denied());
        }
        drop(observed);
        drop(custody);
        Ok(())
    }

    fn require_query(
        &self,
        query: &crate::runtime::TaskContext,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        query.require_original_native_work(work)?;
        let scope = query.scope();
        if !self.custody.work().shares_allocation_context(work)
            || !query.native_metadata_matches(work)
            || !query
                .harness
                .original_host_binding_matches(self.host.as_ref(), self.host.policy.as_deref())
            || query.durable_task_id() != Some(self.task())
            || query.id() != self.admission_operation()
            || scope.grants() != &self.admission.grants
            || scope.limits() != self.admission.limits
            || scope.run_limits() != self.admission.run_limits
            || scope.resource_proofs() != self.admission.resource_proofs
            || scope.extensions() != self.admission.extensions.as_ref()
        {
            return Err(denied());
        }
        Ok(())
    }

    pub(crate) async fn verify_query_with_native_work(
        &self,
        query: &Arc<crate::runtime::TaskContext>,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        self.require_query(query, work)?;
        self.verify_with_native_work(work).await?;
        self.require_query(query, work)
    }

    pub(crate) fn original_preparation_request_with_native_work<'a>(
        &'a self,
        query: &'a Arc<crate::runtime::TaskContext>,
        work: &'a NativeMaterializationWork,
    ) -> Result<
        crate::NativeFutureBundle<
            'a,
            (
                crate::resource_preparation::ResourcePreparationRequest,
                NativeAllocation,
            ),
        >,
    > {
        self.require_query(query, work)?;
        crate::NativeFutureBundle::new(work, || async move {
            self.verify_query_with_native_work(query, work).await?;
            let io = PreparationIo::from_native_work(work)?;
            let (request, held) = crate::resource_preparation::ResourcePreparationRequest::decode(
                &self.admission.input,
                &mut Some(&mut io.history_io()),
            )?;
            let custody = io.take_native_custody(held)?;
            self.verify_query_with_native_work(query, work).await?;
            Ok((request, custody))
        })
    }

    pub(crate) fn verify_preparation_origin_with_native_work(
        &self,
        query: &Arc<crate::runtime::TaskContext>,
        request: &crate::resource_preparation::ResourcePreparationRequest,
        origin: &crate::resource_preparation::ResourceCreationOrigin,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        self.require_query(query, work)?;
        let io = PreparationIo::from_native_work(work)?;
        let mut history = io.history_io();
        let mut observer: Option<&mut dyn HistoryIo> = Some(&mut history);
        request.validate(&mut observer)?;
        let admitted = self.admission.canonical_digest_with_work(&mut observer)?;
        let original = crate::executor::json::canonical_json_digest(
            &self.admission.input,
            acyclic_stream::MAX_RECORD_BYTES as u64,
            &mut observer,
        )?;
        let supplied = crate::executor::json::canonical_json_digest(
            request,
            acyclic_stream::MAX_RECORD_BYTES as u64,
            &mut observer,
        )?;
        if origin.owner != self.host.owner
            || origin.preparation_task != self.task()
            || self.admission.parent != Some(origin.parent_task)
            || request.child.parent != Some(origin.parent_task)
            || origin.intended_child != request.child.operation_id
            || origin.admission_digest != admitted
            || origin.source_grants != self.admission.grants
            || origin.request_digest != original
            || supplied != original
        {
            return Err(denied());
        }
        Ok(())
    }
}

fn denied() -> Error {
    Error::NativeBoundary {
        code: crate::wire::ErrorCode::Unauthorized,
        reason: "preparation reader differs from its authenticated admission or original ledger",
    }
}

impl<P: StreamProvider> CoordinatorTaskHost<P> {
    pub(crate) fn task_preparation_readonly_witness_with_native_work<'a>(
        self: &'a Arc<Self>,
        query: &'a Arc<crate::runtime::TaskContext>,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, TaskAdmissionReadonlyWitness<P>>> {
        query.require_original_native_work(work)?;
        if !query.native_metadata_matches(work)
            || !query
                .harness
                .original_host_binding_matches(self.as_ref(), self.policy.as_deref())
        {
            return Err(denied());
        }
        let task = query.durable_task_id().ok_or_else(denied)?;
        if query.id().into_bytes() != task.into_bytes() {
            return Err(denied());
        }
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            let admission = self
                .admission(query.id(), &mut Some(&mut io.history_io()))
                .await?;
            let mut custody = io.take_native_custody(
                std::mem::size_of::<TaskAdmissionRecord>() as u64
                    + admission.logical_owned_bytes()?,
            )?;
            let access = query.scope().created_access();
            let scratch = if admission.parent.is_none() && admission.resource_proofs.is_empty() {
                None
            } else {
                let proof = access
                    .as_ref()
                    .map(|access| access.current_content(&mut Some(&mut io.history_io())))
                    .transpose()?
                    .flatten()
                    .and_then(|binding| binding.scratch_creation.as_ref())
                    .cloned()
                    .ok_or_else(denied)?;
                if proof.origin().intended_child.into_bytes() != task.into_bytes() {
                    return Err(denied());
                }
                Some(proof)
            };
            custody.absorb(&mut work.claim(
                std::mem::size_of::<TaskAdmissionReadonlyWitness<P>>() as u64,
                1,
                acyclic_fs::WorkCounters::default(),
            )?)?;
            let witness = TaskAdmissionReadonlyWitness {
                host: self.clone(),
                admission,
                _scratch: scratch,
                custody,
            };
            witness.require_query(query, work)?;
            Ok(witness)
        })
    }
}

impl<P: StreamProvider> CoordinatorTaskHost<P> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[allow(
        clippy::type_complexity,
        reason = "The funded future returns existing owner, request and custody types without another carrier."
    )]
    pub(crate) fn readonly_fork_journal_owner_with_native_work<'a>(
        self: &'a Arc<Self>,
        witness: &'a TaskAdmissionReadonlyWitness<P>,
        query: &'a Arc<crate::runtime::TaskContext>,
        claim: (
            crate::filesystem::AllocationClaim,
            crate::resource_preparation::ResourcePreparationRequest,
            acyclic_fs::NativeAllocation,
        ),
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<
        crate::NativeFutureBundle<
            'a,
            (
                Arc<TaskJournalOwner<P>>,
                crate::resource_preparation::ResourcePreparationRequest,
                acyclic_fs::NativeAllocation,
            ),
        >,
    > {
        if !claim.2.work().shares_allocation_context(work) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "protected fork claim differs from its original allocation context",
            });
        }
        let failed = |(_, _, custody), cause| Error::NativeWork {
            cause,
            custody: Some(custody),
        };
        let qualified = witness.require_query(query, work);
        if let Err(error) = qualified {
            return Err(match error {
                Error::NativeWork { cause, .. } => failed(claim, cause),
                error => error,
            });
        }
        let mut pending = Some(claim);
        let bundle = crate::NativeFutureBundle::new(work, || {
            let claim = pending.take();
            async move {
                let claim = claim.ok_or_else(denied)?;
                let result: Result<Arc<TaskJournalOwner<P>>> = async {
                    let origin = claim.0.task_origin.as_ref().ok_or_else(denied)?;
                    witness.verify_query_with_native_work(query, work).await?;
                    witness.verify_preparation_origin_with_native_work(
                        query, &claim.1, origin, work,
                    )?;
                    let io = PreparationIo::from_native_work(work)?;
                    let owner = self.original_journal_owner(
                        witness.task(), witness.admission_operation(), &origin.creation_fence, &io,
                    ).await?;
                    if owner.workflow.request_digest != origin.admission_digest {
                        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                            reason: "original Started admission differs from the protected fork claim" });
                    }
                    owner.require_task_context_binding(query.as_ref(), Some(&io))?;
                    owner.verify_read_access(&mut Some(&mut io.history_io())).await?;
                    witness.verify_query_with_native_work(query, work).await?;
                    owner.into_native_shared(work)
                }.await;
                match result {
                    Ok(owner) => {
                        let (claim, request, mut custody) = claim;
                        let retired = crate::filesystem::UploadRecord::logical_owned_bytes(&claim)?;
                        drop(claim);
                        if let Err(cause) = custody.release(retired) {
                            drop(request);
                            return Err(Error::NativeWork {
                                cause,
                                custody: Some(custody),
                            });
                        }
                        Ok((owner, request, custody))
                    }
                    Err(Error::NativeWork { cause, .. }) => Err(failed(claim, cause)),
                    Err(error) => Err(error),
                }
            }
        });
        match bundle {
            Err(Error::NativeWork { cause, .. }) => Err(Error::NativeWork {
                cause,
                custody: pending.take().map(|(_, _, custody)| custody),
            }),
            result => result,
        }
    }

    pub(crate) async fn original_journal_owner(
        self: &Arc<Self>,
        task: TaskId,
        operation: OperationId,
        fence: &crate::scheduler::LeaseFence,
        io: &PreparationIo,
    ) -> Result<TaskJournalOwner<P>> {
        let (original, _) = crate::distributed::original_reservation_proof(
            &self.stream,
            self.verifier.audience(),
            &self.owner_scope,
            &self.verifier,
            operation,
            fence,
            &mut Some(&mut io.history_io()),
        )
        .await?;
        let retained = original.logical_owned_bytes()?;
        let mut owner = self
            .journal_owner_record(task, fence, Some(original), Some(io))
            .await?
            .0;
        owner
            .native_custody
            .as_mut()
            .ok_or_else(denied)?
            .absorb(&mut io.take_native_custody(retained)?)?;
        Ok(owner)
    }

    #[cfg(not(target_arch = "wasm32"))]
    /// Opens the protected original readonly task owner on the supplied work.
    pub async fn readonly_journal_owner(
        self: &Arc<Self>,
        task: TaskId,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<TaskJournalOwner<P>> {
        #[cfg(not(feature = "native-execution"))]
        {
            let _ = task;
            work.remaining()?;
            Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "protected native preparation requires native-execution",
            })
        }
        #[cfg(feature = "native-execution")]
        {
            let witness = self.admission_reader(task, work).await?;
            let observation = crate::filesystem::preparation_owner(&witness, work)?.await?;
            let Some((recorded_task, original_fence, _custody)) = observation else {
                return self
                    .retained_journal_owner(task, witness.admission_operation(), work)
                    .await;
            };
            if recorded_task != task {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "protected native preparation belongs to another task",
                });
            }
            let io = PreparationIo::from_native_work(work)?;
            self.original_journal_owner(task, witness.admission_operation(), &original_fence, &io)
                .await
        }
    }
}

#[cfg(feature = "native-execution")]
impl<P: StreamProvider> CoordinatorTaskHost<P> {
    /// Opens the readonly owner of a task that has not prepared a native view yet.
    ///
    /// The fence comes from the coordinator's active reservation for the same task, or from
    /// the lease its committed workflow suspension released, and is accepted only after its
    /// original admitted and started origins verify.
    async fn retained_journal_owner(
        self: &Arc<Self>,
        task: TaskId,
        operation: OperationId,
        work: &NativeMaterializationWork,
    ) -> Result<TaskJournalOwner<P>> {
        let fence_io = PreparationIo::from_native_work(work)?;
        let fence = {
            let mut history = Some(&mut fence_io.history_io() as &mut dyn HistoryIo);
            let mut coordinator = self.coordinator.lock().await;
            coordinator.refresh(&mut history).await?;
            let state = coordinator.observe_operation(
                &self.owner,
                &self.owner_scope,
                &self.verifier,
                OperationId::from_bytes(task.into_bytes()),
                &mut history,
            )?;
            if let Some(reservation) = &state.reservation {
                let crate::scheduler::Reservation { id, placement, .. } =
                    reservation.clone_with_work(&mut history)?;
                crate::scheduler::LeaseFence {
                    reservation_id: id,
                    placement,
                }
            } else if let Some(released) = state
                .workflow
                .as_ref()
                .and_then(|suspension| suspension.released.as_ref())
            {
                released.clone_with_work(&mut history)?
            } else {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::NotFound,
                    reason: "protected original native preparation journal is unavailable",
                });
            }
        };
        let _fence_custody = fence_io.take_native_custody(fence_io.owned_bytes()?)?;
        let io = PreparationIo::from_native_work(work)?;
        self.original_journal_owner(task, operation, &fence, &io)
            .await
    }

    async fn admission_reader(
        self: &Arc<Self>,
        task: TaskId,
        work: &NativeMaterializationWork,
    ) -> Result<TaskAdmissionReadonlyWitness<P>> {
        let io = PreparationIo::from_native_work(work)?;
        let admission = self
            .admission(
                OperationId::from_bytes(task.into_bytes()),
                &mut Some(&mut io.history_io()),
            )
            .await?;
        let (scope, _) = self
            .authenticated_scope(&admission, &mut Some(&mut io.history_io()))
            .await?;
        let scratch = scope
            .current_created_content(&mut Some(&mut io.history_io()))?
            .and_then(|binding| binding.scratch_creation.as_ref())
            .cloned();
        if admission.parent.is_some()
            && scratch
                .as_ref()
                .is_none_or(|proof| proof.origin().intended_child != admission.operation_id)
        {
            return Err(denied());
        }
        let mut custody = io.take_native_custody(io.owned_bytes()?)?;
        custody.absorb(&mut work.claim(
            std::mem::size_of::<TaskAdmissionReadonlyWitness<P>>() as u64,
            1,
            acyclic_fs::WorkCounters::default(),
        )?)?;
        Ok(TaskAdmissionReadonlyWitness {
            host: self.clone(),
            admission,
            _scratch: scratch,
            custody,
        })
    }
}
