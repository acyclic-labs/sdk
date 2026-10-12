//! Readonly.
use super::*;

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl<P: StreamProvider> TaskAdmissionReadonlyWitness<P> {
    pub(crate) const fn task(&self) -> TaskId {
        TaskId::from_bytes(self.admission.operation_id.into_bytes())
    }
    pub(crate) const fn admission_operation(&self) -> OperationId {
        self.admission.operation_id
    }
    pub(crate) fn prepared_stream(&self) -> &std::sync::Arc<P> {
        &self.host.stream
    }
    pub(crate) fn scratch_proof(
        &self,
    ) -> Option<&crate::resource_preparation::ChildScratchCreationProof> {
        self.scratch.as_ref()
    }
    pub(crate) async fn verify_with_native_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        if !self._native_custody.matches_work(work) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "preparation admission differs from its original metadata ledger",
            });
        }
        let mut io = crate::store::history::native_work_io(work)?;
        let observed = self
            .host
            .admission_with_work(self.admission_operation(), &mut Some(&mut io))
            .await?;
        let matches = observed == self.admission;
        drop(observed);
        if !matches {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "preparation witness differs from its canonical task admission",
            });
        }
        Ok(())
    }

    fn require_query_with_native_work(
        &self,
        query: &Arc<crate::runtime::NativeTaskContext>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        self.host
            .require_preparation_query_with_native_work(query, work)?;
        let context = query.context();
        let scope = context.scope();
        if !self._native_custody.matches_work(work)
            || context.durable_task_id() != Some(self.task())
            || context.id() != self.admission.operation_id
            || scope.grants() != &self.admission.grants
            || scope.limits() != self.admission.limits
            || scope.resource_proofs() != self.admission.resource_proofs
            || scope.extensions() != self.admission.extensions.as_ref()
            || scope.run_limits() != self.admission.run_limits
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "preparation query differs from its original witnessed admission",
            });
        }
        Ok(())
    }

    pub(crate) async fn verify_query_with_native_work(
        &self,
        query: &Arc<crate::runtime::NativeTaskContext>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        self.require_query_with_native_work(query, work)?;
        self.verify_with_native_work(work).await?;
        self.require_query_with_native_work(query, work)
    }

    pub(crate) fn original_preparation_request_with_native_work<'a>(
        &'a self,
        query: &'a Arc<crate::runtime::NativeTaskContext>,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<
        crate::NativeFutureBundle<
            'a,
            (
                crate::resource_preparation::ResourcePreparationRequest,
                acyclic_fs::NativeAllocation,
            ),
        >,
    > {
        self.require_query_with_native_work(query, work)?;
        crate::NativeFutureBundle::new(work, || async move {
            self.verify_query_with_native_work(query, work).await?;
            let mut io = crate::store::history::native_work_io(work)?;
            let (request, held) = crate::resource_preparation::ResourcePreparationRequest::from_borrowed_value_with_work(
                &self.admission.input, &mut Some(&mut io))?;
            let pair = (request, io.take_native_custody(held)?);
            drop(io);
            if let Err(error) = self.verify_query_with_native_work(query, work).await {
                let (request, custody) = pair;
                drop(request);
                return Err(match error {
                    Error::NativeWork {
                        cause,
                        custody: None,
                    } => Error::NativeWork {
                        cause,
                        custody: Some(custody),
                    },
                    error => error,
                });
            }
            Ok(pair)
        })
    }

    pub(crate) fn verify_preparation_origin_with_native_work(
        &self,
        query: &Arc<crate::runtime::NativeTaskContext>,
        request: &crate::resource_preparation::ResourcePreparationRequest,
        origin: &crate::resource_preparation::ResourceCreationOrigin,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        self.require_query_with_native_work(query, work)?;
        let mut io = crate::store::history::native_work_io(work)?;
        let mut observer = Some(&mut io as &mut dyn crate::store::history::HistoryIo);
        request.validate(&mut observer)?;
        let admission_digest = self.admission.canonical_digest_with_work(&mut observer)?;
        let request_digest = crate::contract::canonical_json_digest_with_work(
            &self.admission.input,
            acyclic_stream::MAX_RECORD_BYTES as u64,
            &mut observer,
        )?;
        let supplied_digest = crate::contract::canonical_json_digest_with_work(
            request,
            acyclic_stream::MAX_RECORD_BYTES as u64,
            &mut observer,
        )?;
        if &origin.owner != self.host.verifier.audience()
            || origin.preparation_task != self.task()
            || self.admission.parent != Some(origin.parent_task)
            || request.child.parent != Some(origin.parent_task)
            || origin.intended_child != request.child.operation_id
            || origin.admission_digest != admission_digest
            || origin.source_grants != self.admission.grants
            || origin.request_digest != request_digest
            || supplied_digest != request_digest
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "protected fork origin differs from its original preparation admission",
            });
        }
        Ok(())
    }
}

impl<P: StreamProvider> CoordinatorTaskHost<P> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) async fn verify_native_command_result(
        &self,
        task: TaskId,
        fence: &crate::scheduler::LeaseFence,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        let mut io = crate::store::history::native_work_io(work)?;
        self.coordinator
            .lock()
            .await
            .journal_condition(
                self.verifier.audience(),
                &self.owner_scope,
                &self.verifier,
                OperationId::from_bytes(task.into_bytes()),
                fence,
                crate::distributed::JournalWrite::Fresh,
                &mut Some(&mut io),
            )
            .await
            .map(|_| ())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super) fn require_preparation_query_with_native_work(
        &self,
        query: &Arc<crate::runtime::NativeTaskContext>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<TaskId> {
        query.context().require_original_native_work(work)?;
        let task = query
            .context()
            .durable_task_id()
            .ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "preparation query has no original SDK task binding",
            })?;
        if !query.native_metadata_matches(work)
            || query.context().id().into_bytes() != task.into_bytes()
            || !query
                .context()
                .original_host_binding_matches(self, self.policy.as_deref())
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "preparation query has no original SDK host and allocation source",
            });
        }
        Ok(task)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn task_preparation_readonly_witness_with_native_work<'a>(
        self: &'a Arc<Self>,
        query: &'a Arc<crate::runtime::NativeTaskContext>,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, TaskAdmissionReadonlyWitness<P>>> {
        let task = self.require_preparation_query_with_native_work(query, work)?;
        crate::NativeFutureBundle::new(work, || async move {
            let witness = self
                .task_admission_readonly_witness_with_native_work(task, work)
                .await?;
            witness.verify_query_with_native_work(query, work).await?;
            Ok(witness)
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    /// Authenticates an original task admission for bounded read-only preparation queries.
    pub async fn task_admission_readonly_witness_with_native_work(
        self: &Arc<Self>,
        task: TaskId,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<TaskAdmissionReadonlyWitness<P>> {
        use crate::store::history::HistoryIo;
        let mut io = crate::store::history::native_work_io(work)?;
        let admission = self
            .admission_with_work(
                OperationId::from_bytes(task.into_bytes()),
                &mut Some(&mut io),
            )
            .await?;
        let scratch = if admission.parent.is_none() && admission.resource_proofs.is_empty() {
            None
        } else {
            let (scope, scope_bytes) = self
                .authenticated_scope_with_work(&admission, &mut Some(&mut io))
                .await?;
            let access = scope.created_access();
            let scratch = access
                .as_ref()
                .map(|access| access.current_content(&mut Some(&mut io)))
                .transpose()?
                .flatten()
                .and_then(|binding| binding.scratch_creation.as_ref())
                .cloned()
                .ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "child preparation has no original scratch creation proof",
                })?;
            if scratch.original_task() != task {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "preparation scratch proof belongs to another task",
                });
            }
            drop(access);
            drop(scope);
            io.release_owned(scope_bytes)?;
            Some(scratch)
        };
        io.retain_owned(std::mem::size_of::<TaskAdmissionReadonlyWitness<P>>() as u64)?;
        Ok(TaskAdmissionReadonlyWitness {
            host: Arc::clone(self),
            admission,
            scratch,
            _native_custody: io.into_custody(),
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn readonly_fork_journal_owner_with_native_work<'a>(
        self: &'a Arc<Self>,
        witness: &'a TaskAdmissionReadonlyWitness<P>,
        query: &'a Arc<crate::runtime::NativeTaskContext>,
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
                (
                    crate::resource_preparation::ResourcePreparationRequest,
                    acyclic_fs::NativeAllocation,
                ),
            ),
        >,
    > {
        if !claim.2.matches_work(work) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "protected fork claim differs from its original allocation context",
            });
        }
        let failed = |(_, _, custody), cause| Error::NativeWork {
            cause,
            custody: Some(custody),
        };
        let qualified = self
            .require_preparation_query_with_native_work(query, work)
            .and_then(|_| witness.require_query_with_native_work(query, work));
        if let Err(error) = qualified {
            return Err(match error {
                Error::NativeWork { cause, .. } => failed(claim, cause),
                error => error,
            });
        }
        let mut pending = Some(claim);
        let bundle = crate::NativeFutureBundle::new(work, || {
            let claim = pending.take().expect("funded original fork claim");
            async move {
                let result: Result<Arc<TaskJournalOwner<P>>> = async {
                    let origin = claim.0.task_origin.as_ref().expect("authenticated original fork origin");
                    witness.verify_query_with_native_work(query, work).await?;
                    witness.verify_preparation_origin_with_native_work(
                        query, &claim.1, origin, work,
                    )?;
                    let mut io = crate::store::history::native_work_io(work)?;
                    let original = crate::distributed::original_reservation_proof(
                        &self.stream, self.verifier.audience(), &self.owner_scope, &self.verifier,
                        witness.admission_operation(), &origin.creation_fence,
                        &mut Some(&mut io),
                    ).await?;
                    let owner = self.journal_owner_with_work(
                        witness.task(), std::borrow::Cow::Borrowed(&origin.creation_fence),
                        Some(original), &mut Some(&mut io),
                    ).await?;
                    if owner.original_admission_digest != origin.admission_digest {
                        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                            reason: "original Started admission differs from the protected fork claim" });
                    }
                    owner.require_task_context_binding(query.context(), Some(&crate::executor::PreparationIo::from_native_work(work)?))?;
                    owner.verify_read_access(&mut Some(&mut crate::store::history::native_work_io(work)?)).await?;
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
                        Ok((owner, (request, custody)))
                    }
                    Err(Error::NativeWork { cause, .. }) => Err(failed(claim, cause)),
                    Err(error) => Err(error),
                }
            }
        });
        match bundle {
            Err(Error::NativeWork { cause, .. }) => Err(failed(
                pending.take().expect("unfunded original fork claim"),
                cause,
            )),
            result => result,
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn readonly_journal_owner(
        self: &Arc<Self>,
        task: TaskId,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<TaskJournalOwner<P>> {
        #[cfg(not(feature = "native-execution"))]
        {
            let _ = task;
            work.remaining()?;
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "protected native preparation requires native-execution",
            });
        }
        #[cfg(feature = "native-execution")]
        {
            let witness = self
                .task_admission_readonly_witness_with_native_work(task, work)
                .await?;
            let observation = crate::filesystem::observe_preparation(&witness, work)?.await?;
            let Some(recorded) = observation else {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::NotFound,
                    reason: "protected original native preparation journal is unavailable",
                });
            };
            if recorded.task != task {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "protected native preparation belongs to another task",
                });
            }
            let mut io = crate::store::history::native_work_io(work)?;
            let original = crate::distributed::original_reservation_proof(
                &self.stream,
                self.verifier.audience(),
                &self.owner_scope,
                &self.verifier,
                witness.admission_operation(),
                &recorded.original_fence,
                &mut Some(&mut io),
            )
            .await?;
            self.journal_owner_with_work(
                task,
                std::borrow::Cow::Borrowed(&recorded.original_fence),
                Some(original),
                &mut Some(&mut io),
            )
            .await
        }
    }
}
