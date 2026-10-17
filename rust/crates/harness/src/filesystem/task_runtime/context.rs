use super::*;

impl<P, A, O> FilesystemTaskRuntime<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    #[cfg(not(target_arch = "wasm32"))]
    pub(in crate::filesystem) async fn readonly_fork_preparation_with_native_work(
        &self,
        task: TaskId,
        query: &Arc<crate::runtime::TaskContext>,
        admitted: &crate::resource_preparation::ResourcePreparationRequest,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<Arc<crate::resource_preparation::TaskResourcePreparation>>> {
        let witness = self
            .host
            .task_preparation_readonly_witness_with_native_work(query, work)?
            .await?;
        if witness.task() != task {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "readonly fork witness belongs to another task",
            });
        }
        let original = witness
            .original_preparation_request_with_native_work(query, work)?
            .await?;
        if &original.0 != admitted {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "readonly fork request differs from its original admission",
            });
        }
        let claim = self
            .filesystem
            .fork_allocation(&witness, query, &original.0, &self.volume, work)?
            .await?;
        let Some(claim) = claim else {
            return Ok(None);
        };
        let (owner, request, custody) = self
            .host
            .readonly_fork_journal_owner_with_native_work(&witness, query, claim, work)?
            .await?;
        let io = crate::executor::PreparationIo::from_native_work(work)?;
        io.absorb_native_custody(custody)?;
        let mut preparation = owner
            .resource_preparation(request, &mut Some(&mut io.history_io()))
            .await?;
        preparation.native_custody = Some(io.take_native_custody(io.owned_bytes()?)?);
        let mut header = work.claim(
            std::mem::size_of::<crate::resource_preparation::TaskResourcePreparation>() as u64,
            u64::from(
                (std::mem::size_of::<crate::resource_preparation::TaskResourcePreparation>()
                    as u64)
                    != 0,
            ),
            acyclic_fs::WorkCounters::UNCHARGED,
        )?;
        preparation
            .native_custody
            .as_mut()
            .ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "native preparation has no original metadata custody",
            })?
            .absorb(&mut header)
            .map_err(|cause| Error::NativeWork {
                cause,
                custody: Some(header),
            })?;
        Ok(Some(Arc::new(preparation)))
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(super) async fn task_context_for_original_owner_with_native_work(
        &self,
        task: TaskId,
        fence: &LeaseFence,
        allow_settlement: bool,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Arc<crate::runtime::TaskContext>> {
        let owner = self
            .host
            .journal_owner(
                task,
                fence,
                Some(&crate::executor::PreparationIo::from_native_work(work)?),
            )
            .await?;
        owner
            .verify(
                allow_settlement,
                &mut Some(
                    &mut crate::filesystem::PreparationIo::from_native_work(work)
                        .map(crate::store::HistoryWork::new)?,
                ),
            )
            .await?;
        let mut scope = owner.scope(work).await?;
        let owner = owner.into_native_shared(work)?;
        if !scope.0.resource_proofs().is_empty() {
            let access = scope
                .0
                .created_access
                .as_mut()
                .and_then(Arc::get_mut)
                .ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Conflict,
                    reason: "created task scope is not uniquely owned",
                })?;
            access.publication_lease = Some(owner.clone());
        }
        let mailbox: Arc<dyn OwnedTaskMailbox> = {
            if !owner.original_host_binding_matches(self.host.as_ref()) {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "native mailbox owner belongs to another original host",
                });
            }
            let io = super::super::PreparationIo::from_native_work(work)?;
            let bytes = std::mem::size_of::<TaskJournalMailbox<P>>() as u64;
            io.retain_owned(bytes)?;
            io.allocation_attempt(bytes)?;
            let custody = io.take_native_custody(io.owned_bytes()?)?;
            Arc::new(TaskJournalMailbox {
                host: Arc::clone(&self.host),
                owner,
                native_custody: Some(custody),
            })
        };
        let operation = OperationId::from_bytes(task.into_bytes());
        let purpose = if allow_settlement {
            crate::runtime::IssuedContextPurpose::Reconciliation
        } else {
            crate::runtime::IssuedContextPurpose::Dispatch
        };
        let io = super::super::PreparationIo::from_native_work(work)?;
        let context = self
            .harness
            .open_durable_context(
                task,
                operation,
                Some(&io),
                purpose,
                Some((mailbox, scope.0, scope.1)),
                None,
            )
            .await?;
        Ok(context)
    }
}
