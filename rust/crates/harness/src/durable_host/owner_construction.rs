//! Owner construction.
use super::*;

impl<P: StreamProvider> CoordinatorTaskHost<P> {
    /// Binds journal publications to the authenticated task's exact current lease.
    #[cfg(feature = "filesystem")]
    pub async fn journal_owner(
        self: &Arc<Self>,
        task_id: TaskId,
        fence: crate::scheduler::LeaseFence,
    ) -> Result<TaskJournalOwner<P>> {
        self.journal_owner_with_work(task_id, std::borrow::Cow::Owned(fence), None, &mut None)
            .await
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    /// Binds a journal owner to its original native ledger.
    pub async fn journal_owner_with_native_work(
        self: &Arc<Self>,
        task: TaskId,
        fence: &crate::scheduler::LeaseFence,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<TaskJournalOwner<P>> {
        let mut io = crate::filesystem::PreparationIo::from_native_work(work)
            .map(crate::store::HistoryWork::new)?;
        self.journal_owner_with_work(
            task,
            std::borrow::Cow::Borrowed(fence),
            None,
            &mut Some(&mut io),
        )
        .await
    }

    #[cfg(feature = "filesystem")]
    pub(super) async fn journal_owner_with_work(
        self: &Arc<Self>,
        task_id: TaskId,
        fence: std::borrow::Cow<'_, crate::scheduler::LeaseFence>,
        original: Option<(OperationSpec, &LeaseFence)>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<TaskJournalOwner<P>> {
        let operation = OperationId::from_bytes(task_id.into_bytes());
        #[cfg(not(target_arch = "wasm32"))]
        let native = work
            .as_deref()
            .and_then(|io| io.original_native_work())
            .cloned();
        #[cfg(not(target_arch = "wasm32"))]
        if native.is_some() {
            work.as_deref_mut()
                .expect("original native observer")
                .retain_owned(std::mem::size_of::<TaskJournalOwner<P>>() as u64)?;
            if original.is_none() {
                self.verify_owner(task_id, &fence, true, work).await?;
            }
        }
        let declaration = match &original {
            Some((declaration, original_fence)) => {
                if declaration.operation_id != operation || *original_fence != fence.as_ref() {
                    return Err(guarded_boundary(
                        work,
                        crate::wire::ErrorCode::Unauthorized,
                        "original READ reservation differs from its protected native preparation",
                    ));
                }
                std::borrow::Cow::Borrowed(declaration)
            }
            None => std::borrow::Cow::Owned(
                self.retained_declaration_with_work(operation, work)
                    .await?
                    .ok_or_else(|| {
                        guarded_boundary(work, crate::wire::ErrorCode::NotFound, "task admission")
                    })?,
            ),
        };
        let declared_agent = declaration.owner_scope.agent();
        let (admission, _) = self
            .read_declared_admission_counted(&declaration, work)
            .await?;
        let declaration_held =
            if work.is_some() && matches!(declaration, std::borrow::Cow::Owned(_)) {
                declaration.logical_owned_bytes()?
            } else {
                0
            };
        drop(declaration);
        if let Some(io) = work.as_deref_mut() {
            io.release_owned(declaration_held)?;
        }
        let (scope, _) = self.authenticated_scope_with_work(&admission, work).await?;
        let input_is_root = admission.parent.is_none() && admission.resource_proofs.is_empty();
        let input_agent = match scope.current_created_agent(work)? {
            Some(agent) => Some(agent),
            None if input_is_root => {
                if declared_agent != self.owner_scope.agent() {
                    return Err(guarded_boundary(
                        work,
                        crate::wire::ErrorCode::Unauthorized,
                        "root current actor differs from original declaration",
                    ));
                }
                declared_agent
            }
            None => None,
        };
        let request_digest = crate::contract::canonical_json_digest_with_work(
            &admission,
            acyclic_stream::MAX_RECORD_BYTES as u64,
            work,
        )?;
        #[cfg(not(target_arch = "wasm32"))]
        let state = if let Some(native) = &native {
            let (state, custody) = self.machines.initialize_with_native_work(
                &admission.machine,
                &admission.input,
                native,
            )?;
            if !custody.work().shares_ledger(native) {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "initial machine state differs from its original metadata ledger",
                });
            }
            work.as_deref_mut()
                .expect("original native observer")
                .absorb_native_custody(custody)?;
            Some(state)
        } else {
            None
        };
        #[cfg(target_arch = "wasm32")]
        let state = None;
        let state = match state {
            Some(state) => state,
            None => self
                .machines
                .resolve(&admission.machine, work)?
                .ok_or_else(|| Error::Unsupported("pinned task machine is unavailable".into()))?
                .initialize(&admission.input)?,
        };
        let workflow = crate::workflow::WorkflowAdmission {
            operation_id: admission.operation_id,
            request_digest,
            initial: crate::workflow::MachineCheckpoint {
                machine: admission.machine,
                revision: 0,
                state,
            },
        };
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(native) = &native {
            self.machines
                .validate_checkpoint_with_native_work(&workflow.initial, native)?;
        } else {
            self.machines.validate_checkpoint(&workflow.initial, None)?;
        }
        #[cfg(target_arch = "wasm32")]
        self.machines.validate_checkpoint(&workflow.initial, None)?;
        let fence = match fence {
            std::borrow::Cow::Owned(fence) => fence,
            std::borrow::Cow::Borrowed(fence) => fence.clone_with_work(work)?,
        };
        let root_binding = if input_is_root {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(native) = &native {
                let slot = native.claim(
                    std::mem::size_of::<RootBootstrapBindingWitness>() as u64,
                    u64::from((std::mem::size_of::<RootBootstrapBindingWitness>() as u64) != 0),
                    acyclic_fs::WorkCounters::UNCHARGED,
                )?;
                work.as_deref_mut()
                    .expect("original native observer")
                    .absorb_native_custody(slot)?;
            }
            Some(Arc::new(RootBootstrapBindingWitness {
                proof: std::sync::OnceLock::new(),
                gate: tokio::sync::Mutex::new(()),
                pending: std::sync::OnceLock::new(),
            }))
        } else {
            None
        };
        let mut owner = TaskJournalOwner {
            host: Arc::clone(self),
            stream: self.coordinator.lock().await.client.clone(),
            original_admission_digest: workflow.request_digest,
            task_id,
            fence,
            workflow,
            output_schema: admission.output_schema,
            input_grants: admission.grants,
            input_created: scope.created_access(),
            input_agent,
            root_binding,
            deadline_epoch_ms: admission.run_limits.deadline_epoch_ms,
            input_limits: admission.limits,
            extensions: admission.extensions,
            invocation_cancellation: None,
            #[cfg(not(target_arch = "wasm32"))]
            readonly_original: original.map(|(declaration, _)| declaration),
            #[cfg(not(target_arch = "wasm32"))]
            _native_shared: false,
            #[cfg(not(target_arch = "wasm32"))]
            _native_custody: None,
        };
        #[cfg(not(target_arch = "wasm32"))]
        if native.is_some() {
            let owned = owner.native_owned_bytes()?;
            owner._native_custody = Some(
                work.as_deref_mut()
                    .expect("original native observer")
                    .take_native_custody(owned)?,
            );
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(native) = &native {
            owner
                .verify_read_access(&mut Some(
                    &mut crate::filesystem::PreparationIo::from_native_work(native)
                        .map(crate::store::HistoryWork::new)?,
                ))
                .await?;
        } else {
            owner.verify(true, &mut None).await?;
        }
        #[cfg(target_arch = "wasm32")]
        owner.verify(true, &mut None).await?;
        Ok(owner)
    }
}
