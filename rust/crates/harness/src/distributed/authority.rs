use super::*;

#[cfg(feature = "filesystem")]
#[derive(Clone, Copy)]
pub(crate) enum JournalWrite {
    Fresh,
    Model {
        attempt_id: OperationId,
        step: u32,
        request_digest: [u8; 32],
    },
    Settlement,
}

impl<P: StreamProvider> DistributedCoordinator<P> {
    #[cfg(feature = "filesystem")]
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn journal_condition(
        &mut self,
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        operation_id: OperationId,
        fence: &LeaseFence,
        write: JournalWrite,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<acyclic_stream::CommitCondition> {
        self.refresh(work).await?;
        let operation = self.observe_operation(owner, scope, verifier, operation_id, work)?;
        crate::scheduler::require_execution_owner(
            operation,
            fence,
            matches!(write, JournalWrite::Settlement),
            work,
        )?;
        if let JournalWrite::Model {
            attempt_id,
            step,
            request_digest,
        } = write
        {
            self.scheduler.require_model_claim(
                operation_id,
                attempt_id,
                step,
                &request_digest,
                work,
            )?;
        }
        Ok(acyclic_stream::CommitCondition::Tail {
            path: self.stream.0.path().clone(),
            expected: self.revision,
        })
    }

    pub(crate) fn observe_operation(
        &self,
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        operation_id: OperationId,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<&OperationState> {
        authorize_operation_scope(owner, scope, verifier, capability::OPERATION_OBSERVE, work)?;
        self.authorize_owner(owner, verifier, operation_id, work)
    }

    /// Returns the page with the exact deep custody produced by its field copies.
    pub(crate) async fn observe_children(
        &mut self,
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        parent: OperationId,
        expected_revision: Option<u64>,
        after_slot: Option<&str>,
        maximum: usize,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<(runtime::TaskChildrenPage, u64)> {
        runtime::validate_child_page_request(
            "child hierarchy",
            parent.into_bytes(),
            after_slot,
            maximum,
        )?;
        authorize_operation_scope(owner, scope, verifier, capability::OPERATION_OBSERVE, work)?;
        if work
            .as_deref()
            .is_some_and(operations::original_native_decode_mode)
            && self.scheduler.operation(parent).is_some()
        {
            self.observe_operation(owner, scope, verifier, parent, work)?;
        }
        self.refresh(work).await?;
        self.observe_operation(owner, scope, verifier, parent, work)?;
        if expected_revision.is_some_and(|revision| revision != self.revision) {
            return Err(guarded_boundary(
                work,
                wire::ErrorCode::Conflict,
                "child hierarchy revision changed",
            ));
        }
        let mut custody = CodecQuota::new(work, 0)?;
        let mut entries =
            crate::core::reserve_copy_vector::<runtime::TaskChild>(maximum, &mut custody)?;
        let mut has_more = false;
        let mut children = self.scheduler.children(parent);
        loop {
            if let Some(work) = custody.work().as_deref_mut() {
                work.check_cancellation()?;
            }
            let Some((slot, child)) = children.next() else {
                break;
            };
            if let Some(work) = custody.work().as_deref_mut() {
                work.codec_charge(crate::executor::json::JsonCodecCharge::Inspect { items: 1 })?;
            }
            if after_slot.is_some_and(|after| slot <= after)
                || child.spec.owner.authority() != owner
            {
                continue;
            }
            if entries.len() == maximum {
                has_more = true;
                break;
            }
            child.spec.verify_owner(owner, verifier, custody.work())?;
            if child.spec.operation_id.into_bytes() == [0; 16] {
                return Err(guarded_boundary(
                    custody.work(),
                    wire::ErrorCode::Invalid,
                    "child hierarchy contains a nil operation",
                ));
            }
            let slot = crate::core::copy_text(slot, &mut custody)?;
            entries.push(runtime::TaskChild {
                slot,
                task_id: runtime::task_id_for_admission(child.spec.operation_id),
            });
        }
        let next_after = if has_more {
            entries
                .last()
                .map(|entry| crate::core::copy_text(&entry.slot, &mut custody))
                .transpose()?
        } else {
            None
        };
        let page = runtime::TaskChildrenPage {
            revision: self.revision,
            entries,
            next_after,
        };
        let retained = custody.retained();
        custody.transfer();
        Ok((page, retained))
    }

    /// Cancels with exact retry; a parent fence always cancels the child's subtree.
    #[allow(clippy::too_many_arguments)]
    pub async fn cancel_operation(
        &mut self,
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        operation_id: OperationId,
        idempotency_key: IdempotencyKey,
        recursive: bool,
        parent_guard: Option<(OperationId, &LeaseFence)>,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<(bool, OperationState)> {
        authorize_operation_scope(owner, scope, verifier, capability::OPERATION_CANCEL, work)?;
        self.refresh(work).await?;
        let child = self.authorize_owner(owner, verifier, operation_id, work)?;
        let verified_revision = if let Some((parent, fence)) = parent_guard {
            if child.spec.parent.as_ref().map(|link| link.operation_id) != Some(parent) {
                return Err(guarded_boundary(
                    work,
                    wire::ErrorCode::Unauthorized,
                    "cancellation requires an authorized direct child",
                ));
            }
            let parent_state = self.authorize_owner(owner, verifier, parent, work)?;
            crate::scheduler::require_execution_owner(parent_state, fence, false, work)?;
            Some(self.revision)
        } else {
            None
        };
        let applied = self
            .apply_internal(
                idempotency_key,
                SchedulerEvent::CancellationRequested {
                    operation_id,
                    recursive: recursive || parent_guard.is_some(),
                },
                parent_guard.map(|(_, fence)| fence),
                None,
                None,
                None,
                verified_revision,
                work,
            )
            .await?;
        let state = self
            .scheduler
            .operation(operation_id)
            .ok_or_else(|| {
                guarded_boundary(
                    work,
                    wire::ErrorCode::NotFound,
                    "operation is absent after cancellation",
                )
            })?
            .clone_with_work(work)?;
        Ok((applied, state))
    }

    pub(super) fn authorize_owner(
        &self,
        owner: &Authority,
        verifier: &AuthorityVerifier,
        operation_id: OperationId,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<&OperationState> {
        let operation = self
            .scheduler
            .operation(operation_id)
            .filter(|operation| operation.spec.owner.authority() == owner)
            .ok_or_else(|| {
                guarded_boundary(
                    work,
                    wire::ErrorCode::NotFound,
                    "operation is absent for this owner",
                )
            })?;
        operation.spec.verify_owner(owner, verifier, work)?;
        Ok(operation)
    }

    /// Fills a durable resume slot, optionally guarded by its selected command.
    #[allow(clippy::too_many_arguments)]
    pub async fn resume_workflow(
        &mut self,
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        operation_id: OperationId,
        workflow_revision: u64,
        input: FileRef,
        idempotency_key: IdempotencyKey,
        waiting_command: Option<OperationId>,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<bool> {
        authorize_operation_scope(owner, scope, verifier, "operation:wake", work)?;
        self.refresh(work).await?;
        self.observe_operation(owner, scope, verifier, operation_id, work)?;
        self.apply_internal(
            idempotency_key,
            SchedulerEvent::WorkflowResumed {
                operation_id,
                workflow_revision,
                input,
            },
            None,
            waiting_command,
            None,
            None,
            None,
            work,
        )
        .await
    }

    /// Authenticates the exact durable owner before publishing a declaration.
    #[allow(clippy::too_many_arguments)]
    pub async fn declare_operation(
        &mut self,
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        spec: OperationSpec,
        idempotency_key: IdempotencyKey,
        parent_fence: Option<LeaseFence>,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<bool> {
        if parent_fence.is_some() && spec.parent.is_none() {
            return Err(guarded_boundary(
                work,
                wire::ErrorCode::Invalid,
                "owned child declaration has no parent",
            ));
        }
        authorize_operation_scope(owner, scope, verifier, capability::OPERATION_DECLARE, work)?;
        spec.verify_owner(owner, verifier, work)?;
        if let Some(parent) = &spec.parent {
            self.refresh(work).await?;
            self.authorize_owner(owner, verifier, parent.operation_id, work)?;
        }
        let box_bytes = std::mem::size_of::<OperationSpec>() as u64;
        let mut custody = CodecQuota::new(work, box_bytes)?;
        if let Some(work) = custody.work().as_deref_mut() {
            work.admit_allocation(box_bytes)?;
        }
        let event = SchedulerEvent::Declared {
            spec: Box::new(spec),
        };
        self.apply_internal(
            idempotency_key,
            event,
            parent_fence.as_ref(),
            None,
            None,
            None,
            None,
            custody.work(),
        )
        .await
    }
}

pub(super) fn authorize_operation_scope(
    owner: &Authority,
    scope: &Scope,
    verifier: &AuthorityVerifier,
    capability: &str,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<()> {
    verifier.verify_audience(owner, work)?;
    verifier.verify_scope_for_with_work(owner, scope, work)?;
    if !scope.capabilities().contains(capability) {
        return Err(guarded_boundary(
            work,
            wire::ErrorCode::Unauthorized,
            "owner scope lacks the required operation capability",
        ));
    }
    Ok(())
}
