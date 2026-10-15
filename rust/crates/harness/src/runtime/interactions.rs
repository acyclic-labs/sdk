use super::*;

impl TaskContext {
    /// Routes an interaction by stable identity; its request/answer bytes stay in owner storage.
    pub async fn interact(
        &self,
        operation_id: OperationId,
        interaction: Interaction,
    ) -> Result<InteractionOutcome> {
        interaction.validate(&mut None)?;
        if !self.scope.grants.contains(capability::INTERACTION_ROUTE) {
            return Err(Error::Unauthorized("scope lacks interaction:route".into()));
        }
        if let Some(task) = self.durable_task {
            if operation_id.into_bytes() == task.into_bytes() {
                return Err(Error::Invalid(
                    "interaction operation must differ from task admission".into(),
                ));
            }
            if let Some(router) = &self.owned_interactions {
                return router.route(operation_id, interaction).await;
            }
            let host = self
                .harness
                .state
                .as_ref()
                .ok_or_else(|| Error::Unsupported("durable task state is not bound".into()))?;
            host.interact(task, operation_id, interaction).await
        } else {
            let router = self.harness.interactions.as_ref().ok_or_else(|| {
                Error::Unsupported("local interaction router is not bound".into())
            })?;
            router.route(operation_id, interaction).await
        }
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super) async fn route_native_interaction(
        &self,
        operation_id: OperationId,
        interaction: Interaction,
        request_custody: acyclic_fs::NativeAllocation,
        work: &acyclic_fs::NativeMaterializationWork,
        command_router: Option<&dyn InteractionRouter>,
        scope: &RuntimeScope,
    ) -> Result<(InteractionOutcome, acyclic_fs::NativeAllocation)> {
        let paid = (interaction, request_custody);
        self.require_original_native_work(work)?;
        if !paid.1.work().shares_ledger(work) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Conflict,
                reason: "interaction request belongs to another original ledger",
            });
        }
        if let Interaction::Approval { prompt, .. } = &paid.0
            && paid.1.bytes() < prompt.capacity() as u64
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Conflict,
                reason: "approval request does not retain its original prompt",
            });
        }
        let router = command_router.or_else(|| {
            if self.durable_task.is_some() {
                self.owned_interactions
                    .as_ref()
                    .map(|router| router.as_ref())
            } else {
                self.harness
                    .interactions
                    .as_ref()
                    .map(|router| router.as_ref())
            }
        });
        let state = if router.is_none() && self.durable_task.is_some() {
            Some(self.harness.state.as_ref().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "durable task state is not bound",
            })?)
        } else {
            None
        };
        if let Some(router) = router {
            router.supports_native_interactions(work)?;
        } else if let Some(state) = state {
            state.supports_native_interactions(work)?;
        } else {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "local interaction router is not bound",
            });
        }
        let mut io = crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(work)?,
        );
        let mut observed: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut io);
        paid.0.validate(&mut observed)?;
        if !scope.grants.contains(capability::INTERACTION_ROUTE) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "scope lacks interaction:route",
            });
        }
        if self
            .durable_task
            .is_some_and(|task| operation_id.into_bytes() == task.into_bytes())
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "interaction operation must differ from task admission",
            });
        }
        let future = if let Some(router) = router {
            let (interaction, request_custody) = paid;
            router.route_with_native_work(operation_id, interaction, request_custody, work)?
        } else if let (Some(task), Some(state)) = (self.durable_task, state) {
            let (interaction, request_custody) = paid;
            state.interact_with_native_work(
                task,
                operation_id,
                interaction,
                request_custody,
                work,
            )?
        } else {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "local interaction router is not bound",
            });
        };
        if !future.work().shares_ledger(work) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Conflict,
                reason: "interaction route belongs to another original ledger",
            });
        }
        let (value, custody) = future.await?;
        let paid = (value, custody);
        Self::validate_native_interaction_outcome(&paid.0, &paid.1, work)?;
        work.remaining()?;
        Ok((paid.0, paid.1))
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn validate_native_interaction_outcome(
        value: &InteractionOutcome,
        custody: &acyclic_fs::NativeAllocation,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        if !custody.work().shares_ledger(work) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Conflict,
                reason: "interaction outcome belongs to another original ledger",
            });
        }
        if let InteractionOutcome::Answered { answer } = value {
            let owned = answer.logical_owned_bytes().map_err(|error| {
                if error.is_native_failure() {
                    error
                } else {
                    Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Invalid,
                        reason: "interaction answer ownership overflows",
                    }
                }
            })?;
            let bytes = owned
                .checked_add(std::mem::size_of::<FileRef>() as u64)
                .ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "interaction answer ownership overflows",
                })?;
            if custody.bytes() < bytes {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Conflict,
                    reason: "interaction answer does not retain its original metadata",
                });
            }
        }
        Ok(())
    }

    /// Sends a donor-authorized parcel under the retained owner fence and route.
    pub async fn send(
        &self,
        recipient: TaskId,
        message_id: OperationId,
        payload: FileRef,
    ) -> Result<crate::conversation::FileRef> {
        payload.validate(&mut None)?;
        let mailbox = self.owned_mailbox.as_ref().ok_or_else(|| {
            Error::Unauthorized("mail requires a retained task mailbox owner".into())
        })?;
        mailbox.validate_scope(&self.scope, None)?;
        mailbox.send(recipient, message_id, payload).await
    }

    /// Reads only one retained parcel selected by this task's original receive command.
    pub async fn read_received_mail(
        &self,
        receive_operation: OperationId,
        item: &InboxItem,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<acyclic_stream::OwnedBytes> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let original = work
            .as_deref()
            .and_then(|io| io.original_native_work())
            .map(crate::executor::PreparationIo::from_native_work)
            .transpose()?;
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let original = None::<crate::conversation::ContentIo>;
        let io = original.as_ref();
        if work.is_some() && io.is_none() {
            return Err(crate::scheduler::diagnostic(
                work,
                crate::wire::ErrorCode::Unsupported,
                "parcel read requires an original native observer",
            ));
        }
        let mailbox = self.owned_mailbox.as_ref().ok_or_else(|| {
            crate::conversation::content_error(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "parcel read requires a retained mailbox owner",
            )
        })?;
        #[cfg(feature = "filesystem")]
        self.owned_task_binding(io)?;
        #[cfg(not(feature = "filesystem"))]
        mailbox.validate_scope(&self.scope, io)?;
        mailbox.read_received(receive_operation, item, io)?.await
    }

    /// Reads a bounded durable inbox page after the last observed sequence.
    pub async fn inbox(&self, after: u64, limit: usize) -> Result<Vec<InboxItem>> {
        if limit == 0 {
            return Err(Error::Invalid("inbox page bound is invalid".into()));
        }
        let mailbox = self.owned_mailbox.as_ref().ok_or_else(|| {
            Error::Unauthorized("inbox requires a retained task mailbox owner".into())
        })?;
        mailbox.validate_scope(&self.scope, None)?;
        mailbox.inbox(after, limit).await
    }
}
