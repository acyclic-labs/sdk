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

    /// Pure support discovery for the original binding, before callback boxing or constructing an owned interaction request.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn supports_native_interactions(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        self.require_original_native_work(work)?;
        if self.durable_task.is_some() {
            if let Some(router) = &self.owned_interactions {
                return router.supports_native_interactions(work);
            }
            self.harness
                .state
                .as_ref()
                .ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unsupported,
                    reason: "durable task state is not bound",
                })?
                .supports_native_interactions(work)
        } else {
            self.harness
                .interactions
                .as_ref()
                .ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unsupported,
                    reason: "local interaction router is not bound",
                })?
                .supports_native_interactions(work)
        }
    }

    /// Routes an already-paid request through this context's original binding.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub async fn interact_with_native_work(
        &self,
        operation_id: OperationId,
        interaction: Interaction,
        request_custody: acyclic_fs::NativeAllocation,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(InteractionOutcome, acyclic_fs::NativeAllocation)> {
        self.interact_with_native_work_and_router(
            operation_id,
            interaction,
            request_custody,
            work,
            None,
            &self.scope,
        )
        .await
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super) async fn interact_with_native_work_and_router(
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
        if let Interaction::Approval { prompt, .. } = &paid.0 {
            if paid.1.bytes() < prompt.capacity() as u64 {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Conflict,
                    reason: "approval request does not retain its original prompt",
                });
            }
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
        if !paid.1.work().shares_ledger(work) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Conflict,
                reason: "interaction outcome belongs to another original ledger",
            });
        }
        if let InteractionOutcome::Answered { answer } = &paid.0 {
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
            if paid.1.bytes() < bytes {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Conflict,
                    reason: "interaction answer does not retain its original metadata",
                });
            }
        }
        work.remaining()?;
        Ok((paid.0, paid.1))
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
        mailbox.validate_scope(&self.scope)?;
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
        if let Some(original) = work.as_deref().and_then(|io| io.original_native_work()) {
            return self
                .native_mailbox(original)?
                .read_received_with_native_work(receive_operation, item, original)?
                .await;
        }
        if work.is_some() {
            return Err(crate::scheduler::diagnostic(
                work,
                crate::wire::ErrorCode::Unsupported,
                "parcel read requires an original native observer",
            ));
        }
        let mailbox = self.owned_mailbox.as_ref().ok_or_else(|| {
            Error::Unauthorized("parcel read requires a retained mailbox owner".into())
        })?;
        mailbox.validate_scope(&self.scope)?;
        mailbox.read_received(receive_operation, item).await
    }

    /// Reads a bounded durable inbox page after the last observed sequence.
    pub async fn inbox(&self, after: u64, limit: usize) -> Result<Vec<InboxItem>> {
        if limit == 0 {
            return Err(Error::Invalid("inbox page bound is invalid".into()));
        }
        let mailbox = self.owned_mailbox.as_ref().ok_or_else(|| {
            Error::Unauthorized("inbox requires a retained task mailbox owner".into())
        })?;
        mailbox.validate_scope(&self.scope)?;
        mailbox.inbox(after, limit).await
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super) fn native_mailbox(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<&Arc<dyn OwnedTaskMailbox>> {
        self.owned_task_binding_with_native_work(work)?;
        self.owned_mailbox.as_ref().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            reason: "mail requires an original native task mailbox owner",
        })
    }

    /// Sends the same exact parcel with the original native owner and work.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn send_with_native_work<'a>(
        &'a self,
        recipient: TaskId,
        message_id: OperationId,
        payload: &'a FileRef,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<
        crate::NativeFutureBundle<'a, (crate::conversation::FileRef, acyclic_fs::NativeAllocation)>,
    > {
        self.native_mailbox(work)?
            .send_with_native_work(recipient, message_id, payload, work)
    }

    /// Reads a bounded original-owner inbox page without rebuilding a scope.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn inbox_with_native_work<'a>(
        &'a self,
        after: u64,
        limit: usize,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Vec<InboxItem>, acyclic_fs::NativeAllocation)>> {
        let mailbox = self.native_mailbox(work)?;
        if limit == 0 {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "inbox page bound is invalid",
            });
        }
        mailbox.inbox_with_native_work(after, limit, work)
    }

    /// Queries an existing effect by identity and never redispatches it.
    pub async fn reconcile_effect(&self, effect_id: EffectId) -> Result<crate::core::EffectStatus> {
        let task = self.durable_task.ok_or_else(|| {
            Error::Unsupported("effect reconciliation requires a durable task".into())
        })?;
        let host = self
            .harness
            .state
            .as_ref()
            .ok_or_else(|| Error::Unsupported("durable task state is not bound".into()))?;
        host.reconcile_effect(task, effect_id).await
    }
}
