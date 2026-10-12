//! State provider.
use super::*;

impl<P: StreamProvider> crate::runtime::TaskStateProvider for CoordinatorTaskHost<P> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn verify_dispatch_owner_with_native_work<'a>(
        &'a self,
        task: TaskId,
        fence: &'a crate::scheduler::LeaseFence,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        crate::NativeFutureBundle::new(work, || async move {
            let mut io = crate::store::history::native_work_io(work)?;
            self.verify_owner(task, fence, false, &mut Some(&mut io))
                .await
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn verify_execution_owner_with_native_work<'a>(
        &'a self,
        task: TaskId,
        fence: &'a crate::scheduler::LeaseFence,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        crate::NativeFutureBundle::new(work, || async move {
            let mut io = crate::store::history::native_work_io(work)?;
            self.verify_owner(task, fence, true, &mut Some(&mut io))
                .await
        })
    }

    fn policy_identity(&self) -> Option<ComponentIdentity> {
        self.policy.as_ref().map(|policy| policy.identity())
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn policy_identity_with_native_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<&ComponentIdentity>> {
        work.charge(acyclic_fs::WorkCounters::default())?;
        if let Some(policy) = &self.policy {
            policy.supports_native_policy(work)?;
            let identity = policy.identity_with_native_work(work)?;
            let needed = (std::mem::size_of::<ComponentIdentity>() as u64)
                .checked_add(identity.0.name.capacity() as u64)
                .and_then(|bytes| bytes.checked_add(identity.0.version.capacity() as u64))
                .ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "native policy identity custody overflows",
                })?;
            if !identity.1.work().shares_allocation_context(work) || identity.1.bytes() < needed {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "policy identity differs from its original native metadata custody",
                });
            }
            if self.policy_binding.as_ref() != Some(&identity.0) {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Conflict,
                    reason: "durable policy implementation changed after native binding",
                });
            }
            drop(identity);
        } else if self.policy_binding.is_some() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Conflict,
                reason: "native durable policy binding has no configured producer",
            });
        }
        Ok(self.policy_binding.as_ref())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn execution_identity_with_native_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<&ComponentIdentity>> {
        work.charge(acyclic_fs::WorkCounters::default())?;
        Ok(None)
    }

    fn observe_admission<'a>(
        &'a self,
        task_id: TaskId,
    ) -> BoxFuture<'a, Result<TaskAdmissionRecord>> {
        Box::pin(async move {
            let operation_id = OperationId::from_bytes(task_id.into_bytes());
            self.admission_with_work(operation_id, &mut None).await
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn observe_admission_with_native_work<'a>(
        &'a self,
        task: TaskId,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (TaskAdmissionRecord, acyclic_fs::NativeAllocation)>>
    {
        crate::NativeFutureBundle::new(work, || async move {
            CoordinatorTaskHost::observe_admission_with_native_work(self, task, work).await
        })
    }

    fn children<'a>(
        &'a self,
        parent: TaskId,
        expected_revision: Option<u64>,
        after_slot: Option<String>,
        maximum: usize,
    ) -> BoxFuture<'a, Result<TaskChildrenPage>> {
        Box::pin(async move {
            let mut coordinator = self.coordinator.lock().await;
            coordinator
                .observe_children(
                    self.verifier.audience(),
                    &self.owner_scope,
                    &self.verifier,
                    OperationId::from_bytes(parent.into_bytes()),
                    expected_revision,
                    after_slot.as_deref(),
                    maximum,
                    &mut None,
                )
                .await
                .map(|(page, _)| page)
        })
    }

    fn resume_scope<'a>(
        &'a self,
        task_id: TaskId,
        operation_id: OperationId,
    ) -> BoxFuture<'a, Result<RuntimeScope>> {
        Box::pin(async move {
            self.resume_scope_with_work(task_id, operation_id, &mut None)
                .await
                .map(|value| value.0)
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn resume_scope_with_native_work<'a>(
        &'a self,
        task: TaskId,
        operation: OperationId,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (RuntimeScope, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            let mut io = crate::store::history::native_work_io(work)?;
            let (scope, bytes) = self
                .resume_scope_with_work(task, operation, &mut Some(&mut io))
                .await?;
            let custody = crate::store::history::HistoryIo::take_native_custody(&mut io, bytes)?;
            Ok((scope, custody))
        })
    }

    fn outcome<'a>(&'a self, task_id: TaskId) -> BoxFuture<'a, Result<Option<Outcome<Value>>>> {
        Box::pin(async move {
            self.outcome_with_work(task_id, &mut None)
                .await
                .map(|(outcome, _)| outcome)
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn outcome_with_native_work<'a>(
        &'a self,
        task: TaskId,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<Outcome<Value>>, acyclic_fs::NativeAllocation)>>
    {
        crate::NativeFutureBundle::new(work, || async move {
            CoordinatorTaskHost::outcome_with_native_work(self, task, work).await
        })
    }

    fn cancel<'a>(&'a self, task_id: TaskId) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.cancel(task_id, None).await })
    }

    fn send<'a>(
        &'a self,
        sender: TaskId,
        recipient: TaskId,
        message_id: OperationId,
        payload: FileRef,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let recipient_scope = self
                .admission_with_work(OperationId::from_bytes(recipient.into_bytes()), &mut None)
                .await?;
            if !self
                .admission_with_work(OperationId::from_bytes(sender.into_bytes()), &mut None)
                .await?
                .grants
                .contains(capability::MAIL_SEND)
                || !recipient_scope.grants.contains(capability::MAIL_READ)
                || !read_granted(&recipient_scope.grants, &payload)?
            {
                return Err(Error::Unauthorized(
                    "unfenced custom mail requires explicit mail and file grants".into(),
                ));
            }
            let bytes = self
                .mail_bytes(sender, recipient, message_id, payload, None)
                .await?;
            self.publish_mail(sender, recipient, message_id, bytes, None, None, None)
                .await
        })
    }

    fn inbox<'a>(
        &'a self,
        task_id: TaskId,
        after: u64,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<InboxItem>>> {
        Box::pin(self.inbox_page(task_id, after, limit, None))
    }

    fn wait_until<'a>(
        &'a self,
        task_id: TaskId,
        operation_id: OperationId,
        deadline_unix_ms: u64,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            if deadline_unix_ms == 0 {
                return Err(Error::Invalid("timer deadline is invalid".into()));
            }
            let admission = self
                .admission_with_work(OperationId::from_bytes(task_id.into_bytes()), &mut None)
                .await?;
            if !admission.grants.contains(capability::TIMER_WAIT) {
                return Err(Error::Unauthorized("task scope lacks timer:wait".into()));
            }
            let event = TimerEvent {
                task_id,
                operation_id,
                deadline_unix_ms,
            };
            let bytes = crate::contract::canonical_json_bytes(&event)?;
            let stream = self
                .timer_stream(task_id, &mut None)
                .map(|(stream, _)| stream)?;
            loop {
                let (tail, found) = self
                    .timer_state_with_work(task_id, operation_id, deadline_unix_ms, &mut None)
                    .await?;
                if found {
                    break;
                }
                if self
                    .publish_control_at(
                        &stream,
                        "timers",
                        task_id,
                        operation_id,
                        &bytes,
                        Some(tail),
                    )
                    .await?
                {
                    break;
                }
            }
            loop {
                let now = self.clock.now_unix_millis();
                if now >= deadline_unix_ms {
                    return Ok(());
                }
                tokio::time::sleep(std::time::Duration::from_millis(
                    deadline_unix_ms.saturating_sub(now).min(60_000),
                ))
                .await;
            }
        })
    }

    fn reconcile_effect<'a>(
        &'a self,
        task_id: TaskId,
        effect_id: EffectId,
    ) -> BoxFuture<'a, Result<crate::core::EffectStatus>> {
        Box::pin(async move {
            let admission = self
                .admission_with_work(OperationId::from_bytes(task_id.into_bytes()), &mut None)
                .await?;
            if !admission.grants.contains(capability::EFFECT_RUN) {
                return Err(Error::Unauthorized("task scope lacks effect:run".into()));
            }
            let effects = self
                .effects
                .as_ref()
                .ok_or_else(|| Error::Unsupported("durable effect observer is not bound".into()))?;
            effects.reconcile(effect_id).await
        })
    }
}
