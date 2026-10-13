use super::*;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
use crate::NativeFutureBundle;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
use acyclic_fs::{NativeAllocation, NativeMaterializationWork};

fn unbound<'a, T>(reason: &'static str) -> BoxFuture<'a, Result<T>> {
    Box::pin(async move { Err(Error::Unsupported(reason.into())) })
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn native_unbound(reason: &'static str) -> Error {
    Error::NativeBoundary {
        code: crate::wire::ErrorCode::Unsupported,
        reason,
    }
}

/// Provider boundary for stable durable admission and outcome observation.
pub trait DurableTaskHost: TaskStateProvider + TaskSpawner {
    /// Native execution support is an allocation-free preflight, not authority.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_execution(&self, _work: &NativeMaterializationWork) -> Result<bool> {
        Ok(false)
    }

    /// Charges one exact durable model attempt under the current task owner.
    fn claim_model_dispatch<'a>(
        &'a self,
        _task_id: TaskId,
        _attempt_id: OperationId,
        _step: u32,
        _request_digest: [u8; 32],
        _fence: crate::scheduler::LeaseFence,
    ) -> BoxFuture<'a, Result<()>> {
        unbound("durable model accounting is not bound")
    }

    /// Claims the same durable model attempt on its original native ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn claim_model_dispatch_with_native_work<'a>(
        &'a self,
        _task: TaskId,
        _attempt: OperationId,
        _step: u32,
        _request_digest: [u8; 32],
        _fence: &'a crate::scheduler::LeaseFence,
        _work: &'a NativeMaterializationWork,
    ) -> Result<NativeFutureBundle<'a, ()>> {
        Err(native_unbound(
            "durable host has no original-ledger model claim",
        ))
    }
}

/// Owner-bound durable task state, independently replaceable from admission and execution.
pub trait TaskStateProvider: acyclic_stream::ProviderPlatform {
    /// Verifies the current dispatch owner on caller IO.
    fn verify_dispatch_owner<'a>(
        &'a self,
        _task: TaskId,
        _fence: &'a crate::scheduler::LeaseFence,
        io: Option<&'a crate::conversation::ContentIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        Err(crate::conversation::content_error(
            io,
            crate::wire::ErrorCode::Unsupported,
            "durable dispatch ownership is not bound",
        ))
    }
    /// Verifies the current execution owner on caller IO.
    fn verify_execution_owner<'a>(
        &'a self,
        _task: TaskId,
        _fence: &'a crate::scheduler::LeaseFence,
        io: Option<&'a crate::conversation::ContentIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        Err(crate::conversation::content_error(
            io,
            crate::wire::ErrorCode::Unsupported,
            "durable execution ownership is not bound",
        ))
    }
    /// Exact policy revision enforced when state-bound effects are reconciled.
    fn policy_identity(&self) -> Option<ComponentIdentity>;
    /// Execution route retained and observable by this state owner.
    fn execution_identity(&self) -> Option<ComponentIdentity> {
        None
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    /// Borrows the policy identity on the original work.
    fn policy_identity_with_native_work(
        &self,
        _work: &NativeMaterializationWork,
    ) -> Result<Option<&ComponentIdentity>> {
        Err(native_unbound(
            "task state has no original native policy binding source",
        ))
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    /// Borrows the execution identity on the original work.
    fn execution_identity_with_native_work(
        &self,
        _work: &NativeMaterializationWork,
    ) -> Result<Option<&ComponentIdentity>> {
        Err(native_unbound(
            "task state has no original native execution binding source",
        ))
    }
    /// Reads the complete immutable admission for cross-provider attestation.
    fn observe_admission<'a>(
        &'a self,
        task_id: TaskId,
    ) -> BoxFuture<'a, Result<TaskAdmissionRecord>>;

    /// Native admission observation must preserve its original metadata custody.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn observe_admission_with_native_work<'a>(
        &'a self,
        _task: TaskId,
        _work: &'a NativeMaterializationWork,
    ) -> Result<NativeFutureBundle<'a, (TaskAdmissionRecord, NativeAllocation)>> {
        Err(native_unbound(
            "task state has no original-ledger admission observer",
        ))
    }
    /// Discovers direct owner-retained children with revision-checked pagination.
    fn children<'a>(
        &'a self,
        _parent: TaskId,
        _expected_revision: Option<u64>,
        _after_slot: Option<String>,
        _maximum: usize,
    ) -> BoxFuture<'a, Result<TaskChildrenPage>> {
        unbound("durable task hierarchy is not bound")
    }
    /// Authenticates and reconstructs the admitted scope after process restart.
    fn resume_scope<'a>(
        &'a self,
        task_id: TaskId,
        operation_id: OperationId,
    ) -> BoxFuture<'a, Result<RuntimeScope>>;

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    /// Restores the admitted scope on the original work.
    fn resume_scope_with_native_work<'a>(
        &'a self,
        _task: TaskId,
        _operation: OperationId,
        _work: &'a NativeMaterializationWork,
    ) -> Result<NativeFutureBundle<'a, (RuntimeScope, NativeAllocation)>> {
        Err(native_unbound(
            "task state has no original-ledger scope producer",
        ))
    }

    /// Observes a terminal result or ordinary pending state without dispatch.
    fn outcome<'a>(&'a self, task_id: TaskId) -> BoxFuture<'a, Result<Option<Outcome<Value>>>>;

    /// Native terminal observation never re-executes the admitted task.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn outcome_with_native_work<'a>(
        &'a self,
        _task: TaskId,
        _work: &'a NativeMaterializationWork,
    ) -> Result<NativeFutureBundle<'a, (Option<Outcome<Value>>, NativeAllocation)>> {
        Err(native_unbound(
            "task state has no original-ledger outcome observer",
        ))
    }
    /// Waits for a terminal result; providers may override polling.
    fn wait_outcome<'a>(&'a self, task_id: TaskId) -> BoxFuture<'a, Result<Outcome<Value>>> {
        Box::pin(async move {
            let mut delay_ms = 50_u64;
            loop {
                if let Some(outcome) = self.outcome(task_id).await? {
                    return Ok(outcome);
                }
                crate::live::sleep_millis(delay_ms).await?;
                delay_ms = delay_ms.saturating_mul(2).min(1_000);
            }
        })
    }
    /// Requests cancellation without claiming a terminal result.
    fn cancel<'a>(&'a self, task_id: TaskId) -> BoxFuture<'a, Result<()>>;
    /// Routes an addressable interaction through owner-retained state.
    fn interact<'a>(
        &'a self,
        _task_id: TaskId,
        _operation_id: OperationId,
        _interaction: Interaction,
    ) -> BoxFuture<'a, Result<InteractionOutcome>> {
        unbound("durable interaction routing is not bound")
    }

    /// Pure discovery before request validation or future construction.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_interactions(&self, _work: &NativeMaterializationWork) -> Result<()> {
        Err(native_unbound(
            "task state has no original-ledger interaction route",
        ))
    }

    /// Native routing is independently replaceable, never an ordinary fallback.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn interact_with_native_work<'a>(
        &'a self,
        _task_id: TaskId,
        _operation_id: OperationId,
        _interaction: Interaction,
        _request_custody: NativeAllocation,
        _work: &'a NativeMaterializationWork,
    ) -> Result<NativeFutureBundle<'a, (InteractionOutcome, NativeAllocation)>> {
        let _paid = (_interaction, _request_custody);
        Err(native_unbound(
            "task state has no original-ledger interaction route",
        ))
    }
    /// Publishes one idempotent ref-only mailbox item.
    fn send<'a>(
        &'a self,
        _sender: TaskId,
        _recipient: TaskId,
        _message_id: OperationId,
        _payload: FileRef,
    ) -> BoxFuture<'a, Result<()>> {
        unbound("durable task mail is not bound")
    }
    /// Reads a bounded ordered inbox page.
    fn inbox<'a>(
        &'a self,
        _task_id: TaskId,
        _after: u64,
        _limit: usize,
    ) -> BoxFuture<'a, Result<Vec<InboxItem>>> {
        unbound("durable task inbox is not bound")
    }
    /// Retains or reconciles an absolute timer wait.
    fn wait_until<'a>(
        &'a self,
        _task_id: TaskId,
        _operation_id: OperationId,
        _deadline_unix_ms: u64,
    ) -> BoxFuture<'a, Result<()>> {
        unbound("durable timers are not bound")
    }
    /// Observes one recorded effect without redispatching it.
    fn reconcile_effect<'a>(
        &'a self,
        _task_id: TaskId,
        _effect_id: EffectId,
    ) -> BoxFuture<'a, Result<crate::core::EffectStatus>> {
        unbound("durable effect reconciliation is not bound")
    }
}

/// Independently replaceable durable admission boundary.
pub trait TaskSpawner: acyclic_stream::ProviderPlatform {
    /// Exact policy revision enforced during child admission.
    fn policy_identity(&self) -> Option<ComponentIdentity>;
    /// Execution route this spawner can actually dispatch to.
    fn execution_identity(&self) -> Option<ComponentIdentity> {
        None
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    /// Borrows the policy identity on the original work.
    fn policy_identity_with_native_work(
        &self,
        _work: &NativeMaterializationWork,
    ) -> Result<Option<&ComponentIdentity>> {
        Err(native_unbound(
            "task spawner has no original native policy binding source",
        ))
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    /// Borrows the execution identity on the original work.
    fn execution_identity_with_native_work(
        &self,
        _work: &NativeMaterializationWork,
    ) -> Result<Option<&ComponentIdentity>> {
        Err(native_unbound(
            "task spawner has no original native execution binding source",
        ))
    }
    /// Publishes or reconciles one complete caller-identified admission.
    fn admit<'a>(
        &'a self,
        request: TaskAdmissionRecord,
    ) -> BoxFuture<'a, Result<Admission<TaskId>>>;
    /// Publishes a child only while this exact original parent lease is current.
    fn admit_owned<'a>(
        &'a self,
        _request: TaskAdmissionRecord,
        _fence: crate::scheduler::LeaseFence,
    ) -> BoxFuture<'a, Result<Admission<TaskId>>> {
        unbound("owned child spawner is not bound")
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    /// Admits a child under its original parent lease.
    fn admit_owned_with_native_work<'a>(
        &'a self,
        request: (TaskAdmissionRecord, NativeAllocation),
        fence: (crate::scheduler::LeaseFence, NativeAllocation),
        _work: &'a NativeMaterializationWork,
    ) -> Result<NativeFutureBundle<'a, (Admission<TaskId>, NativeAllocation)>> {
        drop(request);
        drop(fence);
        Err(native_unbound(
            "task spawner has no original-ledger owned admission producer",
        ))
    }
    /// Observes one possibly lost admission without resubmission.
    fn reconcile_admission<'a>(
        &'a self,
        _operation_id: OperationId,
    ) -> BoxFuture<'a, Result<Option<(TaskId, TaskAdmissionRecord)>>> {
        unbound("durable admission reconciliation is not bound")
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    /// Reconciles admission on the original work.
    fn reconcile_admission_with_native_work<'a>(
        &'a self,
        _operation: OperationId,
        _work: &'a NativeMaterializationWork,
    ) -> Result<NativeFutureBundle<'a, (Option<(TaskId, TaskAdmissionRecord)>, NativeAllocation)>>
    {
        Err(native_unbound(
            "task spawner has no original-ledger admission reconciliation producer",
        ))
    }
    /// Publishes the immutable request before any indexed child admission.
    fn admit_batch<'a>(
        &'a self,
        _request: DurableBatchRequest,
    ) -> BoxFuture<'a, Result<Vec<Admission<TaskId>>>> {
        unbound("durable batch admission is not bound")
    }
    /// Observes the original batch and current indexed admissions only.
    fn reconcile_batch<'a>(
        &'a self,
        _request: DurableBatchRequest,
    ) -> BoxFuture<'a, Result<Option<Vec<Admission<TaskId>>>>> {
        unbound("durable batch reconciliation is not bound")
    }
    /// Loads a retained request by identity after the caller loses its inputs.
    fn load_batch<'a>(
        &'a self,
        _batch_id: BatchId,
    ) -> BoxFuture<'a, Result<Option<DurableBatchRequest>>> {
        unbound("durable batch lookup is not bound")
    }
    /// Retains an idempotent cancellation declaration at the batch owner.
    fn cancel_batch<'a>(
        &'a self,
        _batch_id: BatchId,
    ) -> BoxFuture<'a, Result<Option<BatchCancellationReport>>> {
        unbound("durable batch cancellation is not bound")
    }
    /// Whether this spawner can retain a cancellation declaration.
    fn supports_batch_cancellation(&self) -> bool {
        false
    }
}

/// One replaceable execution route.
pub trait ExecutionProvider: acyclic_stream::ProviderPlatform {
    /// Immutable provider implementation pinned in every admitted placement.
    fn identity(&self) -> ComponentIdentity;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    /// Borrows the provider identity on the original work.
    fn identity_with_native_work(
        &self,
        _work: &NativeMaterializationWork,
    ) -> Result<&ComponentIdentity> {
        Err(native_unbound(
            "execution provider has no original native identity source",
        ))
    }
    /// Durable spawner for this provider's task build and environment.
    fn spawner(&self) -> Arc<dyn TaskSpawner>;
    /// Owner-bound observation and cancellation for the same route.
    fn state(&self) -> Arc<dyn TaskStateProvider>;
    /// Read-only qualification of one exact admitted task and its accessible input.
    fn qualify<'a>(
        &'a self,
        request: &'a TaskAdmissionRecord,
    ) -> BoxFuture<'a, Result<ExecutionPlacement>>;
}

/// Replaceable owner-bound observer for an already planned provider effect.
pub trait DurableEffectObserver: acyclic_stream::ProviderPlatform {
    /// Returns the latest attested status after querying the pinned attempt.
    fn reconcile<'a>(
        &'a self,
        effect_id: EffectId,
    ) -> BoxFuture<'a, Result<crate::core::EffectStatus>>;
}

/// Replaceable local interaction router.
pub trait InteractionRouter: acyclic_stream::ProviderPlatform {
    /// Routes an interaction while retaining its payloads in the provider.
    fn route<'a>(
        &'a self,
        operation_id: OperationId,
        interaction: Interaction,
    ) -> BoxFuture<'a, Result<InteractionOutcome>>;

    /// Pure native support discovery, before building a callback future.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_interactions(&self, _work: &NativeMaterializationWork) -> Result<()> {
        Err(native_unbound(
            "interaction router has no original-ledger implementation",
        ))
    }

    /// Moves the paid request into the original owner's guarded journal route.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn route_with_native_work<'a>(
        &'a self,
        _operation_id: OperationId,
        _interaction: Interaction,
        _request_custody: NativeAllocation,
        _work: &'a NativeMaterializationWork,
    ) -> Result<NativeFutureBundle<'a, (InteractionOutcome, NativeAllocation)>> {
        let _paid = (_interaction, _request_custody);
        Err(native_unbound(
            "interaction router has no original-ledger route",
        ))
    }
}

/// Owner-mediated interaction ledger.
pub type InteractionInspection = Option<(InteractionTicket, Option<InteractionResolution>)>;

/// Owner-mediated resolver for versioned questions and approvals.
pub trait InteractionResolver: acyclic_stream::ProviderPlatform {
    /// Reads the admitted request and current decision before a CAS reply.
    fn inspect<'a>(
        &'a self,
        scope: Scope,
        id: InteractionId,
    ) -> BoxFuture<'a, Result<InteractionInspection>>;

    /// Stages and validates a question, choice, or form answer before CAS.
    fn resolve_answer<'a>(
        &'a self,
        operation_id: OperationId,
        scope: Scope,
        id: InteractionId,
        expected_version: u64,
        response: InteractionResponse,
    ) -> BoxFuture<'a, Result<ResolutionReceipt>>;

    /// Accepts or declines only the ticket's pinned approval action.
    fn resolve_approval<'a>(
        &'a self,
        operation_id: OperationId,
        scope: Scope,
        id: InteractionId,
        expected_version: u64,
        approved: bool,
        reason: Option<String>,
    ) -> BoxFuture<'a, Result<ResolutionReceipt>>;
}
