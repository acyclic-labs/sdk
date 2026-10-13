//! Runs the protocol through the existing task and workflow owners.
use super::*;
use crate::filesystem::PreparationIo;

impl<P: StreamProvider + 'static, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>
    RecursiveExecutor<P, A, O>
{
    pub(super) fn check_authority(
        &self,
        context: &ToolContext,
        invocation: &ToolInvocation,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        self.source
            .as_ref()
            .ok_or_else(|| Error::Unsupported("recursive provider is not bound".into()))?;
        crate::executor::with_history_io(io, |work| {
            ToolInvocation::validate_identity(&invocation.call_id, Some(&invocation.name), work)
        })?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work) {
            work.remaining()?;
            reject_if(
                !context.native_metadata_matches(work),
                io,
                crate::wire::ErrorCode::Unauthorized,
                "recursive metadata belongs to another original context",
            )?;
        }
        let scope = context.effective_scope();
        let (grant, call) = if self.durable.definition.name == "acyclic.fork" {
            ("fork:publish", "tool:call:acyclic.fork")
        } else {
            ("project:merge", "tool:call:acyclic.merge")
        };
        reject_if(
            context.operation_id() != invocation.operation_id
                || context.call_id() != invocation.call_id
                || context.durable_task_id().is_none()
                || invocation.name != self.durable.definition.name
                || !scope.grants().contains(grant)
                || !scope.grants().contains(call),
            io,
            crate::wire::ErrorCode::Unauthorized,
            "recursive invocation changed its admitted caller or effect",
        )?;
        recursive_byte_bound(
            &invocation.arguments,
            self.maximum_request_bytes.min(scope.limits().file_bytes),
            io,
        )
    }

    async fn run(
        &self,
        context: &ToolContext,
        invocation: &ToolInvocation,
        cancellation: &CancellationToken,
        io: Option<&PreparationIo>,
    ) -> Result<(Option<ToolResult>, crate::NativeCustody)> {
        cancellation
            .check()
            .map_err(|error| Error::Storage(error.to_string()))?;
        self.check_authority(context, invocation, io)?;
        let (runtime, workflow) = self
            .source
            .as_ref()
            .expect("authorized source")
            .owner(context, &self.durable, invocation, cancellation, io)?
            .await?;
        let owner = workflow.owner.as_ref().expect("validated tool owner");
        let caller = context
            .durable_task_id()
            .expect("authorized durable caller");
        {
            let temporary = recursive_temporary_io(io)?;
            let admitted = runtime
                .admission_observed(caller, temporary.as_ref())
                .await?;
            reject_if(
                admitted.grants != *context.scope().grants()
                    || admitted.limits != context.scope().limits()
                    || admitted.run_limits != context.scope().run_limits(),
                io,
                crate::wire::ErrorCode::Unauthorized,
                "recursive owner differs from original admission",
            )?;
        }
        runtime
            .owner_observed(caller, owner.task_binding().1, false, io)
            .await?;

        // The SDK sessions differ in their input/output guard contract, not in protocol control flow.
        macro_rules! drive {
            ($session:ident, $local:ident, $input:ident, $identity:ident, $step:expr) => {{
                for _ in 0..8 {
                    cancellation
                        .check()
                        .map_err(|error| Error::Storage(error.to_string()))?;
                    runtime
                        .owner_observed(caller, owner.task_binding().1, false, io)
                        .await?;
                    let temporary = recursive_temporary_io(io)?;
                    let $local = temporary.as_ref();
                    let (state, state_custody) = 'state: {
                        #[cfg(not(target_arch = "wasm32"))]
                        if let Some(io) = $local {
                            let (state, custody) = crate::filesystem::native_decoded(
                                io.value_decode_preparation::<State>(
                                    &$session.checkpoint().state,
                                    crate::wire::ErrorCode::Invalid,
                                )?,
                            )?;
                            break 'state (state, Some(custody));
                        }
                        (decode::<State>(&$session.checkpoint().state, None)?, None)
                    };
                    if let State::Complete { value } = state {
                        return Ok((Some(ToolResult { value }), state_custody));
                    }
                    let Some($input) = self
                        .input(
                            context,
                            invocation,
                            &(runtime.clone(), workflow.clone()),
                            state,
                            cancellation,
                            $local,
                        )
                        .await?
                    else {
                        return Ok((None, None));
                    };
                    let $identity = transition_identity(
                        invocation.operation_id,
                        $session.checkpoint().revision,
                        $local,
                    )?;
                    let (transition, custody): (MachineTransition, crate::NativeCustody) = $step;
                    runtime
                        .owner_observed(caller, owner.task_binding().1, false, io)
                        .await?;
                    match transition.status {
                        MachineStatus::Completed { value } => {
                            return Ok((Some(ToolResult { value }), custody));
                        }
                        MachineStatus::Failed { message } => {
                            #[cfg(not(target_arch = "wasm32"))]
                            if let Some(custody) = custody {
                                return Err(Error::NativeDiagnostic {
                                    code: crate::wire::ErrorCode::Conflict,
                                    message,
                                    custody,
                                });
                            }
                            return Err(Error::Conflict(message));
                        }
                        MachineStatus::Suspended => {}
                    }
                    drop(state_custody);
                }
                if io.is_some() {
                    return Ok((None, None));
                }
                return Err(Error::Invalid(
                    "recursive tool exceeded its finite command sequence".into(),
                ));
            }};
        }

        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work) {
            let mut session = self
                .durable
                .open_with_native_context(context, invocation, workflow.clone(), work)?
                .await?;
            drive!(session, local, input, identity, {
                let custody = local
                    .expect("native input")
                    .take_native_custody(local.expect("native input").owned_bytes()?)?;
                let (transition, custody) = session
                    .step(identity.0, identity.1, input, custody, work)?
                    .await?;
                (transition, Some(custody))
            });
        }
        let mut session = self
            .durable
            .open(context.clone(), invocation.clone(), workflow.clone())
            .await?;
        drive!(
            session,
            local,
            input,
            identity,
            (session.step(identity.0, identity.1, input).await?, None)
        )
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn native_run(
        &self,
        context: &ToolContext,
        invocation: &ToolInvocation,
        original: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Option<ToolResult>, acyclic_fs::NativeAllocation)> {
        let mut custody = original.claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)?;
        let (work, cancellation) = original.invocation_view()?;
        let _stop = InvocationCancellation(cancellation.clone());
        let io = PreparationIo::from_native_work(&work)?;
        let (result, returned) = self
            .run(context, invocation, &cancellation, Some(&io))
            .await?;
        if let Some(mut returned) = returned {
            custody.absorb(&mut returned)?;
        }
        Ok((result, custody))
    }
}
impl<P: StreamProvider + 'static, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>
    ToolExecutor for RecursiveExecutor<P, A, O>
{
    fn resumable_machine_identity(
        &self,
        work: crate::NativeWork<'_>,
    ) -> Result<Option<&MachineIdentity>> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = work {
            return self
                .durable
                .machine
                .identity_with_native_work(work)
                .map(Some);
        }
        let _ = work;
        Ok(Some(self.durable.machine.identity()))
    }
    fn authorize(&self, _: Option<&RuntimeScope>, _: &ToolInvocation) -> Result<()> {
        Err(Error::Unauthorized(
            "recursive tools require the original durable context".into(),
        ))
    }
    fn authorize_with_context(
        &self,
        context: &ToolContext,
        invocation: &ToolInvocation,
    ) -> Result<()> {
        self.check_authority(context, invocation, None)
    }
    fn execute<'a>(&'a self, _: ToolInvocation) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async {
            Err(Error::Unauthorized(
                "recursive tools require their durable context".into(),
            ))
        })
    }
    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async {
            Err(Error::Unauthorized(
                "recursive tools require their durable context".into(),
            ))
        })
    }
    fn execute_with_context<'a>(
        &'a self,
        context: ToolContext,
        invocation: ToolInvocation,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        let cancellation = CancellationToken::new();
        let stop = InvocationCancellation(cancellation.clone());
        Box::pin(async move {
            let _stop = stop;
            self.run(&context, &invocation, &cancellation, None)
                .await?
                .0
                .ok_or(Error::Indeterminate(invocation.operation_id))
        })
    }
    fn reconcile_with_context<'a>(
        &'a self,
        context: ToolContext,
        invocation: ToolInvocation,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        let cancellation = CancellationToken::new();
        let stop = InvocationCancellation(cancellation.clone());
        Box::pin(async move {
            let _stop = stop;
            self.run(&context, &invocation, &cancellation, None)
                .await
                .map(|result| result.0)
        })
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn supports_native_execution(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        work.remaining()?;
        reject_if(
            !self
                .source
                .as_ref()
                .is_some_and(|source| source.supports_native_source())
                || !self.durable.machine.supports_native_work(),
            Some(&PreparationIo::from_native_work(work)?),
            crate::wire::ErrorCode::Unsupported,
            "recursive source has no original native callbacks",
        )
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn authorize_with_native_context(
        &self,
        context: &ToolContext,
        invocation: &ToolInvocation,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        self.check_authority(
            context,
            invocation,
            Some(&PreparationIo::from_native_work(work)?),
        )
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn execute_with_native_context<'a>(
        &'a self,
        context: &'a ToolContext,
        invocation: &'a ToolInvocation,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (ToolResult, acyclic_fs::NativeAllocation)>> {
        self.supports_native_execution(work)?;
        self.authorize_with_native_context(context, invocation, work)?;
        crate::NativeFutureBundle::new(work, || async move {
            let (result, custody) = self.native_run(context, invocation, work).await?;
            Ok((
                result.ok_or(Error::Indeterminate(invocation.operation_id))?,
                custody,
            ))
        })
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn reconcile_with_native_context<'a>(
        &'a self,
        context: &'a ToolContext,
        invocation: &'a ToolInvocation,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<ToolResult>, acyclic_fs::NativeAllocation)>>
    {
        self.supports_native_execution(work)?;
        self.authorize_with_native_context(context, invocation, work)?;
        crate::NativeFutureBundle::new(work, || self.native_run(context, invocation, work))
    }
}
