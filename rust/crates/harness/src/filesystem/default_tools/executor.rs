//! Executor for portable recursive tools.

use super::*;

impl<P, A, O> RecursiveExecutor<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) fn check_authority(
        &self,
        context: &ToolContext,
        invocation: &ToolInvocation,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<()> {
        self.source
            .as_ref()
            .ok_or_else(|| Error::Unsupported("recursive tool provider is not bound".into()))?;
        crate::executor::with_history_io(io, |work| {
            ToolInvocation::validate_identity(&invocation.call_id, Some(&invocation.name), work)
        })?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(|io| io.original_native_work()) {
            work.remaining()?;
            if !context.native_metadata_matches(work) {
                return Err(recursive_reject(
                    io,
                    crate::wire::ErrorCode::Unauthorized,
                    "recursive tool metadata differs from its original native context",
                ));
            }
        }
        let scope = context.effective_scope();
        let (grant, call_grant) = if self.durable.definition.name == "acyclic.fork" {
            ("fork:publish", "tool:call:acyclic.fork")
        } else {
            ("project:merge", "tool:call:acyclic.merge")
        };
        if context.operation_id() != invocation.operation_id
            || context.call_id() != invocation.call_id
            || context.durable_task_id().is_none()
            || invocation.name != self.durable.definition.name
            || !scope.grants().contains(grant)
            || !scope.grants().contains(call_grant)
        {
            return Err(recursive_reject(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "recursive tool lacks its exact admitted caller/effect authority",
            ));
        }
        recursive_byte_bound(
            &invocation.arguments,
            self.maximum_request_bytes.min(scope.limits().file_bytes),
            io,
        )
    }
    #[cfg(not(target_arch = "wasm32"))]
    pub(super) async fn run_with_native_work(
        &self,
        context: &ToolContext,
        invocation: &ToolInvocation,
        original_work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Option<ToolResult>, acyclic_fs::NativeAllocation)> {
        let mut result_custody = original_work.claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)?;
        let (work, cancellation) = original_work.invocation_view()?;
        let _stop_on_drop = InvocationCancellation(cancellation.clone());
        let mut result = async {
            let work = &work;
            let cancellation = &cancellation;
            let owner_io = crate::filesystem::PreparationIo::from_native_work(work)?;
            let owner = self
                .source
                .as_deref()
                .expect("recursive source was authorized")
                .owner(
                    context,
                    &self.durable,
                    invocation,
                    cancellation,
                    Some(&owner_io),
                )?
                .await?;
            let caller = context.durable_task_id().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "recursive native tool has no durable caller",
            })?;
            let admitted = owner
                .runtime
                .task_host()
                .observe_admission_with_native_work(caller, work)
                .await?;
            if admitted.0.grants != *context.scope().grants()
                || admitted.0.limits != context.scope().limits()
                || admitted.0.run_limits != context.scope().run_limits()
            {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "recursive native owner differs from original admitted caller",
                });
            }
            drop(admitted);
            owner
                .runtime
                .task_host()
                .verify_dispatch_owner_with_native_work(
                    caller,
                    owner.journal_owner.task_binding().1,
                    work,
                )?
                .await?;
            let mut session = self
                .durable
                .open_with_native_context(context, invocation, Arc::clone(&owner.workflow), work)?
                .await?;
            for _ in 0..8 {
                owner
                    .runtime
                    .task_host()
                    .verify_dispatch_owner_with_native_work(
                        caller,
                        owner.journal_owner.task_binding().1,
                        work,
                    )?
                    .await?;
                let io = crate::filesystem::PreparationIo::from_native_work(work)?;
                let state =
                    crate::filesystem::native_decoded(io.value_decode_preparation::<State>(
                        &session.checkpoint().state,
                        crate::wire::ErrorCode::Invalid,
                    )?)?;
                if let State::Complete { value } = state.0 {
                    return Ok((Some(ToolResult { value }), state.1));
                }
                let Some(input) = self
                    .input(
                        context,
                        invocation,
                        &owner,
                        state.0,
                        cancellation,
                        Some(&io),
                    )
                    .await?
                else {
                    return Ok((None, work.claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)?));
                };
                let identity = transition_identity(
                    invocation.operation_id,
                    session.checkpoint().revision,
                    Some(&io),
                )?;
                let custody = io.take_native_custody(io.owned_bytes()?)?;
                let transition = session
                    .step(identity.0, identity.1, input, custody, work)?
                    .await?;
                owner
                    .runtime
                    .task_host()
                    .verify_dispatch_owner_with_native_work(
                        caller,
                        owner.journal_owner.task_binding().1,
                        work,
                    )?
                    .await?;
                match transition.0.status {
                    MachineStatus::Completed { value } => {
                        return Ok((Some(ToolResult { value }), transition.1));
                    }
                    MachineStatus::Failed { message } => {
                        return Err(Error::NativeDiagnostic {
                            code: crate::wire::ErrorCode::Conflict,
                            message,
                            custody: transition.1,
                        });
                    }
                    MachineStatus::Suspended => {}
                }
            }
            Ok((None, work.claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)?))
        }
        .await?;
        result_custody.absorb(&mut result.1)?;
        Ok((result.0, result_custody))
    }

    pub(super) async fn run(
        &self,
        context: ToolContext,
        invocation: ToolInvocation,
        cancellation: &CancellationToken,
    ) -> Result<Option<ToolResult>> {
        cancellation
            .check()
            .map_err(|error| Error::Storage(error.to_string()))?;
        self.authorize_with_context(&context, &invocation)?;
        let owner = self
            .source
            .as_deref()
            .expect("recursive source was authorized")
            .owner(&context, &self.durable, &invocation, cancellation, None)?
            .await?;
        let caller = context
            .durable_task_id()
            .ok_or_else(|| Error::Unauthorized("recursive tool has no durable caller".into()))?;
        let admitted = owner.runtime.task_host().observe_admission(caller).await?;
        if admitted.grants != *context.scope().grants()
            || admitted.limits != context.scope().limits()
            || admitted.run_limits != context.scope().run_limits()
        {
            return Err(Error::Unauthorized(
                "recursive owner differs from the original admitted caller".into(),
            ));
        }
        owner
            .runtime
            .task_host()
            .verify_dispatch_owner(caller, owner.journal_owner.task_binding().1.clone())
            .await?;
        let mut session = self
            .durable
            .open(context.clone(), invocation.clone(), owner.workflow.clone())
            .await?;
        for _ in 0..8 {
            cancellation
                .check()
                .map_err(|error| Error::Storage(error.to_string()))?;
            owner
                .runtime
                .task_host()
                .verify_dispatch_owner(caller, owner.journal_owner.task_binding().1.clone())
                .await?;
            let state: State = decode(&session.checkpoint().state, None)?;
            if let State::Complete { value } = state {
                return Ok(Some(ToolResult { value }));
            }
            let Some(input) = self
                .input(&context, &invocation, &owner, state, cancellation, None)
                .await?
            else {
                return Ok(None);
            };
            let (operation, key) =
                transition_identity(invocation.operation_id, session.checkpoint().revision, None)?;
            let transition = session.step(operation, key, input).await?;
            owner
                .runtime
                .task_host()
                .verify_dispatch_owner(caller, owner.journal_owner.task_binding().1.clone())
                .await?;
            match transition.status {
                MachineStatus::Completed { value } => return Ok(Some(ToolResult { value })),
                MachineStatus::Failed { message } => return Err(Error::Conflict(message)),
                MachineStatus::Suspended => {}
            }
        }
        Err(Error::Invalid(
            "recursive tool exceeded its finite command sequence".into(),
        ))
    }
}

impl<P, A, O> ToolExecutor for RecursiveExecutor<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn resumable_machine_identity(
        &self,
        work: crate::NativeWork<'_>,
    ) -> Result<Option<&MachineIdentity>> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = work {
            return self.durable.machine.identity_with_native_work(work).map(Some);
        }
        let _ = work;
        Ok(Some(self.durable.machine.identity()))
    }
    fn authorize(&self, _: Option<&RuntimeScope>, _: &ToolInvocation) -> Result<()> {
        Err(Error::Unauthorized(
            "recursive tools require the original durable ToolContext".into(),
        ))
    }
    fn authorize_with_context(
        &self,
        context: &ToolContext,
        invocation: &ToolInvocation,
    ) -> Result<()> {
        self.check_authority(context, invocation, None)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn supports_native_execution(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        work.remaining()?;
        if !self
            .source
            .as_ref()
            .is_some_and(|source| source.supports_native_source())
            || !self.durable.machine.supports_native_work()
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "recursive producer has no original native execution callbacks",
            });
        }
        Ok(())
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
            Some(&crate::filesystem::PreparationIo::from_native_work(work)?),
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
            let result = self.run_with_native_work(context, invocation, work).await?;
            match result.0 {
                Some(value) => Ok((value, result.1)),
                None => Err(Error::Indeterminate(invocation.operation_id)),
            }
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
        crate::NativeFutureBundle::new(work, || {
            self.run_with_native_work(context, invocation, work)
        })
    }

    fn execute<'a>(&'a self, _: ToolInvocation) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async {
            Err(Error::Unauthorized(
                "recursive tools require original durable task context".into(),
            ))
        })
    }
    fn execute_with_context<'a>(
        &'a self,
        context: ToolContext,
        invocation: ToolInvocation,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        let operation = invocation.operation_id;
        let cancellation = CancellationToken::new();
        let guard = InvocationCancellation(cancellation.clone());
        Box::pin(async move {
            let _guard = guard;
            self.run(context, invocation, &cancellation)
                .await?
                .ok_or(Error::Indeterminate(operation))
        })
    }
    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async {
            Err(Error::Unauthorized(
                "recursive reconciliation requires original durable task context".into(),
            ))
        })
    }
    fn reconcile_with_context<'a>(
        &'a self,
        context: ToolContext,
        invocation: ToolInvocation,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        let cancellation = CancellationToken::new();
        let guard = InvocationCancellation(cancellation.clone());
        Box::pin(async move {
            let _guard = guard;
            self.run(context, invocation, &cancellation).await
        })
    }
}
