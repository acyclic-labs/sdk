//! Recursive operations rely on the existing durable tool runner and SDK journals.
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
            deny(!context.native_metadata_matches(work), io)?;
        }
        let scope = context.effective_scope();
        let (grant, call) = if self.definition.name == "acyclic.fork" {
            ("fork:publish", "tool:call:acyclic.fork")
        } else {
            ("project:merge", "tool:call:acyclic.merge")
        };
        deny(
            context.operation_id() != invocation.operation_id
                || context.call_id() != invocation.call_id
                || context.durable_task_id().is_none()
                || invocation.name != self.definition.name
                || !scope.grants().contains(grant)
                || !scope.grants().contains(call),
            io,
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
    ) -> Result<Option<ToolResult>> {
        cancellation.check()?;
        self.check_authority(context, invocation, io)?;
        crate::executor::with_history_io(io, |work| {
            crate::executor::validate_json_schema_value(
                &self.definition.input_schema,
                &invocation.arguments,
                "tool input",
                work,
            )
        })?;
        let approval = 'approval: {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(work) = io.and_then(PreparationIo::original_native_work) {
                break 'approval context
                    .authorize_tool(&self.definition, invocation, work)
                    .await?;
            }
            context
                .task()
                .authorize_tool(&self.definition, invocation)
                .await?
        };
        if let Some(operation) = approval {
            return Err(Error::Indeterminate(operation));
        }
        let owner = self
            .source
            .as_ref()
            .ok_or_else(|| {
                recursive_reject(
                    io,
                    crate::wire::ErrorCode::Unauthorized,
                    "authorized source",
                )
            })?
            .owner(context, cancellation, io)
            .await?;
        let caller = context.durable_task_id().ok_or_else(|| {
            recursive_reject(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "authorized caller",
            )
        })?;
        {
            let temporary = crate::filesystem::PreparationIo::temporary(io)?;
            let admission = owner
                .0
                .admission_observed(caller, temporary.as_ref())
                .await?;
            reject_if(
                admission.execution.is_some(),
                io,
                crate::wire::ErrorCode::Unsupported,
                "stock routed execution provider is not composed",
            )?;
            deny(
                admission.grants != *context.scope().grants()
                    || admission.limits != context.scope().limits()
                    || admission.run_limits != context.scope().run_limits(),
                io,
            )?;
        }
        owner
            .0
            .owner_observed(caller, owner.1.task_binding().1, false, io)
            .await?;
        let temporary = crate::filesystem::PreparationIo::temporary(io)?;
        let plan = self
            .pinned_plan(
                context,
                invocation,
                &owner,
                cancellation,
                temporary.as_ref(),
            )
            .await?;
        let value = match &plan {
            Plan::Fork(plan) => {
                self.run_fork(context, &owner, plan, cancellation, io)
                    .await?
            }
            Plan::Merge(plan) => Some(
                self.run_merge(context, &owner, plan, cancellation, io)
                    .await?,
            ),
        };
        owner
            .0
            .owner_observed(caller, owner.1.task_binding().1, false, io)
            .await?;
        Ok(value.map(|value| ToolResult { value }))
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn native_run(
        &self,
        context: &ToolContext,
        invocation: &ToolInvocation,
        original: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Option<ToolResult>, acyclic_fs::NativeAllocation)> {
        let (work, cancellation) = original.invocation_view()?;
        let _stop = InvocationCancellation(cancellation.clone());
        let io = PreparationIo::from_native_work(&work)?;
        let result = self
            .run(context, invocation, &cancellation, Some(&io))
            .await?;
        let bytes = result
            .as_ref()
            .map(|result| recursive_value_owned_bytes(&result.value))
            .transpose()?
            .unwrap_or(0);
        Ok((result, io.take_native_custody(bytes)?))
    }
}
impl<P: StreamProvider + 'static, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>
    ToolExecutor for RecursiveExecutor<P, A, O>
{
    fn authorize(
        &self,
        _scope: Option<&RuntimeScope>,
        context: Option<&ToolContext>,
        invocation: &ToolInvocation,
        work: crate::NativeWork<'_>,
    ) -> Result<()> {
        let context = context.ok_or_else(|| {
            #[cfg(not(target_arch = "wasm32"))]
            if work.is_some() {
                return Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "recursive tools require the original durable context",
                };
            }
            Error::Unauthorized("recursive tools require the original durable context".into())
        })?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = work {
            return self.check_authority(
                context,
                invocation,
                Some(&PreparationIo::from_native_work(work)?),
            );
        }
        let _ = work;
        self.check_authority(context, invocation, None)
    }

    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        let Some(context) = context else {
            return Box::pin(async {
                Err(Error::Unauthorized(
                    "recursive tools require their durable context".into(),
                ))
            });
        };

        let cancellation = CancellationToken::new();
        let stop = InvocationCancellation(cancellation.clone());
        Box::pin(async move {
            let _stop = stop;
            self.run(&context, &invocation, &cancellation, None)
                .await?
                .ok_or(Error::Indeterminate(invocation.operation_id))
        })
    }
    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        let Some(context) = context else {
            return Box::pin(async {
                Err(Error::Unauthorized(
                    "recursive tools require their durable context".into(),
                ))
            });
        };

        let cancellation = CancellationToken::new();
        let stop = InvocationCancellation(cancellation.clone());
        Box::pin(async move {
            let _stop = stop;
            self.run(&context, &invocation, &cancellation, None).await
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
                .is_some_and(|source| source.supports_native_source()),
            Some(&PreparationIo::from_native_work(work)?),
            crate::wire::ErrorCode::Unsupported,
            "recursive source has no original native callbacks",
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
        self.authorize(None, Some(context), invocation, Some(work))?;
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
        self.authorize(None, Some(context), invocation, Some(work))?;
        crate::NativeFutureBundle::new(work, || self.native_run(context, invocation, work))
    }
}
