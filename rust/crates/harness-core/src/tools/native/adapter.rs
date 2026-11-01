use super::{arguments::Capability, *};
use crate::{
    runtime::{RuntimeScope, ToolContext},
    tool::{ToolExecutor, ToolInvocation, ToolResult},
};
use acyclic_stream::BoxProviderFuture;

pub(super) struct Adapter {
    pub profile: NativeToolProfile,
    pub capability: Capability,
    pub original: Arc<Tool>,
}

impl ToolExecutor for Adapter {
    fn authorize_replay<'a>(
        &'a self,
        context: Option<&'a ToolContext>,
        invocation: &'a ToolInvocation,
        work: crate::NativeWork<'a>,
    ) -> Result<Option<crate::NativeFutureBundle<'a, ()>>> {
        Ok(Some(crate::NativeFutureBundle::new_optional(
            work,
            || async move {
                let adapted = self.adapt(invocation, work)?;
                if let Some(authorization) =
                    self.original
                        .executor
                        .authorize_replay(context, &adapted.invocation, work)?
                {
                    authorization.await?;
                }
                Ok(())
            },
        )?))
    }

    fn workspace_binding(&self) -> Option<crate::tool::WorkspaceBinding> {
        self.original.executor.workspace_binding()
    }

    fn dispatch(
        &self,
        invocation: &ToolInvocation,
        context: Option<&ToolContext>,
        work: crate::NativeWork<'_>,
    ) -> Result<Option<crate::tool::Dispatch>> {
        let adapted = self.adapt(invocation, work)?;
        self.original
            .executor
            .dispatch(&adapted.invocation, context, work)
    }

    fn prepare_shell_script(
        &self,
        source: &str,
        input: &Value,
        context: Option<&ToolContext>,
        work: crate::NativeWork<'_>,
    ) -> Result<crate::tools::ShellInput> {
        self.original
            .executor
            .prepare_shell_script(source, input, context, work)
    }
    #[cfg(all(feature = "hooks", feature = "filesystem", not(target_arch = "wasm32")))]
    fn run_hooks<'a>(
        &'a self,
        configuration: (&'a crate::hooks::Hooks, [u8; 32]),
        event: crate::hooks::HookEvent,
        input: crate::hooks::HookInput<'a>,
        registry: &'a crate::tool::ToolRegistry,
        context: ToolContext,
        reconcile_only: bool,
    ) -> BoxProviderFuture<'a, Result<Option<crate::hooks::HookOutput>>> {
        self.original.executor.run_hooks(
            configuration,
            event,
            input,
            registry,
            context,
            reconcile_only,
        )
    }
    fn read_path<'a>(
        &'a self,
        path: &'a str,
        work: crate::NativeWork<'_>,
    ) -> Result<(
        &'a crate::conversation::VolumeRef,
        std::borrow::Cow<'a, str>,
    )> {
        self.original.executor.read_path(path, work)
    }

    fn authorize(
        &self,
        scope: Option<&RuntimeScope>,
        context: Option<&ToolContext>,
        invocation: &ToolInvocation,
        work: crate::NativeWork<'_>,
    ) -> Result<()> {
        let adapted = self.adapt(invocation, work)?;
        self.original
            .executor
            .authorize(scope, context, &adapted.invocation, work)
    }

    fn cancellable_on_drop(&self) -> bool {
        self.original.executor.cancellable_on_drop()
    }

    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            let adapted = self.adapt(&invocation, None)?;
            let result = self
                .original
                .executor
                .execute(adapted.invocation, context)
                .await;
            self.present_error(result, &invocation, None)
        })
    }

    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async move {
            let adapted = self.adapt(&invocation, None)?;
            let result = self
                .original
                .executor
                .reconcile(adapted.invocation, context)
                .await;
            self.present_error(result, &invocation, None)
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_execution(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        self.original.executor.supports_native_execution(work)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn execute_with_native_context<'a>(
        &'a self,
        context: &'a ToolContext,
        invocation: &'a ToolInvocation,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (ToolResult, acyclic_fs::NativeAllocation)>> {
        self.supports_native_execution(work)?;
        crate::NativeFutureBundle::new(work, || async move {
            let adapted = self.adapt(invocation, Some(work))?;
            let result = self
                .original
                .executor
                .execute_with_native_context(context, &adapted.invocation, work)?
                .await;
            self.present_error(result, invocation, Some(work))
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn reconcile_with_native_context<'a>(
        &'a self,
        context: &'a ToolContext,
        invocation: &'a ToolInvocation,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<ToolResult>, acyclic_fs::NativeAllocation)>>
    {
        self.supports_native_execution(work)?;
        crate::NativeFutureBundle::new(work, || async move {
            let adapted = self.adapt(invocation, Some(work))?;
            let result = self
                .original
                .executor
                .reconcile_with_native_context(context, &adapted.invocation, work)?
                .await;
            self.present_error(result, invocation, Some(work))
        })
    }
}

impl Adapter {
    fn present_error<T>(
        &self,
        result: Result<T>,
        invocation: &ToolInvocation,
        work: crate::NativeWork<'_>,
    ) -> Result<T> {
        match result {
            Err(Error::ExactEdit(cause))
                if self.profile == NativeToolProfile::Google
                    && self.capability == Capability::Edit =>
            {
                Err(Error::ExactEdit(cause.present(
                    super::arguments::text(&invocation.arguments, "file_path")?,
                    google_edit_error,
                    work,
                )?))
            }
            other => other,
        }
    }
}

// Public Gemini CLI getErrorReplaceResult; the shared matcher owns the cause
// and count. Formatting adds no edit, source read or diagnostic String.
fn google_edit_error(
    occurrences: usize,
    path: &str,
    formatter: &mut std::fmt::Formatter<'_>,
) -> std::fmt::Result {
    if occurrences == 0 {
        write!(
            formatter,
            "Could not find an exact match for 'old_string' in '{path}'. If previous edits modified the file or you are modifying lines outside your recent read window, please use ReadFile to inspect the target lines before retrying with an exact 'old_string'."
        )
    } else {
        write!(
            formatter,
            "Failed to edit, Expected 1 occurrence but found {occurrences} for old_string in file: {path}. If you intended to replace multiple occurrences, set 'allow_multiple' to true."
        )
    }
}

impl crate::tool::ToolProjection for Adapter {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_projection(&self, work: crate::NativeWork<'_>) -> Result<()> {
        self.original.projection.supports_native_projection(work)
    }

    fn project(
        &self,
        invocation: &ToolInvocation,
        result: &ToolResult,
        work: crate::NativeWork<'_>,
    ) -> Result<(Value, crate::NativeCustody)> {
        let adapted = self.adapt(invocation, work)?;
        self.original
            .projection
            .project(&adapted.invocation, result, work)
    }
}

#[cfg(all(test, feature = "filesystem", not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    #[test]
    fn native_edit_errors_keep_original_class_and_diagnostic_custody() -> Result<()> {
        let adapter = Adapter {
            profile: NativeToolProfile::Google,
            capability: Capability::Edit,
            original: Arc::new(crate::tool::files::edit()?),
        };
        let invocation = ToolInvocation {
            operation_id: crate::OperationId::new(),
            call_id: "edit-error".into(),
            name: "replace".into(),
            arguments: serde_json::json!({"file_path":"日本語/🦀.txt"}),
        };
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(64 * 1024, 64 * 1024),
            &cancellation,
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let baseline = work.live_allocation_bytes();
        for source in ["missing", "one one one"] {
            let mut custody = crate::tool::edit::allocation(Some(&work), 0)?;
            let result = crate::tool::edit::replace(source, "one", "two", false, 64, &mut custody);
            let error = adapter
                .present_error(result, &invocation, Some(&work))
                .unwrap_err();
            assert_eq!(error.code(), crate::wire::ErrorCode::Conflict);
            assert!(error.is_tool_failure());
            assert!(!error.is_native_failure());
            assert!(!error.uncertain());
            assert!(error.to_string().contains("日本語/🦀.txt"));
            if source == "one one one" {
                assert!(error.to_string().contains("found 3"));
            }
            assert!(work.live_allocation_bytes() > baseline);
            drop(error);
            drop(custody);
            assert_eq!(work.live_allocation_bytes(), baseline);
        }

        // An unrelated producer with identical diagnostic text must retain
        // its original cause; adapters never classify errors by their text.
        let result: Result<()> = Err(Error::Conflict("exact edit text is missing".into()));
        assert!(
            matches!(adapter.present_error(result, &invocation, Some(&work)),
            Err(Error::Conflict(message)) if message == "exact edit text is missing")
        );

        let mut custody = None;
        let result = crate::tool::edit::replace("missing", "one", "two", false, 64, &mut custody);
        cancellation.cancel();
        let error = adapter
            .present_error(result, &invocation, Some(&work))
            .unwrap_err();
        assert!(error.is_native_failure());
        assert!(!error.is_tool_failure());
        drop(error);
        assert_eq!(work.live_allocation_bytes(), baseline);
        Ok(())
    }
}
