//! Profile functions composed through the original stock tool settlement path.

use super::*;
use crate::tool::{JsonToolProjection, SkillSource, Tool, ToolDefinition, ToolExecutor};
#[cfg(all(feature = "code-mode", not(target_arch = "wasm32")))]
use std::sync::Mutex;

/// Fixed profile binding for the in-process TypeScript tool.
pub const CODE_REVISION: &str = "quickjs-code-1";

#[derive(serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct CodeInput {
    code: String,
}

/// Creates the one fixed code declaration. Registration alone grants no access;
/// original scope, role, approval and shared-memory admission still apply.
pub fn code_tool() -> Result<Tool> {
    let definition = ToolDefinition {
        name: "code".into(),
        revision: CODE_REVISION.into(),
        description: "Run TypeScript in the shared JavaScript sandbox. Only the declared typed host functions can cause effects; each uses ordinary tool approval and replay. Date and Math.random replay deterministically.".into(),
        input_schema: crate::tool::schema::input::<CodeInput>()?,
        output_schema: serde_json::json!({}),
        projection_schema: crate::tool::json_projection_schema(serde_json::json!({})),
    };
    definition.validate(&mut None)?;
    Ok(Tool {
        skill_source: None,
        definition,
        hook_names: &[],
        executor: Arc::new(ProfileFunction),
        projection: Arc::new(JsonToolProjection),
    })
}

/// The generic skill declaration shares this same stock dispatch boundary.
pub fn call_tool() -> Result<Tool> {
    let mut definition = crate::tools::variants::call_definition()?;
    definition.projection_schema = crate::tool::json_projection_schema(serde_json::json!({}));
    Ok(Tool {
        skill_source: None,
        definition,
        hook_names: &[],
        executor: Arc::new(ProfileFunction),
        projection: Arc::new(JsonToolProjection),
    })
}

// This marker has no alternate execution implementation. Only the stock
// dispatcher, after its original authorization/policy/claim, handles it.
struct ProfileFunction;
impl ToolExecutor for ProfileFunction {
    fn authorize(
        &self,
        _: Option<&crate::runtime::RuntimeScope>,
        _: Option<&crate::runtime::ToolContext>,
        _: &ToolInvocation,
        work: crate::NativeWork<'_>,
    ) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = work {
            work.remaining()?;
        }
        let _ = work;
        Ok(())
    }

    fn execute<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<crate::runtime::ToolContext>,
    ) -> BoxFuture<'a, Result<ToolResult>> {
        Box::pin(async {
            Err(Error::tool_rejected(
                "profile function requires original stock dispatch",
            ))
        })
    }

    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<crate::runtime::ToolContext>,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async {
            Err(Error::tool_rejected(
                "profile function requires original stock reconciliation",
            ))
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn supports_native_execution(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        work.remaining()?;
        Ok(())
    }
}

pub(super) enum ProfileDispatch {
    Settled(ToolResult),
    Progress(ExecutionProgress<()>),
}

#[derive(serde::Serialize)]
struct HostMetadata<'a> {
    parent: OperationId,
    revision: &'a str,
    skill: Option<&'a SkillSource>,
}

impl StockExecution<'_> {
    /// The caller already authenticated and claimed the *complete* outer
    /// envelope. Resumption replays pure code and reconciles each original host
    /// call; it never invokes an executor behind policy or repeats an effect.
    pub(super) async fn profile_dispatch(
        &self,
        journal: &dyn ExecutionJournal,
        invocation: &ToolInvocation,
        tool: &Tool,
    ) -> Result<Option<ProfileDispatch>> {
        if invocation.name == "call" && tool.definition.revision == "skill-call-1" {
            let target = crate::tools::variants::CallInput::target(&invocation.arguments)?;
            let admitted = self
                .stock
                .tools
                .get(target.tool, None, self.io)?
                .ok_or_else(|| Error::tool_rejected("skill is not admitted"))?;
            let source = admitted.skill_source.as_ref().ok_or_else(|| {
                Error::tool_rejected("call target is not an admitted filesystem skill")
            })?;
            let readable = with_history_io(self.io, |work| {
                crate::runtime::read_granted_with_work(
                    self.stock.tool_scope.grants(),
                    source.file(),
                    work,
                )
            })?;
            if !readable {
                return Err(Error::tool_rejected(
                    "skill source is outside the original read grant",
                ));
            }
            let metadata = HostMetadata {
                parent: invocation.operation_id,
                revision: &admitted.definition.revision,
                skill: Some(source),
            };
            let (metadata, _metadata_credit) =
                crate::tool::value(&metadata, self.stock.limits.file_bytes, self.native_work())?;
            let nested = ToolInvocation::for_model_call(
                invocation.operation_id,
                0,
                stock_string_copy("skill", self.io)?,
                stock_string_copy(target.tool, self.io)?,
                crate::model::clone_value(target.args, self.io)?,
                self.native_work(),
            )?;
            return self
                .nested_dispatch(journal, nested, &metadata)
                .await
                .map(Some);
        }
        if invocation.name != "code" || tool.definition.revision != CODE_REVISION {
            return Ok(None);
        }
        #[cfg(all(feature = "code-mode", not(target_arch = "wasm32")))]
        {
            let runtime = self.stock.code_runtime.as_ref().ok_or_else(|| {
                Error::tool_rejected("code mode has no admitted shared memory reservation")
            })?;
            let work = self.native_work().ok_or_else(|| {
                Error::tool_rejected("code mode requires original accounted execution custody")
            })?;
            let source = invocation
                .arguments
                .as_object()
                .filter(|object| object.len() == 1)
                .and_then(|object| object.get("code"))
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    Error::tool_rejected("code requires exactly one TypeScript source field")
                })?;
            let mut tools = self
                .stock
                .tools
                .definitions_for_grants(self.stock.tool_scope.grants(), self.io)?;
            if let Some((profiles, pin, model)) = &self.stock.profile {
                let mut failure = None;
                tools.retain(|tool| {
                    match profiles.tool_available(pin, model, &tool.name, &tool.revision) {
                        Ok(available) => available,
                        Err(error) => {
                            failure = Some(error);
                            false
                        }
                    }
                });
                if let Some(error) = failure {
                    return Err(error);
                }
            }
            let progress = Mutex::new(None);
            let host = Arc::new(OriginalCodeHost {
                execution: self,
                journal,
                parent: invocation.operation_id,
                progress: &progress,
            });
            let result = runtime
                .execute(
                    invocation,
                    source,
                    &tools,
                    host,
                    Arc::new(NativeCodeControl { work: work.clone() }),
                )
                .await;
            if let Some(progress) = progress
                .into_inner()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
            {
                return Ok(Some(ProfileDispatch::Progress(progress)));
            }
            result.map(|value| Some(ProfileDispatch::Settled(ToolResult { value })))
        }
        #[cfg(not(all(feature = "code-mode", not(target_arch = "wasm32"))))]
        Err(Error::tool_rejected(
            "code mode is not enabled in this host",
        ))
    }

    async fn nested_dispatch(
        &self,
        journal: &dyn ExecutionJournal,
        invocation: ToolInvocation,
        metadata: &Value,
    ) -> Result<ProfileDispatch> {
        // One existing journal namespace per nested invocation prevents code's
        // internal calls from changing the model step's call count or history.
        // Parallel calls have independent tails, so an unrelated claim cannot
        // make another effect's original conditional claim fail.
        // Authority and native work remain the exact original objects.
        let parent = invocation.operation_id;
        let invocation = ToolInvocation::for_model_call(
            parent,
            0,
            invocation.call_id,
            invocation.name,
            invocation.arguments,
            self.native_work(),
        )?;
        let mut nested = StockExecution::ordinary(self.stock);
        nested.context = self.context.map(|(context, _)| (context, parent));
        nested.io = self.io;
        let call_id = stock_string_copy(&invocation.call_id, self.io)?;
        let mut hooks = Vec::new();
        let settled = Box::pin(
            nested.settle_tool_invocation(journal, parent, 0, invocation, metadata, &mut hooks),
        )
        .await?;
        match settled {
            ExecutionProgress::Ready(_) => {
                let (_, records) = nested.tool_records(journal, parent, 0, &call_id).await?;
                match crate::executor::records::RecordedCall::classify(&records).map_err(
                    |reason| stock_boundary(self.io, crate::wire::ErrorCode::Storage, reason),
                )? {
                    crate::executor::records::RecordedCall::Completed { result, .. } => {
                        let result = retain_preparation(
                            load_json::<ToolResult>(journal, result, self.io).await?,
                        )?;
                        Ok(ProfileDispatch::Settled(result))
                    }
                    crate::executor::records::RecordedCall::Failed { cause, .. } => {
                        if self.native_work().is_some() {
                            let code = match cause.class {
                                ToolErrorClass::NotFound => crate::wire::ErrorCode::NotFound,
                                ToolErrorClass::Conflict => crate::wire::ErrorCode::Conflict,
                                ToolErrorClass::Unsupported => crate::wire::ErrorCode::Unsupported,
                                ToolErrorClass::Unauthorized => {
                                    crate::wire::ErrorCode::Unauthorized
                                }
                                ToolErrorClass::Denied => crate::wire::ErrorCode::InteractionDenied,
                                ToolErrorClass::Invalid | ToolErrorClass::Failed => {
                                    crate::wire::ErrorCode::Invalid
                                }
                            };
                            return Err(Error::ToolRejected {
                                code,
                                reason: "nested host call failed; its original cause remains in the execution journal",
                            });
                        }
                        let message = stock_string_copy(&cause.message, self.io)?;
                        Err(match cause.class {
                            ToolErrorClass::NotFound => Error::NotFound(message),
                            ToolErrorClass::Conflict => Error::Conflict(message),
                            ToolErrorClass::Unsupported => Error::Unsupported(message),
                            ToolErrorClass::Unauthorized | ToolErrorClass::Denied => {
                                Error::Unauthorized(message)
                            }
                            ToolErrorClass::Invalid | ToolErrorClass::Failed => {
                                Error::Invalid(message)
                            }
                        })
                    }
                    _ => Err(Error::Indeterminate(parent)),
                }
            }
            ExecutionProgress::Pending(operation) => Ok(ProfileDispatch::Progress(
                ExecutionProgress::Pending(operation),
            )),
            ExecutionProgress::Failed(reason) => {
                Ok(ProfileDispatch::Progress(ExecutionProgress::Failed(reason)))
            }
            ExecutionProgress::PreDispatchFailed(failure) => Ok(ProfileDispatch::Progress(
                ExecutionProgress::PreDispatchFailed(failure),
            )),
        }
    }
}

#[cfg(all(feature = "code-mode", not(target_arch = "wasm32")))]
struct NativeCodeControl {
    work: acyclic_fs::NativeMaterializationWork,
}

#[cfg(all(feature = "code-mode", not(target_arch = "wasm32")))]
impl crate::code_mode::CodeControl for NativeCodeControl {
    fn check(&self) -> Result<()> {
        self.work.charge(acyclic_fs::WorkCounters {
            items_examined: 1,
            ..Default::default()
        })?;
        Ok(())
    }
    fn interrupted(&self) -> BoxFuture<'_, Error> {
        Box::pin(async move {
            match self.work.cancellation_wait() {
                Ok(cancelled) => {
                    cancelled.await;
                    Error::InteractionRejected(crate::InteractionRejection::Cancelled)
                }
                Err(error) => error.into(),
            }
        })
    }
}

#[cfg(all(feature = "code-mode", not(target_arch = "wasm32")))]
struct OriginalCodeHost<'a, 'b> {
    execution: &'a StockExecution<'b>,
    journal: &'a dyn ExecutionJournal,
    parent: OperationId,
    progress: &'a Mutex<Option<ExecutionProgress<()>>>,
}

#[cfg(all(feature = "code-mode", not(target_arch = "wasm32")))]
impl crate::code_mode::CodeHost for OriginalCodeHost<'_, '_> {
    fn invocation(
        &self,
        outer: &ToolInvocation,
        ordinal: u64,
        name: String,
        arguments: Value,
    ) -> Result<ToolInvocation> {
        let call_id = with_history_io(self.execution.io, |work| {
            preparation_format(format_args!("code-{ordinal}"), work)
        })?;
        ToolInvocation::for_model_call(
            outer.operation_id,
            0,
            call_id,
            name,
            arguments,
            self.execution.native_work(),
        )
    }

    fn dispatch(
        &self,
        invocation: ToolInvocation,
    ) -> BoxFuture<'_, Result<crate::code_mode::CodeHostResult>> {
        Box::pin(async move {
            let work = self
                .execution
                .native_work()
                .ok_or_else(|| Error::tool_rejected("code host lost original native custody"))?;
            let io = PreparationIo::from_native_work(work)?;
            let mut execution = StockExecution::ordinary(self.execution.stock);
            execution.context = self.execution.context;
            execution.io = Some(&io);
            let tool = execution
                .stock
                .tools
                .get(&invocation.name, None, execution.io)?
                .ok_or_else(|| Error::tool_rejected("code host tool is not admitted"))?;
            let metadata = HostMetadata {
                parent: self.parent,
                revision: &tool.definition.revision,
                skill: tool.skill_source.as_ref(),
            };
            let (metadata, _metadata_credit) =
                crate::tool::value(&metadata, execution.stock.limits.file_bytes, Some(work))?;
            let result = execution
                .nested_dispatch(self.journal, invocation, &metadata)
                .await?;
            drop(execution);
            match result {
                ProfileDispatch::Settled(result) => {
                    use crate::filesystem::UploadRecord as _;
                    let retained = (std::mem::size_of::<ToolResult>() as u64)
                        .checked_add(result.logical_owned_bytes()?)
                        .ok_or_else(|| {
                            Error::tool_rejected("code host result ownership overflow")
                        })?;
                    let credit = io.into_native_custody(retained)?;
                    Ok(crate::code_mode::CodeHostResult {
                        result,
                        credit: Some(Box::new(credit)),
                    })
                }
                ProfileDispatch::Progress(progress) => {
                    let mut saved = self
                        .progress
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if saved.is_none() {
                        *saved = Some(progress);
                    }
                    Err(Error::Indeterminate(self.parent))
                }
            }
        })
    }
}
