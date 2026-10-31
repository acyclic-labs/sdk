//! Profile functions composed through the original stock tool settlement path.

use super::*;
use crate::tool::{
    JsonToolProjection, SkillRuntime, SkillSource, Tool, ToolDefinition, ToolExecutor,
};
#[cfg(all(feature = "code-mode", not(target_arch = "wasm32")))]
use std::sync::Mutex;

/// Fixed profile binding for the in-process TypeScript tool.
pub const CODE_REVISION: &str = "quickjs-code-1";

#[derive(serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct CodeInput {
    code: String,
    #[serde(default)]
    input: Option<Value>,
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

impl Tool {
    /// The shared catalog's executable-skill admission. Discovery retains only
    /// the pinned metadata source; no script body is read here. The original
    /// registry remains the sole definition/schema validation boundary.
    pub(crate) fn filesystem_skill(
        definition: ToolDefinition,
        source: crate::context::PinnedContextPath,
        runtime: SkillRuntime,
        io: Option<&PreparationIo>,
    ) -> Result<Self> {
        let source = SkillSource::admitted(source, runtime, &definition, io)?;
        if let Some(io) = io {
            // These two concrete zero-sized implementations still own their
            // Arc headers. Admit them before allocation on the same payload
            // custody that the original registry consumes with this tool.
            let work = acyclic_fs::WorkCounters {
                allocation_operations: 2,
                ..Default::default()
            };
            io.preflight(work)?;
            io.retain_owned(filesystem_skill_marker_bytes())?;
            io.charge(work)?;
        }
        Ok(Self {
            skill_source: Some(source),
            definition,
            hook_names: &[],
            executor: Arc::new(ProfileFunction),
            projection: Arc::new(JsonToolProjection),
        })
    }
}

/// Retained concrete implementations owned by each admitted filesystem skill.
/// The trait-object pointers themselves are already counted in `Tool`.
pub(crate) const fn filesystem_skill_marker_bytes() -> u64 {
    (std::mem::size_of::<ProfileFunction>()
        + std::mem::size_of::<JsonToolProjection>()
        + 4 * std::mem::size_of::<usize>()) as u64
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
        context: Option<&crate::runtime::ToolContext>,
    ) -> Result<Option<ProfileDispatch>> {
        if invocation.name == "call" && tool.definition.revision == "skill-call-1" {
            let target = crate::tools::variants::CallInput::target(&invocation.arguments)?;
            let admitted = self
                .stock
                .tools
                .get_callable_skill(target.tool, self.io)?
                .ok_or_else(|| Error::tool_rejected("skill is not admitted"))?;
            let source = admitted.skill_source.as_ref().ok_or_else(|| {
                Error::tool_rejected("call target is not an admitted filesystem skill")
            })?;
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
                .nested_dispatch(journal, nested, &metadata, true)
                .await
                .map(Some);
        }
        if let Some(source) = &tool.skill_source {
            return self
                .dispatch_skill(journal, invocation, tool, source, context)
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
                .and_then(|object| object.get("code"))
                .and_then(Value::as_str)
                .ok_or_else(|| Error::tool_rejected("code requires a TypeScript source field"))?;
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
                .execute_with_input(
                    invocation,
                    source,
                    invocation.arguments.get("input").unwrap_or(&Value::Null),
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

    async fn dispatch_skill(
        &self,
        journal: &dyn ExecutionJournal,
        invocation: &ToolInvocation,
        tool: &Tool,
        source: &SkillSource,
        context: Option<&crate::runtime::ToolContext>,
    ) -> Result<ProfileDispatch> {
        source.verify(&tool.definition, self.io)?;
        #[cfg(not(target_arch = "wasm32"))]
        {
            // Refuse before loading a script if its original execution producer
            // is unavailable. No discovery path materializes a body to admit it.
            if matches!(source.runtime(), SkillRuntime::Code { .. }) {
                self.native_work().ok_or_else(|| {
                    Error::tool_rejected("skill code requires original accounted execution custody")
                })?;
                #[cfg(feature = "code-mode")]
                if self.stock.code_runtime.is_none() {
                    return Err(Error::ToolRejected {
                        code: crate::wire::ErrorCode::Unsupported,
                        reason: "skill code has no admitted shared memory reservation",
                    });
                }
                #[cfg(not(feature = "code-mode"))]
                return Err(Error::ToolRejected {
                    code: crate::wire::ErrorCode::Unsupported,
                    reason: "skill code is not enabled in this host",
                });
            }
            let pinned = source.source();
            let readable = with_history_io(self.io, |work| {
                crate::core::directory_read_granted_with_work(
                    self.stock.tool_scope.grants(),
                    &pinned.root.volume,
                    &pinned.root.directory,
                    work,
                )
            })?;
            if !readable {
                return Err(Error::tool_rejected(
                    "skill script is outside the original directory read grant",
                ));
            }
            let (task, _) = self.context.ok_or_else(|| {
                Error::tool_rejected("skill script requires its original task binding")
            })?;
            let binding = task
                .content_binding(Some(&pinned.root.volume))
                .filter(|binding| binding.volume == pinned.root.volume)
                .ok_or_else(|| {
                    Error::tool_rejected("skill script reader is not bound to its original volume")
                })?;
            let (file, bytes, _hold) = binding
                .reader
                .read_private_path(
                    &pinned.root.volume,
                    &pinned.root.directory,
                    source.runtime().script(),
                    Some(&pinned.generation),
                    self.stock.limits.file_bytes,
                    crate::conversation::ContentAccess {
                        ceiling: Some(self.stock.tool_scope.grants()),
                        io: self.io,
                    },
                )?
                .await?;
            with_history_io(self.io, |work| {
                self.stock.limits.validate_file(&file, work)?;
                if file.volume() != &pinned.root.volume || file.path() != source.runtime().script()
                {
                    return Err(stock_boundary(
                        self.io,
                        crate::wire::ErrorCode::Storage,
                        "skill reader returned another pinned script",
                    ));
                }
                file.descriptor().verify(&bytes, work)
            })?;
            let script = std::str::from_utf8(&bytes)
                .map_err(|_| Error::tool_rejected("skill script is not UTF-8"))?;
            #[derive(serde::Serialize)]
            struct Input<'a> {
                code: &'a str,
                input: &'a Value,
            }
            let (name, arguments, _input_credit) = match source.runtime() {
                SkillRuntime::Shell { .. } => {
                    let shell = self
                        .stock
                        .tools
                        .get("shell", None, self.io)?
                        .ok_or_else(|| {
                            Error::tool_rejected("skill shell target is not admitted")
                        })?;
                    // Pure target-specific framing; no executor effect occurs
                    // until the complete command enters original settlement.
                    let input = shell.executor.prepare_shell_script(
                        script,
                        &invocation.arguments,
                        context,
                        self.native_work(),
                    )?;
                    let (arguments, credit) = crate::tool::value(
                        &input,
                        self.stock.limits.file_bytes,
                        self.native_work(),
                    )?;
                    ("shell", arguments, credit)
                }
                SkillRuntime::Code { .. } => {
                    let (arguments, credit) = crate::tool::value(
                        &Input {
                            code: script,
                            input: &invocation.arguments,
                        },
                        self.stock.limits.file_bytes,
                        self.native_work(),
                    )?;
                    ("code", arguments, credit)
                }
            };
            let nested = ToolInvocation::for_model_call(
                invocation.operation_id,
                0,
                stock_string_copy("skill-runtime", self.io)?,
                stock_string_copy(name, self.io)?,
                arguments,
                self.native_work(),
            )?;
            let (metadata, _metadata_credit) = crate::tool::value(
                &HostMetadata {
                    parent: invocation.operation_id,
                    revision: &tool.definition.revision,
                    skill: Some(source),
                },
                self.stock.limits.file_bytes,
                self.native_work(),
            )?;
            self.nested_dispatch(journal, nested, &metadata, false)
                .await
        }
        #[cfg(target_arch = "wasm32")]
        {
            let _ = (journal, invocation, context);
            Err(Error::ToolRejected {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "skill runtime has no admitted target execution port",
            })
        }
    }

    async fn nested_dispatch(
        &self,
        journal: &dyn ExecutionJournal,
        invocation: ToolInvocation,
        metadata: &Value,
        callable_skill: bool,
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
        let settled = Box::pin(nested.settle_tool_invocation(
            journal,
            parent,
            0,
            invocation,
            if callable_skill {
                super::tool_execution::InvocationSelection::CallableSkill(metadata)
            } else {
                super::tool_execution::InvocationSelection::Declared(metadata)
            },
            &mut hooks,
        ))
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
                .nested_dispatch(self.journal, invocation, &metadata, false)
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
