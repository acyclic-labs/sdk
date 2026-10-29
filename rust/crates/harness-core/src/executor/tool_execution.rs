use super::records::RecordedCall;
use super::*;
use crate::wire::ErrorCode::{Conflict, Invalid};

mod dispatch;

/// A recoverable model tool call refusal, recorded without dispatch as its model-visible
/// result. Approval rejections use the interaction's terminal disposition instead.
enum ToolCallRejection {
    /// The composition binds no granted tool by this name.
    Unavailable,
    /// The arguments violate the tool's pinned input schema.
    InvalidInput(Error),
    /// The bound policy denied the call; the model can adapt.
    Refused(Error),
}

impl ToolCallRejection {
    fn into_failure(self, io: Option<&PreparationIo>) -> (ToolFailureKind, Error) {
        match self {
            Self::Unavailable => (
                ToolFailureKind::Unavailable,
                stock_boundary(
                    io,
                    crate::wire::ErrorCode::NotFound,
                    "tool is not available",
                ),
            ),
            Self::InvalidInput(violation) => (ToolFailureKind::InvalidInput, violation),
            Self::Refused(denial) => (ToolFailureKind::Refused, denial),
        }
    }
}

/// Why the bound policy stopped a call before dispatch.
pub(in crate::executor) enum PolicyStop {
    /// Approval is pending or was rejected; the turn's progress reports it.
    Progress(ExecutionProgress<()>),
    /// The policy denied the call; the model receives the denial as the call's result.
    Refused(Error),
}

/// A recorded tool failure's model-visible content, received instead of a result.
type ToolSettlement<T> = std::result::Result<T, crate::model::ToolResultContent>;

/// A settled call can be shown to the model; an interrupted effect ends the turn.
enum ToolInvocationOutcome {
    Settled(crate::model::ToolResultContent),
    Interrupted,
}

impl StockExecution<'_> {
    pub(super) async fn settle_tool_call(
        &self,
        journal: &dyn ExecutionJournal,
        operation_id: OperationId,
        step: u32,
        invocation: ToolInvocation,
        invocation_index: usize,
        prior_messages: &mut Vec<ModelMessage>,
        _hook_context: &mut Vec<ModelMessage>,
    ) -> Result<ExecutionProgress<()>> {
        self.verify_task_context_binding(operation_id)?;
        let metadata = metadata_for_call(
            prior_messages,
            invocation_index,
            &invocation.call_id,
            self.io,
        )?;
        let invocation_limits = crate::conversation::Limits {
            file_bytes: self
                .stock
                .limits
                .file_bytes
                .min(self.stock.tool_scope.limits().file_bytes),
            render_bytes: self
                .stock
                .limits
                .render_bytes
                .min(self.stock.tool_scope.limits().render_bytes),
            ..self.stock.limits
        };
        invocation.validate_recorded(metadata, invocation_limits, self.io)?;
        let admitted = match self.stock.tools.get(&invocation.name, None, self.io)? {
            Some(tool) => self
                .authorize_tool_invocation(tool, &invocation)?
                .map(|(context, capability)| (tool, context, capability)),
            None => Err(ToolCallRejection::Unavailable),
        };
        let (tool, mut context, _capability) = match admitted {
            Ok(admitted) => admitted,
            Err(rejection) => {
                let content = self
                    .settle_tool_refusal(journal, operation_id, step, &invocation, rejection)
                    .await?;
                return self.push_tool_result(&invocation, content, prior_messages);
            }
        };
        if let Some(context) = &mut context {
            context.model_turn = (operation_id, step);
        }
        let (_, records) = self
            .tool_records(journal, operation_id, step, &invocation.call_id)
            .await?;
        let recorded = RecordedCall::classify(&records)
            .map_err(|reason| stock_boundary(self.io, Invalid, reason))?;
        #[cfg(all(feature = "hooks", feature = "filesystem", not(target_arch = "wasm32")))]
        let (invocation, mut pre_hook) = {
            let mut output = self
                .tool_hook(
                    operation_id,
                    crate::hooks::HookSite::BeforeTool,
                    &invocation,
                    None,
                    recorded.started().is_some(),
                )
                .await?;
            let mut effective = invocation;
            if !output.blocks()
                && let Some(arguments) = output.updated_input.take()
            {
                effective.arguments = arguments;
                effective.validate_recorded(metadata, invocation_limits, self.io)?;
                match self.authorize_tool_invocation(tool, &effective)? {
                    Ok((authorized, _)) => context = authorized,
                    Err(rejection) => {
                        let content = self
                            .settle_tool_refusal(journal, operation_id, step, &effective, rejection)
                            .await?;
                        self.append_hook_context(_hook_context, output)?;
                        return self.push_tool_result(&effective, content, prior_messages);
                    }
                }
                if let Some(context) = &mut context {
                    context.model_turn = (operation_id, step);
                }
            }
            (effective, output)
        };
        if let Some(reference) = recorded.started() {
            let recorded =
                load_json::<crate::tool::RecordedInvocation>(journal, reference, self.io).await?;
            if recorded.0.invocation != invocation || &recorded.0.metadata != metadata {
                return Err(stock_boundary(
                    self.io,
                    Conflict,
                    "tool call identity is bound to another invocation",
                ));
            }
        }
        #[cfg(all(feature = "hooks", feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(reason) = pre_hook.take_block_reason() {
            let content = self
                .settle_tool_refusal(
                    journal,
                    operation_id,
                    step,
                    &invocation,
                    ToolCallRejection::Refused(Error::Unauthorized(reason)),
                )
                .await?;
            self.append_hook_context(_hook_context, pre_hook)?;
            return self.push_tool_result(&invocation, content, prior_messages);
        }
        #[cfg(all(feature = "hooks", feature = "filesystem", not(target_arch = "wasm32")))]
        let hook_approval = pre_hook.approval.take();
        #[cfg(all(feature = "hooks", feature = "filesystem", not(target_arch = "wasm32")))]
        self.append_hook_context(_hook_context, pre_hook)?;
        // One match over every record state: a settlement replays whether or not it has a
        // start, so the turn's scan never stops at a settled call.
        let content = match recorded {
            RecordedCall::Failed {
                reason: ToolFailureKind::Interrupted,
                ..
            } => {
                return Ok(ExecutionProgress::Failed(ToolFailureKind::Interrupted));
            }
            RecordedCall::Failed { reason, cause, .. } => {
                tool_failure_content(reason, cause, self.io)?
            }
            RecordedCall::Completed {
                result, projection, ..
            } => {
                self.replay_tool_output(
                    journal,
                    operation_id,
                    &invocation,
                    result,
                    projection,
                    tool,
                    _hook_context,
                )
                .await?
            }
            // Calls run in order, so nothing after an unstarted call has started either.
            RecordedCall::Unstarted if self.stock.reconciliation_only => {
                return Err(Error::Indeterminate(operation_id));
            }
            pending @ (RecordedCall::Started(_) | RecordedCall::Unstarted) => {
                let started = matches!(pending, RecordedCall::Started(_));
                #[cfg(all(feature = "hooks", feature = "filesystem", not(target_arch = "wasm32")))]
                if let Some(prompt) = hook_approval
                    && let Some(stop) = self
                        .hook_approval(journal, operation_id, &invocation, tool, prompt, started)
                        .await?
                {
                    return Ok(stop);
                }
                match self
                    .require_tool_policy(journal, operation_id, &invocation, tool, started)
                    .await?
                {
                    Some(PolicyStop::Progress(progress)) => return Ok(progress),
                    Some(PolicyStop::Refused(denial)) => {
                        let content = self
                            .settle_tool_refusal(
                                journal,
                                operation_id,
                                step,
                                &invocation,
                                ToolCallRejection::Refused(denial),
                            )
                            .await?;
                        return self.push_tool_result(&invocation, content, prior_messages);
                    }
                    None => {}
                }
                match self
                    .execute_tool_invocation(
                        journal,
                        (operation_id, step),
                        crate::tool::RecordedInvocation {
                            invocation: &invocation,
                            metadata,
                        },
                        tool,
                        &mut context,
                        started,
                        _hook_context,
                    )
                    .await?
                {
                    ToolInvocationOutcome::Settled(content) => content,
                    ToolInvocationOutcome::Interrupted => {
                        return Ok(ExecutionProgress::Failed(ToolFailureKind::Interrupted));
                    }
                }
            }
        };
        self.push_tool_result(&invocation, content, prior_messages)
    }

    fn push_tool_result(
        &self,
        invocation: &ToolInvocation,
        content: crate::model::ToolResultContent,
        prior_messages: &mut Vec<ModelMessage>,
    ) -> Result<ExecutionProgress<()>> {
        let message = ModelMessage {
            role: ModelRole::Tool,
            content: ModelContent::Part(ModelContentPart::ToolResult {
                call_id: stock_string_copy(&invocation.call_id, self.io)?,
                name: stock_string_copy(&invocation.name, self.io)?,
                content,
            }),
        };
        with_history_io(self.io, |work| {
            message.content.validate_limits(self.stock.limits, work)
        })?;
        stock_vec_push(prior_messages, message, self.io)?;
        Ok(ExecutionProgress::Ready(()))
    }
    /// Records a refusal as the call's settlement, or replays the one recorded; the model
    /// receives the same text either way, and canonical publication reads it from the record.
    async fn settle_tool_refusal(
        &self,
        journal: &dyn ExecutionJournal,
        operation_id: OperationId,
        step: u32,
        invocation: &ToolInvocation,
        rejection: ToolCallRejection,
    ) -> Result<crate::model::ToolResultContent> {
        let (reason, error) = rejection.into_failure(self.io);
        let (_, records) = self
            .tool_records(journal, operation_id, step, &invocation.call_id)
            .await?;
        match RecordedCall::classify(&records)
            .map_err(|reason| stock_boundary(self.io, Invalid, reason))?
        {
            RecordedCall::Failed {
                started: None,
                reason: recorded,
                cause,
            } if recorded == reason => return tool_failure_content(reason, cause, self.io),
            RecordedCall::Unstarted if self.stock.reconciliation_only => {
                return Err(Error::Indeterminate(operation_id));
            }
            RecordedCall::Unstarted => {}
            // A refusal never settles a dispatched call or replaces another settlement.
            RecordedCall::Failed { .. }
            | RecordedCall::Completed { .. }
            | RecordedCall::Started(_) => {
                return Err(stock_boundary(
                    self.io,
                    Conflict,
                    "tool refusal changed on replay",
                ));
            }
        }
        let cause = with_history_io(self.io, |work| {
            ToolFailureCause::refusal(&invocation.name, reason, &error, None, work)
        })?;
        self.settle_tool_cause(
            journal,
            operation_id,
            step,
            &invocation.call_id,
            reason,
            cause,
        )
        .await
    }

    fn authorize_tool_invocation(
        &self,
        tool: &crate::tool::Tool,
        invocation: &ToolInvocation,
    ) -> Result<std::result::Result<(Option<crate::runtime::ToolContext>, String), ToolCallRejection>>
    {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = self.native_work() {
            tool.executor.supports_native_execution(work)?;
            tool.projection.supports_native_projection(Some(work))?;
        }
        let capability = with_history_io(self.io, |work| {
            preparation_format(format_args!("tool:call:{}", tool.definition.name), work)
        })?;
        if !self.stock.tool_scope.grants().contains(&capability) {
            return Ok(Err(ToolCallRejection::Unavailable));
        }
        // Shape precedes resource authorization, so a tool authorizes only well-formed calls.
        if let Err(violation) = with_history_io(self.io, |work| {
            check_json_schema_value(
                &tool.definition.input_schema,
                &invocation.arguments,
                "tool input",
                work,
            )
        })? {
            return Ok(Err(ToolCallRejection::InvalidInput(violation)));
        }
        let context = self.invocation_context(invocation)?;
        match stock_authorize_tool(
            tool,
            context.as_ref(),
            &self.stock.tool_scope,
            invocation,
            self.io,
        ) {
            Ok(()) => Ok(Ok((context, capability))),
            Err(violation @ Error::ToolRejected { .. }) => {
                Ok(Err(ToolCallRejection::InvalidInput(violation)))
            }
            Err(error) => Err(error),
        }
    }

    pub(super) async fn require_tool_policy(
        &self,
        journal: &dyn ExecutionJournal,
        operation_id: OperationId,
        invocation: &ToolInvocation,
        tool: &crate::tool::Tool,
        started: bool,
    ) -> Result<Option<PolicyStop>> {
        // Every call, ordinary or native, passes the one pinned policy. A
        // call reaching here without one has no tool authority, so it is
        // refused: there is no ungated state.
        let scope = &self.stock.tool_scope;
        let Some(policy) = &self.stock.policy else {
            let denial = Error::Unauthorized("no approval policy admits this tool call".into());
            return if started {
                Err(denial)
            } else {
                Ok(Some(PolicyStop::Refused(denial)))
            };
        };
        let identity_matches = || -> Result<bool> {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if let Some(work) = self.native_work() {
                let (identity, custody) = policy.identity_with_native_work(work)?;
                let matches = self.stock.policy_identity.as_ref() == Some(&identity);
                drop(identity);
                drop(custody);
                return Ok(matches);
            }
            Ok(self.stock.policy_identity.as_ref() == Some(&policy.identity()))
        };
        if !identity_matches()? {
            return Err(Error::Conflict("stock policy changed after binding".into()));
        }
        // Evaluation, the prompt and the approval request are charged to the
        // call's original ledger; the prompt's custody is held while it settles.
        let (decision, custody) = policy.evaluate(invocation, scope, self.io)?.await?;
        if !identity_matches()? {
            return Err(Error::Conflict(
                "stock policy changed during evaluation".into(),
            ));
        }
        match decision {
            ToolPolicyDecision::Allow => {}
            // A denial before dispatch is the call's result; the pinned policy
            // re-derives it on replay. After a recorded start it is a conflict.
            ToolPolicyDecision::Deny { reason } if !started => {
                return Ok(Some(PolicyStop::Refused(Error::Unauthorized(reason))));
            }
            ToolPolicyDecision::Deny { reason } => {
                return Err(Error::Unauthorized(reason));
            }
            ToolPolicyDecision::RequireApproval { prompt } => {
                if !scope.grants().contains(capability::INTERACTION_ROUTE) {
                    return Err(Error::Unauthorized(
                        "stock tool approval requires interaction:route".into(),
                    ));
                }
                let task = TaskId::from_bytes(operation_id.into_bytes());
                let ((approval_operation, request), _hold) = tool_approval_request(
                    task,
                    self.stock.policy_identity.as_ref().ok_or_else(|| {
                        Error::Conflict("stock policy identity is missing".into())
                    })?,
                    &tool.definition,
                    invocation,
                    (prompt, custody),
                    self.native_work(),
                    crate::interaction::ApprovalRoute::Policy,
                )?;
                if let Some(progress) = self
                    .settle_approval(
                        journal,
                        operation_id,
                        approval_operation,
                        &request,
                        invocation,
                        started,
                    )
                    .await?
                {
                    return Ok(Some(PolicyStop::Progress(progress)));
                }
            }
        }
        Ok(None)
    }
}

/// The model-visible result of a recorded tool failure; replay derives the same content.
pub(crate) fn tool_failure_content(
    reason: ToolFailureKind,
    cause: &ToolFailureCause,
    io: Option<&PreparationIo>,
) -> Result<crate::model::ToolResultContent> {
    Ok(crate::model::ToolResultContent::Error {
        message: with_history_io(io, |work| {
            preparation_format(format_args!("{}", ToolFailureText(reason, cause)), work)
        })?,
    })
}

pub(super) fn metadata_for_call<'a>(
    messages: &'a [ModelMessage],
    index: usize,
    call_id: &str,
    io: Option<&PreparationIo>,
) -> Result<&'a Value> {
    crate::model::charge_items(io, 1)?;
    let Some(ModelMessage {
        role: ModelRole::Assistant,
        content: ModelContent::Part(ModelContentPart::ToolCall(call)),
    }) = messages.get(index)
    else {
        return Err(stock_boundary(
            io,
            Invalid,
            "tool invocation has no indexed model call",
        ));
    };
    if call.call_id != call_id {
        return Err(stock_boundary(
            io,
            Invalid,
            "tool invocation index refers to another call",
        ));
    }
    Ok(&call.metadata)
}

// This scope retires the invocation future before the caller publishes either
// interruption or a terminal turn. It covers ordinary and native dispatch.
async fn cancel_safe_call<T>(
    execution: impl std::future::Future<Output = T>,
    cancellation: impl std::future::Future<Output = Result<()>>,
) -> Result<Option<T>> {
    futures::pin_mut!(execution, cancellation);
    match futures::future::select(cancellation, execution).await {
        futures::future::Either::Left((cancelled, _)) => {
            cancelled?;
            Ok(None)
        }
        futures::future::Either::Right((result, _)) => Ok(Some(result)),
    }
}

#[cfg(test)]
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod refusal_admission_tests {
    use super::*;

    #[test]
    fn native_unavailable_cause_allocates_nothing_before_ledger_admission() -> Result<()> {
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(65_536, 64 * 1024 * 1024),
            &acyclic_fs::CancellationToken::new(),
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let io = PreparationIo::from_native_work(&work)?;
        let before = work.live_allocation_bytes();
        let mut refusal = None;
        let allocated = acyclic_test_support::measure(|| {
            refusal = Some(ToolCallRejection::Unavailable.into_failure(Some(&io)));
        });
        let (kind, error) = refusal.unwrap();
        assert_eq!(kind, ToolFailureKind::Unavailable);
        assert_eq!(error.code(), crate::wire::ErrorCode::NotFound);
        assert_eq!(allocated.count_total, 0);
        assert_eq!(work.live_allocation_bytes(), before);
        // Negative control: the former portable error allocated without any ledger claim.
        let portable = acyclic_test_support::measure(|| {
            std::hint::black_box(Error::NotFound("tool is not available".into()));
        });
        assert!(portable.count_total > 0);
        assert_eq!(work.live_allocation_bytes(), before);
        Ok(())
    }
}

#[cfg(test)]
mod cancellation_tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    struct Held(Arc<AtomicBool>);
    impl std::future::Future for Held {
        type Output = u8;
        fn poll(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<u8> {
            std::task::Poll::Pending
        }
    }
    impl Drop for Held {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    #[test]
    fn cancellation_retires_owned_io_before_the_caller_can_settle() {
        for failed_follow in [false, true] {
            let retired = Arc::new(AtomicBool::new(false));
            let cancellation = if failed_follow {
                Err(Error::Storage("follow ended".into()))
            } else {
                Ok(())
            };
            let result: Result<Option<u8>> = futures::executor::block_on(async {
                match cancel_safe_call(Held(retired.clone()), futures::future::ready(cancellation))
                    .await
                {
                    Ok(None) => {
                        // Match the dispatcher's await scrutinee: retirement
                        // must precede settlement inside this very match arm,
                        // while the outer caller future is still being polled.
                        assert!(retired.load(Ordering::SeqCst));
                        Ok(None)
                    }
                    Err(error) => {
                        assert!(retired.load(Ordering::SeqCst));
                        Err(error)
                    }
                    Ok(Some(_)) => panic!("held invocation unexpectedly completed"),
                }
            });
            if failed_follow {
                assert!(matches!(result, Err(Error::Storage(_))));
            } else {
                assert!(matches!(result, Ok(None)));
            }
        }
        assert!(matches!(
            futures::executor::block_on(cancel_safe_call(
                futures::future::ready(7),
                futures::future::pending()
            )),
            Ok(Some(7))
        ));
    }
}
