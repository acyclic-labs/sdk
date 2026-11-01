//! QuickJS is a caller of stock settlement, with no host resources of its own.
use super::*;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

mod allocator;
mod interpreter;

struct Call {
    name: allocator::BridgeText,
    arguments: allocator::BridgeText,
    reply: oneshot::Sender<acyclic_stream::OwnedBytes>,
}

struct Stop(CancellationToken);
impl Drop for Stop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

#[derive(Serialize)]
struct CallLog<'a> {
    kind: &'static str,
    call_id: &'a str,
    ordinal: u64,
    tool: &'a str,
    ok: bool,
    input_bytes: usize,
    output_bytes: u64,
}

type CallResult<'a> =
    std::result::Result<(Value, Option<crate::conversation::ContentHold<'a>>), String>;

impl StockExecution<'_> {
    pub(super) fn code_tools_notice(
        &self,
        context: &mut crate::context::Context,
        tools: &[crate::tool::ToolDefinition],
    ) -> Result<()> {
        if !tools.iter().any(|tool| tool.name == "code") {
            return Ok(());
        }
        #[derive(Serialize)]
        struct Body<'a> {
            tools: &'a [crate::tool::ToolDefinition],
        }
        #[derive(Serialize)]
        struct Notice<'a> {
            source: &'static str,
            kind: &'static str,
            body: Body<'a>,
            wake: bool,
            coalesce_key: Option<&'static str>,
        }
        #[derive(Serialize)]
        struct Content<'a> {
            kind: &'static str,
            notices: [Notice<'a>; 1],
        }
        #[derive(Serialize)]
        struct Message<'a> {
            role: ModelRole,
            content: Content<'a>,
        }
        let notice = Message {
            role: ModelRole::User,
            content: Content {
                kind: "notices",
                notices: [Notice {
                    source: "tools",
                    kind: "tools.changed",
                    body: Body { tools },
                    wake: false,
                    coalesce_key: None,
                }],
            },
        };
        #[cfg(feature = "filesystem")]
        let funded = self
            .io
            .map(|io| io.encode(&notice, u64::MAX, false))
            .transpose()?;
        #[cfg(not(feature = "filesystem"))]
        let funded: Option<acyclic_stream::OwnedBytes> = None;
        #[cfg(feature = "filesystem")]
        let _bytes_hold;
        let bytes = match funded {
            Some(bytes) => bytes,
            None => serde_json::to_vec(&notice).map(Into::into).map_err(|_| {
                stock_boundary(
                    self.io,
                    crate::wire::ErrorCode::Invalid,
                    "tool notice encoding failed",
                )
            })?,
        };
        #[cfg(feature = "filesystem")]
        {
            _bytes_hold = self.io.map(|io| io.guard_bytes(&bytes)).transpose()?;
        }
        #[cfg(feature = "filesystem")]
        let decoded = self
            .io
            .map(|io| {
                io.decode(
                    std::marker::PhantomData::<ModelMessage>,
                    &bytes,
                    crate::wire::ErrorCode::Invalid,
                )
            })
            .transpose()?;
        #[cfg(not(feature = "filesystem"))]
        let decoded: Option<(ModelMessage, crate::conversation::ContentHold<'_>)> = None;
        let decoded = match decoded {
            Some((message, hold)) => (message, Some(hold)),
            None => (
                serde_json::from_slice(&bytes).map_err(|_| {
                    stock_boundary(
                        self.io,
                        crate::wire::ErrorCode::Invalid,
                        "tool notice decoding failed",
                    )
                })?,
                None,
            ),
        };
        // The active input cursor is already authenticated. Insert immediately
        // after it, keeping the same prefix when later model steps add results.
        let index = context
            .current_input_index
            .map_or(context.messages.len(), |index| index as usize + 1);
        if context.messages.get(index) == Some(&decoded.0) {
            return Ok(());
        }
        let message = retain_preparation(decoded)?;
        crate::context::ContextInsertion {
            local_index: index,
            message,
        }
        .insert(context, 0, self.io)
    }

    pub(super) fn restore_js_context(
        &self,
        value: &Value,
        messages: &mut Vec<ModelMessage>,
    ) -> Result<()> {
        crate::tools::code_mode::output(value)?;
        let context = value
            .get("context")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                stock_boundary(
                    self.io,
                    crate::wire::ErrorCode::Invalid,
                    "JavaScript receipt has no hook context",
                )
            })?;
        for text in context {
            let text = text.as_str().ok_or_else(|| {
                stock_boundary(
                    self.io,
                    crate::wire::ErrorCode::Invalid,
                    "JavaScript hook context is not text",
                )
            })?;
            stock_vec_push(
                messages,
                ModelMessage {
                    role: ModelRole::System,
                    content: ModelContent::Text(stock_string_copy(text, self.io)?),
                },
                self.io,
            )?;
        }
        Ok(())
    }

    pub(super) fn code<'a>(
        &'a self,
        journal: &'a dyn ExecutionJournal,
        identity: (OperationId, u32),
        parent: &'a ToolInvocation,
        hook_context: &'a mut Vec<ModelMessage>,
    ) -> BoxFuture<'a, Result<ExecutionProgress<ToolResult>>> {
        Box::pin(async move {
            let code = parent
                .arguments
                .get("code")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    stock_boundary(
                        self.io,
                        crate::wire::ErrorCode::Invalid,
                        "code requires a JavaScript source string",
                    )
                })?;
            let definitions =
                stock_tool_definitions(&self.stock.tools, &self.stock.tool_scope, self.io)?;
            #[cfg(feature = "filesystem")]
            let funded = self
                .io
                .map(|io| io.encode(&definitions, u64::MAX, false))
                .transpose()?;
            #[cfg(not(feature = "filesystem"))]
            let funded: Option<acyclic_stream::OwnedBytes> = None;
            let definitions = match funded {
                Some(bytes) => bytes,
                None => serde_json::to_vec(&definitions)
                    .map(Into::into)
                    .map_err(|_| {
                        stock_boundary(
                            self.io,
                            crate::wire::ErrorCode::Invalid,
                            "JavaScript tool schema encoding failed",
                        )
                    })?,
            };
            let maximum = usize::try_from(self.stock.limits.render_bytes)
                .unwrap_or(usize::MAX)
                .saturating_sub(crate::tool::schema::json_envelope_bytes()?);
            let token = CancellationToken::new();
            let stop = Stop(token.clone());
            // Bounded credit: one call is handed to the sole stock dispatcher at a
            // time. Promise.all remains usable without an unbounded host work queue.
            let (send, mut receive) = mpsc::channel(1);
            let allocator = allocator::EngineAllocator::new(self.native_work());
            #[cfg(feature = "filesystem")]
            let engine_failure = allocator.failure();
            let code = stock_string_copy(code, self.io)?;
            let mut worker = Box::pin(acyclic_native_runtime::blocking_future(interpreter::run(
                code,
                definitions,
                send,
                token,
                maximum,
                allocator,
                interpreter::INTERRUPT_BUDGET,
            )));
            let mut ordinal = 0_u64;
            let mut inner_context = Vec::new();
            let program = loop {
                tokio::select! {
                    biased;
                    cancelled = journal.model_cancellation() => {
                        stop.0.cancel();
                        let _ = worker.await;
                        cancelled?;
                        return Err(Error::InteractionRejected(crate::InteractionRejection::Cancelled));
                    }
                    result = &mut worker => break result.map_err(|_| stock_boundary(self.io, crate::wire::ErrorCode::Invalid, "JavaScript worker failed"))?,
                    Some(call) = receive.recv() => {
                        ordinal = ordinal.checked_add(1).ok_or_else(|| stock_boundary(self.io, crate::wire::ErrorCode::Invalid, "JavaScript call identity overflow"))?;
                        let input_bytes = call.arguments.value.len();
                        let result = self.js_call(journal, identity, parent, ordinal, &call, &mut inner_context).await?;
                        let result = match result {
                            ExecutionProgress::Ready(result) => result,
                            ExecutionProgress::Pending(operation) => {
                                stop.0.cancel();
                                drop(call);
                                let _ = worker.await;
                                return Ok(ExecutionProgress::Pending(operation));
                            }
                            ExecutionProgress::Failed(reason) => return Ok(ExecutionProgress::Failed(reason)),
                            ExecutionProgress::PreDispatchFailed(failure) => return Ok(ExecutionProgress::PreDispatchFailed(failure)),
                        };
                        let ok = result.is_ok();
                        let (value, _result_hold) = match result { Ok(result) => result, Err(message) => (Value::String(message), None) };
                        let output_bytes = with_history_io(self.io, |work| crate::executor::json::json_byte_length(&value, u64::MAX, work))?;
                        let entry = CallLog { kind: "code.call", call_id: &parent.call_id, ordinal, tool: &call.name.value, ok, input_bytes, output_bytes };
                        let key = with_history_io(self.io, |work| preparation_format(format_args!("tool:{}:code:{ordinal}:result", parent.operation_id), work))?;
                        #[cfg(feature = "filesystem")]
                        let _key_hold = self.io.map(|io| io.guard_owned(key.capacity() as u64)).transpose()?;
                        // The ordinary journal publishes one immutable tool-output
                        // file per settled call. Following that directory streams
                        // logs without retaining call history in the caller or VM.
                        tokio::select! {
                            biased;
                            cancelled = journal.model_cancellation() => {
                                stop.0.cancel();
                                let _ = worker.await;
                                cancelled?;
                                return Err(Error::InteractionRejected(crate::InteractionRejection::Cancelled));
                            }
                            result = stage_json(journal, identity.0, &key, &entry, self.io) => { result?; }
                        }
                        #[derive(Serialize)]
                        struct Reply<'a> { ok: bool, value: &'a Value }
                        let reply = Reply { ok, value: &value };
                        #[cfg(feature = "filesystem")]
                        let funded = self.io.map(|io| io.encode(&reply, u64::MAX, false)).transpose()?;
                        #[cfg(not(feature = "filesystem"))]
                        let funded: Option<acyclic_stream::OwnedBytes> = None;
                        let reply = match funded {
                            Some(bytes) => bytes,
                            None => serde_json::to_vec(&reply)
                                .map(Into::into)
                                .map_err(|_| stock_boundary(self.io, crate::wire::ErrorCode::Invalid, "JavaScript response encoding failed"))?,
                        };
                        let _ = call.reply.send(reply);
                    }
                }
            };
            #[cfg(feature = "filesystem")]
            if let Some(error) = engine_failure
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
            {
                return Err(error.into());
            }
            let value = match program {
                Ok(value) => {
                    serde_json::json!({"value": value})
                }
                Err(mut error) => {
                    let mut value = serde_json::json!({"error": ""});
                    // Reserve the exact wrapper before fitting escaped text.
                    let overhead = with_history_io(self.io, |work| {
                        crate::executor::json::json_byte_length(&value, maximum as u64, work)
                    })?;
                    let budget =
                        maximum.saturating_sub(usize::try_from(overhead).unwrap_or(usize::MAX));
                    error.truncate(crate::tools::json_string_prefix(&error, budget).len());
                    value["error"] = Value::String(error);
                    value
                }
            };
            // Only the projected result consumes render capacity. The private
            // receipt wrapper is never part of the model's output.
            stock_json_bound(
                value.get("value").unwrap_or(&value),
                maximum as u64,
                self.io,
            )?;
            let mut context = Vec::new();
            for message in stock_into_iter(inner_context, self.io)? {
                let ModelContent::Text(text) = message.content else {
                    return Err(stock_boundary(
                        self.io,
                        crate::wire::ErrorCode::Invalid,
                        "tool hook context is not text",
                    ));
                };
                stock_vec_push(
                    hook_context,
                    ModelMessage {
                        role: ModelRole::System,
                        content: ModelContent::Text(stock_string_copy(&text, self.io)?),
                    },
                    self.io,
                )?;
                stock_vec_push(&mut context, Value::String(text), self.io)?;
            }
            Ok(ExecutionProgress::Ready(ToolResult {
                value: serde_json::json!({"version":2,"output":value,"context":context}),
            }))
        })
    }

    async fn js_call<'a>(
        &'a self,
        journal: &dyn ExecutionJournal,
        (operation, step): (OperationId, u32),
        parent: &ToolInvocation,
        ordinal: u64,
        call: &Call,
        hook_context: &mut Vec<ModelMessage>,
    ) -> Result<ExecutionProgress<CallResult<'a>>> {
        // A separate hash domain keeps inner identities disjoint from provider
        // call IDs. Replaying the script reuses the exact ordinal and receipt.
        let mut hash = blake3::Hasher::new();
        hash.update(b"harness:js-call:v1");
        hash.update(&parent.operation_id.into_bytes());
        hash.update(&ordinal.to_be_bytes());
        let call_id = hash.finalize().to_hex().to_string();
        #[cfg(feature = "filesystem")]
        let decoded = self
            .io
            .map(|io| {
                io.decode(
                    std::marker::PhantomData::<Value>,
                    call.arguments.value.as_bytes(),
                    crate::wire::ErrorCode::Invalid,
                )
            })
            .transpose()?;
        #[cfg(not(feature = "filesystem"))]
        let decoded: Option<(Value, crate::conversation::ContentHold<'_>)> = None;
        let (arguments, _arguments_hold) = match decoded {
            Some((value, hold)) => (value, Some(hold)),
            None => (
                serde_json::from_str(&call.arguments.value).map_err(|_| {
                    stock_boundary(
                        self.io,
                        crate::wire::ErrorCode::Invalid,
                        "JavaScript tool arguments are not JSON",
                    )
                })?,
                None,
            ),
        };
        let invocation = ToolInvocation::for_model_call(
            operation,
            step,
            stock_string_copy(&call_id, self.io)?,
            stock_string_copy(&call.name.value, self.io)?,
            arguments,
            self.native_work(),
        )?;
        // Local call/result messages supply the identical stock metadata contract;
        // they never become model history. The journal retains the ordinary records.
        let mut messages = vec![ModelMessage {
            role: ModelRole::Assistant,
            content: ModelContent::Part(ModelContentPart::ToolCall(
                // Only metadata and identity are read from this local envelope.
                crate::model::ToolCall::new(call_id.clone(), call.name.value.clone(), Value::Null),
            )),
        }];
        let result = match Box::pin(self.settle_tool_call(
            journal,
            operation,
            step,
            invocation,
            0,
            &mut messages,
            hook_context,
        ))
        .await?
        {
            ExecutionProgress::Ready(receipt) => receipt,
            ExecutionProgress::Pending(operation) => {
                return Ok(ExecutionProgress::Pending(operation));
            }
            ExecutionProgress::Failed(reason) => return Ok(ExecutionProgress::Failed(reason)),
            ExecutionProgress::PreDispatchFailed(failure) => {
                return Ok(ExecutionProgress::PreDispatchFailed(failure));
            }
        };
        if let Some(ModelMessage {
            content:
                ModelContent::Part(ModelContentPart::ToolResult {
                    content: crate::model::ToolResultContent::Error { message },
                    ..
                }),
            ..
        }) = messages.pop()
        {
            return Ok(ExecutionProgress::Ready(Err(message)));
        }
        let result = result.ok_or_else(|| {
            stock_boundary(
                self.io,
                crate::wire::ErrorCode::Invalid,
                "JavaScript call has no receipt",
            )
        })?;
        let value = if self
            .stock
            .tools
            .get(&call.name.value, None, self.io)?
            .is_some_and(|tool| tool.executor.is_code_mode())
        {
            crate::model::clone_value(crate::tools::code_mode::output(&result.0.value)?, self.io)?
        } else {
            result.0.value
        };
        Ok(ExecutionProgress::Ready(Ok((value, result.1))))
    }
}
