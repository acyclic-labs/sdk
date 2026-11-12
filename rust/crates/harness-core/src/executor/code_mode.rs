//! QuickJS is a caller of stock settlement, with no host resources of its own.
use super::*;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

mod allocator;
mod interpreter;
#[cfg(feature = "test-support")]
pub(super) mod model;
pub(crate) use interpreter::MEMORY_BYTES;

struct Call {
    module: bool,
    name: allocator::BridgeText,
    arguments: allocator::BridgeText,
    reply: oneshot::Sender<Option<acyclic_stream::OwnedBytes>>,
}

enum Request {
    Tool(Call),
    Log {
        arguments: allocator::BridgeText,
        reply: oneshot::Sender<Option<acyclic_stream::OwnedBytes>>,
    },
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

/// console.log lines the program printed, returned beside its result: a
/// model that prints a value expects to read it. Lines beyond the render
/// budget remain in the journal; `logs_omitted` reports them when its framing fits.
#[derive(Default)]
struct ProgramLogs {
    lines: Vec<Value>,
    bytes: usize,
    omitted: u64,
}

impl ProgramLogs {
    /// Both field names, array framing and the largest u64 omission count.
    const FIELDS: usize = 48;

    fn observe(
        &mut self,
        values: &serde_json::value::RawValue,
        maximum: usize,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        if self.omitted > 0 {
            self.omitted += 1;
            return Ok(());
        }
        // The rendered line cannot exceed the original JSON array's bytes.
        // Refuse its copy and argument index before decoding either of them.
        let available = maximum.saturating_sub(self.bytes.saturating_add(Self::FIELDS));
        if values.get().len() > available {
            self.omitted = 1;
            return Ok(());
        }
        let values = Self::decode::<Vec<&serde_json::value::RawValue>>(values.get(), io)?;
        let mut line = String::new();
        for (index, value) in values.0.iter().enumerate() {
            if index > 0 {
                stock_string_push(
                    &mut line,
                    " ",
                    available as u64,
                    "JavaScript log exceeds render capacity",
                    io,
                )?;
            }
            // Strings print as their text, like console.log; other values
            // print as their JSON encoding.
            if value.get().starts_with('"') {
                let text =
                    Self::decode::<crate::executor::json::BorrowedText<'_>>(value.get(), io)?;
                stock_string_push(
                    &mut line,
                    &text.0.0,
                    available as u64,
                    "JavaScript log exceeds render capacity",
                    io,
                )?;
            } else {
                stock_string_push(
                    &mut line,
                    value.get(),
                    available as u64,
                    "JavaScript log exceeds render capacity",
                    io,
                )?;
            }
        }
        let cost = with_history_io(io, |work| {
            crate::executor::json::json_byte_length(line.as_str(), None, work)
        })? as usize
            + 1;
        if self.bytes + cost + Self::FIELDS > maximum {
            self.omitted = 1;
            return Ok(());
        }
        self.bytes += cost;
        stock_vec_push(&mut self.lines, Value::String(line), io)
    }

    /// The existing typed codec admits the argument index and escape decoding
    /// before allocation, using the caller's original ledger. The tuple drops
    /// each temporary before releasing its reservation.
    fn decode<'a, 'de, T: serde::Deserialize<'de> + crate::HeapLayout>(
        source: &'de str,
        io: Option<&'a PreparationIo>,
    ) -> Result<(T, Option<crate::conversation::ContentHold<'a>>)> {
        if let Some(io) = io {
            let (value, hold) = io.decode_seed(
                std::marker::PhantomData::<T>,
                source.as_bytes(),
                crate::executor::json::JsonValueSchema::None,
                0,
                crate::wire::ErrorCode::Invalid,
            )?;
            return Ok((value, Some(hold)));
        }
        let _ = io;
        crate::executor::json::json_from_slice_bounded(
            std::marker::PhantomData::<T>,
            source.as_bytes(),
            crate::executor::json::JsonBounds {
                maximum_bytes: Some(source.len() as u64),
                maximum_items: None,
                maximum_depth: crate::executor::json::RECORD_MAXIMUM_DEPTH,
            },
            None,
            None,
            &mut |_| Ok(()),
        )
        .map(|value| (value, None))
    }

    /// The program's result has priority. Attach the batch only when its
    /// complete framing fits; the journal retains every original log.
    fn attach(self, output: &mut Value, maximum: usize, io: Option<&PreparationIo>) -> Result<()> {
        let Self {
            mut lines,
            bytes,
            mut omitted,
        } = self;
        if lines.is_empty() && omitted == 0 {
            return Ok(());
        }
        let result = with_history_io(io, |work| {
            crate::executor::json::json_byte_length(&*output, None, work)
        })? as usize;
        let Some(available) = maximum.checked_sub(result.saturating_add(Self::FIELDS)) else {
            return Ok(());
        };
        if bytes > available {
            omitted += lines.len() as u64;
            lines.clear();
        }
        let Some(fields) = output.as_object_mut() else {
            return Ok(());
        };
        fields.insert("logs".into(), Value::Array(lines));
        if omitted > 0 {
            fields.insert("logs_omitted".into(), Value::from(omitted));
        }
        Ok(())
    }
}

type CallResult<'a> =
    std::result::Result<(Value, Option<crate::conversation::ContentHold<'a>>), ToolFailureCause>;

fn encode_reply(
    ok: bool,
    value: &Value,
    version: Option<&str>,
    io: Option<&PreparationIo>,
) -> Result<acyclic_stream::OwnedBytes> {
    #[derive(Serialize)]
    struct Reply<'a> {
        ok: bool,
        value: &'a Value,
        #[serde(skip_serializing_if = "Option::is_none")]
        version: Option<&'a str>,
    }
    let reply = Reply { ok, value, version };

    if let Some(io) = io {
        return io.encode(&reply, u64::MAX, false);
    }
    serde_json::to_vec(&reply).map(Into::into).map_err(|_| {
        stock_boundary(
            io,
            crate::wire::ErrorCode::Invalid,
            "JavaScript response encoding failed",
        )
    })
}

impl StockExecution<'_> {
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

    #[cfg(test)]
    pub(super) fn code<'a>(
        &'a self,
        journal: &'a dyn ExecutionJournal,
        identity: (OperationId, u32),
        parent: &'a ToolInvocation,
        hook_context: &'a mut Vec<ModelMessage>,
    ) -> BoxFuture<'a, Result<ExecutionProgress<ToolResult>>> {
        self.code_selected(journal, identity, parent, hook_context, None)
    }

    pub(super) fn code_selected<'a, 'selection>(
        &'a self,
        journal: &'a dyn ExecutionJournal,
        identity: (OperationId, u32),
        parent: &'a ToolInvocation,
        hook_context: &'a mut Vec<ModelMessage>,
        selection: Option<&'a ToolSelection<'selection>>,
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
            let funded = self
                .io
                .map(|io| io.encode(&definitions, u64::MAX, false))
                .transpose()?;
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
            let mut log_ordinal = 0_u64;
            let mut logs = ProgramLogs::default();
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
                    Some(request) = receive.recv() => {
                        let call = match request {
                            Request::Tool(call) => call,
                            Request::Log { arguments, reply } => {
                                log_ordinal = log_ordinal.checked_add(1).ok_or_else(|| stock_boundary(self.io, crate::wire::ErrorCode::Invalid, "JavaScript log identity overflow"))?;
                                #[derive(Serialize)]
                                struct Log<'a> {
                                    kind: &'static str,
                                    call_id: &'a str,
                                    ordinal: u64,
                                    values: &'a serde_json::value::RawValue,
                                }
                                // Borrow the engine's JSON without allocating a second value tree.
                                let values = serde_json::from_str(&arguments.value).map_err(|_| stock_boundary(self.io, crate::wire::ErrorCode::Invalid, "JavaScript log arguments are not JSON"))?;
                                let entry = Log { kind: "code.log", call_id: &parent.call_id, ordinal: log_ordinal, values };
                                logs.observe(values, maximum, self.io)?;
                                let key = with_history_io(self.io, |work| preparation_format(format_args!("tool:{}:code:log:{log_ordinal}:result", parent.operation_id), work))?;

                                let _key_hold = self.io.map(|io| io.guard_owned(key.capacity() as u64)).transpose()?;
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
                                let _ = reply.send(None);
                                continue;
                            }
                        };
                        ordinal = ordinal.checked_add(1).ok_or_else(|| stock_boundary(self.io, crate::wire::ErrorCode::Invalid, "JavaScript call identity overflow"))?;
                        let input_bytes = call.arguments.value.len();
                        let result = self.js_call(journal, identity, parent, ordinal, &call, &mut inner_context, selection).await?;
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
                        let (value, _result_hold, _failure_hold) = match result {
                            Ok((value, hold)) => (value, hold, None),
                            Err(cause) => {
                                let (value, hold) = crate::tool::value(&cause, u64::MAX, self.native_work())?;
                                (value, None, hold)
                            },
                        };
                        let version = if ok {
                            crate::tool::files::result_version(
                                if call.module {
                                    call.name.value.strip_prefix("acyclic.").unwrap_or("")
                                } else {
                                    ""
                                },
                                &value,
                                self.native_work(),
                            )?
                        } else { None };
                        let output_bytes = with_history_io(self.io, |work| crate::executor::json::json_byte_length(&value, None, work))?;
                        let entry = CallLog { kind: "code.call", call_id: &parent.call_id, ordinal, tool: &call.name.value, ok, input_bytes, output_bytes };
                        let key = with_history_io(self.io, |work| preparation_format(format_args!("tool:{}:code:{ordinal}:result", parent.operation_id), work))?;
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
                        let reply = encode_reply(ok, &value, version.as_ref().map(|v| v.0.as_str()), self.io)?;
                        let _ = call.reply.send(Some(reply));
                    }
                }
            };
            if let Some(error) = engine_failure
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
            {
                return Err(error);
            }
            let mut value = match program {
                Ok(result) => {
                    if let Some(custody) = result.custody {
                        let io = self.io.ok_or_else(|| {
                            stock_boundary(
                                self.io,
                                crate::wire::ErrorCode::Invalid,
                                "JavaScript result has no original ledger",
                            )
                        })?;
                        io.absorb_native_custody(custody)?;
                    }
                    let slots =
                        crate::executor::json::object_slot_bytes(1, true).ok_or_else(|| {
                            stock_boundary(
                                self.io,
                                crate::wire::ErrorCode::Invalid,
                                "JavaScript result wrapper size overflow",
                            )
                        })?;

                    if let Some(io) = self.io {
                        io.retain_owned(slots)?;
                    }
                    let key = stock_string_copy("value", self.io)?;
                    let mut fields = serde_json::Map::new();
                    fields.insert(key, result.value);
                    Value::Object(fields)
                }
                Err(mut error) => {
                    let mut value = serde_json::json!({"error": ""});
                    // Reserve the exact wrapper before fitting escaped text.
                    let overhead = with_history_io(self.io, |work| {
                        crate::executor::json::json_byte_length(&value, Some(maximum as u64), work)
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
            logs.attach(&mut value, maximum, self.io)?;
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
        selection: Option<&ToolSelection<'_>>,
    ) -> Result<ExecutionProgress<CallResult<'a>>> {
        // A separate hash domain keeps inner identities disjoint from provider
        // call IDs. Replaying the script reuses the exact ordinal and receipt.
        // The identity is the parent call's place in its turn, never the
        // turn's id, so a replayed run's inner calls keep recorded identities.
        let mut hash = blake3::Hasher::new();
        hash.update(b"harness:js-call:v2");
        hash.update(&step.to_be_bytes());
        hash.update(&(parent.call_id.len() as u64).to_be_bytes());
        hash.update(parent.call_id.as_bytes());
        hash.update(&ordinal.to_be_bytes());
        let call_id = hash.finalize().to_hex().to_string();
        let canonical = call
            .module
            .then(|| call.name.value.strip_prefix("acyclic."))
            .flatten();
        let name = match canonical {
            Some(name) => match self.stock.tools.capability(name, self.io)? {
                Some(tool) => with_history_io(self.io, |work| {
                    preparation_format(
                        format_args!("{}@{}", tool.definition.name, tool.definition.revision),
                        work,
                    )
                })?,
                None => stock_string_copy(name, self.io)?,
            },
            None => stock_string_copy(&call.name.value, self.io)?,
        };

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
            stock_string_copy(&name, self.io)?,
            arguments,
            self.native_work(),
        )?;
        // Local call/result messages supply the identical stock metadata contract;
        // they never become model history. The journal retains the ordinary records.
        let mut messages = vec![ModelMessage {
            role: ModelRole::Assistant,
            content: ModelContent::Part(ModelContentPart::ToolCall(
                // Only metadata and identity are read from this local envelope.
                crate::model::ToolCall::new(call_id.clone(), name.clone(), Value::Null),
            )),
        }];
        let result = match Box::pin(self.settle_selected_tool_call(
            journal,
            operation,
            step,
            invocation,
            0,
            &mut messages,
            hook_context,
            selection,
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
            // Classification comes from the exact indexed receipt, never from
            // parsing model-visible error text. This extra bounded read is
            // needed only for failures, until settlement exposes its cause.
            let (_, records) = self
                .tool_records(journal, operation, step, &call_id)
                .await?;
            let super::records::RecordedCall::Failed { reason, cause, .. } =
                super::records::RecordedCall::classify(&records).map_err(|reason| {
                    stock_boundary(self.io, crate::wire::ErrorCode::Invalid, reason)
                })?
            else {
                return Err(stock_boundary(
                    self.io,
                    crate::wire::ErrorCode::Invalid,
                    "failed call has no failure receipt",
                ));
            };
            return Ok(ExecutionProgress::Ready(Err(ToolFailureCause {
                class: if reason == ToolFailureKind::Unavailable {
                    ToolErrorClass::Denied
                } else {
                    cause.class
                },
                message,
            })));
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
            .get(&name, None, self.io)?
            .is_some_and(|tool| tool.executor.is_code_mode())
        {
            crate::model::clone_value(crate::tools::code_mode::output(&result.0.value)?, self.io)?
        } else {
            result.0.value
        };
        Ok(ExecutionProgress::Ready(Ok((value, result.1))))
    }
}
