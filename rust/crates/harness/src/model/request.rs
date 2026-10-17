use super::*;
use std::collections::BTreeMap;
#[cfg(feature = "filesystem")]
use std::mem::size_of;

/// Complete immutable provider-neutral model request.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ModelRequest {
    /// Pinned model selection.
    pub model: Model,
    /// Ordered reconstructable context.
    pub messages: Vec<ModelMessage>,
    /// Visible registered tools.
    pub tools: Vec<crate::tool::ToolDefinition>,
    /// Optional output token ceiling.
    pub max_output_tokens: Option<u32>,
}
impl ModelRequest {
    pub(super) fn validate(&mut self, limits: Limits, io: Option<&PreparationIo>) -> Result<()> {
        with_history_io(io, |work| limits.validate(work))?;
        charge_items(io, 1)?;
        charge_items(
            io,
            (self.model.provider.len() as u64)
                .saturating_add(self.model.name.len() as u64)
                .saturating_add(self.model.revision.len() as u64),
        )?;
        self.model.validate()?;
        if self.messages.is_empty() || self.messages.len() > limits.context_messages {
            return Err(Error::Invalid("model context count is invalid".into()));
        }
        if self.max_output_tokens == Some(0) {
            return Err(Error::Invalid(
                "model output token bound must be positive".into(),
            ));
        }
        let mut remaining = limits.render_bytes;
        reserve_instruction_bodies(&self.messages, &mut remaining, io)?;
        #[cfg(feature = "filesystem")]
        let _maps = if let Some(io) = io {
            let mut calls = 0u64;
            for message in &self.messages {
                charge_items(Some(io), 1)?;
                calls = calls
                    .checked_add(message.content.parts().len() as u64)
                    .ok_or_else(|| Error::Invalid("model validation slot count overflow".into()))?;
            }
            let bytes = (self.tools.len() as u64)
                .checked_mul(size_of::<(&str, &crate::tool::ToolDefinition)>() as u64)
                .and_then(|tools| {
                    calls
                        .checked_mul(size_of::<(&String, PendingCall<'_>)>() as u64)
                        .and_then(|pending| tools.checked_add(pending))
                })
                .ok_or_else(|| Error::Invalid("model validation ownership overflow".into()))?;
            Some(io.reserve_decode(bytes)?)
        } else {
            None
        };
        let mut tools = BTreeMap::new();
        for tool in &self.tools {
            charge_items(io, 1)?;
            with_history_io(io, |work| tool.validate(work))?;
            if tools.insert(tool.name.as_str(), tool).is_some() {
                return Err(Error::Invalid("model tool definition is repeated".into()));
            }
        }
        let mut pending = BTreeMap::new();
        for message in &mut self.messages {
            charge_items(io, 1)?;
            with_history_io(io, |work| message.content.validate_limits(limits, work))?;
            let empty = message.content.parts().is_empty();
            let parts = match &mut message.content {
                ModelContent::Text(_) => &mut [][..],
                ModelContent::Part(part) => std::slice::from_mut(part),
                ModelContent::Parts(parts) => parts.as_mut_slice(),
            };
            for part in parts {
                charge_items(io, 1)?;
                Self::validate_part(message.role, part, &tools, &mut pending, io)?;
            }
            if message.role == ModelRole::Tool && empty {
                return Err(Error::Invalid("tool role requires a paired result".into()));
            }
            if !pending.is_empty() && matches!(message.role, ModelRole::System | ModelRole::User) {
                return Err(Error::Invalid("model tool exchange is interrupted".into()));
            }
        }
        if !pending.is_empty() {
            return Err(Error::Invalid("model tool exchange is incomplete".into()));
        }
        Ok(())
    }
    fn validate_tool_result(
        tool: &crate::tool::ToolDefinition,
        content: &mut ToolResultContent,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        match io {
            #[cfg(feature = "filesystem")]
            Some(io) => {
                let _hold;
                let bytes = io.encode(content, io.remaining()?.bytes_encoded, false)?;
                _hold = io.guard_bytes(&bytes)?;
                let envelope = io.decode(std::marker::PhantomData::<Value>, &bytes, Storage)?;
                crate::executor::with_history_io(Some(io), |work| {
                    crate::executor::validate_json_schema_value(
                        &tool.projection_schema,
                        &envelope.0,
                        "model tool result",
                        work,
                    )
                })?;
            }
            _ => {
                let validate = |envelope: &Value| {
                    crate::executor::validate_json_schema_value(
                        &tool.projection_schema,
                        envelope,
                        "model tool result",
                        &mut None,
                    )
                };
                if let ToolResultContent::Json { value } = content {
                    let mut envelope = serde_json::json!({"kind":"json", "value":null});
                    envelope["value"] = std::mem::take(value);
                    let result = validate(&envelope);
                    *value = envelope["value"].take();
                    result?;
                } else {
                    let envelope = serde_json::to_value(content)
                        .map_err(|error| Error::Invalid(error.to_string()))?;
                    validate(&envelope)?;
                }
            }
        }
        Ok(())
    }
    fn bound_tool<'a>(
        name: &str,
        tools: &BTreeMap<&str, &'a crate::tool::ToolDefinition>,
    ) -> Option<&'a crate::tool::ToolDefinition> {
        match name.rsplit_once('@') {
            Some((name, revision)) => tools
                .get(name)
                .copied()
                .filter(|tool| tool.revision == revision),
            None => tools.get(name).copied(),
        }
    }
    fn validate_part<'a>(
        role: ModelRole,
        part: &'a mut ModelContentPart,
        tools: &BTreeMap<&str, &crate::tool::ToolDefinition>,
        pending: &mut BTreeMap<&'a String, PendingCall<'a>>,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        match part {
            ModelContentPart::ToolCall(crate::model::ToolCall {
                call_id,
                name,
                arguments,
                ..
            }) => {
                charge_items(io, (call_id.len() as u64).saturating_add(name.len() as u64))?;
                crate::tool::ToolInvocation::validate_identity(call_id, Some(name), &mut None)?;
                if role != ModelRole::Assistant || pending.contains_key(&*call_id) {
                    return Err(Error::Invalid(
                        "model tool call role or identity is invalid".into(),
                    ));
                }
                // Binding and arguments are checked when the paired result arrives: a call the
                // harness refused is history the model must see beside its typed error.
                pending.insert(call_id, (name, arguments));
            }
            ModelContentPart::ToolResult {
                call_id,
                name,
                content,
            } => {
                charge_items(io, (call_id.len() as u64).saturating_add(name.len() as u64))?;
                crate::tool::ToolInvocation::validate_identity(call_id, Some(name), &mut None)?;
                let call = pending.remove(&*call_id);
                let Some((_, arguments)) =
                    call.filter(|(called, _)| role == ModelRole::Tool && **called == *name)
                else {
                    return Err(Error::Invalid(
                        "model tool result has no matching call".into(),
                    ));
                };
                // Only a failure the harness recorded is exempt from the call's contract; a
                // supplied error result must not stand in for the result a bound call owes.
                match content {
                    ToolResultContent::Error { provenance, .. } if provenance.is_recorded() => {
                        return Ok(());
                    }
                    ToolResultContent::Error { .. } => {
                        return Err(Error::Invalid(
                            "model tool error result has no recorded harness failure".into(),
                        ));
                    }
                    _ => {}
                }
                let tool = Self::bound_tool(name, tools)
                    .ok_or_else(|| Error::Invalid("model tool call is unbound".into()))?;
                crate::executor::with_history_io(io, |work| {
                    crate::executor::validate_json_schema_value(
                        &tool.input_schema,
                        arguments,
                        "model tool arguments",
                        work,
                    )
                })?;
                Self::validate_tool_result(tool, content, io)?;
            }
            _ if role == ModelRole::Tool => {
                return Err(Error::Invalid("tool role requires a paired result".into()));
            }
            _ => {}
        }
        Ok(())
    }
}

/// Marks every error result in harness-retained `messages` as a failure the harness recorded.
pub(super) fn mark_recorded_failures(messages: &mut [ModelMessage]) {
    for message in messages {
        let parts = match &mut message.content {
            ModelContent::Text(_) => &mut [][..],
            ModelContent::Part(part) => std::slice::from_mut(part),
            ModelContent::Parts(parts) => parts.as_mut_slice(),
        };
        for part in parts {
            if let ModelContentPart::ToolResult {
                content: ToolResultContent::Error { provenance, .. },
                ..
            } = part
            {
                *provenance = FailureProvenance::RECORDED;
            }
        }
    }
}

/// A call awaiting its paired result: name and the arguments the model emitted.
type PendingCall<'a> = (&'a String, &'a Value);

/// Admitted values, exact canonical request bytes and their digest.
#[derive(Clone, Debug)]
pub struct PreparedModelRequest {
    request: ModelRequest,
    bytes: acyclic_stream::OwnedBytes,
    request_digest: [u8; 32],
}
impl PreparedModelRequest {
    /// Decodes and verifies exact canonical input on the optional ledger. Supplied bytes carry
    /// no recorded failure, so an error result must not stand in for a bound call's result.
    pub fn decode(bytes: &[u8], limits: Limits, io: Option<&PreparationIo>) -> Result<Self> {
        Self::decode_with(bytes, limits, io, false)
    }
    /// Decodes a request the harness retained in its own private journal; its error results are
    /// the failures it recorded.
    pub(crate) fn decode_retained(
        bytes: &[u8],
        limits: Limits,
        io: Option<&PreparationIo>,
    ) -> Result<Self> {
        Self::decode_with(bytes, limits, io, true)
    }
    fn decode_with(
        bytes: &[u8],
        limits: Limits,
        io: Option<&PreparationIo>,
        retained: bool,
    ) -> Result<Self> {
        let mut request: ModelRequest = match io {
            #[cfg(feature = "filesystem")]
            Some(io) => crate::filesystem::retain_decoded(io.decode(
                std::marker::PhantomData::<ModelRequest>,
                bytes,
                Storage,
            )?)?,
            _ => crate::contract::json_from_slice(bytes)
                .map_err(|error| Error::Invalid(error.to_string()))?,
        };
        if retained {
            mark_recorded_failures(&mut request.messages);
        }
        let prepared = Self::encode_admitted(request, limits, io)?;
        charge_items(io, bytes.len() as u64)?;
        if prepared.bytes() != bytes {
            return Err(Error::Invalid(
                "retained model request is not exact canonical input".into(),
            ));
        }
        Ok(prepared)
    }
    /// Admits explicit input on the optional preparation ledger.
    pub fn prepare(
        request: ModelRequest,
        limits: Limits,
        io: Option<&PreparationIo>,
    ) -> Result<Self> {
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            #[cfg(not(target_arch = "wasm32"))]
            if io.original_native_work().is_some() {
                return Self::encode_admitted(request, limits, Some(io));
            }
            let mut request = request;
            let request_hold;
            let bytes_hold;
            // Bound JSON depth before the recursive capacity inventory.
            let bytes = io.encode(&request, io.remaining()?.bytes_encoded, true)?;
            bytes_hold = io.guard_bytes(&bytes)?;
            request_hold =
                io.reserve_decode(add(size_of::<Self>() as u64, request.payload_bytes()?)?)?;
            request.validate(limits, Some(io))?;
            let request_digest = hash_bytes(&bytes, Some(io))?;
            if let Some(hold) = bytes_hold {
                hold.into_retained()?;
            }
            request_hold.into_retained()?;
            return Ok(Self {
                request,
                bytes,
                request_digest,
            });
        }
        Self::encode_admitted(request, limits, io)
    }
    pub(super) fn encode_admitted(
        mut request: ModelRequest,
        limits: Limits,
        io: Option<&PreparationIo>,
    ) -> Result<Self> {
        #[cfg(all(test, feature = "filesystem", not(target_arch = "wasm32")))]
        acyclic_test_support::differential::record(("prepare request", 1u64));
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(io) = io.filter(|io| io.original_native_work().is_some()) {
            io.retain_owned((size_of::<Self>() - size_of::<ModelRequest>()) as u64)?;
        }
        request.validate(limits, io)?;
        #[cfg(feature = "filesystem")]
        let bytes_hold;
        let bytes: acyclic_stream::OwnedBytes = match io {
            #[cfg(feature = "filesystem")]
            Some(io) => io.encode(&request, io.remaining()?.bytes_encoded, true)?,
            _ => crate::contract::canonical_json_bytes(&request)?.into(),
        };
        #[cfg(feature = "filesystem")]
        {
            bytes_hold = io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten();
        }
        let request_digest = hash_bytes(&bytes, io)?;
        #[cfg(feature = "filesystem")]
        if let Some(hold) = bytes_hold {
            hold.into_retained()?;
        }
        Ok(Self {
            request,
            bytes,
            request_digest,
        })
    }
    /// Admitted values for provider-specific encoding.
    #[must_use]
    pub fn request(&self) -> &ModelRequest {
        &self.request
    }
    pub(crate) fn into_messages(self) -> Vec<ModelMessage> {
        self.request.messages
    }
    // The context temporarily borrows ownership of the messages for read-only
    // validation. Neither a changed request nor an incomplete prepared value
    // can escape the closure, including on failure.
    pub(crate) fn with_context<T>(
        mut self,
        mut context: crate::context::Context,
        inspect: impl FnOnce(&crate::context::Context) -> Result<T>,
    ) -> Result<(Self, crate::context::Context, T)> {
        debug_assert!(context.messages.is_empty());
        std::mem::swap(&mut self.request.messages, &mut context.messages);
        let result = inspect(&context)?;
        std::mem::swap(&mut self.request.messages, &mut context.messages);
        Ok((self, context, result))
    }
    /// Exact canonical provider-neutral bytes.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    /// BLAKE3 digest of the exact request bytes.
    #[must_use]
    pub const fn request_digest(&self) -> [u8; 32] {
        self.request_digest
    }
    /// Additive token bound including every pinned file.
    pub fn serialized_token_upper_bound(
        &self,
        work: crate::NativeWork<'_>,
    ) -> Result<(crate::context::ModelTokenCount, crate::NativeCustody)> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let observer = work.map(PreparationIo::from_native_work).transpose()?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let io = observer.as_ref();
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let io: Option<&PreparationIo> = None;
        let _ = work;
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            io.retain_owned(size_of::<crate::context::ModelTokenCount>() as u64)?;
        }
        let mut message_tokens = Vec::new();
        let mut fixed_bytes = self.bytes.len();
        for message in &self.request.messages {
            charge_items(io, 1)?;
            #[cfg(feature = "filesystem")]
            let _hold;
            let bytes: acyclic_stream::OwnedBytes = match io {
                #[cfg(feature = "filesystem")]
                Some(io) => {
                    let bytes = io.encode(message, io.remaining()?.bytes_encoded, false)?;
                    _hold = io.guard_bytes(&bytes)?;
                    bytes
                }
                _ => serde_json::to_vec(message)
                    .map_err(|error| Error::Invalid(error.to_string()))?
                    .into(),
            };
            fixed_bytes = fixed_bytes
                .checked_sub(bytes.len())
                .ok_or_else(|| Error::Invalid("message framing exceeds prepared request".into()))?;
            let mut tokens = bytes.len() as u64;
            for file in message.content.file_refs() {
                charge_items(io, 1)?;
                tokens = tokens
                    .checked_add(file.descriptor().byte_length())
                    .ok_or_else(|| Error::Invalid("deterministic token count overflows".into()))?;
            }
            let tokens = u32::try_from(tokens)
                .map_err(|_| Error::Invalid("deterministic token count exceeds u32".into()))?;
            crate::executor::stock_vec_push(&mut message_tokens, tokens, io)?;
        }
        let fixed_tokens = u32::try_from(fixed_bytes)
            .map_err(|_| Error::Invalid("deterministic framing exceeds u32".into()))?;
        let count = crate::context::ModelTokenCount::from_upper_bounds(
            self.request_digest,
            fixed_tokens,
            message_tokens,
        );
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let custody = observer
            .map(|io| {
                io.into_native_custody(add(
                    size_of::<crate::context::ModelTokenCount>() as u64,
                    slots(count.message_tokens.capacity(), size_of::<u32>())?,
                )?)
            })
            .transpose()?;
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let custody = None;
        Ok((count, custody))
    }
}

#[cfg(test)]
#[path = "request_tokens_tests.rs"]
mod tokens_tests;
