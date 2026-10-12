use super::*;
use std::{collections::BTreeMap, mem::size_of};

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
    pub(super) fn validate(&self, limits: Limits, io: Option<&PreparationIo>) -> Result<()> {
        with_history_io(io, |work| limits.validate(work))?;
        charge_items(io, 1)?;
        charge_items(io, (self.model.provider.len() as u64).saturating_add(self.model.name.len() as u64).saturating_add(self.model.revision.len() as u64))?;
        self.model.validate()?;
        if self.messages.is_empty() || self.messages.len() > limits.context_messages { return Err(Error::Invalid("model context count is invalid".into())); }
        if self.max_output_tokens == Some(0) { return Err(Error::Invalid("model output token bound must be positive".into())); }
        #[cfg(feature = "filesystem")]
        let _maps = if let Some(io) = io {
            let mut calls = 0u64;
            for message in &self.messages {
                charge_items(Some(io), 1)?;
                calls = calls.checked_add(message.content.parts().len() as u64).ok_or_else(|| Error::Invalid("model validation slot count overflow".into()))?;
            }
            let bytes = (self.tools.len() as u64).checked_mul(size_of::<(&String, &crate::tool::ToolDefinition)>() as u64)
                .and_then(|tools| calls.checked_mul(size_of::<(&String, &String)>() as u64).and_then(|pending| tools.checked_add(pending)))
                .ok_or_else(|| Error::Invalid("model validation ownership overflow".into()))?;
            Some(io.reserve_decode(bytes)?)
        } else { None };
        let mut tools = BTreeMap::new();
        for tool in &self.tools {
            charge_items(io, 1)?;
            with_history_io(io, |work| tool.validate(work))?;
            if tools.insert(&tool.name, tool).is_some() { return Err(Error::Invalid("model tool definition is repeated".into())); }
        }
        let mut pending = BTreeMap::new();
        for message in &self.messages {
            charge_items(io, 1)?;
            with_history_io(io, |work| message.content.validate_limits(limits, work))?;
            for part in message.content.parts() {
                charge_items(io, 1)?;
                match part {
                    ModelContentPart::ToolCall { call_id, name, arguments } => {
                        charge_items(io, (call_id.len() as u64).saturating_add(name.len() as u64))?;
                        crate::tool::ToolInvocation::validate_identity(call_id, Some(name), &mut None)?;
                        if message.role != ModelRole::Assistant || pending.contains_key(call_id) { return Err(Error::Invalid("model tool call role or identity is invalid".into())); }
                        let tool = tools.get(name).ok_or_else(|| Error::Invalid("model tool call is unbound".into()))?;
                        crate::executor::with_history_io(io, |work| crate::contract::validate_json_schema_value(&tool.input_schema, arguments, "model tool arguments", work))?;
                        pending.insert(call_id, name);
                    }
                    ModelContentPart::ToolResult { call_id, name, content } => {
                        charge_items(io, (call_id.len() as u64).saturating_add(name.len() as u64))?;
                        crate::tool::ToolInvocation::validate_identity(call_id, Some(name), &mut None)?;
                        if message.role != ModelRole::Tool || pending.remove(call_id) != Some(name) { return Err(Error::Invalid("model tool result has no matching call".into())); }
                        let tool = tools.get(name).ok_or_else(|| Error::Invalid("model tool result is unbound".into()))?;
                        match io {
                            #[cfg(feature = "filesystem")]
                            Some(io) => {
                                let _hold;
                                let bytes = io.encode(content, io.remaining()?.bytes_encoded, false)?;
                                _hold = io.guard_bytes(&bytes)?;
                                let envelope = io.decode(std::marker::PhantomData::<Value>, &bytes, Storage)?;
                                crate::executor::with_history_io(Some(io), |work| crate::contract::validate_json_schema_value(&tool.projection_schema, &envelope.0, "model tool result", work))?;
                            }
                            _ => {
                                let envelope = serde_json::to_value(content).map_err(|error| Error::Invalid(error.to_string()))?;
                                crate::contract::validate_json_schema_value(&tool.projection_schema, &envelope, "model tool result", &mut None)?;
                            }
                        }
                    }
                    _ if message.role == ModelRole::Tool => return Err(Error::Invalid("tool role requires a paired result".into())),
                    _ => {},
                }
            }
            if message.role == ModelRole::Tool && message.content.parts().is_empty() { return Err(Error::Invalid("tool role requires a paired result".into())); }
            if !pending.is_empty() && matches!(message.role, ModelRole::System | ModelRole::User) { return Err(Error::Invalid("model tool exchange is interrupted".into())); }
        }
        if !pending.is_empty() { return Err(Error::Invalid("model tool exchange is incomplete".into())); }
        Ok(())
    }
}

/// Admitted values, exact canonical request bytes and their digest.
#[derive(Clone, Debug)]
pub struct PreparedModelRequest {
    request: ModelRequest,
    bytes: acyclic_stream::OwnedBytes,
    request_digest: [u8; 32],
}
impl PreparedModelRequest {
    /// Decodes and verifies exact canonical input on the optional ledger.
    pub fn decode(bytes: &[u8], limits: Limits, io: Option<&PreparationIo>) -> Result<Self> {
        let request = match io {
            #[cfg(feature = "filesystem")]
            Some(io) => crate::filesystem::retain_decoded(io.decode(std::marker::PhantomData::<ModelRequest>, bytes, Storage)?),
            _ => crate::contract::json_from_slice(bytes).map_err(|error| Error::Invalid(error.to_string()))?,
        };
        let prepared = Self::encode_admitted(request, limits, io)?;
        charge_items(io, bytes.len() as u64)?;
        if prepared.bytes() != bytes { return Err(Error::Invalid("retained model request is not exact canonical input".into())); }
        Ok(prepared)
    }
    /// Admits explicit input on the optional preparation ledger.
    pub fn prepare(request: ModelRequest, limits: Limits, io: Option<&PreparationIo>) -> Result<Self> {
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            #[cfg(not(target_arch = "wasm32"))]
            if io.original_native_work().is_some() { return Self::encode_admitted(request, limits, Some(io)); }
            // Guards outlive payloads if preparation fails.
            let request_hold;
            let bytes_hold;
            let request = request;
            let before = io.receipt().items_examined;
            let bytes = io.encode(&request, io.remaining()?.bytes_encoded, true)?;
            bytes_hold = io.guard_bytes(&bytes)?;
            let items = io.receipt().items_examined.checked_sub(before).ok_or_else(|| Error::Invalid("model request encoding count underflow".into()))?;
            let owned = items.checked_mul(<ModelRequest as crate::filesystem::PreparationRecord>::maximum_slot_bytes())
                .and_then(|slots| slots.checked_mul(8))
                .and_then(|slots| (bytes.len() as u64).checked_mul(4).and_then(|strings| slots.checked_add(strings)))
                .and_then(|owned| owned.checked_add(size_of::<ModelRequest>() as u64))
                .ok_or_else(|| Error::Invalid("model request ownership overflow".into()))?;
            request_hold = io.reserve_decode(owned)?;
            request.validate(limits, Some(io))?;
            let request_digest = hash_bytes(&bytes, Some(io))?;
            if let Some(hold) = bytes_hold { hold.into_retained(); }
            request_hold.into_retained();
            return Ok(Self { request, bytes, request_digest });
        }
        Self::encode_admitted(request, limits, io)
    }
    pub(super) fn encode_admitted(request: ModelRequest, limits: Limits, io: Option<&PreparationIo>) -> Result<Self> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(io) = io.filter(|io| io.original_native_work().is_some()) { io.retain_owned((size_of::<Self>() - size_of::<ModelRequest>()) as u64)?; }
        request.validate(limits, io)?;
        #[cfg(feature = "filesystem")]
        let bytes_hold;
        let bytes: acyclic_stream::OwnedBytes = match io {
            #[cfg(feature = "filesystem")]
            Some(io) => io.encode(&request, io.remaining()?.bytes_encoded, true)?,
            _ => crate::contract::canonical_json_bytes(&request)?.into(),
        };
        #[cfg(feature = "filesystem")]
        { bytes_hold = io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten(); }
        let request_digest = hash_bytes(&bytes, io)?;
        #[cfg(feature = "filesystem")]
        if let Some(hold) = bytes_hold { hold.into_retained(); }
        Ok(Self { request, bytes, request_digest })
    }
    /// Admitted values for provider-specific encoding.
    #[must_use]
    pub fn request(&self) -> &ModelRequest { &self.request }
    /// Exact canonical provider-neutral bytes.
    #[must_use]
    pub fn bytes(&self) -> &[u8] { &self.bytes }
    /// BLAKE3 digest of the exact request bytes.
    #[must_use]
    pub const fn request_digest(&self) -> [u8; 32] { self.request_digest }
    /// Additive token bound including every pinned file.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn serialized_token_upper_bound_with_native_work(&self, work: &NativeMaterializationWork)
        -> Result<(crate::context::ModelTokenCount, NativeAllocation)> {
        let io = PreparationIo::from_native_work(work)?;
        io.retain_owned(size_of::<crate::context::ModelTokenCount>() as u64)?;
        let mut message_tokens = Vec::new();
        for message in &self.request.messages {
            charge_items(Some(&io), 1)?;
            let _hold;
            let bytes = io.encode(message, io.remaining()?.bytes_encoded, false)?;
            _hold = io.guard_bytes(&bytes)?;
            let mut tokens = bytes.len() as u64;
            for file in message.content.file_refs() {
                charge_items(Some(&io), 1)?;
                tokens = tokens.checked_add(file.descriptor().byte_length()).ok_or_else(|| Error::Invalid("deterministic token count overflows".into()))?;
            }
            let tokens = u32::try_from(tokens).map_err(|_| Error::Invalid("deterministic token count exceeds u32".into()))?;
            crate::executor::stock_vec_push(&mut message_tokens, tokens, Some(&io))?;
        }
        let fixed_tokens = u32::try_from(self.bytes.len()).map_err(|_| Error::Invalid("deterministic framing exceeds u32".into()))?;
        let count = crate::context::ModelTokenCount { request_digest: self.request_digest, fixed_tokens, message_tokens };
        let retained = add(size_of::<crate::context::ModelTokenCount>() as u64, slots(count.message_tokens.capacity(), size_of::<u32>())?)?;
        let custody = io.into_native_custody(retained)?;
        Ok((count, custody))
    }
}
