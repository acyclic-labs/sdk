use super::*;
use std::collections::BTreeMap;
use std::mem::size_of;
use std::sync::atomic::{AtomicUsize, Ordering};

mod admission;
mod continuation;
mod encoding;
pub use encoding::InputAnchor;

struct BoundTool<'a> {
    definition: &'a crate::tool::ToolDefinition,
    native: bool,
}

/// Complete immutable provider-neutral model request.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct ModelRequest {
    /// Generate a summary from the context preceding the final user instruction.
    /// Declarations stay in the header; the renderer disables invocation in the trailer.
    pub summary: bool,
    /// Pinned model selection.
    pub model: Model,
    /// Ordered reconstructable context.
    pub messages: Vec<ModelMessage>,
    /// Expose frozen catalog declarations and announce dynamic tool changes.
    pub compose_tools: bool,
    /// Registered tool definitions, including schemas retained for history validation.
    pub tools: Vec<crate::tool::ToolDefinition>,
    /// Sampling values retained in the original request digest.
    #[serde(
        default,
        skip_serializing_if = "crate::model::ModelSampling::is_default"
    )]
    pub sampling: crate::model::ModelSampling,
}
impl ModelRequest {
    /// Separates the transient summary instruction from its selected context.
    /// Ordinary requests expose the whole conversation without an instruction.
    pub fn split_summary_instruction(&self) -> Result<(&[ModelMessage], Option<&ModelContent>)> {
        if !self.summary {
            return Ok((&self.messages, None));
        }
        let Some((instruction, messages)) = self.messages.split_last() else {
            return Err(Error::Invalid(
                "summary request lacks its final user instruction".into(),
            ));
        };
        if instruction.role != ModelRole::User {
            return Err(Error::Invalid(
                "summary request must end with its user instruction".into(),
            ));
        }
        Ok((messages, Some(&instruction.content)))
    }

    /// Native declarations sent to providers. Dynamic code capabilities retain
    /// their validation schemas here and are announced through tools.changed.
    pub fn model_tools(&self) -> impl Iterator<Item = &crate::tool::ToolDefinition> {
        self.tools.iter().filter(move |tool| {
            !self.compose_tools || crate::tools::catalog::is_native_declaration(&tool.name)
        })
    }

    pub(super) fn validate(&mut self, limits: Limits, io: Option<&PreparationIo>) -> Result<()> {
        self.validate_with_instruction_budget(limits, io)
            .map(|_| ())
    }

    fn validate_with_instruction_budget(
        &mut self,
        limits: Limits,
        io: Option<&PreparationIo>,
    ) -> Result<Option<u64>> {
        #[cfg(not(target_arch = "wasm32"))]
        let _span = tracing::debug_span!("acyclic.harness.request.validate").entered();
        with_history_io(io, |work| limits.validate(work))?;
        charge_items(io, 1)?;
        charge_items(
            io,
            (self.model.provider.len() as u64)
                .saturating_add(self.model.name.len() as u64)
                .saturating_add(self.model.revision.len() as u64),
        )?;
        self.model.validate()?;
        self.sampling.validate()?;
        if self.messages.is_empty()
            || limits
                .context_messages
                .is_some_and(|maximum| self.messages.len() > maximum.get())
        {
            return Err(Error::Invalid("model context count is invalid".into()));
        }
        let mut remaining = limits.render_bytes;
        reserve_instruction_bodies(&self.messages, &mut remaining, io)?;
        Self::validate_messages(
            &mut self.messages,
            &self.tools,
            self.compose_tools,
            false,
            limits,
            io,
            |tool| with_history_io(io, |work| tool.validate(work)),
        )?;
        Ok(remaining)
    }

    fn validate_messages(
        messages: &mut [ModelMessage],
        definitions: &[crate::tool::ToolDefinition],
        compose_tools: bool,
        retained_environment: bool,
        limits: Limits,
        io: Option<&PreparationIo>,
        validate_declaration: impl Fn(&crate::tool::ToolDefinition) -> Result<()>,
    ) -> Result<()> {
        let _maps = if let Some(io) = io {
            let mut calls = 0u64;
            for message in messages.iter() {
                charge_items(Some(io), 1)?;
                calls = calls
                    .checked_add(message.content.parts().len() as u64)
                    .ok_or_else(|| Error::Invalid("model validation slot count overflow".into()))?;
            }
            let bytes = (definitions.len() as u64)
                .checked_mul(size_of::<(&str, BoundTool<'_>)>() as u64)
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
        for tool in definitions {
            charge_items(io, 1)?;
            validate_declaration(tool)?;
            if tools
                .insert(
                    tool.name.as_str(),
                    BoundTool {
                        definition: tool,
                        native: !compose_tools
                            || crate::tools::catalog::is_native_declaration(&tool.name),
                    },
                )
                .is_some()
            {
                return Err(Error::Invalid("model tool definition is repeated".into()));
            }
        }
        let mut pending = BTreeMap::new();
        // Each notice table supplies the schemas in force at its original
        // position. Revoking a tool cannot invalidate its earlier results.
        let mut tables = Vec::new();
        let mut native_fragments = String::new();
        let mut dynamic_fragments = String::new();
        let mut native_open = false;
        let mut dynamic_open = false;
        for (index, message) in messages.iter().enumerate() {
            if let Some((notices, _)) = message.content.notice_parts() {
                for notice in notices.iter().filter(|notice| {
                    matches!(notice.kind.as_str(), "tools.changed" | "tools.bound")
                        && notice.source == "tools"
                }) {
                    let (fragments, open) = if notice.kind == "tools.bound" {
                        (&mut native_fragments, &mut native_open)
                    } else {
                        (&mut dynamic_fragments, &mut dynamic_open)
                    };
                    let offset = notice
                        .body
                        .get("offset")
                        .and_then(Value::as_u64)
                        .ok_or_else(|| Error::Invalid("tool notice lacks offset".into()))?;
                    if offset == 0 {
                        if *open {
                            return Err(Error::Invalid("tool notice table is incomplete".into()));
                        }
                        fragments.clear();
                        *open = true;
                    } else if !*open {
                        return Err(Error::Invalid(
                            "tool notice continuation has no start".into(),
                        ));
                    }
                    if offset != fragments.len() as u64 {
                        return Err(Error::Invalid(
                            "tool notice fragments are discontinuous".into(),
                        ));
                    }
                    let fragment = notice
                        .body
                        .get("json")
                        .and_then(Value::as_str)
                        .ok_or_else(|| Error::Invalid("tool notice lacks schemas".into()))?;
                    crate::executor::stock_string_push(
                        fragments,
                        fragment,
                        u64::MAX,
                        "tool notice schema size overflows",
                        io,
                    )?;
                    if notice.body.get("complete").and_then(Value::as_bool) == Some(true) {
                        *open = false;
                        let table = crate::executor::decode_tool_table(
                            fragments.as_bytes(),
                            io,
                            crate::wire::ErrorCode::Storage,
                        )?;
                        crate::executor::stock_vec_push(
                            &mut tables,
                            (index, notice.kind == "tools.bound", table),
                            io,
                        )?;
                    }
                }
            }
        }
        if native_open || dynamic_open {
            return Err(Error::Invalid("tool notice table is incomplete".into()));
        }

        let _historical_bindings = if let Some(io) = io {
            // A parent's header can contain more bindings than this worker's.
            // Fund the largest pair of retained tables, not their history sum.
            let native = tables
                .iter()
                .filter(|(_, native, _)| *native)
                .map(|(_, _, table)| table.len())
                .max()
                .unwrap_or(0);
            let dynamic = tables
                .iter()
                .filter(|(_, native, _)| !*native)
                .map(|(_, _, table)| table.len())
                .max()
                .unwrap_or(0);
            let additional = native
                .saturating_add(dynamic)
                .saturating_sub(definitions.len());
            let bytes = (additional as u64)
                .checked_mul(size_of::<(&str, BoundTool<'_>)>() as u64)
                .ok_or_else(|| {
                    Error::Invalid("historical tool binding ownership overflow".into())
                })?;
            Some(io.reserve_decode(bytes)?)
        } else {
            None
        };
        let mut original = true;
        let mut dynamic_announced = retained_environment;
        for (index, message) in messages.iter_mut().enumerate() {
            for (_, native, table) in tables.iter().filter(|(at, _, _)| *at == index) {
                if *native {
                    if original && !dynamic_announced {
                        tools.clear();
                    } else {
                        tools.retain(|_, binding| !binding.native);
                    }
                    original = false;
                } else {
                    dynamic_announced = true;
                    tools.retain(|_, binding| binding.native);
                }
                for tool in table {
                    validate_declaration(tool)?;
                    tools.insert(
                        tool.name.as_str(),
                        BoundTool {
                            definition: tool,
                            native: *native,
                        },
                    );
                }
            }
            charge_items(io, 1)?;
            with_history_io(io, |work| message.content.validate_limits(limits, work))?;
            let standalone = matches!(message.content, ModelContent::Part(_))
                || message.content.notice_parts().is_some();
            let empty = message.content.parts().is_empty();
            let parts = match &mut message.content {
                ModelContent::Text(_) => &mut [][..],
                ModelContent::Part(part) => std::slice::from_mut(part),
                ModelContent::Parts(parts) => parts.as_mut_slice(),
            };
            for part in parts {
                charge_items(io, 1)?;
                Self::validate_part(message.role, part, standalone, &tools, &mut pending, io)?;
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
        if let ToolResultContent::Json { value } = content {
            let _hold;
            let mut envelope = match io {
                Some(io) => {
                    let (envelope, hold) = io.decode(
                        std::marker::PhantomData::<Value>,
                        br#"{"kind":"json","value":null}"#,
                        Storage,
                    )?;
                    _hold = Some(hold);
                    envelope
                }
                _ => {
                    {
                        _hold = None;
                    }
                    serde_json::json!({"kind":"json", "value":null})
                }
            };
            // Only the fixed envelope is new. The admitted payload moves into
            // it for validation and returns even when its schema refuses it.
            std::mem::swap(value, &mut envelope["value"]);
            let result = tool
                .check_projection_schema(&envelope, io)
                .and_then(|result| result);
            std::mem::swap(value, &mut envelope["value"]);
            return result;
        }
        match io {
            Some(io) => {
                let _hold;
                let bytes = io.encode(content, io.remaining()?.bytes_encoded, false)?;
                _hold = io.guard_bytes(&bytes)?;
                let envelope = io.decode(std::marker::PhantomData::<Value>, &bytes, Storage)?;
                tool.check_projection_schema(&envelope.0, Some(io))??;
            }
            _ => {
                let envelope = serde_json::to_value(content)
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                tool.check_projection_schema(&envelope, None)??;
            }
        }
        Ok(())
    }
    fn bound_tool<'a>(
        name: &str,
        tools: &BTreeMap<&str, BoundTool<'a>>,
    ) -> Option<&'a crate::tool::ToolDefinition> {
        match name.rsplit_once('@') {
            Some((name, revision)) => tools
                .get(name)
                .map(|binding| binding.definition)
                .filter(|tool| tool.revision == revision),
            None => tools.get(name).map(|binding| binding.definition),
        }
    }
    fn validate_part<'a>(
        role: ModelRole,
        part: &'a mut ModelContentPart,
        standalone: bool,
        tools: &BTreeMap<&str, BoundTool<'_>>,
        pending: &mut BTreeMap<&'a String, PendingCall<'a>>,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        match part {
            ModelContentPart::Notices { notices } => {
                if !standalone
                    || !pending.is_empty()
                    || notices.is_empty()
                    || matches!(role, ModelRole::Assistant | ModelRole::Tool)
                {
                    return Err(Error::Invalid(
                        "notices require an event boundary with only file attachments".into(),
                    ));
                }
            }
            ModelContentPart::Configuration { effort, .. } => {
                if role != ModelRole::User
                    || !standalone
                    || !pending.is_empty()
                    || *effort == acyclic_model_render::Effort::Adaptive
                {
                    return Err(Error::Invalid(
                        "configuration update must be a concrete user-boundary item".into(),
                    ));
                }
            }
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
                // One admission path for every result, whatever its source: the paired call's
                // binding and arguments are always checked, and they decide which results it
                // admits. A bound call with valid arguments admits a projected result held to the
                // tool's projection contract, or a typed error. A call the harness must refuse
                // (unbound, or arguments that violate the schema) admits only a typed error. The
                // error's own shape was checked with the message (`validate_limits`).
                let tool = match Self::bound_tool(name, tools) {
                    Some(tool) => crate::executor::with_history_io(io, |work| {
                        crate::executor::check_json_schema_value(
                            &tool.input_schema,
                            arguments,
                            "model tool arguments",
                            work,
                        )
                    })?
                    .map(|()| tool),
                    None => Err(Error::Invalid("model tool call is unbound".into())),
                };
                match (tool, content) {
                    (_, ToolResultContent::Error { .. }) => {}
                    (Ok(tool), content) => Self::validate_tool_result(tool, content, io)?,
                    (Err(refused), _) => return Err(refused),
                }
            }
            _ if role == ModelRole::Tool => {
                return Err(Error::Invalid("tool role requires a paired result".into()));
            }
            _ => {}
        }
        Ok(())
    }
}

/// A call awaiting its paired result: name and the arguments the model emitted.
type PendingCall<'a> = (&'a String, &'a Value);

/// Admitted values, exact canonical request bytes and their digest.
#[derive(Debug)]
pub struct PreparedModelRequest {
    request: ModelRequest,
    bytes: acyclic_stream::OwnedBytes,
    request_digest: [u8; 32],
    context_window: Option<crate::context::ContextWindow>,
    replay_key: std::sync::OnceLock<[u8; 32]>,
    projection_nodes: AtomicUsize,
    admitted_limits: Limits,
    instruction_remaining: Option<u64>,
    encoding: Vec<encoding::EncodingPrefix>,
    input: encoding::Input,
    content_admission: Option<admission::ContentAdmission>,
    resident_payload: Option<u64>,
}

impl Clone for PreparedModelRequest {
    fn clone(&self) -> Self {
        Self {
            request: self.request.clone(),
            bytes: self.bytes.clone(),
            request_digest: self.request_digest,
            context_window: self.context_window,
            replay_key: self.replay_key.clone(),
            projection_nodes: AtomicUsize::new(self.projection_nodes.load(Ordering::Relaxed)),
            admitted_limits: self.admitted_limits,
            instruction_remaining: self.instruction_remaining,
            encoding: self.encoding.clone(),
            input: self.input.clone(),
            content_admission: None,
            // Vec and String clones need not retain the producer's capacity.
            // The next continuation inventories its new storage once.
            resident_payload: None,
        }
    }
}

impl PreparedModelRequest {
    pub(crate) fn retain_context_window(
        &mut self,
        window: crate::context::ContextWindow,
    ) -> Result<()> {
        window.validate()?;
        self.context_window = Some(window);
        Ok(())
    }

    /// Original verified admission, separate from provider-visible request bytes.
    pub(crate) const fn context_window(&self) -> Option<crate::context::ContextWindow> {
        self.context_window
    }

    /// Semantic replay identity, retained after its first successful use.
    /// Warm consumers need no decode, hash, payload copy or history walk;
    /// requests outside replay pay no hash work. Concurrent cold consumers may
    /// compute independently, so a failed producer cannot leave a waiter behind.
    pub fn replay_key(&self) -> Result<[u8; 32]> {
        if let Some(key) = self.replay_key.get() {
            return Ok(*key);
        }
        let key = super::recorded::replay_key_of(&self.request)?;
        let _ = self.replay_key.set(key);
        Ok(key)
    }

    pub(crate) fn take_content_admission(
        &mut self,
        executor: crate::executor::ExecutionJournalCacheId,
        journal: crate::executor::ExecutionJournalCacheId,
    ) -> admission::ContentAdmission {
        let mut admission = self
            .content_admission
            .take()
            .unwrap_or_else(|| admission::ContentAdmission::new(executor, journal));
        admission.reset_binding(executor, journal);
        admission
    }

    pub(crate) fn retain_content_admission(&mut self, admission: admission::ContentAdmission) {
        self.content_admission = Some(admission);
    }

    pub(crate) const fn admitted_limits(&self) -> Limits {
        self.admitted_limits
    }
    pub(super) fn encoding_owned_bytes(&self) -> Result<u64> {
        add(
            slots(
                self.encoding.capacity(),
                size_of::<encoding::EncodingPrefix>(),
            )?,
            self.content_admission
                .as_ref()
                .map_or(Ok(0), admission::ContentAdmission::owned_bytes)?,
        )
    }

    pub(crate) fn cached_message_digests(
        &self,
        prefix: Option<usize>,
        io: Option<&PreparationIo>,
    ) -> Result<Option<encoding::MessageDigests>> {
        self.encoding
            .first()
            .map_or(Ok(None), |state| state.digests(prefix, io))
    }
    pub(super) fn resident_payload_bytes(&self) -> Result<u64> {
        match self.resident_payload {
            Some(bytes) => Ok(bytes),
            None => self.request.retained_payload_bytes(),
        }
    }
    pub(crate) fn preparation_owned_bytes(&self, io: &PreparationIo) -> Result<u64> {
        let bytes = add(
            add(size_of::<Self>() as u64, self.resident_payload_bytes()?)?,
            self.encoding_owned_bytes()?,
        )?;
        #[cfg(not(target_arch = "wasm32"))]
        if io.original_native_work().is_some() {
            return Ok(bytes);
        }
        let _ = io;
        add(bytes, self.bytes.retained_capacity().unwrap_or(0) as u64)
    }

    /// Decodes and verifies exact canonical input on the optional ledger.
    pub fn decode(bytes: &[u8], limits: Limits, io: Option<&PreparationIo>) -> Result<Self> {
        let request: ModelRequest = match io {
            Some(io) => crate::filesystem::retain_decoded(io.decode(
                std::marker::PhantomData::<ModelRequest>,
                bytes,
                Storage,
            )?)?,
            _ => crate::contract::json_from_slice(bytes)
                .map_err(|error| Error::Invalid(error.to_string()))?,
        };
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
            let retained_payload = request.retained_payload_bytes()?;
            request_hold = io.reserve_decode(add(size_of::<Self>() as u64, retained_payload)?)?;
            request.split_summary_instruction()?;
            let instruction_remaining =
                request.validate_with_instruction_budget(limits, Some(io))?;
            let request_digest = hash_bytes(&bytes, Some(io))?;
            if let Some(hold) = bytes_hold {
                hold.into_retained()?;
            }
            request_hold.into_retained()?;
            let resident_payload = Some(retained_payload);
            return Ok(Self {
                request,
                bytes,
                request_digest,
                context_window: None,
                replay_key: std::sync::OnceLock::new(),
                projection_nodes: AtomicUsize::new(0),
                admitted_limits: limits,
                instruction_remaining,
                encoding: Vec::new(),
                input: Default::default(),
                content_admission: None,
                resident_payload,
            });
        }
        Self::encode_admitted(request, limits, io)
    }
    pub(super) fn encode_admitted(
        mut request: ModelRequest,
        limits: Limits,
        io: Option<&PreparationIo>,
    ) -> Result<Self> {
        #[cfg(not(target_arch = "wasm32"))]
        let _span = tracing::debug_span!("acyclic.harness.request.prepare").entered();
        Self::begin_preparation(io)?;
        let instruction_remaining = request.validate_with_instruction_budget(limits, io)?;
        Self::encode_validated(request, limits, instruction_remaining, io)
    }

    fn record_preparation() {
        #[cfg(all(any(test, feature = "test-support"), not(target_arch = "wasm32")))]
        acyclic_differential::record(("prepare request", 1u64));
    }

    fn begin_preparation(io: Option<&PreparationIo>) -> Result<()> {
        Self::record_preparation();
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io.filter(|io| io.original_native_work().is_some()) {
            io.retain_owned((size_of::<Self>() - size_of::<ModelRequest>()) as u64)?;
        }
        let _ = io;
        Ok(())
    }

    fn encode_validated(
        request: ModelRequest,
        limits: Limits,
        instruction_remaining: Option<u64>,
        io: Option<&PreparationIo>,
    ) -> Result<Self> {
        request.split_summary_instruction()?;
        let bytes_hold;
        let bytes: acyclic_stream::OwnedBytes = match io {
            Some(io) => io.encode(&request, io.remaining()?.bytes_encoded, true)?,
            _ => crate::contract::canonical_json_bytes(&request)?.into(),
        };
        {
            bytes_hold = io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten();
        }
        let request_digest = hash_bytes(&bytes, io)?;
        if let Some(hold) = bytes_hold {
            hold.into_retained()?;
        }
        let resident_payload = Some(request.retained_payload_bytes()?);
        Ok(Self {
            request,
            bytes,
            request_digest,
            context_window: None,
            replay_key: std::sync::OnceLock::new(),
            projection_nodes: AtomicUsize::new(0),
            admitted_limits: limits,
            instruction_remaining,
            encoding: Vec::new(),
            input: Default::default(),
            content_admission: None,
            resident_payload,
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
    /// Exact canonical provider-neutral bytes.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    /// Exact byte length and conservative JSON allocation nodes for projection.
    /// Sequential callers scan the immutable bytes at most once. Warm reads are
    /// fixed work; concurrent cold callers may each scan, without waiting.
    #[must_use]
    pub fn projection_shape(&self) -> (u64, u64) {
        let mut nodes = self.projection_nodes.load(Ordering::Relaxed);
        if nodes == 0 {
            nodes = crate::executor::json::encoded_json_shape(&self.bytes).1;
            // Every publisher writes the same fact about these immutable bytes;
            // no other storage or authority is published through this cache.
            self.projection_nodes.store(nodes, Ordering::Relaxed);
        }
        (self.bytes.len() as u64, nodes as u64)
    }
    /// BLAKE3 digest of the exact request bytes.
    #[must_use]
    pub const fn request_digest(&self) -> [u8; 32] {
        self.request_digest
    }
    /// Additive token bound including every pinned file. Private validation
    /// metadata is retained for replay but does not consume the model window.
    pub fn serialized_token_upper_bound(
        &self,
        work: acyclic_fs::NativeWork<'_>,
    ) -> Result<(crate::context::ModelTokenCount, acyclic_fs::NativeCustody)> {
        #[cfg(not(target_arch = "wasm32"))]
        let observer = work.map(PreparationIo::from_native_work).transpose()?;
        #[cfg(not(target_arch = "wasm32"))]
        let io = observer.as_ref();
        #[cfg(target_arch = "wasm32")]
        let io: Option<&PreparationIo> = None;
        let _ = work;
        if let Some(io) = io {
            io.retain_owned(size_of::<crate::context::ModelTokenCount>() as u64)?;
        }
        let mut message_tokens = Vec::new();
        let mut fixed_bytes = self.bytes.len() as u64;
        fn encoded_bytes<T: Serialize>(value: &T, io: Option<&PreparationIo>) -> Result<u64> {
            let maximum = io
                .map(|io| io.remaining().map(|budget| budget.bytes_encoded))
                .transpose()?;
            with_history_io(io, |work| {
                crate::executor::json::json_byte_length_at_depth(value, maximum, 256, work)
            })
        }
        if self.request.compose_tools {
            for tool in &self.request.tools {
                charge_items(io, 1)?;
                if !crate::tools::catalog::is_native_declaration(&tool.name) {
                    // Dynamic schemas reach the model through tools.changed,
                    // not through the native declarations as well. Keep the
                    // original array framing as a conservative constant bound.
                    fixed_bytes = fixed_bytes
                        .checked_sub(encoded_bytes(tool, io)?)
                        .ok_or_else(|| {
                            Error::Invalid("tool framing exceeds prepared request".into())
                        })?;
                }
            }
        }
        for message in &self.request.messages {
            charge_items(io, 1)?;
            let bytes = match io {
                Some(io) => {
                    let maximum = io.remaining()?.bytes_encoded;
                    with_history_io(Some(io), |work| {
                        crate::executor::json::json_byte_length_at_depth(
                            message,
                            Some(maximum),
                            256,
                            work,
                        )
                    })?
                }
                _ => crate::executor::json::json_byte_length(message, None, &mut None)?,
            };
            fixed_bytes = fixed_bytes
                .checked_sub(bytes)
                .ok_or_else(|| Error::Invalid("message framing exceeds prepared request".into()))?;
            let mut tokens = bytes;
            if let Some((notices, attachments)) = message.content.notice_parts() {
                if attachments.is_empty() && notices.iter().all(|notice| !notice.model_visible()) {
                    tokens = 0;
                } else {
                    for notice in notices.iter().filter(|notice| !notice.model_visible()) {
                        tokens =
                            tokens
                                .checked_sub(encoded_bytes(notice, io)?)
                                .ok_or_else(|| {
                                    Error::Invalid("notice framing exceeds prepared message".into())
                                })?;
                    }
                }
            }
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
        #[cfg(not(target_arch = "wasm32"))]
        let custody = observer
            .map(|io| {
                io.into_native_custody(add(
                    size_of::<crate::context::ModelTokenCount>() as u64,
                    slots(count.message_tokens.capacity(), size_of::<u32>())?,
                )?)
            })
            .transpose()?;
        #[cfg(target_arch = "wasm32")]
        let custody = None;
        Ok((count, custody))
    }
}

#[cfg(test)]
#[path = "request_tokens_tests.rs"]
mod tokens_tests;

#[cfg(test)]
mod inventory_tests;
