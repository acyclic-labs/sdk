use super::*;

/// One provider-neutral prompt item.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ModelMessage {
    /// Semantic prompt role.
    pub role: ModelRole,
    /// Transient content; canonical history retains references.
    pub content: ModelContent,
}
/// Closed semantic prompt roles.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum ModelRole {
    /// Host instruction.
    System,
    /// Human input.
    User,
    /// Model output.
    Assistant,
    /// Paired tool result.
    Tool,
}

/// Transient model-visible text or structured parts.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant, reason = "retain the public untagged content shape")]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum ModelContent {
    /// Plain text.
    Text(String),
    /// One structured part.
    Part(ModelContentPart),
    /// Ordered structured parts.
    Parts(Vec<ModelContentPart>),
}

/// Structured content requiring an explicit provider projection.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[allow(clippy::large_enum_variant, reason = "immutable file references remain inline")]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum ModelContentPart {
    /// Plain text.
    Text {
        /// Model-visible text.
        text: String,
    },
    /// Immutable file with an explicit resolution policy.
    File {
        /// Immutable file identity.
        #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
        file: FileRef,
        /// Resolution policy.
        policy: FileProjectionPolicy,
    },
    /// Assistant tool request.
    ToolCall {
        /// Stable call identity.
        #[serde(alias = "callId")]
        call_id: String,
        /// Registered tool name.
        name: String,
        /// Schema-defined arguments.
        #[cfg_attr(feature = "wasm", tsify(type = "WasmModelJsonValue"))]
        arguments: Value,
    },
    /// Paired tool result.
    ToolResult {
        /// Matching call identity.
        #[serde(alias = "callId")]
        call_id: String,
        /// Registered tool name.
        name: String,
        /// Complete projection envelope.
        content: ToolResultContent,
    },
}

/// Nonrecursive text and file data inside a tool result.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[allow(clippy::large_enum_variant, reason = "immutable file references remain inline")]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum ModelDataPart {
    /// Plain text.
    Text {
        /// Exact text.
        text: String,
    },
    /// Immutable file with an explicit policy.
    File {
        /// Immutable media identity.
        #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
        file: FileRef,
        /// Resolution policy.
        policy: FileProjectionPolicy,
    },
}

/// Complete model-visible tool projection envelope.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum ToolResultContent {
    /// JSON projection.
    Json {
        /// Schema-defined JSON value.
        #[cfg_attr(feature = "wasm", tsify(type = "WasmModelJsonValue"))]
        value: Value,
    },
    /// Ordered nonrecursive data.
    Parts {
        /// Text and file parts.
        parts: Vec<ModelDataPart>,
    },
}

impl ModelContent {
    /// Structured parts; plain text has no structured parts.
    #[must_use]
    pub fn parts(&self) -> &[ModelContentPart] {
        match self { Self::Text(_) => &[], Self::Part(part) => std::slice::from_ref(part), Self::Parts(parts) => parts }
    }
    /// Validates direct input and projected history against the same limits.
    pub fn validate_limits(&self, limits: Limits, work: &mut Option<&mut dyn HistoryIo>) -> Result<()> {
        history_step(work, 1)?;
        limits.validate(work)?;
        if let Self::Text(text) = self {
            return if text.len() as u64 <= limits.render_bytes { Ok(()) } else { Err(invalid(work, "model text exceeds render limit")) };
        }
        if self.parts().len() > limits.attachments.saturating_add(1) { return Err(invalid(work, "model content exceeds attachment limit")); }
        for part in self.parts() {
            history_step(work, 1)?;
            if let ModelContentPart::ToolCall { name, .. } | ModelContentPart::ToolResult { name, .. } = part {
                history_step(work, name.len() as u64)?;
                if !crate::contract::is_valid_component_label(name) { return Err(invalid(work, "tool name is invalid")); }
            }
            match part {
                ModelContentPart::Text { text } if text.len() as u64 > limits.render_bytes => return Err(invalid(work, "model text exceeds render limit")),
                ModelContentPart::File { file, policy } => policy.validate_file(file, limits, work)?,
                ModelContentPart::ToolCall { arguments, .. } => value_bound(arguments, limits.render_bytes, work)?,
                ModelContentPart::ToolResult { content, .. } => content.validate_limits(limits, work)?,
                ModelContentPart::Text { .. } => {},
            }
        }
        let mut references = 0usize;
        for _ in self.file_refs() {
            history_step(work, 1)?;
            references = references.checked_add(1).ok_or_else(|| invalid(work, "model content reference count overflow"))?;
        }
        if references > limits.attachments.saturating_add(1) { return Err(invalid(work, "model content reference count exceeds attachment limit")); }
        Ok(())
    }
    /// Rejects empty or tool content in a human input.
    pub fn validate_user_input(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<()> {
        history_step(work, 1)?;
        if let Self::Text(text) = self {
            return if text.is_empty() { Err(invalid(work, "user input is empty")) } else { Ok(()) };
        }
        if self.parts().is_empty() { return Err(invalid(work, "user input part count is invalid")); }
        for part in self.parts() {
            history_step(work, 1)?;
            match part {
                ModelContentPart::Text { text } if !text.is_empty() => {},
                ModelContentPart::File { file, .. } => file.validate(work)?,
                _ => return Err(invalid(work, "user input contains an unsupported part")),
            }
        }
        Ok(())
    }
    fn file_parts(&self) -> impl Iterator<Item = (&FileRef, &FileProjectionPolicy)> {
        self.parts().iter().flat_map(|part| {
            let direct = match part { ModelContentPart::File { file, policy } => Some((file, policy)), _ => None };
            let data = match part {
                ModelContentPart::ToolResult { content: ToolResultContent::Parts { parts }, .. } => parts.as_slice(), _ => &[],
            };
            direct.into_iter().chain(data.iter().filter_map(|part| match part {
                ModelDataPart::File { file, policy } => Some((file, policy)), _ => None,
            }))
        })
    }
    pub(crate) fn file_bindings(&self) -> impl Iterator<Item = (&FileRef, Option<&NativeConfigurationBinding>)> {
        self.file_parts().flat_map(|(file, policy)| {
            let binding = match policy { FileProjectionPolicy::Native(policy) => policy.configuration.as_ref(), _ => None };
            std::iter::once((file, None)).chain(binding.map(|binding| (&binding.configuration.content, Some(binding))))
        })
    }
    /// Media references followed by their immutable option references.
    #[must_use]
    pub fn file_refs(&self) -> impl Iterator<Item = &FileRef> { self.file_bindings().map(|(file, _)| file) }
    /// Whether ordinary or paired result data contains native media.
    #[must_use]
    pub fn contains_native_media(&self) -> bool { self.file_parts().any(|(_, policy)| matches!(policy, FileProjectionPolicy::Native(_))) }
    /// Configuration claims for the authenticated host to verify.
    #[must_use]
    pub fn native_configurations(&self) -> impl Iterator<Item = &NativeConfigurationBinding> { self.file_bindings().filter_map(|(_, binding)| binding) }
}

impl ToolResultContent {
    pub(crate) fn validate_limits(&self, limits: Limits, work: &mut Option<&mut dyn HistoryIo>) -> Result<()> {
        history_step(work, 1)?;
        limits.validate(work)?;
        if let Self::Parts { parts } = self {
            if parts.len() > limits.attachments.saturating_add(1) { return Err(invalid(work, "tool result exceeds attachment limit")); }
            let mut references = 0usize;
            for part in parts {
                history_step(work, 1)?;
                match part {
                    ModelDataPart::Text { text } if text.len() as u64 > limits.render_bytes => return Err(invalid(work, "tool result text exceeds render limit")),
                    ModelDataPart::File { file, policy } => {
                        policy.validate_file(file, limits, work)?;
                        let count = 1 + usize::from(matches!(policy, FileProjectionPolicy::Native(policy) if policy.configuration.is_some()));
                        references = references.checked_add(count).ok_or_else(|| invalid(work, "tool result reference count overflow"))?;
                    }
                    ModelDataPart::Text { .. } => {},
                }
            }
            if references > limits.attachments.saturating_add(1) { return Err(invalid(work, "tool result reference count exceeds limit")); }
        }
        preparation::result_bound(self, limits.render_bytes, work)
    }
}
