//! Fixed protocol configuration: no dynamic root keys or map-dependent order.
//!
//! Root order is this table's order. Every stored value is canonical (all
//! nested objects sorted) at every write, so encoded bytes never depend on
//! whether `serde_json` was unified with `preserve_order`.
use crate::{Family, RenderError, ToolDeclarations};
use serde::Serialize;
use serde_json::{Value, value::RawValue};

macro_rules! fields {
    ($($field:ident => $name:literal),+ $(,)?) => {
        /// One root request field. Each family populates only its own fields.
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        pub enum Field { $(#[doc = concat!("Root field `", $name, "`.")] $field),+ }
        const NAMES: &[(Field, &str)] = &[$((Field::$field, $name)),+];
    };
}

fields! {
    Model => "model",
    Stream => "stream",
    StreamOptions => "stream_options",
    Include => "include",
    Store => "store",
    Instructions => "instructions",
    ParallelToolCalls => "parallel_tool_calls",
    System => "system",
    SystemInstruction => "systemInstruction",
    PromptCacheKey => "prompt_cache_key",
    CacheControl => "cache_control",
    Reasoning => "reasoning",
    ReasoningEffort => "reasoning_effort",
    Thinking => "thinking",
    OutputConfig => "output_config",
    Text => "text",
    Personality => "personality",
    Conversation => "conversation",
    PreviousResponseId => "previous_response_id",
    Truncation => "truncation",
    AddGenerationPrompt => "add_generation_prompt",
    // Call controls follow the conversation, in the one trailer encoder.
    MaxCompletionTokens => "max_completion_tokens",
    MaxOutputTokens => "max_output_tokens",
    MaxTokens => "max_tokens",
    Temperature => "temperature",
    TopP => "top_p",
    Seed => "seed",
    GenerationConfig => "generationConfig",
    ToolChoice => "tool_choice",
    ToolConfig => "toolConfig",
}

const TRAILER: usize = Field::MaxCompletionTokens as usize;

impl Field {
    /// The slot with this root key, if the schema has one.
    pub fn named(name: &str) -> Option<Self> {
        NAMES
            .iter()
            .find_map(|(field, key)| (*key == name).then_some(*field))
    }
}

/// Slots are the finite protocol schema, not a bound on caller data. Tool
/// declarations are encoded once, between the header fields and the
/// conversation, from the profile's borrowed schema views.
#[derive(Clone, Debug)]
pub struct Fields {
    values: [Option<Value>; NAMES.len()],
    tools: Option<Box<RawValue>>,
    summary_tools: Option<Box<RawValue>>,
    server_tools: bool,
}

impl PartialEq for Fields {
    fn eq(&self, other: &Self) -> bool {
        self.values == other.values
            && self.tools.as_deref().map(RawValue::get) == other.tools.as_deref().map(RawValue::get)
            && self.summary_tools.as_deref().map(RawValue::get)
                == other.summary_tools.as_deref().map(RawValue::get)
    }
}

impl Default for Fields {
    fn default() -> Self {
        Self {
            values: std::array::from_fn(|_| None),
            tools: None,
            summary_tools: None,
            server_tools: false,
        }
    }
}

impl Fields {
    /// Stores a canonical copy of `value` in its fixed slot.
    pub fn insert(&mut self, field: Field, mut value: Value) {
        value.sort_all_objects();
        self.values[field as usize] = Some(value);
    }

    /// Clears one slot.
    pub fn remove(&mut self, field: Field) {
        self.values[field as usize] = None;
    }

    /// The stored value of one slot.
    pub fn get(&self, field: Field) -> Option<&Value> {
        self.values[field as usize].as_ref()
    }

    /// Edits one present slot in place and restores its canonical order.
    pub fn update<R>(&mut self, field: Field, edit: impl FnOnce(&mut Value) -> R) -> Option<R> {
        let value = self.values[field as usize].as_mut()?;
        let result = edit(value);
        value.sort_all_objects();
        Some(result)
    }

    /// Keeps only the named root fields, including `tools`, for another
    /// endpoint that accepts a subset of the same request.
    pub fn retain(&mut self, keep: impl Fn(&str) -> bool) {
        for (field, name) in NAMES {
            if !keep(name) {
                self.values[*field as usize] = None;
            }
        }
        if !keep("tools") {
            self.tools = None;
            self.summary_tools = None;
            self.server_tools = false;
        }
    }

    /// Encodes each selected declaration list once, without a staging tree.
    /// An explicit summary choice encodes a second list only when it differs.
    ///
    /// # Errors
    /// The declarations' family must match `family`, and encoding must succeed.
    pub fn set_tools<P: Serialize>(
        &mut self,
        family: Family,
        tools: &ToolDeclarations<'_, P>,
    ) -> Result<(), RenderError> {
        if tools.family() != family {
            return Err(RenderError::InvalidItem(
                "tool declarations target another protocol",
            ));
        }
        let encoded = serde_json::to_string(tools).map_err(RenderError::Json)?;
        let summary = tools
            .summary_tools()
            .map(|tools| {
                let encoded = serde_json::to_string(&tools).map_err(RenderError::Json)?;
                RawValue::from_string(encoded).map_err(RenderError::Json)
            })
            .transpose()?;
        self.tools = Some(RawValue::from_string(encoded).map_err(RenderError::Json)?);
        self.summary_tools = summary;
        self.server_tools = tools.has_server_tools();
        Ok(())
    }

    /// Whether the encoded declarations include a provider-owned server tool.
    pub(crate) const fn server_tools(&self) -> bool {
        self.server_tools
    }

    /// The encoded tool declarations, if any.
    pub fn tools(&self) -> Option<&RawValue> {
        self.tools.as_deref()
    }

    /// A separately admitted summary declaration list, only when tools differ.
    pub(crate) fn summary_tools(&self) -> Option<&RawValue> {
        self.summary_tools.as_deref()
    }

    fn entries(&self, start: usize, end: usize) -> impl Iterator<Item = (&'static str, &Value)> {
        NAMES[start..end]
            .iter()
            .filter_map(|(field, name)| self.get(*field).map(|value| (*name, value)))
    }

    /// Fields frozen in the epoch header, before the conversation.
    pub fn header(&self) -> impl Iterator<Item = (&'static str, &Value)> {
        self.entries(0, TRAILER)
    }

    /// Per-call controls, encoded after the conversation by [`crate::Tail`].
    pub fn call_controls(&self) -> impl Iterator<Item = (&'static str, &Value)> {
        self.entries(TRAILER, NAMES.len())
    }
}
