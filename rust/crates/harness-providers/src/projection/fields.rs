//! Fixed protocol configuration: no dynamic root keys or map-dependent order.
use serde_json::Value;

macro_rules! fields {
    ($($field:ident => $name:literal),+ $(,)?) => {
        #[derive(Clone, Copy)]
        pub(super) enum Field { $($field),+ }
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
    Tools => "tools",
    // Call controls occupy the final slots and share one encoder.
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

/// Slots are the finite protocol schema, not a bound on caller data. Each
/// family populates only its own fields. Values retain their original owners.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct Fields([Option<Value>; NAMES.len()]);

impl Default for Fields {
    fn default() -> Self {
        Self(std::array::from_fn(|_| None))
    }
}

impl Fields {
    pub(super) fn insert(&mut self, field: Field, value: Value) {
        self.0[field as usize] = Some(value);
    }
    pub(super) fn remove(&mut self, field: Field) {
        self.0[field as usize] = None;
    }
    pub(super) fn get_mut(&mut self, field: Field) -> Option<&mut Value> {
        self.0[field as usize].as_mut()
    }
    fn entries(&self, start: usize, end: usize) -> impl Iterator<Item = (&'static str, &Value)> {
        NAMES[start..end].iter().filter_map(|(field, name)| {
            self.0[*field as usize].as_ref().map(|value| (*name, value))
        })
    }
    pub(super) fn header(&self) -> impl Iterator<Item = (&'static str, &Value)> {
        self.entries(0, Field::MaxCompletionTokens as usize)
    }
    pub(super) fn call_controls(&self) -> impl Iterator<Item = (&'static str, &Value)> {
        self.entries(Field::MaxCompletionTokens as usize, NAMES.len())
    }
    #[cfg(all(test, not(target_arch = "wasm32")))]
    pub(super) fn iter(&self) -> impl Iterator<Item = (&'static str, &Value)> {
        self.entries(0, NAMES.len())
    }
    /// Freeze nested configuration ordering once at projection admission. No
    /// request-sized buffer or second encoding is constructed. Without
    /// preserve_order, serde_json's objects are already sorted and this is free.
    pub(super) fn freeze(&mut self) {
        for value in self.0.iter_mut().flatten() {
            value.sort_all_objects();
        }
    }
    #[cfg(all(test, not(target_arch = "wasm32")))]
    pub(super) fn from_values(values: serde_json::Map<String, Value>) -> Self {
        let mut fields = Self::default();
        for (name, value) in values {
            let field = NAMES
                .iter()
                .find(|(_, key)| *key == name)
                .expect("generated input must use the protocol schema")
                .0;
            fields.insert(field, value);
        }
        fields.freeze();
        fields
    }
}
