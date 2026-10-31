//! Gemini's Part data union, shared by stream admission and native replay.
//! https://ai.google.dev/api/generate-content#Part
use super::present;
use serde::Deserialize;
use serde_json::{Value, value::RawValue};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Part<T> {
    pub text: Option<T>,
    pub thought: Option<T>,
    thought_signature: Option<T>,
    pub function_call: Option<T>,
    inline_data: Option<T>,
    function_response: Option<T>,
    file_data: Option<T>,
    executable_code: Option<T>,
    code_execution_result: Option<T>,
    tool_call: Option<T>,
    tool_response: Option<T>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    String,
    Bool,
    Object,
    Other,
}

impl<T> Part<T> {
    fn valid(&self, kind: impl Fn(&T) -> Kind) -> bool {
        if self
            .text
            .as_ref()
            .is_some_and(|text| kind(text) != Kind::String)
            || self
                .thought
                .as_ref()
                .is_some_and(|thought| kind(thought) != Kind::Bool)
            || self
                .thought_signature
                .as_ref()
                .is_some_and(|signature| kind(signature) != Kind::String)
        {
            return false;
        }
        let objects = [
            &self.function_call,
            &self.inline_data,
            &self.function_response,
            &self.file_data,
            &self.executable_code,
            &self.code_execution_result,
            &self.tool_call,
            &self.tool_response,
        ];
        // The public Part contract permits at most one data field. A
        // signature-only part remains valid, as do unknown metadata keys.
        let mut data = self.text.is_some();
        for object in objects.into_iter().flatten() {
            if data || kind(object) != Kind::Object {
                return false;
            }
            data = true;
        }
        true
    }
}

impl<'a> Part<&'a Value> {
    pub fn of(value: &'a Value) -> Option<Self> {
        value.as_object()?;
        let field = |name| present(value.get(name));
        let part = Self {
            text: field("text"),
            thought: field("thought"),
            thought_signature: field("thoughtSignature"),
            function_call: field("functionCall"),
            inline_data: field("inlineData"),
            function_response: field("functionResponse"),
            file_data: field("fileData"),
            executable_code: field("executableCode"),
            code_execution_result: field("codeExecutionResult"),
            tool_call: field("toolCall"),
            tool_response: field("toolResponse"),
        };
        part.valid(|value| match value {
            Value::String(_) => Kind::String,
            Value::Bool(_) => Kind::Bool,
            Value::Object(_) => Kind::Object,
            _ => Kind::Other,
        })
        .then_some(part)
    }
}

impl Part<&RawValue> {
    /// Raw tokens have already passed the shared literal-safe traversal.
    /// Inspecting their kind neither decodes signatures nor constructs a tree.
    fn valid_raw(&self) -> bool {
        self.valid(|value| match value.get().as_bytes().first() {
            Some(b'"') => Kind::String,
            Some(b't' | b'f') => Kind::Bool,
            Some(b'{') => Kind::Object,
            _ => Kind::Other,
        })
    }
}

/// Admits known fields after literal-safe JSON validation. All returned tokens
/// borrow the original part; admission never reconstructs its native bytes.
pub(crate) fn native_part(raw: &RawValue) -> crate::Result<Part<&RawValue>> {
    let invalid = || {
        crate::invalid(
            "Gemini part admission failed; recovery: return one correctly typed data field per native part",
        )
    };
    let part: Part<&RawValue> = serde_json::from_str(raw.get()).map_err(|_| invalid())?;
    if !part.valid_raw() {
        return Err(invalid());
    }
    Ok(part)
}
