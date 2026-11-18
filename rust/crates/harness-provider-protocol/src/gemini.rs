//! Gemini's Part data union, shared by stream admission and native replay.
//! https://ai.google.dev/api/generate-content#Part
use super::present;
use serde::Deserialize;
use serde_json::{Value, value::RawValue};

/// Known fields of a Gemini part, keeping source tokens when requested.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", bound(deserialize = "T: Deserialize<'de>"))]
pub struct Part<T> {
    #[serde(default, deserialize_with = "supplied")]
    /// Text supplied by the provider.
    pub text: Option<T>,
    #[serde(default, deserialize_with = "supplied")]
    /// Whether the text represents a thought.
    pub thought: Option<T>,
    #[serde(default, deserialize_with = "supplied")]
    thought_signature: Option<T>,
    #[serde(default, deserialize_with = "supplied")]
    /// Function call supplied by the provider.
    pub function_call: Option<T>,
    #[serde(default, deserialize_with = "supplied")]
    inline_data: Option<T>,
    #[serde(default, deserialize_with = "supplied")]
    function_response: Option<T>,
    #[serde(default, deserialize_with = "supplied")]
    file_data: Option<T>,
    #[serde(default, deserialize_with = "supplied")]
    executable_code: Option<T>,
    #[serde(default, deserialize_with = "supplied")]
    code_execution_result: Option<T>,
    #[serde(default, deserialize_with = "supplied")]
    tool_call: Option<T>,
    #[serde(default, deserialize_with = "supplied")]
    tool_response: Option<T>,
}

fn supplied<'de, D: serde::Deserializer<'de>, T: Deserialize<'de>>(
    deserializer: D,
) -> Result<Option<T>, D::Error> {
    T::deserialize(deserializer).map(Some)
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
    /// Admit a decoded value against the Gemini part data union.
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
    // Preserve the existing unset meaning of JSON null, after the raw decoder
    // has checked field presence. A first null must not hide a duplicate field.
    fn without_nulls(mut self) -> Self {
        for field in [
            &mut self.text,
            &mut self.thought,
            &mut self.thought_signature,
            &mut self.function_call,
            &mut self.inline_data,
            &mut self.function_response,
            &mut self.file_data,
            &mut self.executable_code,
            &mut self.code_execution_result,
            &mut self.tool_call,
            &mut self.tool_response,
        ] {
            *field = field.filter(|raw| raw.get() != "null");
        }
        self
    }

    /// Raw tokens have already passed the shared literal-safe traversal.
    /// Inspecting their kind neither decodes signatures nor constructs a tree.
    fn valid_raw(&self) -> bool {
        self.valid(|value| raw_kind(value))
    }
}

fn raw_kind(value: &RawValue) -> Kind {
    match value.get().as_bytes().first() {
        Some(b'"') => Kind::String,
        Some(b't' | b'f') => Kind::Bool,
        Some(b'{') => Kind::Object,
        _ => Kind::Other,
    }
}

/// Borrowed executable identity, validated before either execution or replay.
/// https://ai.google.dev/api/generate-content#FunctionCall
#[derive(Clone, Copy, Deserialize)]
pub struct Function<'a> {
    #[serde(default, borrow, deserialize_with = "supplied")]
    /// Optional call identifier as its original JSON string.
    pub id: Option<&'a RawValue>,
    #[serde(borrow)]
    /// Function name as its original JSON string.
    pub name: &'a RawValue,
    #[serde(default, borrow, deserialize_with = "supplied")]
    /// Optional arguments as their original JSON object.
    pub args: Option<&'a RawValue>,
}

/// An admitted Gemini part borrowing the input JSON.
pub struct NativePart<'a> {
    /// Validated fields of the original part.
    pub fields: Part<&'a RawValue>,
    /// Validated function call, when present.
    pub function: Option<Function<'a>>,
}

/// Admits known fields after literal-safe JSON validation. All returned tokens
/// borrow the original part; admission never reconstructs its native bytes.
pub fn native_part(raw: &RawValue) -> Result<NativePart<'_>, &'static str> {
    const INVALID: &str = "Gemini part admission failed; recovery: return one typed data member and a well-formed functionCall per native part";
    if raw_kind(raw) != Kind::Object {
        return Err(INVALID);
    }
    let part = serde_json::from_str::<Part<&RawValue>>(raw.get())
        .map_err(|_| INVALID)?
        .without_nulls();
    if !part.valid_raw() {
        return Err(INVALID);
    }
    let function = part
        .function_call
        .map(|raw| {
            let function: Function<'_> = serde_json::from_str(raw.get()).map_err(|_| INVALID)?;
            if raw_kind(function.name) != Kind::String
                || function.id.is_some_and(|id| raw_kind(id) != Kind::String)
                || function
                    .args
                    .is_some_and(|args| raw_kind(args) != Kind::Object)
            {
                return Err(INVALID);
            }
            Ok(function)
        })
        .transpose()?;
    Ok(NativePart {
        fields: part,
        function,
    })
}
