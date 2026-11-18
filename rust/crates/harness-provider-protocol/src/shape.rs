//! Required wire fields, shared by local validators and native replay checks.
use super::name::{CALL_ID, Name};
use serde_json::{Value, value::RawValue};

/// Required value at one wire field.
#[derive(Clone, Copy)]
pub enum Constraint {
    /// A JSON string.
    String {
        /// Whether an empty string is rejected.
        nonempty: bool,
    },
    /// A JSON array.
    Array,
    /// A string matching a wire identifier grammar.
    Name(Name),
    /// A string containing a JSON object.
    ObjectJson,
}
/// Wire discriminator and required field constraints.
pub struct Shape {
    /// Required discriminator field and value.
    pub discriminator: (&'static str, &'static str),
    /// Required fields addressed by JSON pointer.
    pub fields: &'static [(&'static str, Constraint)],
}
impl Constraint {
    /// Check an already decoded field against this constraint.
    pub fn accepts(self, value: &Value) -> bool {
        match self {
            Self::String { nonempty } => value
                .as_str()
                .is_some_and(|text| !nonempty || !text.is_empty()),
            Self::Array => value.is_array(),
            Self::Name(rule) => value.as_str().is_some_and(|text| rule.accepts(text)),
            Self::ObjectJson => value
                .as_str()
                .is_some_and(acyclic_model_render::strict::object),
        }
    }
    /// The enclosing payload must already have passed `strict::valid`.
    pub fn accepts_valid_raw(self, value: Option<&RawValue>) -> bool {
        let Some(value) = value else {
            return false;
        };
        match self {
            Self::String { nonempty } => {
                value.get().starts_with('"') && (!nonempty || value.get().len() > 2)
            }
            Self::Array => value.get().starts_with('['),
            _ => false,
        }
    }
}
impl Shape {
    /// Check every required field and the discriminator.
    pub fn accepts(&self, value: &Value) -> bool {
        value.is_object()
            && value[self.discriminator.0] == self.discriminator.1
            && self.fields.iter().all(|(path, constraint)| {
                value
                    .pointer(path)
                    .is_some_and(|value| constraint.accepts(value))
            })
    }
}
const STRING: Constraint = Constraint::String { nonempty: false };
const NONEMPTY: Constraint = Constraint::String { nonempty: true };
const FUNCTION_NAME: Constraint = Constraint::Name(Name {
    maximum: 64,
    leading_letter: false,
    punctuation: b"_-",
});
const TEXT_FIELDS: &[(&str, Constraint)] = &[("/text", STRING)];
/// Text part shape.
pub const TEXT: Shape = Shape {
    discriminator: ("type", "text"),
    fields: TEXT_FIELDS,
};
/// Responses input text part shape.
pub const INPUT_TEXT: Shape = Shape {
    discriminator: ("type", "input_text"),
    fields: TEXT_FIELDS,
};
/// Responses output text part shape.
pub const OUTPUT_TEXT: Shape = Shape {
    discriminator: ("type", "output_text"),
    fields: TEXT_FIELDS,
};
/// Responses refusal part shape.
pub const REFUSAL: Shape = Shape {
    discriminator: ("type", "refusal"),
    fields: &[("/refusal", STRING)],
};
/// Chat Completions image URL part shape.
pub const IMAGE_URL: Shape = Shape {
    discriminator: ("type", "image_url"),
    fields: &[("/image_url/url", NONEMPTY)],
};
/// Responses image URL part shape.
pub const INPUT_IMAGE_URL: Shape = Shape {
    discriminator: ("type", "input_image"),
    fields: &[("/image_url", NONEMPTY)],
};
/// Responses image file reference shape.
pub const INPUT_IMAGE_ID: Shape = Shape {
    discriminator: ("type", "input_image"),
    fields: &[("/file_id", NONEMPTY)],
};
/// Inline audio part shape.
pub const INPUT_AUDIO: Shape = Shape {
    discriminator: ("type", "input_audio"),
    fields: &[
        ("/input_audio/data", NONEMPTY),
        ("/input_audio/format", NONEMPTY),
    ],
};
// OAC-MED-01: a file reference or a named inline file, never an empty container.
/// Chat Completions file reference shape.
pub const FILE_ID: Shape = Shape {
    discriminator: ("type", "file"),
    fields: &[("/file/file_id", NONEMPTY)],
};
/// Chat Completions named inline file shape.
pub const FILE_DATA: Shape = Shape {
    discriminator: ("type", "file"),
    fields: &[("/file/file_data", NONEMPTY), ("/file/filename", NONEMPTY)],
};
/// Responses file reference shape.
pub const INPUT_FILE_ID: Shape = Shape {
    discriminator: ("type", "input_file"),
    fields: &[("/file_id", NONEMPTY)],
};
/// Responses file URL shape.
pub const INPUT_FILE_URL: Shape = Shape {
    discriminator: ("type", "input_file"),
    fields: &[("/file_url", NONEMPTY)],
};
/// Responses named inline file shape.
pub const INPUT_FILE_DATA: Shape = Shape {
    discriminator: ("type", "input_file"),
    fields: &[("/file_data", NONEMPTY), ("/filename", NONEMPTY)],
};
/// Chat Completions function call shape.
pub const CHAT_CALL: Shape = Shape {
    discriminator: ("type", "function"),
    fields: &[
        ("/id", Constraint::Name(CALL_ID)),
        ("/function/name", FUNCTION_NAME),
        ("/function/arguments", Constraint::ObjectJson),
    ],
};
/// Responses function call shape.
pub const RESPONSE_CALL: Shape = Shape {
    discriminator: ("type", "function_call"),
    fields: &[
        (
            "/call_id",
            Constraint::Name(Name {
                maximum: 64,
                ..CALL_ID
            }),
        ),
        ("/name", FUNCTION_NAME),
        ("/arguments", Constraint::ObjectJson),
    ],
};
/// Responses custom tool call shape.
pub const RESPONSE_CUSTOM_CALL: Shape = Shape {
    discriminator: ("type", "custom_tool_call"),
    fields: &[
        (
            "/call_id",
            Constraint::Name(Name {
                maximum: 64,
                ..CALL_ID
            }),
        ),
        ("/name", FUNCTION_NAME),
        ("/input", STRING),
    ],
};
/// Anthropic signed thinking block shape.
pub const THINKING: Shape = Shape {
    discriminator: ("type", "thinking"),
    fields: &[("/thinking", STRING), ("/signature", NONEMPTY)],
};
/// Anthropic redacted thinking block shape.
pub const REDACTED: Shape = Shape {
    discriminator: ("type", "redacted_thinking"),
    fields: &[("/data", NONEMPTY)],
};
/// Responses reasoning item shape.
pub const REASONING: Shape = Shape {
    discriminator: ("type", "reasoning"),
    fields: &[
        ("/id", NONEMPTY),
        ("/summary", Constraint::Array),
        ("/encrypted_content", NONEMPTY),
    ],
};
