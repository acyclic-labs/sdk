//! Required wire fields, shared by local validators and native replay checks.
use super::name::{CALL_ID, Name};
use serde_json::{Value, value::RawValue};

#[derive(Clone, Copy)]
pub(crate) enum Constraint {
    String { nonempty: bool },
    Array,
    Name(Name),
    ObjectJson,
}
pub(crate) struct Shape {
    pub discriminator: (&'static str, &'static str),
    pub fields: &'static [(&'static str, Constraint)],
}
impl Constraint {
    pub fn accepts(self, value: &Value) -> bool {
        match self {
            Self::String { nonempty } => value
                .as_str()
                .is_some_and(|text| !nonempty || !text.is_empty()),
            Self::Array => value.is_array(),
            Self::Name(rule) => value.as_str().is_some_and(|text| rule.accepts(text)),
            Self::ObjectJson => value.as_str().is_some_and(super::strict::object),
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
pub(crate) const TEXT: Shape = Shape {
    discriminator: ("type", "text"),
    fields: TEXT_FIELDS,
};
pub(crate) const INPUT_TEXT: Shape = Shape {
    discriminator: ("type", "input_text"),
    fields: TEXT_FIELDS,
};
pub(crate) const OUTPUT_TEXT: Shape = Shape {
    discriminator: ("type", "output_text"),
    fields: TEXT_FIELDS,
};
pub(crate) const REFUSAL: Shape = Shape {
    discriminator: ("type", "refusal"),
    fields: &[("/refusal", STRING)],
};
pub(crate) const IMAGE_URL: Shape = Shape {
    discriminator: ("type", "image_url"),
    fields: &[("/image_url/url", NONEMPTY)],
};
pub(crate) const INPUT_IMAGE_URL: Shape = Shape {
    discriminator: ("type", "input_image"),
    fields: &[("/image_url", NONEMPTY)],
};
pub(crate) const INPUT_IMAGE_ID: Shape = Shape {
    discriminator: ("type", "input_image"),
    fields: &[("/file_id", NONEMPTY)],
};
pub(crate) const INPUT_AUDIO: Shape = Shape {
    discriminator: ("type", "input_audio"),
    fields: &[
        ("/input_audio/data", NONEMPTY),
        ("/input_audio/format", NONEMPTY),
    ],
};
// OAC-MED-01: a file reference or a named inline file, never an empty container.
pub(crate) const FILE_ID: Shape = Shape {
    discriminator: ("type", "file"),
    fields: &[("/file/file_id", NONEMPTY)],
};
pub(crate) const FILE_DATA: Shape = Shape {
    discriminator: ("type", "file"),
    fields: &[("/file/file_data", NONEMPTY), ("/file/filename", NONEMPTY)],
};
pub(crate) const INPUT_FILE_ID: Shape = Shape {
    discriminator: ("type", "input_file"),
    fields: &[("/file_id", NONEMPTY)],
};
pub(crate) const INPUT_FILE_URL: Shape = Shape {
    discriminator: ("type", "input_file"),
    fields: &[("/file_url", NONEMPTY)],
};
pub(crate) const INPUT_FILE_DATA: Shape = Shape {
    discriminator: ("type", "input_file"),
    fields: &[("/file_data", NONEMPTY), ("/filename", NONEMPTY)],
};
pub(crate) const CHAT_CALL: Shape = Shape {
    discriminator: ("type", "function"),
    fields: &[
        ("/id", Constraint::Name(CALL_ID)),
        ("/function/name", FUNCTION_NAME),
        ("/function/arguments", Constraint::ObjectJson),
    ],
};
pub(crate) const RESPONSE_CALL: Shape = Shape {
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
pub(crate) const THINKING: Shape = Shape {
    discriminator: ("type", "thinking"),
    fields: &[("/thinking", STRING), ("/signature", NONEMPTY)],
};
pub(crate) const REDACTED: Shape = Shape {
    discriminator: ("type", "redacted_thinking"),
    fields: &[("/data", NONEMPTY)],
};
pub(crate) const REASONING: Shape = Shape {
    discriminator: ("type", "reasoning"),
    fields: &[
        ("/id", NONEMPTY),
        ("/summary", Constraint::Array),
        ("/encrypted_content", NONEMPTY),
    ],
};
