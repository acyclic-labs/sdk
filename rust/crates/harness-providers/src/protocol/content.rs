//! Content positions and their allowed shapes (OAC-SEQ-02/03, OAR-SEQ-03,
//! OAR-TOOL-05). The same walker checks message content and tool outputs.
use super::shape::{self, Shape};
use serde_json::Value;

pub(crate) struct Content {
    pub parts: &'static [&'static Shape],
    pub null_with_calls: bool,
    array_rule: &'static str,
    scalar_rule: &'static str,
}
impl Content {
    pub fn violation(&self, value: &Value, has_calls: bool) -> Option<&'static str> {
        match value {
            Value::String(_) => None,
            Value::Null if self.null_with_calls && has_calls => None,
            Value::Array(parts) => parts
                .iter()
                .any(|part| !self.parts.iter().any(|shape| shape.accepts(part)))
                .then_some(self.array_rule),
            _ => Some(self.scalar_rule),
        }
    }
}
const CHAT_TEXT: Content = Content {
    parts: &[&shape::TEXT],
    null_with_calls: false,
    array_rule: "OAC-SEQ-02",
    scalar_rule: "OAC-SEQ-03",
};
const CHAT_USER: Content = Content {
    parts: &[
        &shape::TEXT,
        &shape::IMAGE_URL,
        &shape::INPUT_AUDIO,
        &shape::FILE_ID,
        &shape::FILE_DATA,
    ],
    ..CHAT_TEXT
};
const CHAT_ASSISTANT: Content = Content {
    parts: &[&shape::TEXT, &shape::REFUSAL],
    null_with_calls: true,
    ..CHAT_TEXT
};
pub(crate) const CHAT: &[(&str, &Content)] = &[
    ("system", &CHAT_TEXT),
    ("developer", &CHAT_TEXT),
    ("user", &CHAT_USER),
    ("assistant", &CHAT_ASSISTANT),
    ("tool", &CHAT_TEXT),
];
const RESPONSE_INPUT: Content = Content {
    parts: &[
        &shape::INPUT_TEXT,
        &shape::INPUT_IMAGE_URL,
        &shape::INPUT_IMAGE_ID,
        &shape::INPUT_FILE_ID,
        &shape::INPUT_FILE_URL,
        &shape::INPUT_FILE_DATA,
    ],
    null_with_calls: false,
    array_rule: "OAR-SEQ-03",
    scalar_rule: "OAR-SEQ-01",
};
const RESPONSE_ASSISTANT: Content = Content {
    parts: &[&shape::OUTPUT_TEXT, &shape::REFUSAL],
    ..RESPONSE_INPUT
};
pub(crate) const RESPONSES: &[(&str, &Content)] = &[
    ("system", &RESPONSE_INPUT),
    ("developer", &RESPONSE_INPUT),
    ("user", &RESPONSE_INPUT),
    ("assistant", &RESPONSE_ASSISTANT),
];
pub(crate) const RESPONSE_TOOL_OUTPUT: Content = Content {
    array_rule: "OAR-TOOL-05",
    scalar_rule: "OAR-TOOL-05",
    ..RESPONSE_INPUT
};

pub(crate) fn position<'a>(positions: &'a [(&str, &Content)], role: &str) -> Option<&'a Content> {
    positions
        .iter()
        .find_map(|(name, content)| (*name == role).then_some(*content))
}
