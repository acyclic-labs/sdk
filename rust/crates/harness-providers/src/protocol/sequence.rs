//! Position and adjacency rules shared by input validation and projection.
use serde_json::Value;

/// OAR-RSN-02: a reasoning item requires an immediately following output.
/// End is a position so projection finalization and validation use the same rule.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResponseItem {
    Reasoning,
    Output,
    Other,
    End,
}

impl ResponseItem {
    pub fn of(item: &Value) -> Self {
        match item["type"].as_str().unwrap_or("message") {
            "reasoning" => Self::Reasoning,
            "function_call" => Self::Output,
            "message" if item["role"] == "assistant" => Self::Output,
            _ => Self::Other,
        }
    }

    pub fn allows(self, next: Self) -> bool {
        self != Self::Reasoning || next == Self::Output
    }
}

/// ANT-SEQ-04/06, GEM-SEQ-05: role-alternating conversations open and close
/// on a user turn. `None` is the missing turn of an empty conversation.
pub(crate) fn user_boundary(role: Option<&str>) -> bool {
    role == Some("user")
}

/// GEM-TOOL-05/06: a part is a call, else a response, else other content.
pub(crate) enum GeminiPart<'a> {
    Call(&'a Value),
    Response(&'a Value),
    Other,
}

impl<'a> GeminiPart<'a> {
    pub fn of(part: &'a Value) -> Self {
        if let Some(call) = part.get("functionCall") {
            Self::Call(call)
        } else if let Some(response) = part.get("functionResponse") {
            Self::Response(response)
        } else {
            Self::Other
        }
    }
}

/// GEM-TOOL-05: the wire `(name, id)` a `functionCall` or `functionResponse` pairs on.
pub(crate) type Exchange<'a> = (Option<&'a str>, Option<&'a str>);

pub(crate) fn exchange(value: &Value) -> Exchange<'_> {
    (value["name"].as_str(), value["id"].as_str())
}

/// GEM-TOOL-05: a call turn is answered by exactly its responses, in call order.
pub(crate) fn answered<'a>(
    calls: impl IntoIterator<Item = Exchange<'a>>,
    responses: impl IntoIterator<Item = Exchange<'a>>,
) -> bool {
    calls.into_iter().eq(responses)
}
