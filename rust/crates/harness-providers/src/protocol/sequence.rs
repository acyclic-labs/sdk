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
