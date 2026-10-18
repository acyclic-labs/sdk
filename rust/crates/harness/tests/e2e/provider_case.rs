use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, num::NonZeroUsize};

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Protocol {
    Anthropic,
    Responses,
    Chat,
    Gemini,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderCase {
    pub protocol: Protocol,
    pub prompt: String,
    pub seed: BTreeMap<String, String>,
    pub responses: Vec<Vec<String>>,
    pub chunk_bytes: NonZeroUsize,
    /// Registered tools beyond the defaults, named like installed MCP tools.
    #[serde(default)]
    pub tools: Vec<String>,
    pub expect: Expectations,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Expectations {
    pub text: String,
    pub events: Vec<Vec<Value>>,
    pub endpoint: String,
    pub requests: Vec<BTreeMap<String, Value>>,
    pub frames: Vec<Value>,
    pub statuses: Vec<Value>,
    pub files: BTreeMap<String, Option<String>>,
}

impl ProviderCase {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.prompt.is_empty()
            || self.responses.is_empty()
            || self.responses.len() > 8
            || self.responses.len() != self.expect.events.len()
            || self.responses.len() != self.expect.requests.len()
            || self.responses.iter().any(|response| {
                response.is_empty() || response.iter().map(String::len).sum::<usize>() > 1_048_576
            })
            || self.expect.requests.iter().skip(1).any(BTreeMap::is_empty)
            || self
                .expect
                .requests
                .iter()
                .flat_map(|fields| fields.keys())
                .any(|pointer| !pointer.starts_with('/'))
            || self.expect.frames.is_empty()
            || self.expect.statuses.len() < 2
            || self.expect.statuses.iter().any(|status| {
                status["state"].as_str().is_none() || !status["attention"].is_boolean()
            })
        {
            return Err("invalid bounded provider scenario");
        }
        Ok(())
    }
}
