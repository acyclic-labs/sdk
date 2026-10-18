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
    /// HTTP status of each response; omitted statuses are 200.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub statuses: Vec<u16>,
    /// Leading exchanges, counts included, refused for want of a connection.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub unreachable: u8,
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
    #[serde(default, skip_serializing_if = "is_succeeded")]
    pub outcome: super::local_case::ExpectedOutcome,
}

fn is_zero(count: &u8) -> bool {
    *count == 0
}

fn is_succeeded(outcome: &super::local_case::ExpectedOutcome) -> bool {
    *outcome == super::local_case::ExpectedOutcome::Succeeded
}

impl ProviderCase {
    /// Status of response `index`.
    pub fn status(&self, index: usize) -> u16 {
        self.statuses.get(index).copied().unwrap_or(200)
    }
    /// Responses that complete a model step; refused ones are redelivered within it.
    pub fn completed_steps(&self) -> usize {
        (0..self.responses.len())
            .filter(|&index| self.status(index) == 200)
            .count()
    }
}

impl ProviderCase {
    pub fn validate(&self) -> Result<(), &'static str> {
        let succeeded = self.expect.outcome == super::local_case::ExpectedOutcome::Succeeded;
        // No response is consumed only when every attempt was refused.
        if self.responses.is_empty() {
            return if self.prompt.is_empty()
                || succeeded
                || self.unreachable == 0
                || !self.statuses.is_empty()
                || !self.expect.events.is_empty()
                || !self.expect.requests.is_empty()
                || self.expect.frames.is_empty()
                || self.expect.statuses.len() < 2
            {
                Err("invalid bounded provider scenario")
            } else {
                Ok(())
            };
        }
        if self.prompt.is_empty()
            || self.responses.len() > 8
            || self.responses.len() != self.expect.events.len()
            || self.responses.len() != self.expect.requests.len()
            || self.statuses.len() > self.responses.len()
            || self
                .statuses
                .iter()
                .any(|status| !(200..600).contains(status))
            || succeeded != (self.status(self.responses.len() - 1) == 200)
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
