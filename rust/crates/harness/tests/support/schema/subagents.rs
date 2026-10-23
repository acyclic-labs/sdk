#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SubagentCase {
    /// Subagents fanned out under the root, at most 64.
    pub agents: usize,
    /// Each subagent's streamed reply, one Text chunk per publication.
    pub chunks: Vec<String>,
    /// Each subagent's final `status.json` state, as it publishes it.
    pub status: String,
    pub expect: Expect,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Expect {
    /// Children discovered under the root, in creation order.
    pub children: Vec<String>,
    /// Each subagent's reply chunks, in order, as its follow delivered them.
    pub chunks: Vec<String>,
    /// Each subagent's final state, as its follow decoded it.
    pub status: String,
}

impl SubagentCase {
    pub(super) fn validate(&self) -> Result<(), &'static str> {
        if !(1..=64).contains(&self.agents) || self.chunks.is_empty() {
            return Err("a subagent case needs 1..=64 agents and a nonempty reply");
        }
        Ok(())
    }
}
