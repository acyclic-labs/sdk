//! Client adapters over the shared pure UI projection.
mod attention;
mod cut;
mod reply;
use crate::{AgentPath, AgentRecord, Reader, reader::Want};
use acyclic_harness_core::command_request::CommandFileRoot;
pub use acyclic_harness_core::ui::{
    Affected, AgentRow, Children, Cut, Error, Node, Page, Result, UiPath, View, blocks,
    title_matches,
};
pub use attention::{attention, attention_wants};
pub use cut::ReaderCut;
pub use reply::{Reply, ReplyPayload, approval_reply};

#[cfg(test)]
mod tests;

/// The selected agent's own adoption, using the inbox's primitive fork origin.
/// Absence stays unknown until the existing reader satisfies role_want.
pub fn agent_role(reader: &Reader, agent: &AgentPath) -> Option<String> {
    let stream = agent.inbox().ok()?;
    let cache = reader.cache();
    if !cache.origin_known(&stream) {
        return None;
    }
    let from = cache
        .fork(&stream)
        .map_or(0, |origin| origin.inherited.forked_at);
    let record =
        acyclic_harness_core::agent::InboxRecord::decode(cache.record(&stream, from)?).ok()?;
    let acyclic_harness_core::agent::InboxRecord::ForkAdopted(adopted) = record else {
        return None;
    };
    if adopted.agent != agent.id() {
        return None;
    }
    adopted.profile.as_ref()?.role().map(str::to_owned)
}

/// Bounded immutable role demand over the same reader and visible-agent pages.
pub fn role_want(reader: &Reader, agent: &AgentPath) -> Result<Want> {
    let stream = agent.inbox()?;
    let cache = reader.cache();
    if !cache.origin_known(&stream) {
        return Ok(Want::Last { stream, count: 1 });
    }
    let from = cache
        .fork(&stream)
        .map_or(0, |origin| origin.inherited.forked_at);
    Ok(Want::Range {
        stream,
        from,
        to: from
            .checked_add(1)
            .ok_or(Error::Invalid("adoption cursor"))?,
    })
}

/// Input stays with the closest known orchestrator while secondary roles are
/// inspected. An unknown ancestor keeps the root as the conservative target.
pub fn input_target(reader: &Reader, inspected: &AgentPath) -> AgentPath {
    let mut current = inspected.clone();
    loop {
        if agent_role(reader, &current).as_deref() == Some("orchestrator") {
            return current;
        }
        match current.parent() {
            Some(parent) => current = parent,
            None => return current,
        }
    }
}
