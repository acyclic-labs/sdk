//! A frozen selection names original append-only inbox records, never copied bodies.
//!
//! Lifecycle notices are non-waking, uncoalesced records of the same commit as
//! their transition. `agent.started` and `agent.forked` mean adoption, not a
//! model dispatch. `agent.resumed` means the driver entered a previously paused
//! turn, not a process reopen or an internal continuation alarm. `agent.settled`
//! means the actor committed idle/done, not that external processes are gone.
//! Lost acknowledgements replay the original step and its original notices.
use super::{AgentPath, AgentState, Error, InboxRecord, IntentId, Out};
use serde::{Deserialize, Serialize};
use std::ops::Range;

/// Adds an observation to the existing inbox in the transition's own commit.
/// The original delivery cursor and prior status tail distinguish transitions;
/// no separate event identity, publisher, actor or history search is needed.
pub(super) fn lifecycle(
    state: &AgentState,
    kind: &str,
    cursor: u64,
    body: serde_json::Value,
) -> Result<Out, Error> {
    Ok(Out::Send {
        to: state.path.clone(),
        record: InboxRecord::Notice {
            id: IntentId::try_from(format!("{kind}:{cursor}:{}", state.status_tail))?,
            notice: crate::conversation::wire::Notice {
                source: state.path.as_str().to_owned(),
                kind: kind.to_owned(),
                body,
                wake: false,
                coalesce_key: None,
            },
        },
    })
}

/// Original source and exclusive range of a turn's non-waking notices.
/// A conversation fork retains this source rather than reading its own inbox.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, crate::HeapLayout)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
#[serde(deny_unknown_fields)]
pub struct NoticeSelection {
    /// Agent whose inbox owns the immutable events.
    #[cfg_attr(feature = "wasm", tsify(type = "string"))]
    pub source: AgentPath,
    /// Exclusive original inbox range; other admitted inputs may interleave.
    pub range: Range<u64>,
}

impl NoticeSelection {
    pub(super) fn pending(agent: &AgentState) -> Option<Self> {
        agent.pending_notices.clone().map(|range| Self {
            source: agent.path.clone(),
            range,
        })
    }

    /// Checks that a selection belongs to this conversation or an ancestor.
    ///
    /// # Errors
    /// Empty ranges and unrelated source subtrees refuse before any read.
    pub fn validate(&self, agent: &AgentPath) -> Result<(), Error> {
        let source = self.source.as_str();
        let target = agent.as_str();
        if self.range.start >= self.range.end
            || !(source == target
                || target
                    .strip_prefix(source)
                    .is_some_and(|tail| tail.starts_with('/')))
        {
            return Err(Error::Record(
                "notice selection has a foreign source or empty range",
            ));
        }
        Ok(())
    }

    /// Consumes exactly the frozen prefix, retaining events delivered meanwhile.
    pub(super) fn consume(&self, agent: &mut AgentState) -> Result<(), Error> {
        let Some(pending) = &agent.pending_notices else {
            return Err(Error::Record("notice selection has no pending source"));
        };
        if self.source != agent.path
            || self.range.start != pending.start
            || self.range.end > pending.end
            || self.range.start >= self.range.end
        {
            return Err(Error::Record(
                "notice selection differs from its pending frontier",
            ));
        }
        agent.pending_notices =
            (self.range.end < pending.end).then_some(self.range.end..pending.end);
        Ok(())
    }
}
