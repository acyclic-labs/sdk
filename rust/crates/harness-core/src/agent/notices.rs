//! A frozen selection names original append-only inbox records, never copied bodies.
use super::{AgentPath, AgentState, Error};
use serde::{Deserialize, Serialize};
use std::ops::Range;

/// Original source and exclusive range of a turn's non-waking notices.
/// A conversation fork retains this source rather than reading its own inbox.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
