//! File-driven goal state and bounded continuation decisions.

use crate::{Error, OperationId, Result, TaskId, conversation::VolumeClass};
use serde::{Deserialize, Serialize};

/// Validated goal frontmatter status.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalStatus {
    /// Continue after every settled turn, subject to the continuation policy.
    #[default]
    Active,
    /// Waiting for input or an external change.
    Blocked,
    /// The objective has been completed.
    Done,
}

/// Disposable rendering of one immutable private goal file.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoalView {
    /// Exact file observed through the owner's ordinary filesystem grant.
    pub source: crate::conversation::FileRef,
    /// Frontmatter status; plain Markdown defaults to active.
    pub status: GoalStatus,
    /// Explanation attached to a blocked or completed goal.
    #[serde(default)]
    pub reason: Option<String>,
    /// Markdown body, excluding frontmatter.
    pub markdown: String,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct Frontmatter {
    #[serde(default)]
    status: GoalStatus,
    reason: Option<String>,
}

/// Finite allowance for one goal loop; edits do not reset the allowance.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GoalContinuationPolicy {
    /// Maximum unique automatic admissions; zero blocks at the first settle.
    pub max_continuations: u32,
}

impl Default for GoalContinuationPolicy {
    fn default() -> Self {
        Self {
            max_continuations: 32,
        }
    }
}

/// Action to publish through the existing task stream or ordinary file writer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GoalContinuation {
    /// Admit once for this settled turn, reusing this identity on retry.
    Continue(OperationId),
    /// Write these GOAL.md contents before reporting the loop as blocked.
    Block(String),
}

impl GoalContinuationPolicy {
    /// Decides after a settle. The stream owner must reuse an existing admission
    /// before evaluating this policy; retries never consume another allowance.
    /// `admitted_continuations` counts unique automatic admissions in this loop.
    #[must_use]
    pub fn on_settle(
        &self,
        goal: &GoalView,
        task: TaskId,
        settled_turn: OperationId,
        admitted_continuations: u32,
    ) -> Option<GoalContinuation> {
        let operation = goal.continuation_operation(task, settled_turn)?;
        Some(if admitted_continuations >= self.max_continuations {
            GoalContinuation::Block(format!(
                "---\nstatus: blocked\nreason: Maximum goal continuations reached (limit {}).\n---\n{}",
                self.max_continuations, goal.markdown,
            ))
        } else {
            GoalContinuation::Continue(operation)
        })
    }
}

impl GoalView {
    /// Interprets verified file bytes within the caller's existing file bound.
    pub fn from_file(
        source: crate::conversation::FileRef,
        bytes: &[u8],
        maximum_bytes: u64,
    ) -> Result<Self> {
        source.validate(&mut None)?;
        if source.path() != "GOAL.md"
            || source.volume().class() != VolumeClass::AgentPrivate
            || bytes.len() as u64 > maximum_bytes
        {
            return Err(Error::Invalid(
                "invalid private goal file or byte bound".into(),
            ));
        }
        source.descriptor().verify(bytes, &mut None)?;
        let text = std::str::from_utf8(bytes)
            .map_err(|_| Error::Invalid("GOAL.md requires UTF-8".into()))?;
        let (frontmatter, markdown) = parse(text)?;
        Ok(Self {
            source,
            status: frontmatter.status,
            reason: frontmatter.reason,
            markdown: markdown.to_owned(),
        })
    }

    /// Content revision, unaffected by unrelated writes to the same volume.
    #[must_use]
    pub fn revision(&self) -> [u8; 32] {
        *self.source.descriptor().sha256()
    }

    /// Stable admission identity for one active goal's settled turn.
    /// Goal edits and revisions do not change this identity.
    #[must_use]
    pub fn continuation_operation(
        &self,
        task: TaskId,
        settled_turn: OperationId,
    ) -> Option<OperationId> {
        if self.status != GoalStatus::Active {
            return None;
        }
        let mut hash = blake3::Hasher::new();
        hash.update(b"acyclic.goal.continue/v2\0");
        hash.update(&task.into_bytes());
        hash.update(&settled_turn.into_bytes());
        let mut id = [0; 16];
        id.copy_from_slice(hash.finalize().as_bytes().get(..16)?);
        Some(OperationId::from_bytes(id))
    }
}

fn parse(text: &str) -> Result<(Frontmatter, &str)> {
    let mut lines = text.split_inclusive('\n');
    if lines.next().map(|line| line.trim_end_matches(['\r', '\n'])) != Some("---") {
        return Ok((Frontmatter::default(), text));
    }
    let start = text
        .find('\n')
        .ok_or_else(|| Error::Invalid("unclosed goal frontmatter".into()))?
        + 1;
    let mut end = start;
    for line in lines {
        if line.trim_end_matches(['\r', '\n']) == "---" {
            let header = text
                .get(start..end)
                .ok_or_else(|| Error::Invalid("invalid goal frontmatter range".into()))?;
            let options = serde_saphyr::options! {
                budget: serde_saphyr::budget! {
                    max_events: header.len().saturating_add(1),
                    max_nodes: header.len().saturating_add(1),
                    max_depth: 2, flow_nesting_limit: 2, max_documents: 1,
                    max_aliases: 0, max_anchors: 0, max_merge_keys: 0,
                    max_total_scalar_bytes: header.len(),
                },
                duplicate_keys: serde_saphyr::DuplicateKeyPolicy::Error,
                merge_keys: serde_saphyr::MergeKeyPolicy::Error,
                reject_unsupported_tags: true,
                emit_comments: false, with_snippet: false,
            };
            let frontmatter: Frontmatter = if header.trim().is_empty() {
                Frontmatter::default()
            } else {
                serde_saphyr::from_str_with_options(header, options)
                    .map_err(|_| Error::Invalid("invalid goal frontmatter".into()))?
            };
            let body = text
                .get(end + line.len()..)
                .ok_or_else(|| Error::Invalid("invalid goal body range".into()))?;
            return Ok((frontmatter, body));
        }
        end += line.len();
    }
    Err(Error::Invalid("unclosed goal frontmatter".into()))
}
