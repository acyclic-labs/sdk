//! Lowers one [`Step`] into its single Streams commit.
//!
//! Own streams (conversation, status) are conditioned on the exact tails in the
//! prior state, because the agent is their only writer. Recipients are only the
//! parent and spawned children, which exist by construction, so their appends
//! are unconditional: a moved tail or a closed recipient (a parent archiving
//! its subtree awaits every child's `ChildDone`) never blocks a step. Spawns fork
//! the agent's own subtree at its consumed cursor; the same-commit conversation
//! append is a source-descendant write, which Streams excludes from the child's
//! inherited cut, so the child sees exactly the conversation before this step.

use super::{
    AgentPath, AgentRecord, AgentState, Closed, Control, Error, ForkAdopted, InboxRecord, Out,
    Sender, Step, commit,
};
use acyclic_stream::{
    AppendRecord, CommitCondition, CommitMutation, CommitRequest, IdempotencyKey, StreamPath,
};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Default)]
struct Builder {
    conditions: BTreeMap<StreamPath, CommitCondition>,
    appends: BTreeMap<StreamPath, Vec<AppendRecord>>,
    forks: Vec<CommitMutation>,
    created: BTreeSet<StreamPath>,
}

impl Builder {
    fn condition(&mut self, condition: CommitCondition) {
        let path = match &condition {
            CommitCondition::Head { path, .. }
            | CommitCondition::Tail { path, .. }
            | CommitCondition::Absent { path } => path.clone(),
        };
        self.conditions.entry(path).or_insert(condition);
    }

    fn append(&mut self, path: StreamPath, record: Vec<u8>) {
        self.appends.entry(path).or_default().push(record.into());
    }

    /// Rejects a write into a child this same commit forks; Streams forbids
    /// mutations under a fork destination.
    fn recipient(&self, to: &AgentPath) -> Result<(), Error> {
        let inbox = to.inbox()?;
        if self.created.iter().any(|created| {
            inbox == *created
                || inbox
                    .as_str()
                    .strip_prefix(created.as_str())
                    .is_some_and(|rest| rest.starts_with('/'))
        }) {
            return Err(Error::Record("a step cannot write to a child it spawns"));
        }
        Ok(())
    }
}

/// Builds the step's one commit.
///
/// `cursor` is the number of inbox records consumed after this step, and `key`
/// is the delivery's stable identity, so a retried delivery replays exactly.
pub fn lower(step: &Step, cursor: u64, key: IdempotencyKey) -> Result<CommitRequest, Error> {
    let state = &step.state;
    let path = &state.path;
    let sender = Sender::Agent { path: path.clone() };
    let mut builder = Builder::default();
    let conversation = path.conversation()?;
    let conversation_count = step
        .outs
        .iter()
        .filter(|out| matches!(out, Out::Conversation(_)))
        .count() as u64;
    let prior_conversation = state
        .conversation_tail
        .checked_sub(conversation_count)
        .ok_or(Error::Record("conversation tail precedes its appends"))?;
    for out in &step.outs {
        if let Out::Spawn {
            intent,
            brief,
            volumes,
        } = out
        {
            let spawn = commit::spawn(
                path,
                intent,
                cursor,
                ForkAdopted {
                    agent: path.id(),
                    parent: None,
                    forked_at: cursor,
                    conversation_tail: prior_conversation,
                    volumes: volumes.clone(),
                    brief: brief.as_deref().cloned(),
                    policy: state.policy.clone(),
                },
            )?;
            builder.created.insert(spawn.child.inbox()?);
            for condition in spawn.request.conditions {
                builder.condition(condition);
            }
            builder.forks.extend(spawn.request.mutations);
        }
    }
    for out in &step.outs {
        match out {
            Out::Conversation(record) => builder.append(conversation.clone(), record.clone()),
            Out::Send { to, record } => {
                if matches!(
                    record,
                    InboxRecord::ForkAdopted(_) | InboxRecord::Archive(_)
                ) {
                    return Err(Error::Record(
                        "send carries neither ForkAdopted nor Archive",
                    ));
                }
                builder.recipient(to)?;
                builder.append(to.inbox()?, record.encode()?);
            }
            Out::Archive { target, id } => {
                builder.recipient(target)?;
                let control = Control {
                    by: sender.clone(),
                    id: id.clone(),
                };
                builder.append(target.inbox()?, InboxRecord::Archive(control).encode()?);
                // Unconditional: the first marker record closes the child; a
                // cascade racing an independent archive adds a redundant one.
                builder.append(
                    target.closed()?,
                    Closed {
                        by: sender.clone(),
                        id: id.clone(),
                    }
                    .encode()?,
                );
            }
            Out::Spawn { .. } => {}
        }
    }
    if conversation_count > 0 {
        builder.condition(CommitCondition::Tail {
            path: conversation,
            expected: prior_conversation,
        });
    }
    if let Some(status) = &step.status {
        let path = path.status()?;
        builder.condition(CommitCondition::Tail {
            path: path.clone(),
            expected: state.status_tail - 1,
        });
        builder.append(path, status.encode()?);
    }
    let mut mutations = builder.forks;
    mutations.extend(
        builder
            .appends
            .into_iter()
            .map(|(path, records)| CommitMutation::Append { path, records }),
    );
    Ok(CommitRequest {
        conditions: builder.conditions.into_values().collect(),
        mutations,
        idempotency_key: key,
    })
}

/// Stable key of one delivery: the agent and the A4 trigger identity.
pub fn delivery_key(agent: &AgentState, trigger: &str) -> Result<IdempotencyKey, Error> {
    commit::intent_key("step", &[agent.path.as_str(), trigger])
}
