//! Lowers one [`Step`] into its single Streams commit.
//!
//! Own streams (conversation, status) are conditioned on the exact tails in the
//! prior state, because the agent is their only writer. A message to another
//! agent is one soft Streams effect, `Present{inbox}` and `Absent{closed}`
//! guarding the append to the recipient's inbox: an absent or archived
//! recipient skips it, which the receipt reports, and never fails the step.
//! Protocol records (a parent archiving its subtree awaits every child's
//! `ChildDone`) go to tree members that exist by construction and stay
//! unconditional appends, so a moved tail or a closed recipient never blocks a
//! step. Spawns fork the agent's own subtree at its consumed cursor; the
//! same-commit conversation
//! append is a source-descendant write, which Streams excludes from the child's
//! inherited cut, so the child sees exactly the conversation before this step.

use super::{
    AgentPath, AgentRecord, AgentState, Closed, Control, Error, ForkAdopted, InboxRecord, Out,
    Sender, Step, WorkspaceEntry, commit,
};
use acyclic_stream::{
    AppendRecord, CommitCondition, CommitEffect, CommitMutation, CommitRequest, IdempotencyKey,
    StreamPath,
};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Default)]
struct Builder {
    conditions: BTreeMap<StreamPath, CommitCondition>,
    appends: BTreeMap<StreamPath, Vec<AppendRecord>>,
    /// Messages to other agents by inbox, each with the recipient's closed
    /// marker; every inbox becomes its own soft effect.
    soft: BTreeMap<StreamPath, (Vec<AppendRecord>, StreamPath)>,
    forks: Vec<CommitMutation>,
    created: BTreeSet<StreamPath>,
}

impl Builder {
    fn condition(&mut self, condition: CommitCondition) -> Result<(), Error> {
        match self.conditions.entry(condition.path().clone()) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(condition);
            }
            std::collections::btree_map::Entry::Occupied(entry) if entry.get() != &condition => {
                return Err(Error::Record(
                    "source contributions have different conditions",
                ));
            }
            _ => {}
        }
        Ok(())
    }

    fn contribute(&mut self, effect: CommitEffect) -> Result<(), Error> {
        if effect.soft {
            return Err(Error::Record("a source contribution must be hard"));
        }
        for condition in effect.conditions {
            self.condition(condition)?;
        }
        for mutation in effect.mutations {
            match mutation {
                CommitMutation::Append { path, records } => {
                    for record in records {
                        self.append(path.clone(), record);
                    }
                }
                mutation => self.forks.push(mutation),
            }
        }
        Ok(())
    }

    /// A hard append to a path that also has soft records keeps their order
    /// and makes them hard: the hard append decides the path in one mutation.
    fn append(&mut self, path: StreamPath, record: impl Into<AppendRecord>) {
        let records = self.appends.entry(path.clone()).or_default();
        if let Some((soft, _)) = self.soft.remove(&path) {
            records.extend(soft);
        }
        records.push(record.into());
    }

    fn offer(&mut self, path: StreamPath, closed: StreamPath, record: impl Into<AppendRecord>) {
        match self.appends.get_mut(&path) {
            Some(records) => records.push(record.into()),
            None => self
                .soft
                .entry(path)
                .or_insert_with(|| (Vec::new(), closed))
                .0
                .push(record.into()),
        }
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
    let mut peer_guards = BTreeMap::new();
    for out in &step.outs {
        if let Out::PeerGuard { to, admission } = out {
            let conditions = admission.conditions(path, to)?;
            peer_guards.entry(to.inbox()?).or_insert(conditions);
        }
    }
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
        if let Out::Commit(effect) = out {
            builder.contribute(effect.clone())?;
        }
        if let Out::Spawn {
            intent,
            brief,
            profile,
            volumes,
            history,
        } = out
        {
            let mut adopted = adoption(state, volumes, cursor, prior_conversation);
            adopted.brief.clone_from(brief);
            adopted.profile.clone_from(profile);
            // The root pins a run policy. A client-selected child role or
            // context cannot replace it with an unrestricted subtree.
            if let Some(topology) = state
                .profile
                .as_ref()
                .and_then(|profile| profile.topology.as_ref())
            {
                let child = adopted
                    .profile
                    .get_or_insert_with(|| super::CreationConfig {
                        question_plugin: false,
                        topology: None,
                        model: None,
                        continuation: None,
                        context: super::SpawnContext::Fork,
                        template: None,
                        peers: Default::default(),
                    });
                child.topology = Some(topology.clone());
            }
            if let Some(history) = history {
                if history.conversation > prior_conversation {
                    return Err(Error::Record("history fork exceeds committed conversation"));
                }
                adopted.conversation_tail = history.conversation;
                adopted.checkpoint = history.checkpoint;
            }
            let spawn = commit::spawn_at(path, intent, cursor, adopted, history.as_ref())?;
            builder.created.insert(spawn.child.inbox()?);
            for effect in spawn.request.effects {
                builder.contribute(effect)?;
            }
        }
    }
    let mut sequence = prior_conversation;
    for out in &step.outs {
        match out {
            Out::InputCancelled {
                input,
                conversation,
            } => {
                let (path, record) =
                    super::cancellation::cancellation(path, *input, *conversation)?;
                builder.append(path, record);
            }
            Out::Diagnostic { diagnostic, .. } => {
                builder.condition(CommitCondition::Tail {
                    path: diagnostic.path()?,
                    expected: diagnostic.records(),
                })?;
            }
            Out::Commit(_) => {}
            Out::Supervised { run, control } => {
                let event = match control.action {
                    crate::supervision::Action::Pause => crate::supervision::Lifecycle::Paused {
                        signal: control.signal,
                    },
                    crate::supervision::Action::Stop => crate::supervision::Lifecycle::Stopped {
                        signal: control.signal,
                    },
                    crate::supervision::Action::Alert => {
                        return Err(Error::Record("alert cannot close execution admission"));
                    }
                };
                builder.append(
                    crate::supervision::streams::path(path)
                        .map_err(|_| Error::Record("invalid agent event path"))?,
                    crate::supervision::streams::lifecycle_record(&crate::supervision::Event {
                        version: 1,
                        agent: path.clone(),
                        run: Some(*run),
                        source: control.source.clone(),
                        event,
                    })
                    .map_err(|_| Error::Record("agent event did not encode"))?,
                );
            }
            Out::Conversation(record) => {
                builder.append(conversation.clone(), record.clone());
                sequence = sequence
                    .checked_add(1)
                    .ok_or(Error::Record("agent conversation tail overflow"))?;
            }
            Out::TurnDone { run, turn } => {
                let event = crate::supervision::Event {
                    version: 1,
                    agent: path.clone(),
                    run: Some(*run),
                    source: crate::supervision::Position {
                        stream: conversation.clone(),
                        sequence: sequence
                            .checked_sub(1)
                            .filter(|sequence| *sequence >= prior_conversation)
                            .ok_or(Error::Record("turn completion has no conversation entry"))?,
                    },
                    event: crate::supervision::Lifecycle::TurnDone { turn: *turn },
                };
                builder.append(
                    crate::supervision::streams::path(path)
                        .map_err(|_| Error::Record("invalid agent event path"))?,
                    crate::supervision::streams::lifecycle_record(&event)
                        .map_err(|_| Error::Record("agent event did not encode"))?,
                );
            }
            Out::GoalCompleted { run } => {
                let sequence = sequence
                    .checked_sub(1)
                    .filter(|sequence| *sequence >= prior_conversation)
                    .ok_or(Error::Record("goal completion has no conversation entry"))?;
                let path = super::goal::result(path, *run)?;
                builder.condition(CommitCondition::Absent { path: path.clone() })?;
                builder.append(path, super::goal::Completed { sequence }.encode()?);
            }
            Out::Checkpoint
            | Out::Announced(_)
            | Out::Steered(_)
            | Out::Notices(_)
            | Out::PendingSpawns(_)
            | Out::AwaitWatches(_)
            | Out::PeerGuard { .. } => {}
            Out::Send { to, record } => {
                if let InboxRecord::Cancel(cancel) = record {
                    let (path, record) =
                        super::cancellation::cancellation(to, cancel.input, cancel.conversation)?;
                    builder.append(path, record);
                }
                if matches!(
                    record,
                    InboxRecord::ForkAdopted(_)
                        | InboxRecord::Archive(_)
                        | InboxRecord::WorkspaceChanged(_)
                ) {
                    return Err(Error::Record(
                        "send cannot carry adoption, archive or workspace changes",
                    ));
                }
                builder.recipient(to)?;
                if let InboxRecord::Message(message) | InboxRecord::FollowUp(message) = record
                    && message.from == sender
                {
                    let marker = path.sent_message(&message.id)?;
                    builder.condition(CommitCondition::Absent {
                        path: marker.clone(),
                    })?;
                    builder.append(
                        marker,
                        super::sent::SentRecord {
                            to: to.clone(),
                            id: message.id.clone(),
                        }
                        .encode()?,
                    );
                }
                if matches!(record, InboxRecord::Message(_) | InboxRecord::FollowUp(_))
                    && to != path
                {
                    builder.offer(to.inbox()?, to.closed()?, record.encode()?);
                } else {
                    builder.append(to.inbox()?, record.encode()?);
                }
            }
            Out::Archive { target, id } => {
                if !builder.created.contains(&target.inbox()?) {
                    builder.recipient(target)?;
                }
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
        })?;
    }
    if let Some(status) = &step.status {
        let path = path.status()?;
        builder.condition(CommitCondition::Tail {
            path: path.clone(),
            expected: state.status_tail - 1,
        })?;
        builder.append(path, status.append_record()?);
    }
    // The primitive fork carries records for its own destination. Fold hard
    // records into that mutation rather than emitting a conflicting append.
    for mutation in &mut builder.forks {
        if let CommitMutation::Fork {
            destination,
            records,
            ..
        } = mutation
            && let Some(appended) = builder.appends.remove(destination)
        {
            records.extend(appended);
        }
    }
    let mut mutations = builder.forks;
    mutations.extend(
        builder
            .appends
            .into_iter()
            .map(|(path, records)| CommitMutation::Append { path, records }),
    );
    let conditions: Vec<_> = builder.conditions.into_values().collect();
    let hard = (!conditions.is_empty() || !mutations.is_empty())
        .then(|| CommitEffect::hard(conditions, mutations));
    let effects = hard
        .into_iter()
        .chain(builder.soft.into_iter().map(|(inbox, (records, closed))| {
            let mut conditions = vec![
                CommitCondition::Present {
                    path: inbox.clone(),
                },
                CommitCondition::Absent { path: closed },
            ];
            conditions.extend(peer_guards.remove(&inbox).unwrap_or_default());
            conditions.sort_by(|left, right| {
                CommitCondition::path(left).cmp(CommitCondition::path(right))
            });
            CommitEffect {
                conditions,
                mutations: vec![CommitMutation::Append {
                    path: inbox,
                    records,
                }],
                soft: true,
            }
        }))
        .collect();
    Ok(CommitRequest {
        deadline_unix_millis: None,
        effects,
        idempotency_key: key,
    })
}

/// The adoption a spawn of `state` writes as its child's first record; the
/// spawn names the child, and the fork cut is `cursor`.
pub(super) fn adoption(
    state: &AgentState,
    volumes: &[WorkspaceEntry],
    cursor: u64,
    conversation_tail: u64,
) -> ForkAdopted {
    ForkAdopted {
        profile: None,
        agent: state.path.id(),
        parent: None,
        forked_at: cursor,
        conversation_tail,
        // The checkpoint inherited at the cut, never one this step appends.
        checkpoint: state.checkpoint.filter(|at| *at < conversation_tail),
        volumes: volumes.to_vec(),
        brief: None,
        policy: state.policy.clone(),
    }
}

/// Stable key of one delivery: the agent and the A4 trigger identity.
pub fn delivery_key(agent: &AgentState, trigger: &str) -> Result<IdempotencyKey, Error> {
    commit::intent_key("step", &[agent.path.as_str(), trigger])
}
