//! The three agent commit shapes. Each is exactly one Streams `CommitRequest`.
//!
//! - `send`: `Absent{<to>/closed}` + `Tail{<to>}` + `Append{<to>}`.
//! - `archive`: `Absent{<to>/closed}` + `Tail{<to>}` + `Append{<to>, Archive}` +
//!   `Append{<to>/closed}`.
//! - `spawn`: `Absent{<child>}` + `Fork{<parent> -> <parent>/children/<id>, exclude
//!   NOT_INHERITED, records [ForkAdopted]}`; actor-state and volume contributions
//!   join the same request through its public `conditions`/`mutations`.
//!
//! Streams requires a condition on every appended path, so a sender names the
//! recipient inbox tail it observed. A `Tail` conflict on that inbox alone is
//! re-decided by re-reading the tail; the key is unspent by any conflict.
//!
//! Requests are pure values: a caller that composes them (the actor publishing its
//! checkpoint, the filesystem contributing `fork_many`) extends the request and
//! submits it once with [`submit`].

use super::{
    AgentId, AgentPath, AgentRecord, Closed, Control, Error, ForkAdopted, InboxRecord, IntentId,
    Phase, Sender, StatusRecord, layout,
};
use acyclic_stream::{
    CommitCondition, CommitConflict, CommitMutation, CommitOutcome, CommitRequest,
    CommittedEnvelope, IdempotencyKey, StreamError, StreamPath, StreamProvider,
};

/// Domain-separated idempotency key; one stable key per intent.
pub fn intent_key(kind: &str, parts: &[&str]) -> Result<IdempotencyKey, Error> {
    let mut hasher = blake3::Hasher::new_derive_key("acyclic.harness.agent.intent-key.v1");
    hasher.update(kind.as_bytes());
    for part in parts {
        hasher.update(&[0]);
        hasher.update(&(part.len() as u64).to_le_bytes());
        hasher.update(part.as_bytes());
    }
    IdempotencyKey::new(hasher.finalize().to_hex().as_bytes().to_vec()).map_err(Error::Stream)
}

fn sender_text(sender: &Sender) -> String {
    match sender {
        Sender::User { principal } => format!("user:{principal}"),
        Sender::Agent { path } => format!("agent:{path}"),
        Sender::Host { host } => format!("host:{host}"),
    }
}

/// One conditional append of `record` to the open inbox of `to`.
///
/// Used for `Message`, `Cancel`, `Approval`, `ApprovalRequested`, `ChildDone` and
/// `Hook`. The key binds the sender and intent, so a lost acknowledgement retries
/// the identical request.
pub fn send(
    to: &AgentPath,
    inbox_tail: u64,
    sender: &Sender,
    record: &InboxRecord,
) -> Result<CommitRequest, Error> {
    let kind = match record {
        InboxRecord::ForkAdopted(_) => {
            return Err(Error::Record("ForkAdopted is written by spawn"));
        }
        InboxRecord::Archive(_) => return Err(Error::Record("archive has its own commit")),
        InboxRecord::Message(_) => "message",
        InboxRecord::ChildDone(_) => "child-done",
        InboxRecord::Approval(_) => "approval",
        InboxRecord::ApprovalRequested(_) => "approval-requested",
        InboxRecord::Cancel(_) => "cancel",
        InboxRecord::Hook(_) => "hook",
        InboxRecord::Spawn(_) => "spawn",
    };
    let intent = intent_of(record);
    existing(inbox_tail)?;
    Ok(CommitRequest {
        conditions: vec![
            CommitCondition::Absent { path: to.closed()? },
            CommitCondition::Tail {
                path: to.inbox()?,
                expected: inbox_tail,
            },
        ],
        mutations: vec![CommitMutation::Append {
            path: to.inbox()?,
            records: vec![record.encode()?.into()],
        }],
        idempotency_key: intent_key(kind, &[to.as_str(), &sender_text(sender), &intent])?,
    })
}

/// Every inbox begins with `ForkAdopted`, so a live inbox tail is at least one,
/// and `Tail{n >= 1}` never matches an absent path: a send cannot create an agent.
fn existing(inbox_tail: u64) -> Result<(), Error> {
    if inbox_tail == 0 {
        return Err(Error::NoSuchAgent);
    }
    Ok(())
}

/// Observed inbox tail of `agent`; [`Error::NoSuchAgent`] when it was never created.
pub async fn inbox_tail<P: StreamProvider + ?Sized>(
    provider: &P,
    agent: &AgentPath,
) -> Result<u64, Error> {
    match provider.tail(agent.inbox()?).await {
        Ok(0) | Err(StreamError::NotFound) => Err(Error::NoSuchAgent),
        Ok(tail) => Ok(tail),
        Err(error) => Err(Error::Stream(error)),
    }
}

fn intent_of(record: &InboxRecord) -> String {
    match record {
        InboxRecord::Message(message) => message.id.as_str().to_owned(),
        InboxRecord::Cancel(control) | InboxRecord::Archive(control) => {
            control.id.as_str().to_owned()
        }
        InboxRecord::Approval(approval) => approval.id.as_str().to_owned(),
        InboxRecord::ApprovalRequested(request) => {
            format!("{}:{}", request.child, request.id.as_str())
        }
        InboxRecord::ChildDone(done) => done.child.as_str().to_owned(),
        InboxRecord::Hook(hook) => hook.id.as_str().to_owned(),
        InboxRecord::Spawn(spawn) => spawn.id.as_str().to_owned(),
        InboxRecord::ForkAdopted(adopted) => adopted.agent.as_str().to_owned(),
    }
}

/// One conditional commit closing the inbox of `target`.
pub fn archive(
    target: &AgentPath,
    inbox_tail: u64,
    by: &Sender,
    id: &IntentId,
) -> Result<CommitRequest, Error> {
    let control = Control {
        by: by.clone(),
        id: id.clone(),
    };
    existing(inbox_tail)?;
    let closed = target.closed()?;
    Ok(CommitRequest {
        conditions: vec![
            CommitCondition::Absent {
                path: closed.clone(),
            },
            CommitCondition::Tail {
                path: target.inbox()?,
                expected: inbox_tail,
            },
        ],
        mutations: vec![
            CommitMutation::Append {
                path: target.inbox()?,
                records: vec![InboxRecord::Archive(control).encode()?.into()],
            },
            CommitMutation::Append {
                path: closed,
                records: vec![
                    Closed {
                        by: by.clone(),
                        id: id.clone(),
                    }
                    .encode()?
                    .into(),
                ],
            },
        ],
        idempotency_key: intent_key("archive", &[target.as_str(), &sender_text(by), id.as_str()])?,
    })
}

/// The Streams part of one spawn.
pub struct Spawn {
    /// New child's path.
    pub child: AgentPath,
    /// The request; actor and filesystem contributions extend it before [`submit`].
    pub request: CommitRequest,
}

/// Forks `parent`'s subtree to its new child at inbox prefix `at_tail`.
///
/// `at_tail` is the parent's consumed inbox cursor, so the child's first delivery
/// is `ForkAdopted` at that sequence. The conversation is inherited at the commit
/// cut; `children`, `status` and `closed` are not. The key binds the parent and
/// the spawning intent, so a retry names the same child and replays exactly.
pub fn spawn(
    parent: &AgentPath,
    intent: &IntentId,
    at_tail: u64,
    mut adopted: ForkAdopted,
) -> Result<Spawn, Error> {
    let child_id = AgentId::derive(&parent.id(), intent.as_str());
    let child = parent.child(&child_id)?;
    adopted.agent = child_id;
    adopted.parent = Some(parent.clone());
    adopted.forked_at = at_tail;
    let record = InboxRecord::ForkAdopted(Box::new(adopted)).encode()?;
    Ok(Spawn {
        request: CommitRequest {
            conditions: vec![
                CommitCondition::Absent {
                    path: child.inbox()?,
                },
                CommitCondition::Absent {
                    path: child.status()?,
                },
            ],
            mutations: vec![
                CommitMutation::Fork {
                    exclude: layout::not_inherited()?,
                    source: parent.inbox()?,
                    destination: child.inbox()?,
                    at_tail,
                    records: vec![record.into()],
                },
                first_status(&child, at_tail)?,
            ],
            idempotency_key: intent_key("spawn", &[parent.as_str(), intent.as_str()])?,
        },
        child,
    })
}

/// An agent's first status rides with its membership (its subtree's creation),
/// so every listed agent has a status: `Idle` at its `ForkAdopted` sequence.
fn first_status(agent: &AgentPath, forked_at: u64) -> Result<CommitMutation, Error> {
    Ok(CommitMutation::Append {
        path: agent.status()?,
        records: vec![
            StatusRecord {
                phase: Phase::Idle,
                cursor: forked_at,
            }
            .encode()?
            .into(),
        ],
    })
}

/// Creates a root agent. Its volumes and actor state join the same request.
pub fn create_root(intent: &IntentId, mut adopted: ForkAdopted) -> Result<Spawn, Error> {
    let child = AgentPath::root(&adopted.agent);
    adopted.parent = None;
    adopted.forked_at = 0;
    adopted.conversation_tail = 0;
    let inbox = child.inbox()?;
    Ok(Spawn {
        request: CommitRequest {
            conditions: vec![
                CommitCondition::Absent {
                    path: inbox.clone(),
                },
                CommitCondition::Absent {
                    path: child.status()?,
                },
            ],
            mutations: vec![
                CommitMutation::Append {
                    path: inbox,
                    records: vec![InboxRecord::ForkAdopted(Box::new(adopted)).encode()?.into()],
                },
                first_status(&child, 0)?,
            ],
            idempotency_key: intent_key("create", &[child.as_str(), intent.as_str()])?,
        },
        child,
    })
}

/// Result of submitting one agent commit.
#[derive(Debug)]
pub enum Submitted {
    /// Every mutation committed (also returned for an exact replay).
    Committed(CommittedEnvelope),
    /// A recipient inbox is closed; nothing changed and the key is unspent.
    RecipientClosed(Vec<StreamPath>),
    /// Only recipient inbox tails moved; re-read them and re-decide.
    InboxMoved(Vec<StreamPath>),
    /// Another condition failed; nothing changed and the key is unspent.
    Conflict(Vec<CommitConflict>),
}

/// Submits a request once. An uncertain completion returns
/// [`Error::Unknown`] carrying the identical request, which the caller retries
/// unchanged until it resolves; it never mints a new key.
pub async fn submit<P: StreamProvider + ?Sized>(
    provider: &P,
    request: CommitRequest,
) -> Result<Submitted, Error> {
    match provider.commit(request.clone()).await {
        Ok(CommitOutcome::Committed(envelope)) => Ok(Submitted::Committed(envelope)),
        Ok(CommitOutcome::Conflict(conflicts)) => Ok(classify(conflicts)),
        Err(StreamError::Unavailable | StreamError::DeadlineElapsed) => {
            Err(Error::Unknown(Box::new(request)))
        }
        Err(error) => Err(Error::Stream(error)),
    }
}

/// Classifies a definitive conflict. Closed recipients win over moved tails: a
/// closed inbox never reopens, so re-reading its tail cannot help.
fn classify(conflicts: Vec<CommitConflict>) -> Submitted {
    let mut closed = Vec::new();
    let mut moved = Vec::new();
    for conflict in &conflicts {
        match conflict {
            CommitConflict::Exists { path } if is_closed_marker(path) => closed.push(path.clone()),
            CommitConflict::Tail { path, .. }
                if AgentPath::try_from(path.as_str().to_owned()).is_ok() =>
            {
                moved.push(path.clone());
            }
            _ => return Submitted::Conflict(conflicts),
        }
    }
    if !closed.is_empty() {
        Submitted::RecipientClosed(closed)
    } else if moved.is_empty() {
        Submitted::Conflict(conflicts)
    } else {
        Submitted::InboxMoved(moved)
    }
}

fn is_closed_marker(path: &StreamPath) -> bool {
    path.as_str()
        .strip_suffix("/closed")
        .is_some_and(|agent| AgentPath::try_from(agent.to_owned()).is_ok())
}
