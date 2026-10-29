//! Agent-shaped histories: one subtree per agent whose root stream is its inbox.
//!
//! These are the exact commit shapes of the wave-2 agent model, expressed with
//! public provider operations only: spawn (subtree fork + adoption record +
//! companion writes), a closed inbox, inherited follows and fork-of-fork lineage.
use super::fork::{absent, append, facts, key, path, values};
use super::{Failure, History, Invariant};
use crate::StreamProviderExt as _;
use crate::{
    ChildrenPageRequest, CommitCondition, CommitMutation, CommitOutcome, CommitRequest,
    CommittedEnvelope, IdempotencyOutcome, OwnedBytes, ReadRequest, Record, StreamError,
    StreamPath, StreamProvider,
};
use futures::TryFutureExt as _;
use futures::{StreamExt as _, TryStreamExt as _};

/// Never inherited by a spawned child.
const EXCLUDE: [&str; 3] = ["children", "status", "closed"];

fn exclude<P: StreamProvider + ?Sized>(
    history: &History<'_, P>,
) -> Result<Vec<StreamPath>, Failure> {
    EXCLUDE
        .iter()
        .map(|segment| history.result("derive_exclusion", StreamPath::new(*segment)))
        .collect()
}

fn records_of(values: &[&'static [u8]]) -> Vec<crate::AppendRecord> {
    values
        .iter()
        .map(|value| OwnedBytes::from_static(value).into())
        .collect()
}

async fn records<P: StreamProvider + ?Sized>(
    history: &History<'_, P>,
    target: &StreamPath,
) -> Result<Vec<Record>, Failure> {
    let reader = history.result(
        "read_agent_records",
        history
            .provider
            .read(ReadRequest {
                key: None,
                path: target.clone(),
                from: 0,
                limit: 8,
                at_least: Default::default(),
            })
            .await,
    )?;
    history.result(
        "collect_agent_records",
        reader.take(9).try_collect::<Vec<_>>().await,
    )
}

/// The A4-style checkpoint condition: the stream's current tail and last commit.
async fn head<P: StreamProvider + ?Sized>(
    history: &History<'_, P>,
    target: &StreamPath,
) -> Result<CommitCondition, Failure> {
    let records = records(history, target).await?;
    let Some(last) = records.last() else {
        return Err(Failure::Violation {
            case: history.case,
            step: "read_actor_head",
            invariant: Invariant::SpawnAtomicity,
        });
    };
    Ok(CommitCondition::Head {
        path: target.clone(),
        expected: last.sequence + 1,
        commit_id: last.commit_id,
    })
}

async fn commit<P: StreamProvider + ?Sized>(
    history: &History<'_, P>,
    step: &'static str,
    request: CommitRequest,
) -> Result<CommittedEnvelope, Failure> {
    match history.result(step, history.provider.commit(request).await)? {
        CommitOutcome::Committed(receipt) => facts(history, &receipt).await,
        CommitOutcome::Conflict(_) => Err(Failure::Violation {
            case: history.case,
            step,
            invariant: Invariant::StableKeyRetry,
        }),
    }
}

fn spawn_request<P: StreamProvider + ?Sized>(
    history: &History<'_, P>,
    parent: &StreamPath,
    child: &StreamPath,
    at_tail: u64,
    adopted: &'static [u8],
    conversation_tail: u64,
    label: &str,
) -> Result<CommitRequest, Failure> {
    let conversation = history.result(
        "derive_conversation",
        StreamPath::new(format!("{}/conversation", parent.as_str())),
    )?;
    Ok(CommitRequest {
        deadline_unix_millis: None,
        conditions: vec![
            CommitCondition::Absent {
                path: child.clone(),
            },
            CommitCondition::Tail {
                path: conversation.clone(),
                expected: conversation_tail,
            },
        ],
        mutations: vec![
            CommitMutation::Fork {
                exclude: exclude(history)?,
                source: parent.clone(),
                destination: child.clone(),
                at_tail,
                records: records_of(&[adopted]),
            },
            CommitMutation::Append {
                path: conversation,
                records: records_of(&[b"spawn-result"]),
            },
        ],
        idempotency_key: key(history, label)?,
    })
}

/// The parent subtree and companion streams of one spawn history.
struct Spawn {
    conversation: StreamPath,
    status: StreamPath,
    old: StreamPath,
    child: StreamPath,
    actor: StreamPath,
    parent_actor: StreamPath,
    volume: StreamPath,
    child_volume: StreamPath,
}

impl Spawn {
    fn new<P: StreamProvider + ?Sized>(history: &History<'_, P>) -> Result<Self, Failure> {
        Ok(Self {
            conversation: path(history, "/conversation")?,
            status: path(history, "/status")?,
            old: path(history, "/children/old")?,
            child: path(history, "/children/new")?,
            actor: path(history, "-actors/new")?,
            parent_actor: path(history, "-actors/parent")?,
            volume: path(history, "-volumes/parent")?,
            child_volume: path(history, "-volumes/new")?,
        })
    }

    /// Seeds the parent and returns a parent actor head that is already stale.
    async fn seed<P: StreamProvider + ?Sized>(
        &self,
        history: &History<'_, P>,
    ) -> Result<CommitCondition, Failure> {
        let parent = &history.path;
        append(history, parent, "inbox-adopted", b"adopted-root", 0).await?;
        append(history, parent, "inbox-message", b"message", 1).await?;
        append(history, &self.conversation, "conversation-turn", b"turn", 0).await?;
        append(history, &self.status, "status-running", b"running", 0).await?;
        append(history, &self.old, "old-child", b"old-adopted", 0).await?;
        append(history, &self.volume, "parent-volume", b"volume", 0).await?;
        append(
            history,
            &self.parent_actor,
            "parent-state-0",
            b"parent-state-0",
            0,
        )
        .await?;
        let stale = head(history, &self.parent_actor).await?;
        append(
            history,
            &self.parent_actor,
            "parent-state-1",
            b"parent-state-1",
            1,
        )
        .await?;
        Ok(stale)
    }

    /// The wave-2 spawn as one commit, guarded by the given parent actor head.
    fn request<P: StreamProvider + ?Sized>(
        &self,
        history: &History<'_, P>,
        parent_head: CommitCondition,
    ) -> Result<CommitRequest, Failure> {
        Ok(CommitRequest {
            deadline_unix_millis: None,
            conditions: vec![
                CommitCondition::Absent {
                    path: self.actor.clone(),
                },
                CommitCondition::Absent {
                    path: self.child_volume.clone(),
                },
                parent_head,
                CommitCondition::Absent {
                    path: self.child.clone(),
                },
            ],
            mutations: vec![
                CommitMutation::Fork {
                    exclude: exclude(history)?,
                    source: history.path.clone(),
                    destination: self.child.clone(),
                    at_tail: 2,
                    records: records_of(&[b"fork-adopted"]),
                },
                CommitMutation::Append {
                    path: self.actor.clone(),
                    records: records_of(&[b"state-0"]),
                },
                CommitMutation::Fork {
                    exclude: Vec::new(),
                    source: self.volume.clone(),
                    destination: self.child_volume.clone(),
                    at_tail: 1,
                    records: Vec::new(),
                },
                CommitMutation::Append {
                    path: self.parent_actor.clone(),
                    records: records_of(&[b"parent-state-2"]),
                },
                // Unconditioned companion: a same-commit write to a source descendant.
                CommitMutation::Append {
                    path: self.conversation.clone(),
                    records: records_of(&[b"spawn-result"]),
                },
            ],
            idempotency_key: history.key.clone(),
        })
    }

    /// A refused spawn left no fork, companion write or key binding.
    async fn require_nothing<P: StreamProvider + ?Sized>(
        &self,
        history: &History<'_, P>,
    ) -> Result<(), Failure> {
        for nothing in [&self.child, &self.actor, &self.child_volume] {
            absent(history, nothing, Invariant::SpawnAtomicity).await?;
        }
        values(
            history,
            &self.conversation,
            &[b"turn"],
            Invariant::SpawnAtomicity,
        )
        .await?;
        values(
            history,
            &self.parent_actor,
            &[b"parent-state-0", b"parent-state-1"],
            Invariant::SpawnAtomicity,
        )
        .await?;
        history.require_unspent().await
    }

    /// The child's inbox is the parent's prefix plus its adoption record, and
    /// the same-commit parent append is invisible to the child's cut.
    async fn require_child<P: StreamProvider + ?Sized>(
        &self,
        history: &History<'_, P>,
        envelope: &CommittedEnvelope,
    ) -> Result<(), Failure> {
        let parent_records = records(history, &history.path).await?;
        let child_records = records(history, &self.child).await?;
        history.check(
            "child_inbox",
            Invariant::SubtreeMembership,
            child_records.len() == 3
                && child_records
                    .iter()
                    .zip(&parent_records)
                    .all(|(left, right)| left == right)
                && child_records.get(2).is_some_and(|record| {
                    record.sequence == 2
                        && record.value.as_ref() == b"fork-adopted"
                        && record.commit_id == envelope.commit_id
                }),
        )?;
        let inherited = path(history, "/children/new/conversation")?;
        values(history, &inherited, &[b"turn"], Invariant::PreCommitCut).await?;
        values(
            history,
            &self.conversation,
            &[b"turn", b"spawn-result"],
            Invariant::PreCommitCut,
        )
        .await?;
        for excluded in [
            "/children/new/status",
            "/children/new/children/old",
            "/children/new/children/new",
            "/children/new/closed",
        ] {
            absent(
                history,
                &path(history, excluded)?,
                Invariant::SubtreeMembership,
            )
            .await?;
        }
        Ok(())
    }

    /// Every companion write landed exactly once and the parent kept its own.
    async fn require_companions<P: StreamProvider + ?Sized>(
        &self,
        history: &History<'_, P>,
    ) -> Result<(), Failure> {
        values(
            history,
            &self.status,
            &[b"running"],
            Invariant::ForkIsolation,
        )
        .await?;
        values(
            history,
            &self.old,
            &[b"old-adopted"],
            Invariant::ForkIsolation,
        )
        .await?;
        values(
            history,
            &self.actor,
            &[b"state-0"],
            Invariant::SpawnAtomicity,
        )
        .await?;
        values(
            history,
            &self.parent_actor,
            &[b"parent-state-0", b"parent-state-1", b"parent-state-2"],
            Invariant::SpawnAtomicity,
        )
        .await?;
        values(
            history,
            &self.child_volume,
            &[b"volume"],
            Invariant::SpawnAtomicity,
        )
        .await
    }
}

/// Spawn: subtree fork with exclusions and the adoption record, a volume fork,
/// child and parent actor state and a parent conversation append, all in one
/// commit. A stale parent head publishes nothing and leaves the same key usable.
pub(super) async fn spawn<P: StreamProvider + ?Sized>(
    history: &History<'_, P>,
) -> Result<(), Failure> {
    let shape = Spawn::new(history)?;
    let stale = shape.seed(history).await?;
    let refused = history.result(
        "stale_spawn",
        history
            .provider
            .commit(shape.request(history, stale)?)
            .await,
    )?;
    history.check(
        "stale_spawn",
        Invariant::ExactConflict,
        matches!(refused, CommitOutcome::Conflict(_)),
    )?;
    shape.require_nothing(history).await?;
    let request = shape.request(history, head(history, &shape.parent_actor).await?)?;
    let outcome = history.result("spawn", history.provider.commit(request.clone()).await)?;
    let CommitOutcome::Committed(receipt) = &outcome else {
        return history.check("spawn", Invariant::StableKeyRetry, false);
    };
    let envelope = facts(history, receipt).await?;
    shape.require_child(history, &envelope).await?;
    shape.require_companions(history).await?;
    let replay = history.result("replay_spawn", history.provider.commit(request).await)?;
    history.check("replay_spawn", Invariant::ExactReplay, replay == outcome)?;
    history
        .require_recovery(IdempotencyOutcome::Commit(envelope))
        .await
}

fn send<P: StreamProvider + ?Sized>(
    history: &History<'_, P>,
    closed: &StreamPath,
    value: &'static [u8],
    label: &str,
) -> Result<CommitRequest, Failure> {
    Ok(CommitRequest {
        deadline_unix_millis: None,
        conditions: vec![CommitCondition::Absent {
            path: closed.clone(),
        }],
        mutations: vec![CommitMutation::Append {
            path: history.path.clone(),
            records: records_of(&[value]),
        }],
        idempotency_key: key(history, label)?,
    })
}

/// Archive closes the inbox with one marker stream; every later send is a
/// typed conflict that writes nothing and spends no key, while the archive's
/// own key still replays its original receipt.
pub(super) async fn closed_inbox<P: StreamProvider + ?Sized>(
    history: &History<'_, P>,
) -> Result<(), Failure> {
    let closed = path(history, "/closed")?;
    commit(
        history,
        "send_open",
        send(history, &closed, b"message", "send-open")?,
    )
    .await?;
    let archive = CommitRequest {
        deadline_unix_millis: None,
        conditions: vec![CommitCondition::Absent {
            path: closed.clone(),
        }],
        mutations: vec![
            CommitMutation::Append {
                path: history.path.clone(),
                records: records_of(&[b"archive"]),
            },
            CommitMutation::Append {
                path: closed.clone(),
                records: records_of(&[b"closed"]),
            },
        ],
        idempotency_key: history.key.clone(),
    };
    let archived = history.result("archive", history.provider.commit(archive.clone()).await)?;
    history.check(
        "archive",
        Invariant::StableKeyRetry,
        matches!(archived, CommitOutcome::Committed(_)),
    )?;
    let late = send(history, &closed, b"late", "send-late")?;
    let late_key = late.idempotency_key.clone();
    let refused = history.result("send_closed", history.provider.commit(late).await)?;
    history.check(
        "send_closed",
        Invariant::ExactConflict,
        matches!(refused, CommitOutcome::Conflict(ref conflicts)
            if conflicts.len() == 1 && conflicts.iter().all(|conflict| matches!(conflict, crate::CommitConflict::Exists { path } if *path == closed))),
    )?;
    let late_observed = history.result(
        "inspect_closed_send",
        history.provider.inspect_idempotency(late_key).await,
    )?;
    history.check(
        "inspect_closed_send",
        Invariant::RejectedKeyUnspent,
        late_observed.is_none(),
    )?;
    history.require_values(&[b"message", b"archive"]).await?;
    values(
        history,
        &closed,
        &[b"closed"],
        Invariant::NoDuplicateRecords,
    )
    .await?;
    let replay = history.result("replay_archive", history.provider.commit(archive).await)?;
    history.check("replay_archive", Invariant::ExactReplay, replay == archived)?;
    let CommitOutcome::Committed(receipt) = &archived else {
        return history.check("archive_receipt", Invariant::StableKeyRetry, false);
    };
    let envelope = facts(history, receipt).await?;
    history
        .require_recovery(IdempotencyOutcome::Commit(envelope))
        .await
}

/// A path inherited through a root-only fork publication is followable at
/// once and wakes for its own appends, never the source's; a cursor-zero
/// follow of a missing path is `NotFound` and creates nothing.
pub(super) async fn inherited_follow<P: StreamProvider + ?Sized>(
    history: &History<'_, P>,
) -> Result<(), Failure> {
    let source = path(history, "/conversation")?;
    let child = path(history, "/children/c")?;
    let inherited = path(history, "/children/c/conversation")?;
    let missing = path(history, "/children/missing/conversation")?;
    match history
        .provider
        .follow(crate::FollowRequest::new(missing.clone(), 0))
        .map_ok(crate::records_only)
        .await
    {
        Err(StreamError::NotFound) => {}
        Ok(mut follow) => history.check(
            "follow_missing",
            Invariant::MissingFollow,
            matches!(follow.next().await, Some(Err(StreamError::NotFound))),
        )?,
        Err(source) => {
            return Err(super::provider_failure(
                history.case,
                "follow_missing",
                source,
            ));
        }
    }
    absent(history, &missing, Invariant::MissingFollow).await?;
    append(history, &history.path, "inbox", b"adopted", 0).await?;
    append(history, &source, "turn", b"turn", 0).await?;
    let request = spawn_request(
        history,
        &history.path,
        &child,
        1,
        b"child-adopted",
        1,
        "spawn",
    )?;
    commit(history, "spawn", request).await?;
    let mut source_follow = history.result(
        "follow_source",
        history
            .provider
            .follow(crate::FollowRequest::new(source.clone(), 2))
            .map_ok(crate::records_only)
            .await,
    )?;
    let mut inherited_follow = history.result(
        "follow_inherited",
        history
            .provider
            .follow(crate::FollowRequest::new(inherited.clone(), 1))
            .map_ok(crate::records_only)
            .await,
    )?;
    // Source first: a shared wake or shared history would surface it in the child.
    append(history, &source, "source-live", b"source-live", 2).await?;
    append(history, &inherited, "child-live", b"child-live", 1).await?;
    for (step, follow, sequence, value) in [
        (
            "source_wake",
            &mut source_follow,
            2,
            b"source-live".as_slice(),
        ),
        ("inherited_wake", &mut inherited_follow, 1, b"child-live"),
    ] {
        let next = follow.next().await.unwrap_or(Err(StreamError::Unavailable));
        let record = history.result(step, next)?;
        history.check(
            step,
            Invariant::ForkIsolation,
            record.sequence == sequence && record.value.as_ref() == value,
        )?;
    }
    Ok(())
}

/// Lineage is the inherited inbox prefix: a grandchild carries every
/// ancestor's adoption record with its original commit identity, inherits
/// conversation at each cut and nests nowhere but under its own parent.
pub(super) async fn fork_lineage<P: StreamProvider + ?Sized>(
    history: &History<'_, P>,
) -> Result<(), Failure> {
    let root = history.path.clone();
    let child = path(history, "/children/c")?;
    let grandchild = path(history, "/children/c/children/g")?;
    append(history, &root, "root-adopted", b"root-adopted", 0).await?;
    append(
        history,
        &path(history, "/conversation")?,
        "root-turn",
        b"root-turn",
        0,
    )
    .await?;
    let first = spawn_request(
        history,
        &root,
        &child,
        1,
        b"child-adopted",
        1,
        "spawn-child",
    )?;
    let child_spawn = commit(history, "spawn_child", first).await?;
    append(history, &child, "child-message", b"child-message", 2).await?;
    append(
        history,
        &path(history, "/children/c/conversation")?,
        "child-turn",
        b"child-turn",
        1,
    )
    .await?;
    let second = CommitRequest {
        idempotency_key: history.key.clone(),
        ..spawn_request(
            history,
            &child,
            &grandchild,
            3,
            b"grandchild-adopted",
            2,
            "unused",
        )?
    };
    let grandchild_spawn = commit(history, "spawn_grandchild", second).await?;
    append(
        history,
        &path(history, "/conversation")?,
        "root-late",
        b"root-late",
        2,
    )
    .await?;
    let root_records = records(history, &root).await?;
    let child_records = records(history, &child).await?;
    let lineage = records(history, &grandchild).await?;
    history.check(
        "grandchild_lineage",
        Invariant::Lineage,
        lineage.len() == 4
            && lineage.first() == root_records.first()
            && lineage.get(1) == child_records.get(1)
            && lineage.get(2) == child_records.get(2)
            && lineage.get(1).is_some_and(|record| {
                record.commit_id == child_spawn.commit_id
                    && record.value.as_ref() == b"child-adopted"
            })
            && lineage.get(3).is_some_and(|record| {
                record.commit_id == grandchild_spawn.commit_id
                    && record.value.as_ref() == b"grandchild-adopted"
            }),
    )?;
    values(
        history,
        &path(history, "/children/c/children/g/conversation")?,
        &[b"root-turn", b"child-turn"],
        Invariant::Lineage,
    )
    .await?;
    values(
        history,
        &path(history, "/children/c/conversation")?,
        &[b"root-turn", b"child-turn", b"spawn-result"],
        Invariant::Lineage,
    )
    .await?;
    require_nesting(history, &child, &grandchild).await
}

/// Each spawned agent nests only under its own parent and pages alone there.
async fn require_nesting<P: StreamProvider + ?Sized>(
    history: &History<'_, P>,
    child: &StreamPath,
    grandchild: &StreamPath,
) -> Result<(), Failure> {
    for nowhere in [
        "/children/g",
        "/children/c/children/g/children/g",
        "/children/c/children/c",
    ] {
        absent(
            history,
            &path(history, nowhere)?,
            Invariant::SubtreeMembership,
        )
        .await?;
    }
    for (parent, expected) in [("/children", child), ("/children/c/children", grandchild)] {
        let page = history.result(
            "lineage_page",
            history
                .provider
                .children_page(ChildrenPageRequest {
                    tail_stream: None,
                    parent: Some(path(history, parent)?),
                    after: None,
                    hierarchy_version: None,
                    limit: 4,
                })
                .await,
        )?;
        history.check(
            "lineage_page",
            Invariant::SubtreeMembership,
            page.children.len() == 1
                && page
                    .children
                    .first()
                    .is_some_and(|entry| entry.path == *expected),
        )?;
    }
    Ok(())
}
