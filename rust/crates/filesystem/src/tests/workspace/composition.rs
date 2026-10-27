//! Volume forks and creations contributed to one caller-built Streams commit.
use super::*;
use crate::{
    AuthorityPlacement, CreateSpec, FilesystemCommit, FilesystemCommitOutcome, ForkSpec,
    StreamAuthorityStore, WorkspaceName,
};
use acyclic_stream::{
    AppendRecord, CommitCondition, CommitMutation, CommitRequest, MemoryStream, StreamPath,
    StreamProvider,
};

type PlacedFs = Fs<StreamAuthorityStore<MemoryStream>, crate::MemoryObjectBackend>;
type PlacedGeneration =
    crate::Generation<StreamAuthorityStore<MemoryStream>, crate::MemoryObjectBackend>;
type PlacedFork<'g> = ForkSpec<'g, StreamAuthorityStore<MemoryStream>, crate::MemoryObjectBackend>;

const VOLUMES: &str = "harness/volumes";

fn placed(stream: &Arc<MemoryStream>) -> Result<PlacedFs, Box<dyn Error>> {
    placed_at(stream, VOLUMES)
}

fn placed_at(stream: &Arc<MemoryStream>, root: &str) -> Result<PlacedFs, Box<dyn Error>> {
    let (objects, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
    Ok(Fs::new(
        StreamAuthorityStore::with_placement(Arc::clone(stream), AuthorityPlacement::new(root)?),
        crate::LogicalObjectStore::new(Arc::new(objects), bucket),
        crate::EmbeddedCapabilities::MEMORY,
    ))
}

fn name(text: &str) -> Result<WorkspaceName, Box<dyn Error>> {
    Ok(value!(WorkspaceName::new(
        text,
        &WorkBudget::UNBOUNDED,
        &CancellationToken::new()
    ))?)
}

fn record(bytes: &'static [u8]) -> AppendRecord {
    AppendRecord::from(OwnedBytes::from_static(bytes))
}

/// The spawn shape: the caller's agent state append, a Streams fork of the
/// parent's log into the child's subtree, and two volume forks, under one key.
fn spawn_request(stream_fork_at: u64, state_absent: bool) -> Result<CommitRequest, Box<dyn Error>> {
    let state = StreamPath::new("harness/agents/child/state")?;
    let log = StreamPath::new("harness/agents/child/log")?;
    // A Streams fork proves its destination absent.
    let mut conditions = vec![CommitCondition::Absent { path: log.clone() }];
    conditions.push(if state_absent {
        CommitCondition::Absent {
            path: state.clone(),
        }
    } else {
        CommitCondition::Tail {
            path: state.clone(),
            expected: 7,
        }
    });
    Ok(CommitRequest {
        conditions,
        mutations: vec![
            CommitMutation::Append {
                path: state,
                records: vec![record(b"spawned")],
            },
            CommitMutation::Fork {
                exclude: Vec::new(),
                source: StreamPath::new("harness/agents/root/log")?,
                destination: log,
                at_tail: stream_fork_at,
                records: vec![record(b"adopted")],
            },
        ],
        idempotency_key: acyclic_stream::IdempotencyKey::new(OwnedBytes::from_static(
            b"spawn-child",
        ))?,
    })
}

async fn parent(fs: &PlacedFs, stream: &MemoryStream) -> Result<PlacedGeneration, Box<dyn Error>> {
    let parent = unbounded!(fs, create_workspace("root-project"))?;
    parent.write_text("/plan.md", "ship it").await?;
    let log = StreamPath::new("harness/agents/root/log")?;
    stream
        .commit(CommitRequest {
            conditions: vec![CommitCondition::Absent { path: log.clone() }],
            mutations: vec![CommitMutation::Append {
                path: log,
                records: vec![record(b"turn-1")],
            }],
            idempotency_key: acyclic_stream::IdempotencyKey::new(OwnedBytes::from_static(
                b"root-log",
            ))?,
        })
        .await?;
    Ok(unbounded!(parent, head())?)
}

fn forks(source: &PlacedGeneration) -> Result<Vec<PlacedFork<'_>>, Box<dyn Error>> {
    Ok(vec![
        ForkSpec {
            destination: name("child-project")?,
            source,
            paths: None,
            idempotency_key: IdempotencyKey::from_bytes([0xa1; 16]),
        },
        ForkSpec {
            destination: name("child-private")?,
            source,
            paths: Some(Vec::new()),
            idempotency_key: IdempotencyKey::from_bytes([0xa2; 16]),
        },
    ])
}

async fn compose(
    fs: &PlacedFs,
    source: &PlacedGeneration,
    state_absent: bool,
) -> Result<FilesystemCommit, Box<dyn Error>> {
    let mut commit = FilesystemCommit::new(spawn_request(1, state_absent)?);
    unbounded!(fs, prepare_fork_many(&forks(source)?, &mut commit))?;
    Ok(commit)
}

#[tokio::test]
async fn spawn_shaped_composition_commits_every_fork_with_the_caller_mutations()
-> Result<(), Box<dyn Error>> {
    let stream = Arc::new(MemoryStream::default());
    let fs = placed(&stream)?;
    let source = parent(&fs, &stream).await?;
    let placed_store = StreamAuthorityStore::with_placement(
        Arc::clone(&stream),
        AuthorityPlacement::new(VOLUMES)?,
    );
    let before = placed_store.authorities(64).await?.len();
    let commit = compose(&fs, &source, true).await?;
    assert_eq!(
        commit.authorities().len(),
        4,
        "two forks, each with its retention"
    );
    let FilesystemCommitOutcome::Committed(_) = unbounded!(fs, submit_commit(commit))? else {
        return Err("spawn composition did not commit".into());
    };
    let project = unbounded!(fs, open_workspace("child-project"))?;
    assert_eq!(
        unbounded!(project, read("/plan.md", 16))?.as_ref(),
        b"ship it"
    );
    let private = unbounded!(fs, open_workspace("child-private"))?;
    assert!(unbounded!(private, read("/plan.md", 16)).is_err());
    assert_eq!(
        stream
            .tail(StreamPath::new("harness/agents/child/state")?)
            .await?,
        1
    );
    assert_eq!(
        stream
            .tail(StreamPath::new("harness/agents/child/log")?)
            .await?,
        2
    );
    // Volume authorities stay at the caller's placement, outside the agent
    // subtree the same commit forks into, and nothing lands under `fs/`.
    assert_eq!(placed_store.authorities(64).await?.len(), before + 4);
    assert!(
        StreamAuthorityStore::new(Arc::clone(&stream))
            .authorities(64)
            .await?
            .is_empty()
    );
    Ok(())
}

/// Negative control: a failed caller condition leaves no volume behind. Forks
/// published one by one (the replaced behaviour) would survive this rejection.
#[tokio::test]
async fn a_rejected_caller_condition_leaves_no_forked_volume() -> Result<(), Box<dyn Error>> {
    let stream = Arc::new(MemoryStream::default());
    let fs = placed(&stream)?;
    let source = parent(&fs, &stream).await?;
    let placed_store = StreamAuthorityStore::with_placement(
        Arc::clone(&stream),
        AuthorityPlacement::new(VOLUMES)?,
    );
    let before = placed_store.authorities(64).await?.len();
    let commit = compose(&fs, &source, false).await?;
    let FilesystemCommitOutcome::Rejected(conflicts) = unbounded!(fs, submit_commit(commit))?
    else {
        return Err("a failed caller condition committed".into());
    };
    assert!(!conflicts.is_empty());
    assert!(unbounded!(fs, open_workspace("child-project")).is_err());
    assert!(unbounded!(fs, open_workspace("child-private")).is_err());
    assert!(
        stream
            .tail(StreamPath::new("harness/agents/child/log")?)
            .await
            .is_err()
    );
    assert_eq!(placed_store.authorities(64).await?.len(), before);
    Ok(())
}

/// A head-conditioned spawn of an unchanged source commits.
#[tokio::test]
async fn a_head_conditioned_fork_of_an_unchanged_source_commits() -> Result<(), Box<dyn Error>> {
    let stream = Arc::new(MemoryStream::default());
    let fs = placed(&stream)?;
    let source = parent(&fs, &stream).await?;
    let mut commit = compose(&fs, &source, true).await?;
    unbounded!(fs, require_head(&source, &mut commit))?;
    match unbounded!(fs, submit_commit(commit))? {
        FilesystemCommitOutcome::Committed(_) => {}
        FilesystemCommitOutcome::Rejected(conflicts) => {
            return Err(format!("head-conditioned fork rejected: {conflicts:?}").into());
        }
        FilesystemCommitOutcome::Unknown { error, .. } => return Err(error.into()),
    }
    let project = unbounded!(fs, open_workspace("child-project"))?;
    assert_eq!(
        unbounded!(project, read("/plan.md", 16))?.as_ref(),
        b"ship it"
    );
    Ok(())
}

/// Negative vector: the source publishes past the read generation before the
/// spawn commits, so the head condition rejects the whole commit and nothing
/// is published: no forked volume, no caller mutation, no new authority.
#[tokio::test]
async fn a_source_advancing_before_commit_refuses_the_spawn() -> Result<(), Box<dyn Error>> {
    let stream = Arc::new(MemoryStream::default());
    let fs = placed(&stream)?;
    let source = parent(&fs, &stream).await?;
    let placed_store = StreamAuthorityStore::with_placement(
        Arc::clone(&stream),
        AuthorityPlacement::new(VOLUMES)?,
    );
    let mut commit = compose(&fs, &source, true).await?;
    unbounded!(fs, require_head(&source, &mut commit))?;
    let before = placed_store.authorities(64).await?.len();
    let parent = unbounded!(fs, open_workspace("root-project"))?;
    parent.write_text("/plan.md", "changed").await?;
    let FilesystemCommitOutcome::Rejected(conflicts) = unbounded!(fs, submit_commit(commit))?
    else {
        return Err("a spawn of an advanced source committed".into());
    };
    assert!(!conflicts.is_empty());
    assert!(unbounded!(fs, open_workspace("child-project")).is_err());
    assert!(unbounded!(fs, open_workspace("child-private")).is_err());
    assert!(
        stream
            .tail(StreamPath::new("harness/agents/child/state")?)
            .await
            .is_err()
    );
    assert_eq!(placed_store.authorities(64).await?.len(), before);
    Ok(())
}

/// A lost acknowledgement, then a crash that rebuilds the composition from
/// the same inputs: both retries replay the one original receipt.
#[tokio::test]
async fn lost_acknowledgement_and_rebuilt_composition_replay_exactly() -> Result<(), Box<dyn Error>>
{
    let stream = Arc::new(MemoryStream::default());
    let fs = placed(&stream)?;
    let source = parent(&fs, &stream).await?;
    let mut commit = compose(&fs, &source, true).await?;
    value!(fs.seal_commit(
        &mut commit,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new()
    ))?;
    // The caller's own provider committed it, but the acknowledgement was lost.
    let acyclic_stream::CommitOutcome::Committed(original) =
        stream.commit(commit.request().clone()).await?
    else {
        return Err("first submission did not commit".into());
    };
    let FilesystemCommitOutcome::Committed(replayed) = unbounded!(fs, submit_commit(commit))?
    else {
        return Err("exact retry did not replay".into());
    };
    assert_eq!(replayed.commit_id, original.commit_id);
    let rebuilt = compose(&fs, &source, true).await;
    // The destinations now exist, so a fresh composition is refused before
    // submission or replays the same bytes; either way nothing new commits.
    if let Ok(rebuilt) = rebuilt {
        match unbounded!(fs, submit_commit(rebuilt))? {
            FilesystemCommitOutcome::Committed(again) => {
                assert_eq!(again.commit_id, original.commit_id);
            }
            FilesystemCommitOutcome::Rejected(_) => {}
            FilesystemCommitOutcome::Unknown { error, .. } => return Err(error.into()),
        }
    }
    assert_eq!(
        stream
            .tail(StreamPath::new("harness/agents/child/state")?)
            .await?,
        1
    );
    let project = unbounded!(fs, open_workspace("child-project"))?;
    assert_eq!(
        unbounded!(project, read("/plan.md", 16))?.as_ref(),
        b"ship it"
    );
    Ok(())
}

#[tokio::test]
async fn one_composition_cannot_create_a_volume_twice() -> Result<(), Box<dyn Error>> {
    let stream = Arc::new(MemoryStream::default());
    let fs = placed(&stream)?;
    let source = parent(&fs, &stream).await?;
    let mut commit = FilesystemCommit::new(spawn_request(1, true)?);
    let mut twice = forks(&source)?;
    twice.truncate(1);
    twice.push(ForkSpec {
        destination: name("child-project")?,
        source: &source,
        paths: None,
        idempotency_key: IdempotencyKey::from_bytes([0xa3; 16]),
    });
    let Err(error) = unbounded!(fs, prepare_fork_many(&twice, &mut commit)) else {
        return Err("one commit created the same volume twice".into());
    };
    assert!(
        format!("{error:?}").contains("names one authority twice"),
        "{error:?}"
    );
    // Negative control for partial preparation: the first fork's two
    // creations were already in the commit when the second failed. They are
    // gone, so the caller's commit is exactly its own again and still usable.
    assert_eq!(commit.request().conditions.len(), 2);
    assert_eq!(commit.request().mutations.len(), 2);
    assert!(commit.authorities().is_empty());
    unbounded!(fs, prepare_fork_many(&forks(&source)?, &mut commit))?;
    let FilesystemCommitOutcome::Committed(_) = unbounded!(fs, submit_commit(commit))? else {
        return Err("the rolled-back commit did not commit".into());
    };
    assert!(unbounded!(fs, open_workspace("child-project")).is_ok());
    Ok(())
}

#[tokio::test]
async fn root_creation_creates_every_volume_in_the_caller_commit() -> Result<(), Box<dyn Error>> {
    let stream = Arc::new(MemoryStream::default());
    let fs = placed(&stream)?;
    let initialization = crate::VolumeInitialization {
        config: VolumeConfig::portable(Lifecycle::Ephemeral),
        access_policy: Some(crate::access::INITIAL_POLICY),
    };
    let files = [("/AGENTS.md", OwnedBytes::from_static(b"be brief"))];
    let creations = [
        CreateSpec {
            name: name("root-private")?,
            initialization,
            creation: crate::CreationOperation::populated(
                IdempotencyKey::from_bytes([0xc1; 16]).operation_id(),
                &files,
            ),
        },
        CreateSpec {
            name: name("root-result")?,
            initialization,
            creation: IdempotencyKey::from_bytes([0xc2; 16]).operation_id().into(),
        },
    ];
    let mut commit = FilesystemCommit::new(spawn_request(0, true)?);
    // The root has no parent log to fork; keep only the state append.
    commit
        .request_mut()
        .ok_or("an unsubmitted commit accepts caller entries")?
        .mutations
        .truncate(1);
    let created = unbounded!(fs, prepare_create_many(&creations, &mut commit))?;
    assert_eq!(created.len(), 2);
    let FilesystemCommitOutcome::Committed(_) = unbounded!(fs, submit_commit(commit))? else {
        return Err("root creation did not commit".into());
    };
    let private = unbounded!(fs, open_workspace("root-private"))?;
    assert_eq!(
        unbounded!(private, read("/AGENTS.md", 16))?.as_ref(),
        b"be brief"
    );
    assert!(unbounded!(fs, open_workspace("root-result")).is_ok());
    assert_eq!(
        stream
            .tail(StreamPath::new("harness/agents/child/state")?)
            .await?,
        1
    );
    Ok(())
}

#[test]
fn placement_roots_must_be_canonical_stream_paths() {
    assert!(AuthorityPlacement::new("harness/volumes").is_ok());
    assert!(AuthorityPlacement::new("").is_err());
    assert!(AuthorityPlacement::new("/harness").is_err());
    assert!(AuthorityPlacement::new("harness//volumes").is_err());
}

/// Negative control for placement isolation: the same workspace name, and so
/// the same authority identity, created under two roots on one provider. With
/// unscoped retry keys the second creation hit the first one's key with other
/// bytes; with a shared actor registry it read the first root's cached head.
#[tokio::test]
async fn distinct_placements_on_one_provider_never_share_keys_or_actors()
-> Result<(), Box<dyn Error>> {
    let stream = Arc::new(MemoryStream::default());
    let first = placed_at(&stream, "tenant-a/volumes")?;
    let second = placed_at(&stream, "tenant-b/volumes")?;
    let a = unbounded!(first, create_workspace("shared"))?;
    a.write_text("/owner", "a").await?;
    let b = unbounded!(second, create_workspace("shared"))?;
    b.write_text("/owner", "b").await?;
    a.write_text("/owner", "a2").await?;
    let a = unbounded!(first, open_workspace("shared"))?;
    let b = unbounded!(second, open_workspace("shared"))?;
    assert_eq!(unbounded!(a, read("/owner", 8))?.as_ref(), b"a2");
    assert_eq!(unbounded!(b, read("/owner", 8))?.as_ref(), b"b");
    Ok(())
}

/// Negative control for key size: retry keys once copied the whole root, so
/// a long root that forms valid stream paths overflowed the 256-byte key
/// limit on its first write. Keys now carry a fixed-size placement id.
#[tokio::test]
async fn long_placement_roots_keep_retry_keys_within_the_limit() -> Result<(), Box<dyn Error>> {
    let stream = Arc::new(MemoryStream::default());
    let root = vec!["tenant-volumes"; 16].join("/");
    assert!(root.len() > 200);
    let fs = placed_at(&stream, &root)?;
    let workspace = unbounded!(fs, create_workspace("long-root"))?;
    workspace.write_text("/owner", "long").await?;
    let workspace = unbounded!(fs, open_workspace("long-root"))?;
    assert_eq!(unbounded!(workspace, read("/owner", 8))?.as_ref(), b"long");
    Ok(())
}

/// Negative control for change-stream placement: the same workspace name
/// created in caller-built commits under two roots on one provider. Both
/// creations once named `.acyclic/fs/<volume>` absent, so the second failed
/// its condition; the change stream now carries the placement id.
#[tokio::test]
async fn distinct_placements_keep_separate_change_streams() -> Result<(), Box<dyn Error>> {
    let stream = Arc::new(MemoryStream::default());
    let initialization = crate::VolumeInitialization {
        config: VolumeConfig::portable(Lifecycle::Ephemeral),
        access_policy: Some(crate::access::INITIAL_POLICY),
    };
    for (root, key, operation) in [
        ("tenant-a/volumes", &b"shared-a"[..], 0xd1),
        ("tenant-b/volumes", &b"shared-b"[..], 0xd2),
    ] {
        let fs = placed_at(&stream, root)?;
        let creations = [CreateSpec {
            name: name("shared")?,
            initialization,
            creation: IdempotencyKey::from_bytes([operation; 16])
                .operation_id()
                .into(),
        }];
        let mut commit = FilesystemCommit::new(CommitRequest {
            conditions: Vec::new(),
            mutations: Vec::new(),
            idempotency_key: acyclic_stream::IdempotencyKey::new(OwnedBytes::from_static(key))?,
        });
        unbounded!(fs, prepare_create_many(&creations, &mut commit))?;
        let FilesystemCommitOutcome::Committed(_) = unbounded!(fs, submit_commit(commit))? else {
            return Err(format!("creation under {root} collided with another placement").into());
        };
        assert!(unbounded!(fs, open_workspace("shared")).is_ok());
    }
    Ok(())
}

/// Negative control for settlement: the caller's provider committed the
/// request, then the cached-head refresh could not run (here, cancelled
/// before or while taking the actor lease). That error once replaced the
/// receipt, through the store and through `Fs::settle_commit`; the receipt
/// now survives both and the next open re-reads the durable head.
#[tokio::test]
async fn a_failed_cache_refresh_keeps_the_committed_receipt() -> Result<(), Box<dyn Error>> {
    for public in [false, true] {
        let stream = Arc::new(MemoryStream::default());
        let fs = placed(&stream)?;
        let source = parent(&fs, &stream).await?;
        let mut commit = compose(&fs, &source, true).await?;
        value!(fs.seal_commit(
            &mut commit,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new()
        ))?;
        let acyclic_stream::CommitOutcome::Committed(envelope) =
            stream.commit(commit.request().clone()).await?
        else {
            return Err("the caller's submission did not commit".into());
        };
        let committed = envelope.commit_id;
        let outcome = acyclic_stream::CommitOutcome::Committed(envelope);
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        let settled = if public {
            value!(
                fs.settle_commit(commit, outcome, WorkBudget::UNBOUNDED, &cancelled)
                    .await
            )?
        } else {
            StreamAuthorityStore::with_placement(
                Arc::clone(&stream),
                AuthorityPlacement::new(VOLUMES)?,
            )
            .settle(
                commit,
                outcome,
                &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancelled),
            )
            .await?
        };
        let FilesystemCommitOutcome::Committed(settled) = settled else {
            return Err("a committed outcome settled as something else".into());
        };
        assert_eq!(settled.commit_id, committed);
        let project = unbounded!(fs, open_workspace("child-project"))?;
        assert_eq!(
            unbounded!(project, read("/plan.md", 16))?.as_ref(),
            b"ship it"
        );
    }
    Ok(())
}

/// Negative control for dropped preparation: a preparing future dropped
/// mid-batch (a caller timeout) never resumes, so a rollback written after
/// its awaits was skipped and the caller kept a partial, submittable batch.
/// Batch preparation now holds a guard that rolls back on drop unless the
/// whole batch succeeded.
#[tokio::test]
async fn a_dropped_preparation_rolls_the_commit_back() -> Result<(), Box<dyn Error>> {
    let stream = Arc::new(MemoryStream::default());
    let fs = placed(&stream)?;
    let source = parent(&fs, &stream).await?;
    let mut commit = FilesystemCommit::new(spawn_request(1, true)?);
    let first = forks(&source)?;
    {
        let mut guard = commit.guard();
        unbounded!(fs, prepare_fork_many(&first[..1], guard.commit()))?;
        assert_eq!(guard.commit().authorities().len(), 2);
        // Dropped armed, as the outer batch future is when it times out
        // after its first part.
    }
    assert_eq!(commit.request().conditions.len(), 2);
    assert_eq!(commit.request().mutations.len(), 2);
    assert!(commit.authorities().is_empty());
    {
        let mut guard = commit.guard();
        unbounded!(fs, prepare_fork_many(&first, guard.commit()))?;
        guard.disarm();
    }
    assert_eq!(commit.authorities().len(), 4);
    let FilesystemCommitOutcome::Committed(_) = unbounded!(fs, submit_commit(commit))? else {
        return Err("the disarmed batch did not commit".into());
    };
    Ok(())
}
