//! Volume forks and creations contributed to one caller-built Streams commit.
use super::*;
use crate::{
    AuthorityPlacement, CreateSpec, FilesystemCommit, FilesystemCommitOutcome, ForkSpec,
    StreamAuthorityStore, WorkspaceName,
};
use acyclic_stream::StreamProviderExt as _;
use acyclic_stream::{
    AppendRecord, CommitCondition, CommitEffect, CommitMutation, CommitRequest, MemoryStream,
    StreamPath,
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
    let objects = acyclic_objects::MemoryObjects::memory();
    Ok(Fs::new(
        StreamAuthorityStore::with_placement(Arc::clone(stream), AuthorityPlacement::new(root)?),
        crate::LogicalObjectStore::new(Arc::new(objects)),
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
        deadline_unix_millis: None,
        effects: vec![CommitEffect::hard(
            conditions,
            vec![
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
        )],
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
            deadline_unix_millis: None,
            effects: vec![CommitEffect::hard(
                vec![CommitCondition::Absent { path: log.clone() }],
                vec![CommitMutation::Append {
                    path: log,
                    records: vec![record(b"turn-1")],
                }],
            )],
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
    #[cfg(not(target_arch = "wasm32"))]
    crate::initialize_test_native_io();
    let stream = Arc::new(MemoryStream::default());
    let fs = placed(&stream)?;
    let source = parent(&fs, &stream).await?;
    let placed_store = StreamAuthorityStore::with_placement(
        Arc::clone(&stream),
        AuthorityPlacement::new(VOLUMES)?,
    );
    let before = placed_store.authorities(64).await?.len();
    #[cfg(not(target_arch = "wasm32"))]
    let cancel = CancellationToken::new();
    #[cfg(not(target_arch = "wasm32"))]
    let work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancel)
        .map_err(|(error, _)| error)?;
    #[cfg(not(target_arch = "wasm32"))]
    let commit = {
        let mut commit = FilesystemCommit::new(spawn_request(1, true)?);
        let (forked, allocation) = fs
            .prepare_fork_many(&forks(&source)?, &mut commit, &work, &cancel)
            .await?;
        assert_eq!(forked.len(), 2);
        assert_eq!(
            allocation.bytes(),
            (forked.capacity() * std::mem::size_of::<crate::ForkedGeneration>()) as u64
        );
        assert!(allocation.work().shares_ledger(&work));
        drop(forked);
        drop(allocation);
        let before = work.receipt();
        fs.require_head(&source, &mut commit, &work, &cancel)
            .await?;
        assert!(work.receipt().items_examined > before.items_examined);
        let before = work.receipt();
        fs.seal_commit(&mut commit, &work, &cancel)?;
        assert!(
            work.receipt().items_examined > before.items_examined,
            "seal lost its original request traversal"
        );
        commit
    };
    #[cfg(target_arch = "wasm32")]
    let commit = compose(&fs, &source, true).await?;
    assert_eq!(
        commit.authorities().len(),
        4,
        "two forks, each with its retention"
    );
    #[cfg(not(target_arch = "wasm32"))]
    let outcome = fs.submit_commit(commit, &work, &cancel).await?;
    #[cfg(target_arch = "wasm32")]
    let outcome = unbounded!(fs, submit_commit(commit))?;
    let FilesystemCommitOutcome::Committed(_) = outcome else {
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

/// The history marker and restored generation have one decision. A user write
/// between preparation and submission must also reject the complete restore.
#[tokio::test]
async fn restoring_generation_and_context_cut_is_atomic() -> Result<(), Box<dyn Error>> {
    #[cfg(not(target_arch = "wasm32"))]
    crate::initialize_test_native_io();
    let stream = Arc::new(MemoryStream::default());
    let fs = placed(&stream)?;
    let original = parent(&fs, &stream).await?;
    let mut retention = FilesystemCommit::new(CommitRequest {
        deadline_unix_millis: None,
        effects: vec![CommitEffect::hard(Vec::new(), Vec::new())],
        idempotency_key: acyclic_stream::IdempotencyKey::new(vec![0])?,
    });
    let saved = unbounded!(
        fs,
        prepare_fork_many(
            &[ForkSpec {
                destination: name("saved-project")?,
                source: &original,
                paths: None,
                idempotency_key: IdempotencyKey::from_bytes([0x42; 16]),
            }],
            &mut retention
        )
    )?;
    let saved = saved
        .first()
        .ok_or("retained generation missing")?
        .generation;
    assert!(matches!(
        unbounded!(fs, submit_commit(retention))?,
        FilesystemCommitOutcome::Committed(_)
    ));
    let retained = unbounded!(fs, open_workspace("saved-project"))?;
    let retained = unbounded!(retained, generation(saved))?;
    let workspace = unbounded!(fs, open_workspace("root-project"))?;
    workspace.write_text("/plan.md", "later").await?;
    let expected = unbounded!(workspace, head())?;
    let marker = StreamPath::new("harness/history/selected")?;
    for (attempt, caller_tail, concurrent) in [(1, 7, false), (2, 0, true), (3, 0, false)] {
        let expected = if attempt == 3 {
            unbounded!(workspace, head())?
        } else {
            expected.clone()
        };
        let mut commit = FilesystemCommit::new(CommitRequest {
            deadline_unix_millis: None,
            effects: vec![CommitEffect::hard(
                vec![CommitCondition::Tail {
                    path: marker.clone(),
                    expected: caller_tail,
                }],
                vec![CommitMutation::Append {
                    path: marker.clone(),
                    records: vec![record(b"cut-1")],
                }],
            )],
            idempotency_key: acyclic_stream::IdempotencyKey::new(vec![attempt])?,
        });
        let restores = [crate::RestoreSpec {
            expected: &expected,
            source: &retained,
            operation: OperationId::from_bytes([attempt; 16]),
            permit: crate::PublicationPermit::Unrestricted,
        }];
        #[cfg(not(target_arch = "wasm32"))]
        {
            let cancel = CancellationToken::new();
            let work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancel)
                .map_err(|(error, _)| error)?;
            let (generations, allocation) = fs
                .prepare_restore_many(&restores, &mut commit, &work, &cancel)
                .await?;
            assert_eq!(generations.len(), 1);
            assert_eq!(generations[0].workspace_id(), expected.workspace_id());
            assert_eq!(
                allocation.bytes(),
                (generations.capacity() * std::mem::size_of::<PlacedGeneration>()) as u64
            );
            assert!(allocation.work().shares_ledger(&work));
            drop(generations);
            drop(allocation);
        }
        #[cfg(target_arch = "wasm32")]
        unbounded!(fs, prepare_restore_many(&restores, &mut commit))?;
        assert_eq!(unbounded!(workspace, head())?.id(), expected.id());
        if concurrent {
            workspace.write_text("/plan.md", "user edit").await?;
        }
        let outcome = unbounded!(fs, submit_commit(commit))?;
        if attempt == 3 {
            assert!(matches!(outcome, FilesystemCommitOutcome::Committed(_)));
            assert_eq!(
                unbounded!(workspace, read("/plan.md", 32))?.as_ref(),
                b"ship it"
            );
            assert_eq!(stream.tail(marker.clone()).await?, 1);
        } else {
            assert!(matches!(outcome, FilesystemCommitOutcome::Rejected(_)));
            assert!(stream.tail(marker.clone()).await.is_err());
            assert_eq!(
                unbounded!(workspace, read("/plan.md", 32))?.as_ref(),
                if concurrent {
                    b"user edit".as_slice()
                } else {
                    b"later".as_slice()
                }
            );
        }
    }
    Ok(())
}

/// A file control and its caller's actor/event decision share one publication.
/// Either side's stale cut refuses both, including a concurrent disjoint write.
#[tokio::test]
async fn transaction_candidates_and_caller_controls_publish_atomically()
-> Result<(), Box<dyn Error>> {
    #[cfg(not(target_arch = "wasm32"))]
    crate::initialize_test_native_io();
    for (attempt, caller_tail, concurrent) in [(1, 7, false), (2, 0, true), (3, 0, false)] {
        let stream = Arc::new(MemoryStream::default());
        let fs = placed(&stream)?;
        let workspace = unbounded!(fs, create_workspace("control"))?;
        let original = unbounded!(workspace, head())?;
        let mut transaction = unbounded!(
            workspace,
            begin_transaction(IdempotencyKey::from_bytes([attempt; 16]))
        )?;
        transaction.write_text("/pause", "").await?;
        let marker = StreamPath::new("harness/actor/control")?;
        let mut commit = FilesystemCommit::new(CommitRequest {
            deadline_unix_millis: None,
            effects: vec![CommitEffect::hard(
                vec![CommitCondition::Tail {
                    path: marker.clone(),
                    expected: caller_tail,
                }],
                vec![CommitMutation::Append {
                    path: marker.clone(),
                    records: vec![record(b"paused")],
                }],
            )],
            idempotency_key: acyclic_stream::IdempotencyKey::new(vec![attempt])?,
        });
        let specs = [crate::TransactionSpec {
            transaction: &transaction,
            permit: crate::PublicationPermit::Unrestricted,
        }];
        #[cfg(not(target_arch = "wasm32"))]
        {
            let cancel = CancellationToken::new();
            let work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancel)
                .map_err(|(error, _)| error)?;
            let (generations, allocation) = fs
                .prepare_transaction_many(&specs, &mut commit, &work, &cancel)
                .await?;
            assert_eq!(generations.len(), 1);
            assert_eq!(generations[0].workspace_id(), original.workspace_id());
            assert_ne!(generations[0].id(), original.id());
            assert!(allocation.work().shares_ledger(&work));
            drop(generations);
            drop(allocation);
        }
        #[cfg(target_arch = "wasm32")]
        unbounded!(fs, prepare_transaction_many(&specs, &mut commit))?;
        assert_eq!(unbounded!(workspace, head())?.id(), original.id());
        assert!(stream.tail(marker.clone()).await.is_err());
        if concurrent {
            workspace.write_text("/unrelated", "owner edit").await?;
        }
        let replay = commit.request().clone();
        let outcome = unbounded!(fs, submit_commit(commit))?;
        if attempt == 3 {
            assert!(matches!(outcome, FilesystemCommitOutcome::Committed(_)));
            assert_eq!(unbounded!(workspace, read("/pause", 1))?.as_ref(), b"");
            assert_eq!(stream.tail(marker.clone()).await?, 1);
            let head = unbounded!(workspace, head())?.id();
            stream.commit(replay).await?;
            assert_eq!(unbounded!(workspace, head())?.id(), head);
            assert_eq!(stream.tail(marker.clone()).await?, 1);
        } else {
            assert!(matches!(outcome, FilesystemCommitOutcome::Rejected(_)));
            assert!(unbounded!(workspace, stat("/pause")).is_err());
            assert!(stream.tail(marker.clone()).await.is_err());
            if concurrent {
                assert_eq!(
                    unbounded!(workspace, read("/unrelated", 32))?.as_ref(),
                    b"owner edit"
                );
            } else {
                assert_eq!(unbounded!(workspace, head())?.id(), original.id());
            }
        }
    }
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

/// Retirement withdraws a required source without publishing a new generation.
#[tokio::test]
async fn a_retired_required_source_refuses_the_consuming_commit() -> Result<(), Box<dyn Error>> {
    for retired in [false, true] {
        let stream = Arc::new(MemoryStream::default());
        let fs = placed(&stream)?;
        let source = parent(&fs, &stream).await?;
        let destination = StreamPath::new("observations/accepted")?;
        let mut commit = FilesystemCommit::new(CommitRequest {
            deadline_unix_millis: None,
            effects: vec![CommitEffect::hard(
                vec![],
                vec![CommitMutation::Append {
                    path: destination.clone(),
                    records: vec![record(b"accepted")],
                }],
            )],
            idempotency_key: acyclic_stream::IdempotencyKey::new(b"consume-source".to_vec())?,
        });
        unbounded!(fs, require_head(&source, &mut commit))?;
        let tails: Vec<_> = commit
            .request()
            .conditions()
            .filter_map(|condition| match condition {
                CommitCondition::Tail { path, expected } => Some((path.clone(), *expected)),
                _ => None,
            })
            .collect();
        assert!(!tails.is_empty());
        if retired {
            assert_eq!(
                fs.delete_workspace("root-project", crate::IdempotencyKey::new())
                    .await?,
                crate::WorkspaceDelete::Deleted
            );
            for (path, expected) in tails {
                assert_eq!(
                    stream.tail(path).await?,
                    expected,
                    "retirement did not publish another generation"
                );
            }
        }
        let outcome = unbounded!(fs, submit_commit(commit))?;
        if retired {
            assert!(matches!(outcome, FilesystemCommitOutcome::Rejected(_)));
            assert_eq!(
                stream.tail(destination).await,
                Err(acyclic_stream::StreamError::NotFound)
            );
        } else {
            assert!(matches!(outcome, FilesystemCommitOutcome::Committed(_)));
            assert_eq!(stream.tail(destination).await?, 1);
        }
    }
    Ok(())
}
/// Two children forking one source require its head once, and commit; one
/// commit requiring two different heads of a source is refused.
#[tokio::test]
async fn repeated_source_requirements_do_not_duplicate_conditions() -> Result<(), Box<dyn Error>> {
    let stream = Arc::new(MemoryStream::default());
    let fs = placed(&stream)?;
    let source = parent(&fs, &stream).await?;
    let mut commit = compose(&fs, &source, true).await?;
    unbounded!(fs, require_head(&source, &mut commit))?;
    let conditions = commit.request().effects[0].conditions.len();
    unbounded!(fs, require_head(&source, &mut commit))?;
    assert_eq!(commit.request().effects[0].conditions.len(), conditions);
    let parent = unbounded!(fs, open_workspace("root-project"))?;
    parent.write_text("/plan.md", "changed").await?;
    let advanced = unbounded!(parent, head())?;
    assert!(unbounded!(fs, require_head(&advanced, &mut commit)).is_err());
    assert_eq!(commit.request().effects[0].conditions.len(), conditions);
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
    assert_eq!(commit.request().effects[0].conditions.len(), 2);
    assert_eq!(commit.request().effects[0].mutations.len(), 2);
    assert!(commit.authorities().is_empty());
    unbounded!(fs, prepare_fork_many(&forks(&source)?, &mut commit))?;
    let FilesystemCommitOutcome::Committed(_) = unbounded!(fs, submit_commit(commit))? else {
        return Err("the rolled-back commit did not commit".into());
    };
    assert!(unbounded!(fs, open_workspace("child-project")).is_ok());
    Ok(())
}

#[tokio::test]
// `composition_unfunded.patch` restores unadmitted output backing in all three
// production batch methods. Applying it must fail this unchanged control at
// "batch output admission was not first" or the original backing assertion.
async fn root_creation_creates_every_volume_in_the_caller_commit() -> Result<(), Box<dyn Error>> {
    #[cfg(not(target_arch = "wasm32"))]
    crate::initialize_test_native_io();
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
        .effects[0]
        .mutations
        .truncate(1);
    #[cfg(not(target_arch = "wasm32"))]
    let (created, allocation) = {
        let cancel = CancellationToken::new();
        let denied = crate::NativeMaterializationWork::new(
            WorkBudget {
                peak_allocation_bytes: crate::NativeMaterializationWork::accounting_state_bytes(),
                ..WorkBudget::UNBOUNDED
            },
            &cancel,
        )
        .map_err(|(error, _)| error)?;
        let original = commit.request().clone();
        let before = denied.receipt();
        let failure = fs
            .prepare_create_many(&creations, &mut commit, &denied, &cancel)
            .await
            .err()
            .ok_or("unfunded composed output was allocated")?;
        assert!(
            matches!(
                failure,
                WorkspaceError::Work(crate::WorkError::BudgetExceeded {
                    counter: "peak_allocation_bytes",
                    observed,
                    maximum,
                }) if observed == maximum + (creations.len() * std::mem::size_of::<crate::WorkspaceId>()) as u64
            ),
            "batch output admission was not first: {failure:?}"
        );
        assert_eq!(commit.request(), &original);
        assert!(commit.authorities().is_empty());
        assert_eq!(denied.receipt(), before);
        let work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancel)
            .map_err(|(error, _)| error)?;
        let result = fs
            .prepare_create_many(&creations, &mut commit, &work, &cancel)
            .await?;
        assert_eq!(
            result.1.bytes(),
            (result.0.capacity() * std::mem::size_of::<crate::WorkspaceId>()) as u64,
            "composed output lost its original backing allocation"
        );
        assert!(result.1.work().shares_ledger(&work));
        assert!(work.receipt().items_examined > 0);
        result
    };
    #[cfg(target_arch = "wasm32")]
    let created = unbounded!(fs, prepare_create_many(&creations, &mut commit))?;
    assert_eq!(created.len(), 2);
    #[cfg(not(target_arch = "wasm32"))]
    {
        let retained = allocation.work().live_allocation_bytes();
        let bytes = allocation.bytes();
        let work = allocation.work().clone();
        drop(created);
        assert_eq!(work.live_allocation_bytes(), retained);
        drop(allocation);
        assert_eq!(work.live_allocation_bytes(), retained - bytes);
    }
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
            deadline_unix_millis: None,
            effects: vec![CommitEffect::hard(Vec::new(), Vec::new())],
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
    #[cfg(not(target_arch = "wasm32"))]
    crate::initialize_test_native_io();
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
        #[cfg(not(target_arch = "wasm32"))]
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancelled)
            .map_err(|(error, _)| error)?;
        cancelled.cancel();
        let settled = if public {
            #[cfg(not(target_arch = "wasm32"))]
            let outcome = fs
                .settle_commit(commit, outcome, &original, &cancelled)
                .await?;
            #[cfg(target_arch = "wasm32")]
            let outcome = value!(
                fs.settle_commit(commit, outcome, WorkBudget::UNBOUNDED, &cancelled)
                    .await
            )?;
            outcome
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
    assert_eq!(commit.request().effects[0].conditions.len(), 2);
    assert_eq!(commit.request().effects[0].mutations.len(), 2);
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

/// Ordinary transactions retain every external source across the publication
/// combiner; an edit or retirement of either source refuses the whole write.
#[derive(Clone, Copy)]
enum PolicyPublication {
    Standalone,
    #[cfg(not(target_arch = "wasm32"))]
    Native,
}

async fn transaction_policy_sources(
    pinned: bool,
    publication: PolicyPublication,
) -> Result<(), Box<dyn Error>> {
    for changed in [
        None,
        Some((0_usize, false)),
        Some((1, false)),
        Some((0, true)),
        Some((1, true)),
    ] {
        let stream = Arc::new(MemoryStream::default());
        let fs = placed(&stream)?;
        let token = CancellationToken::new();
        let names = ["first-source", "second-source"];
        let sources = [
            unbounded!(fs, create_workspace(names[0]))?,
            unbounded!(fs, create_workspace(names[1]))?,
        ];
        let target = unbounded!(fs, create_workspace("target"))?;
        let before = unbounded!(target, head())?.id();
        let original = if pinned {
            Some([
                unbounded!(sources[0], head())?,
                unbounded!(sources[1], head())?,
            ])
        } else {
            None
        };
        let captured = if pinned {
            None
        } else {
            Some([
                unbounded!(sources[0], capture_policy_cut())?,
                unbounded!(sources[1], capture_policy_cut())?,
            ])
        };
        #[cfg(not(target_arch = "wasm32"))]
        let native_original = match publication {
            PolicyPublication::Standalone => None,
            PolicyPublication::Native => Some(
                crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &token)
                    .map_err(|(error, _)| error)?,
            ),
        };
        let mut transaction = match publication {
            PolicyPublication::Standalone => {
                let mut transaction = unbounded!(target, begin_transaction(IdempotencyKey::new()))?;
                unbounded!(
                    transaction,
                    write("/answer", OwnedBytes::from_static(b"accepted"))
                )?;
                transaction
            }
            #[cfg(not(target_arch = "wasm32"))]
            PolicyPublication::Native => {
                let original = native_original.as_ref().unwrap();
                let mut transaction = target
                    .begin_transaction(IdempotencyKey::new(), original, &token)
                    .await?;
                transaction
                    .write(
                        "/answer",
                        OwnedBytes::from_static(b"accepted"),
                        original,
                        &token,
                    )
                    .await?;
                transaction
            }
        };
        if let Some((position, retired)) = changed {
            if retired {
                assert_eq!(
                    fs.delete_workspace(names[position], IdempotencyKey::new())
                        .await?,
                    crate::WorkspaceDelete::Deleted
                );
            } else {
                sources[position].write_text("/settings", "changed").await?;
            }
        }
        // Capture from the original pins AFTER an edit or retirement. Opening
        // current source heads here would silently replace the admitted input.
        let cuts = match original {
            Some(original) => [
                unbounded!(original[0], capture_policy_cut())?,
                unbounded!(original[1], capture_policy_cut())?,
            ],
            None => captured.unwrap(),
        };
        let committed = match publication {
            PolicyPublication::Standalone => {
                let policy = crate::PolicyGroup::new(&[&cuts], WorkBudget::UNBOUNDED, &token)
                    .map_err(|failure| failure.error)?
                    .value;
                policy
                    .scope(transaction.commit::<WorkspaceError, _, _>(
                        None,
                        crate::PublicationPermit::Unrestricted,
                        WorkBudget::UNBOUNDED,
                        &token,
                        allow::<WorkspaceError>,
                    ))
                    .await
                    .map(|receipt| receipt.value)
                    .map_err(|failure| failure.error)
            }
            #[cfg(not(target_arch = "wasm32"))]
            PolicyPublication::Native => {
                let original = native_original.as_ref().unwrap();
                let policy = crate::PolicyGroup::new(&[&cuts], original, &token)?;
                let mut ready = |_: &crate::NativeMaterializationWork| {
                    std::future::ready(Ok::<(), FsError>(()))
                };
                let committed = policy
                    .clone()
                    .scope(transaction.commit_with_native_work::<FsError, _, _>(
                        None,
                        crate::PublicationPermit::Unrestricted,
                        original,
                        &token,
                        &mut ready,
                    ))
                    .await
                    .map_err(WorkspaceError::from);
                if changed.is_none() {
                    assert!(matches!(
                        committed,
                        Ok(crate::TransactionCommit::Committed(_))
                    ));
                    // Revocation cannot erase the receipt of the operation
                    // that already committed under this original native owner.
                    for source in &sources {
                        source
                            .write_text("/settings", "revoked after commit")
                            .await?;
                    }
                    let retry = policy
                        .scope(transaction.commit_with_native_work::<FsError, _, _>(
                            None,
                            crate::PublicationPermit::Unrestricted,
                            original,
                            &token,
                            &mut ready,
                        ))
                        .await?;
                    assert!(matches!(
                        retry,
                        crate::TransactionCommit::AlreadyCommitted(_)
                    ));
                }
                committed
            }
        };
        if changed.is_some() {
            assert!(
                committed.is_err(),
                "an unchanged target must refuse stale sources without requesting rebase"
            );
        }
        let accepted = matches!(
            committed,
            Ok(crate::TransactionCommit::Committed(_)
                | crate::TransactionCommit::AlreadyCommitted(_))
        );
        assert_eq!(accepted, changed.is_none());
        let after = unbounded!(target, head())?.id();
        if changed.is_some() {
            assert_eq!(after, before, "a stale source published the candidate");
        } else {
            assert_ne!(after, before);
            assert_eq!(
                unbounded!(target, read("/answer", 8))?.as_ref(),
                b"accepted"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn transaction_publication_retains_all_external_policy_sources() -> Result<(), Box<dyn Error>>
{
    transaction_policy_sources(false, PolicyPublication::Standalone).await?;
    transaction_policy_sources(true, PolicyPublication::Standalone).await?;
    #[cfg(not(target_arch = "wasm32"))]
    {
        transaction_policy_sources(false, PolicyPublication::Native).await?;
        transaction_policy_sources(true, PolicyPublication::Native).await?;
    }
    Ok(())
}
