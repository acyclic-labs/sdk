//! Coupled-cut simulation using the existing real-memory-provider cut harness.
use super::*;
use crate::{FilesystemCommit, FilesystemCommitOutcome, PublishSpec};
use acyclic_stream::{
    AppendRecord, CommitCondition, CommitEffect, CommitMutation, CommitRequest, StreamPath,
};

type CutGeneration<O = crate::MemoryObjectBackend> =
    crate::Generation<crate::StreamAuthorityStore<CutStream>, O>;
type CutWorkspace<O = crate::MemoryObjectBackend> =
    crate::Workspace<crate::StreamAuthorityStore<CutStream>, O>;

#[cfg(feature = "conformance")]
#[tokio::test]
async fn coupled_publication_shared_conformance() -> Result<(), Box<dyn Error>> {
    let stream = Arc::new(CutStream::new(Arc::new(
        acyclic_stream::MemoryStream::default(),
    )));
    let fs: CutFs = Fs::new(
        crate::StreamAuthorityStore::new(stream),
        crate::LogicalObjectStore::new(Arc::new(acyclic_objects::MemoryObjects::memory())),
        crate::EmbeddedCapabilities::MEMORY,
    );
    crate::conformance::verify_coupled_publication(&fs).await?;
    Ok(())
}

fn caller() -> Result<CommitRequest, Box<dyn Error>> {
    let path = StreamPath::new("caller/events")?;
    Ok(CommitRequest {
        deadline_unix_millis: None,
        effects: vec![CommitEffect::hard(
            vec![CommitCondition::Absent { path: path.clone() }],
            vec![CommitMutation::Append {
                path,
                records: vec![AppendRecord::from(acyclic_stream::OwnedBytes::from_static(
                    b"coupled-cut",
                ))],
            }],
        )],
        idempotency_key: acyclic_stream::IdempotencyKey::new(b"coupled-cut".to_vec())?,
    })
}

async fn staged<O: crate::AsyncObjectStore + 'static>(
    fs: &Fs<crate::StreamAuthorityStore<CutStream>, O>,
) -> Result<
    (
        Vec<CutWorkspace<O>>,
        Vec<CutGeneration<O>>,
        Vec<GenerationId>,
    ),
    Box<dyn Error>,
> {
    let mut workspaces = Vec::new();
    let mut expected = Vec::new();
    let mut staged = Vec::new();
    for name in ["first", "second"] {
        let workspace = unbounded!(fs, create_workspace(name))?;
        workspace.write_text("/file", "before").await?;
        expected.push(unbounded!(workspace, head())?);
        let mut transaction = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
        transaction.write_text("/file", "after").await?;
        staged.push(unbounded!(transaction.checkout, checkpoint())?);
        workspaces.push(workspace);
    }
    Ok((workspaces, expected, staged))
}

fn specs<'a, O: crate::AsyncObjectStore + 'static>(
    expected: &'a [CutGeneration<O>],
    staged: &[GenerationId],
) -> Vec<PublishSpec<'a, crate::StreamAuthorityStore<CutStream>, O>> {
    expected
        .iter()
        .zip(staged)
        .enumerate()
        .map(|(index, (expected, generation))| PublishSpec {
            expected,
            generation: *generation,
            operation: OperationId::from_bytes([u8::try_from(index + 71).unwrap(); 16]),
            permit: crate::PublicationPermit::Unrestricted,
        })
        .collect()
}

fn merge_states<O: crate::AsyncObjectStore + 'static>(
    expected: &[CutGeneration<O>],
) -> Vec<crate::GitCompatState> {
    expected
        .iter()
        .enumerate()
        .map(|(index, before)| {
            let source = &expected[(index + 1) % expected.len()];
            let mut state = crate::GitCompatState::new("main", before.workspace_id());
            state.revision = 1;
            state.pending = Some(crate::GitPendingTransition {
                id: crate::GitTransitionId::from_bytes([u8::try_from(index + 81).unwrap(); 16]),
                request: None,
                action: crate::GitFilesystemAction::Join {
                    target_tree: crate::GitTreeRef::exact(before.workspace_id(), before.id()),
                    source_workspace: source.workspace_id(),
                    source_generation: Some(source.id()),
                    rebase: false,
                    tracked_paths: ["file".into()].into(),
                },
                mutation: crate::GitPendingMutation::Join {
                    source_head: None,
                    source_branch: "child".into(),
                    rebase: false,
                    conflicts: Some([("file".into(), None)].into()),
                },
            });
            state
        })
        .collect()
}

fn merge_changes<'a, O: crate::AsyncObjectStore + 'static>(
    expected: &[CutGeneration<O>],
    states: &'a [crate::GitCompatState],
) -> Vec<crate::GitCompatCas<'a>> {
    expected
        .iter()
        .zip(states)
        .map(|(before, state)| crate::GitCompatCas {
            workspace: before.workspace_id(),
            expected: 0,
            replacement: Some(state),
        })
        .collect()
}

async fn assert_cut<O: crate::AsyncObjectStore + 'static>(
    stream: &Arc<acyclic_stream::MemoryStream>,
    workspaces: &[CutWorkspace<O>],
    expected: &[CutGeneration<O>],
    staged: &[GenerationId],
) -> Result<bool, Box<dyn Error>> {
    let committed = match stream.tail(StreamPath::new("caller/events")?).await {
        Ok(tail) => {
            assert_eq!(tail, 1);
            true
        }
        Err(acyclic_stream::StreamError::NotFound) => false,
        Err(error) => return Err(error.into()),
    };
    for ((workspace, before), after) in workspaces.iter().zip(expected).zip(staged) {
        assert_eq!(
            unbounded!(workspace, head())?.id(),
            if committed { *after } else { before.id() }
        );
        assert_eq!(
            unbounded!(workspace, read("/file", 16))?.as_ref(),
            if committed {
                b"after".as_slice()
            } else {
                b"before".as_slice()
            }
        );
    }
    let store =
        crate::StreamGitCompatStore::new(crate::StreamAuthorityStore::new(Arc::clone(stream)));
    for (before, state) in expected.iter().zip(merge_states(expected)) {
        assert_eq!(
            crate::GitCompatStore::load(&store, before.workspace_id()).await?,
            committed.then_some(state)
        );
    }
    Ok(committed)
}

/// Inject a transport error or drop the live preparation at each external
/// Stream await. A dropped partial preparation restores the caller's request.
#[tokio::test]
async fn coupled_publication_preparation_is_atomic_at_every_stream_cut()
-> Result<(), Box<dyn Error>> {
    for crash in [false, true] {
        for fail_at in 1.. {
            let stream = Arc::new(acyclic_stream::MemoryStream::default());
            let cutting = Arc::new(CutStream::new(Arc::clone(&stream)));
            let fs: CutFs = Fs::new(
                crate::StreamAuthorityStore::new(Arc::clone(&cutting)),
                crate::LogicalObjectStore::new(Arc::new(acyclic_objects::MemoryObjects::memory())),
                crate::EmbeddedCapabilities::MEMORY,
            );
            let (workspaces, expected, staged) = staged(&fs).await?;
            let states = merge_states(&expected);
            let state_changes = merge_changes(&expected, &states);
            let specs = specs(&expected, &staged);
            let original = caller()?;
            let mut commit = FilesystemCommit::new(original.clone());
            cutting.arm(fail_at, ForkCut::Before);
            cutting
                .crash_on_cut
                .store(crash, std::sync::atomic::Ordering::SeqCst);
            let cancel = CancellationToken::new();
            let result = {
                let future = fs.prepare_publish_many(
                    &specs,
                    &state_changes,
                    &mut commit,
                    WorkBudget::UNBOUNDED,
                    &cancel,
                );
                tokio::pin!(future);
                tokio::select! {
                    result = &mut future => Some(result),
                    () = cutting.crashed.notified(), if crash => None,
                }
            };
            cutting
                .crash_on_cut
                .store(false, std::sync::atomic::Ordering::SeqCst);
            let fired = cutting.disarm();
            assert!(!assert_cut(&stream, &workspaces, &expected, &staged).await?);
            if !fired {
                result.ok_or("unexpected crash")??;
                let FilesystemCommitOutcome::Committed(_) = unbounded!(fs, submit_commit(commit))?
                else {
                    return Err("coupled commit failed".into());
                };
                assert!(assert_cut(&stream, &workspaces, &expected, &staged).await?);
                break;
            }
            assert!(result.is_none_or(|result| result.is_err()));
            assert_eq!(commit.request(), &original);
            assert!(commit.authorities().is_empty());
        }
    }
    Ok(())
}

/// A lost acknowledgement is the same coupled cut; retry the retained sealed
/// request once, without rebuilding heads against the now-advanced generation.
#[tokio::test]
async fn coupled_publication_submission_replays_at_every_stream_cut() -> Result<(), Box<dyn Error>>
{
    for cut in [ForkCut::Before, ForkCut::AfterCommit] {
        for fail_at in 1.. {
            let stream = Arc::new(acyclic_stream::MemoryStream::default());
            let cutting = Arc::new(CutStream::new(Arc::clone(&stream)));
            let fs: CutFs = Fs::new(
                crate::StreamAuthorityStore::new(Arc::clone(&cutting)),
                crate::LogicalObjectStore::new(Arc::new(acyclic_objects::MemoryObjects::memory())),
                crate::EmbeddedCapabilities::MEMORY,
            );
            let (workspaces, expected, staged) = staged(&fs).await?;
            let states = merge_states(&expected);
            let state_changes = merge_changes(&expected, &states);
            let mut commit = FilesystemCommit::new(caller()?);
            unbounded!(
                fs,
                prepare_publish_many(&specs(&expected, &staged), &state_changes, &mut commit)
            )?;
            value!(fs.seal_commit(
                &mut commit,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            ))?;
            let request = commit.request().clone();
            cutting.arm(fail_at, cut);
            let result = unbounded!(fs, submit_commit(commit));
            let fired = cutting.disarm();
            assert_cut(&stream, &workspaces, &expected, &staged).await?;
            match result? {
                FilesystemCommitOutcome::Unknown { commit, .. } => {
                    assert_eq!(commit.request(), &request);
                    assert!(matches!(
                        unbounded!(fs, submit_commit(commit))?,
                        FilesystemCommitOutcome::Committed(_)
                    ));
                }
                FilesystemCommitOutcome::Committed(_) => {}
                FilesystemCommitOutcome::Rejected(_) => {
                    return Err("uncontended publication was rejected".into());
                }
            }
            assert!(assert_cut(&stream, &workspaces, &expected, &staged).await?);
            if !fired {
                break;
            }
        }
    }
    Ok(())
}

/// Either a caller-record conflict or a concurrent edit in just one workspace
/// refuses every publication. Neither can leak the other workspace's new head.
#[tokio::test]
async fn coupled_publication_refuses_the_whole_cut_on_any_conflict() -> Result<(), Box<dyn Error>> {
    for conflict in 0..3 {
        let stream = Arc::new(acyclic_stream::MemoryStream::default());
        let cutting = Arc::new(CutStream::new(Arc::clone(&stream)));
        let fs: CutFs = Fs::new(
            crate::StreamAuthorityStore::new(cutting),
            crate::LogicalObjectStore::new(Arc::new(acyclic_objects::MemoryObjects::memory())),
            crate::EmbeddedCapabilities::MEMORY,
        );
        let (workspaces, mut expected, staged) = staged(&fs).await?;
        let states = merge_states(&expected);
        let state_changes = merge_changes(&expected, &states);
        let mut request = caller()?;
        if conflict == 0 {
            request.effects[0].conditions[0] = CommitCondition::Tail {
                path: StreamPath::new("caller/events")?,
                expected: 7,
            };
        }
        let mut commit = FilesystemCommit::new(request);
        unbounded!(
            fs,
            prepare_publish_many(&specs(&expected, &staged), &state_changes, &mut commit)
        )?;
        if conflict == 1 {
            workspaces[1].write_text("/file", "user edit").await?;
            expected[1] = unbounded!(workspaces[1], head())?;
        }
        if conflict == 2 {
            let store = crate::StreamGitCompatStore::new(crate::StreamAuthorityStore::new(
                fs.stream_provider(),
            ));
            let mut concurrent =
                crate::GitCompatState::new("concurrent", expected[0].workspace_id());
            concurrent.revision = 1;
            assert!(
                crate::GitCompatStore::compare_and_swap(
                    &store,
                    expected[0].workspace_id(),
                    0,
                    concurrent
                )
                .await?
            );
        }
        assert!(matches!(
            unbounded!(fs, submit_commit(commit))?,
            FilesystemCommitOutcome::Rejected(_)
        ));
        assert!(matches!(
            stream.tail(StreamPath::new("caller/events")?).await,
            Err(acyclic_stream::StreamError::NotFound)
        ));
        let store = crate::StreamGitCompatStore::new(crate::StreamAuthorityStore::new(
            fs.stream_provider(),
        ));
        for (index, before) in expected.iter().enumerate() {
            let observed = crate::GitCompatStore::load(&store, before.workspace_id()).await?;
            if conflict == 2 && index == 0 {
                assert_eq!(
                    observed.as_ref().map(|state| state.current_branch.as_str()),
                    Some("concurrent")
                );
            } else {
                assert!(observed.is_none(), "rejected cut wrote merge state");
            }
        }
        for (workspace, before) in workspaces.iter().zip(&expected) {
            assert_eq!(unbounded!(workspace, head())?.id(), before.id());
        }
    }
    Ok(())
}

/// The shared object simulator stops at each actual read, batch, presence,
/// write or durability-barrier await. Both errors and dropped futures must
/// unwind every partial contribution, including the first workspace's proof.
#[tokio::test]
async fn coupled_publication_preparation_is_atomic_at_every_object_cut()
-> Result<(), Box<dyn Error>> {
    use crate::{ScheduledSimulationFault, Simulation, SimulationFault, SimulationOperation};
    for fault in [SimulationFault::RejectBefore, SimulationFault::PauseBefore] {
        for operation in [
            SimulationOperation::ObjectPut,
            SimulationOperation::ObjectRead,
            SimulationOperation::ObjectReadMany,
            SimulationOperation::ObjectContains,
            SimulationOperation::ObjectFlush,
        ] {
            for fail_at in 1.. {
                let stream = Arc::new(acyclic_stream::MemoryStream::default());
                let cutting = Arc::new(CutStream::new(Arc::clone(&stream)));
                let simulation = Simulation::wrap(
                    crate::StreamAuthorityStore::new(Arc::clone(&cutting)),
                    crate::LogicalObjectStore::new(Arc::new(
                        acyclic_objects::MemoryObjects::memory(),
                    )),
                );
                let (_, objects) = simulation.stores();
                let fs = Fs::new(
                    crate::StreamAuthorityStore::new(cutting),
                    objects,
                    crate::EmbeddedCapabilities::MEMORY,
                );
                let (workspaces, expected, staged) = staged(&fs).await?;
                let states = merge_states(&expected);
                let state_changes = merge_changes(&expected, &states);
                let baseline = simulation
                    .trace()?
                    .iter()
                    .filter(|event| event.operation == operation)
                    .map(|event| event.occurrence)
                    .max()
                    .unwrap_or(0);
                simulation.schedule(ScheduledSimulationFault {
                    operation,
                    occurrence: baseline + fail_at,
                    fault,
                })?;
                let original = caller()?;
                let mut commit = FilesystemCommit::new(original.clone());
                let specs = specs(&expected, &staged);
                let cancel = CancellationToken::new();
                let result = {
                    let future = fs.prepare_publish_many(
                        &specs,
                        &state_changes,
                        &mut commit,
                        WorkBudget::UNBOUNDED,
                        &cancel,
                    );
                    tokio::pin!(future);
                    tokio::select! {
                        result = &mut future => Some(result),
                        () = simulation.wait_for_pause(), if fault == SimulationFault::PauseBefore => None,
                    }
                };
                let fired = simulation
                    .trace()?
                    .iter()
                    .any(|event| event.fault == Some(fault));
                if !fired {
                    result.ok_or("unscheduled object pause")??;
                    break;
                }
                assert!(result.is_none_or(|result| result.is_err()));
                assert_eq!(commit.request(), &original);
                assert!(commit.authorities().is_empty());
                assert!(!assert_cut(&stream, &workspaces, &expected, &staged).await?);
            }
        }
    }
    Ok(())
}

/// Clearing merge state (abort/continue) uses the same conditional contribution
/// as creating it; a stale logical revision unwinds the already staged heads.
#[tokio::test]
async fn coupled_publication_clears_merge_state_and_rolls_back_stale_revision()
-> Result<(), Box<dyn Error>> {
    for stale in [true, false] {
        let stream = Arc::new(acyclic_stream::MemoryStream::default());
        let cutting = Arc::new(CutStream::new(Arc::clone(&stream)));
        let fs: CutFs = Fs::new(
            crate::StreamAuthorityStore::new(cutting),
            crate::LogicalObjectStore::new(Arc::new(acyclic_objects::MemoryObjects::memory())),
            crate::EmbeddedCapabilities::MEMORY,
        );
        let (workspaces, expected, staged) = staged(&fs).await?;
        let store = crate::StreamGitCompatStore::new(crate::StreamAuthorityStore::new(
            fs.stream_provider(),
        ));
        for (before, state) in expected.iter().zip(merge_states(&expected)) {
            assert!(
                crate::GitCompatStore::compare_and_swap(&store, before.workspace_id(), 0, state)
                    .await?
            );
        }
        let changes = expected
            .iter()
            .enumerate()
            .map(|(index, before)| crate::GitCompatCas {
                workspace: before.workspace_id(),
                expected: if stale && index == 1 { 2 } else { 1 },
                replacement: None,
            })
            .collect::<Vec<_>>();
        let mut original = caller()?;
        let state_path = StreamPath::new(format!(
            "fs/git-state-v1/{}/refs",
            hex::encode(expected[0].workspace_id().into_bytes())
        ))?;
        original.effects[0].conditions.push(CommitCondition::Tail {
            expected: stream.tail(state_path.clone()).await?,
            path: state_path,
        });
        let mut commit = FilesystemCommit::new(original.clone());
        let prepared = unbounded!(
            fs,
            prepare_publish_many(&specs(&expected, &staged), &changes, &mut commit)
        );
        if stale {
            assert!(prepared.is_err());
            assert_eq!(commit.request(), &original);
            assert!(commit.authorities().is_empty());
        } else {
            prepared?;
            assert!(matches!(
                unbounded!(fs, submit_commit(commit))?,
                FilesystemCommitOutcome::Committed(_)
            ));
        }
        for ((before, workspace), after) in expected.iter().zip(workspaces).zip(staged) {
            assert_eq!(
                unbounded!(workspace, head())?.id(),
                if stale { before.id() } else { after }
            );
            assert_eq!(
                crate::GitCompatStore::load(&store, before.workspace_id())
                    .await?
                    .is_some(),
                stale
            );
        }
    }
    Ok(())
}

/// A caller's weaker fence must survive both errors and cancellation after
/// a state contributor has strengthened it but the whole batch is unfinished.
#[tokio::test]
async fn coupled_publication_restores_strengthened_conditions_at_every_stream_cut()
-> Result<(), Box<dyn Error>> {
    for crash in [false, true] {
        for fail_at in 1.. {
            let stream = Arc::new(acyclic_stream::MemoryStream::default());
            let cutting = Arc::new(CutStream::new(Arc::clone(&stream)));
            let fs: CutFs = Fs::new(
                crate::StreamAuthorityStore::new(Arc::clone(&cutting)),
                crate::LogicalObjectStore::new(Arc::new(acyclic_objects::MemoryObjects::memory())),
                crate::EmbeddedCapabilities::MEMORY,
            );
            let (_, expected, staged) = staged(&fs).await?;
            let store = crate::StreamGitCompatStore::new(crate::StreamAuthorityStore::new(
                Arc::clone(&cutting),
            ));
            for (before, state) in expected.iter().zip(merge_states(&expected)) {
                assert!(
                    crate::GitCompatStore::compare_and_swap(
                        &store,
                        before.workspace_id(),
                        0,
                        state
                    )
                    .await?
                );
            }
            let changes = expected
                .iter()
                .map(|before| crate::GitCompatCas {
                    workspace: before.workspace_id(),
                    expected: 1,
                    replacement: None,
                })
                .collect::<Vec<_>>();
            let state_path = StreamPath::new(format!(
                "fs/git-state-v1/{}/refs",
                hex::encode(expected[0].workspace_id().into_bytes())
            ))?;
            let mut original = caller()?;
            original.effects[0].conditions.push(CommitCondition::Tail {
                expected: stream.tail(state_path.clone()).await?,
                path: state_path.clone(),
            });
            let mut commit = FilesystemCommit::new(original.clone());
            let specs = specs(&expected, &staged);
            cutting.arm(fail_at, ForkCut::Before);
            cutting
                .crash_on_cut
                .store(crash, std::sync::atomic::Ordering::SeqCst);
            let cancel = CancellationToken::new();
            let result = {
                let future = fs.prepare_publish_many(
                    &specs,
                    &changes,
                    &mut commit,
                    WorkBudget::UNBOUNDED,
                    &cancel,
                );
                tokio::pin!(future);
                tokio::select! {
                    result = &mut future => Some(result),
                    () = cutting.crashed.notified(), if crash => None,
                }
            };
            cutting
                .crash_on_cut
                .store(false, std::sync::atomic::Ordering::SeqCst);
            if !cutting.disarm() {
                result.ok_or("unexpected crash")??;
                assert!(commit.request().effects[0].conditions.iter().any(|condition|
                    matches!(condition, CommitCondition::Head {path, ..} if path == &state_path)
                ));
                break;
            }
            assert!(result.is_none_or(|result| result.is_err()));
            assert_eq!(commit.request(), &original);
            assert!(commit.authorities().is_empty());
        }
    }
    Ok(())
}
