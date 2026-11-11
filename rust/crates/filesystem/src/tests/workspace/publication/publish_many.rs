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
    let path = StreamPath::new("agent/conversation")?;
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
    for name in ["sdk", "cloud"] {
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

async fn assert_cut<O: crate::AsyncObjectStore + 'static>(
    stream: &acyclic_stream::MemoryStream,
    workspaces: &[CutWorkspace<O>],
    expected: &[CutGeneration<O>],
    staged: &[GenerationId],
) -> Result<bool, Box<dyn Error>> {
    let committed = match stream.tail(StreamPath::new("agent/conversation")?).await {
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
            let specs = specs(&expected, &staged);
            let original = caller()?;
            let mut commit = FilesystemCommit::new(original.clone());
            cutting.arm(fail_at, ForkCut::Before);
            cutting
                .crash_on_cut
                .store(crash, std::sync::atomic::Ordering::SeqCst);
            let cancel = CancellationToken::new();
            let result = {
                let future =
                    fs.prepare_publish_many(&specs, &mut commit, WorkBudget::UNBOUNDED, &cancel);
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
            let mut commit = FilesystemCommit::new(caller()?);
            unbounded!(
                fs,
                prepare_publish_many(&specs(&expected, &staged), &mut commit)
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
    for concurrent in [false, true] {
        let stream = Arc::new(acyclic_stream::MemoryStream::default());
        let cutting = Arc::new(CutStream::new(Arc::clone(&stream)));
        let fs: CutFs = Fs::new(
            crate::StreamAuthorityStore::new(cutting),
            crate::LogicalObjectStore::new(Arc::new(acyclic_objects::MemoryObjects::memory())),
            crate::EmbeddedCapabilities::MEMORY,
        );
        let (workspaces, mut expected, staged) = staged(&fs).await?;
        let mut request = caller()?;
        if !concurrent {
            request.effects[0].conditions[0] = CommitCondition::Tail {
                path: StreamPath::new("agent/conversation")?,
                expected: 7,
            };
        }
        let mut commit = FilesystemCommit::new(request);
        unbounded!(
            fs,
            prepare_publish_many(&specs(&expected, &staged), &mut commit)
        )?;
        if concurrent {
            workspaces[1].write_text("/file", "user edit").await?;
            expected[1] = unbounded!(workspaces[1], head())?;
        }
        assert!(matches!(
            unbounded!(fs, submit_commit(commit))?,
            FilesystemCommitOutcome::Rejected(_)
        ));
        assert!(matches!(
            stream.tail(StreamPath::new("agent/conversation")?).await,
            Err(acyclic_stream::StreamError::NotFound)
        ));
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
    use crate::{
        ScheduledSimulationFault, Simulation, SimulationFault, SimulationOperation,
        SimulationOptions,
    };
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
                    SimulationOptions::default(),
                )?;
                let (_, objects) = simulation.stores();
                let fs = Fs::new(
                    crate::StreamAuthorityStore::new(cutting),
                    objects,
                    crate::EmbeddedCapabilities::MEMORY,
                );
                let (workspaces, expected, staged) = staged(&fs).await?;
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
