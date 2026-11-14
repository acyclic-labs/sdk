use super::*;
use crate::agent::{AgentId, ForkAdopted, IntentId, WorkspaceEntry, WorkspaceRole};
use crate::conversation::{VolumeOwner, VolumeRef};
use crate::filesystem::FilesystemHost;
use acyclic_fs::{FilesystemCommitOutcome, MemoryAuthorityBackend, MemoryObjectBackend};
use acyclic_stream::StreamProviderExt as _;

type Host = FilesystemHost<MemoryAuthorityBackend, MemoryObjectBackend>;
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

async fn prepare(
    host: &Host,
    agent: &AgentPath,
    project: &VolumeRef,
) -> TestResult<FilesystemCommit> {
    let private = VolumeRef::new(
        host.provider.clone(),
        format!("private-{}", agent.id()),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(crate::AgentId::from_bytes(agent.id_bytes())),
        &mut None,
    )?;
    let spawn = crate::agent::create_root(
        &IntentId::try_from(format!("create-{}", agent.id()))?,
        ForkAdopted {
            agent: agent.id(),
            parent: None,
            profile: None,
            forked_at: 0,
            conversation_tail: 0,
            checkpoint: None,
            volumes: vec![
                WorkspaceEntry::initial_volume(
                    WorkspaceRole::Private,
                    "private".into(),
                    private.clone(),
                ),
                WorkspaceEntry::initial_volume(
                    WorkspaceRole::User,
                    "project".into(),
                    project.clone(),
                ),
            ],
            brief: None,
            policy: None,
        },
    )?;
    let fs = &host.filesystem;
    let cancel = CancellationToken::new();
    let mut commit = FilesystemCommit::new(spawn.request);
    let original = fs
        .prepare_create_many(
            &[host.volume_create_spec(&private)?],
            &mut commit,
            WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await?
        .value;
    capture_creation(fs, agent, &original, &mut commit).await?;
    Ok(commit)
}

#[tokio::test]
async fn adoption_retains_the_selected_existing_project_and_refuses_a_racing_head() -> TestResult {
    use crate::resources::ProviderRef;
    let host = FilesystemHost::new(
        acyclic_fs::Fs::memory(),
        &ProviderRef::new(
            "adoption".into(),
            "filesystem".into(),
            "1".into(),
            &mut None,
        )?,
        None,
    )?;
    let project = VolumeRef::new(
        host.provider.clone(),
        "project".into(),
        VolumeClass::Project,
        VolumeOwner::Project("project".into()),
        &mut None,
    )?;
    host.create_volume(&project).await?;
    let fs = &host.filesystem;
    let cancel = CancellationToken::new();
    let workspace = fs
        .open_workspace(
            project.storage_name(&mut None)?,
            WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await?
        .value;
    let birth = workspace
        .head(WorkBudget::UNBOUNDED, &cancel)
        .await?
        .value
        .id();
    workspace
        .write_text("/context.txt", "selected before adoption")
        .await?;
    let selected = workspace
        .head(WorkBudget::UNBOUNDED, &cancel)
        .await?
        .value
        .id();
    assert_ne!(birth, selected);
    let agent = AgentPath::root(&AgentId::from_bytes([81; 16]));
    let commit = prepare(&host, &agent, &project).await?;
    let stream = fs.stream_provider();
    assert!(matches!(
        stream.tail(agent.history()?).await,
        Err(acyclic_stream::StreamError::NotFound)
    ));
    assert!(matches!(
        fs.submit_commit(commit, WorkBudget::UNBOUNDED, &cancel)
            .await?
            .value,
        FilesystemCommitOutcome::Committed(_)
    ));
    let baseline = super::super::read_boundary(stream.as_ref(), &agent, 0).await?;
    assert_eq!(baseline.conversation, 0);
    assert_eq!(baseline.context.tail, 0);
    assert_eq!(
        baseline.volumes.len(),
        1,
        "private state entered project history"
    );
    assert_eq!(baseline.volumes[0].generation, selected);
    workspace
        .write_text("/context.txt", "later project state")
        .await?;
    let retained = fs
        .open_workspace(
            &baseline.volumes[0].retained_workspace,
            WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await?
        .value;
    let original = retained
        .generation(
            baseline.volumes[0].retained_generation,
            WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await?
        .value;
    assert_eq!(
        original
            .read("/context.txt", 64, WorkBudget::UNBOUNDED, &cancel)
            .await?
            .value
            .as_ref(),
        b"selected before adoption"
    );
    let racing = AgentPath::root(&AgentId::from_bytes([82; 16]));
    let commit = prepare(&host, &racing, &project).await?;
    workspace
        .write_text("/context.txt", "changed after preparation")
        .await?;
    assert!(matches!(
        fs.submit_commit(commit, WorkBudget::UNBOUNDED, &cancel)
            .await?
            .value,
        FilesystemCommitOutcome::Rejected(_)
    ));
    for path in [racing.inbox()?, racing.history()?] {
        assert!(matches!(
            stream.tail(path).await,
            Err(acyclic_stream::StreamError::NotFound)
        ));
    }
    Ok(())
}
