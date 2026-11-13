use super::*;
use crate::{AgentId, conversation::VolumeOwner, core::Authority};
use crate::{
    Capabilities,
    context::native_skills::NativeSkills,
    core::{AggregateKind, AuthorityRoot},
};
use futures::FutureExt as _;

type MemoryFs = Fs<acyclic_fs::MemoryAuthorityBackend, acyclic_fs::MemoryObjectBackend>;
type Host = FilesystemHost<acyclic_fs::MemoryAuthorityBackend, acyclic_fs::MemoryObjectBackend>;
type Reader = crate::filesystem::FilesystemContentVerifier<
    acyclic_fs::MemoryAuthorityBackend,
    acyclic_fs::MemoryObjectBackend,
>;
type Publisher = crate::filesystem::FilesystemContentPublisher<
    acyclic_fs::MemoryAuthorityBackend,
    acyclic_fs::MemoryObjectBackend,
>;

struct CaptureBindings {
    reader: Arc<Reader>,
    publisher: Arc<Publisher>,
    journal_reader: Arc<Reader>,
    _owner: AuthorityRoot,
}

impl Fixture {
    fn source_reader(&self, host: Arc<Host>) -> Result<Arc<Reader>> {
        let scope = self._issuer.bootstrap_scope(
            "source-reader",
            &Capabilities::new([self.volume.capability(VolumeOperation::Read, &mut None)?]),
            Some(AgentId::from_bytes([1; 16])),
            &mut None,
        )?;
        Reader::new(host, &self._issuer.verifier(&mut None)?, &scope, None, None)
    }

    async fn capture_bindings(&self) -> Result<CaptureBindings> {
        let journal = crate::conversation::journal_volume(
            self.volume.provider().clone(),
            &crate::agent::AgentId::from_bytes([2; 16]),
        )?;
        self.host.create_volume(&journal).await?;
        let owner = AuthorityRoot::host_owned(
            "skill-capture",
            [7; 32],
            Authority {
                kind: AggregateKind::Conversation,
                id: "capture".into(),
            },
        );
        let scope = owner.bootstrap_scope(
            "capture",
            &Capabilities::new([
                crate::contract::capability::HARNESS_HOST.to_owned(),
                journal.capability(VolumeOperation::Read, &mut None)?,
                journal.capability(VolumeOperation::Write, &mut None)?,
            ]),
            Some(AgentId::from_bytes([2; 16])),
            &mut None,
        )?;
        let verifier = owner.verifier(&mut None)?;
        Ok(CaptureBindings {
            reader: self.source_reader(self.host.clone())?,
            publisher: Publisher::new(self.host.clone(), &journal, &verifier, &scope, None, None)?,
            journal_reader: Reader::new(self.host.clone(), &verifier, &scope, None, None)?,
            _owner: owner,
        })
    }
}

#[tokio::test]
async fn captured_skill_replays_from_separate_journal_and_rejects_another_source() -> Result<()> {
    use crate::conversation::{
        ContentAccess, ContentCapture, ContentPath, ContentPublisher, ContentResidencyVerifier,
    };
    let fixture = fixture().await?;
    let bindings = fixture.capture_bindings().await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::BOUNDARY_BUDGET,
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = PreparationIo::from_native_work(&work)?;
    let capture = |index: usize| {
        let source = &fixture.skills[index].source;
        ContentCapture {
            source: ContentPath {
                volume: &source.root.volume,
                granted_prefix: &source.root.directory,
                path: &source.path,
                generation: &source.generation,
            },
            publisher: bindings.publisher.as_ref(),
            reader: bindings.journal_reader.as_ref(),
            operation: crate::OperationId::from_bytes([29; 16]),
            path: "inputs/skills/captured/SKILL.md",
            media_type: "text/markdown",
            display_name: "SKILL.md",
        }
    };
    let access = ContentAccess {
        ceiling: None,
        io: Some(&io),
    };
    let (original, captured_bytes) = bindings
        .reader
        .capture_private_path(capture(0), Some(4096), access)?
        .await?;
    assert!(String::from_utf8_lossy(&captured_bytes).contains("first body"));
    assert_eq!(original.volume(), bindings.publisher.volume());
    assert_ne!(original.volume(), &fixture.volume);
    let bytes = bindings
        .journal_reader
        .read(&original, 4096, None, Some(&io))?
        .await?;
    assert!(String::from_utf8_lossy(&bytes).contains("first body"));
    assert!(
        fixture
            .host
            .stat_private_path(
                &fixture.volume,
                &fixture.grant,
                &fixture.skills[0].source.path,
                None,
                access
            )
            .await?
            .is_none()
    );

    // Lose both native bodies and all process-local materialization state.
    for name in ["first", "second"] {
        std::fs::remove_file(fixture.home.path().join(name).join("SKILL.md"))
            .map_err(|error| Error::Storage(error.to_string()))?;
    }
    let sources = NativeSkills::new(
        fixture.filesystem.stream_provider(),
        fixture.volume.clone(),
        None,
        Vec::new(),
    )?;
    let reopened =
        FilesystemHost::new(fixture.filesystem.clone(), fixture.volume.provider(), None)?
            .with_native_skills(sources)?;
    let reader = fixture.source_reader(reopened)?;
    assert_eq!(
        reader
            .capture_private_path(capture(0), Some(4096), access)?
            .await?
            .0,
        original
    );
    // Recovering by operation alone, or deriving a new operation from the
    // changed source, would accept a request that the original receipt never bound.
    assert_eq!(
        reader
            .capture_private_path(capture(1), Some(4096), access)?
            .await
            .unwrap_err()
            .code(),
        crate::wire::ErrorCode::Conflict
    );
    assert_eq!(
        reader
            .capture_private_path(capture(0), Some(8), access)?
            .await
            .unwrap_err()
            .code(),
        crate::wire::ErrorCode::Invalid
    );
    for (path, expires_at) in [("public/**", None), ("**", Some(0))] {
        use acyclic_fs::access::{Action, Capability, Generations, Grant, ResourcePattern};
        let ceiling = Capabilities::new([Capability {
            layers: vec![vec![Grant {
                actions: [Action::List, Action::Read, Action::ReadHistory]
                    .into_iter()
                    .collect(),
                resource: ResourcePattern {
                    lineage_root: None,
                    volume: None,
                    generations: Generations::All,
                    path: path.into(),
                },
            }]],
            expires_at,
        }
        .to_scope_label()
        .map_err(|error| Error::Invalid(error.to_string()))?]);
        assert_eq!(
            reader
                .capture_private_path(
                    capture(0),
                    Some(4096),
                    ContentAccess {
                        ceiling: Some(&ceiling),
                        io: Some(&io),
                    }
                )?
                .await
                .unwrap_err()
                .code(),
            crate::wire::ErrorCode::Unauthorized
        );
    }
    // Journal custody never supplies original source authority.
    assert_eq!(
        bindings
            .journal_reader
            .capture_private_path(capture(0), Some(4096), access)?
            .await
            .unwrap_err()
            .code(),
        crate::wire::ErrorCode::Unauthorized
    );
    Ok(())
}

struct Fixture {
    _issuer: AuthorityRoot,
    home: tempfile::TempDir,
    filesystem: MemoryFs,
    host: Arc<Host>,
    volume: VolumeRef,
    grant: ContentGrant,
    skills: Vec<crate::context::SkillMetadata>,
}

async fn fixture() -> Result<Fixture> {
    acyclic_native_runtime::initialize_native_io_services()
        .map_err(|error| Error::Storage(error.to_string()))?;
    acyclic_fs::watch::initialize_native_watch_services()
        .map_err(|error| Error::Storage(error.to_string()))?;
    let home = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    for name in ["first", "second"] {
        let directory = home.path().join(name);
        std::fs::create_dir_all(&directory).map_err(|error| Error::Storage(error.to_string()))?;
        std::fs::write(
            directory.join("SKILL.md"),
            format!("---\nname: {name}\ndescription: {name}\n---\n{name} body\n"),
        )
        .map_err(|error| Error::Storage(error.to_string()))?;
    }
    let provider = ProviderRef::new(
        "skill-race".into(),
        "filesystem".into(),
        "1".into(),
        &mut None,
    )?;
    let agent = AgentId::from_bytes([1; 16]);
    let volume = VolumeRef::new(
        provider.clone(),
        "private".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )?;
    let filesystem = Fs::memory();
    let mut host = FilesystemHost::new(filesystem.clone(), &provider, None)?;
    host.create_volume(&volume).await?;
    let issuer = AuthorityRoot::new(
        "skill-race",
        [3; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "owner".into(),
        },
    );
    let scope = issuer.bootstrap_scope(
        "owner",
        &Capabilities::new([volume.capability(VolumeOperation::Read, &mut None)?]),
        Some(agent),
        &mut None,
    )?;
    let grant = ContentGrant::verify_directory_read(
        &issuer.verifier(&mut None)?,
        &scope,
        &volume,
        "inputs/skills",
        &mut None,
    )?;
    let sources = NativeSkills::new(
        filesystem.stream_provider(),
        volume.clone(),
        None,
        vec![home.path().into()],
    )?;
    let skills = sources
        .user_catalog(&mut crate::context::DiscoveryWork::default())?
        .await?;
    assert_eq!(skills.len(), 2);
    host = host.with_native_skills(sources.clone())?;
    Ok(Fixture {
        _issuer: issuer,
        home,
        filesystem,
        host,
        volume,
        grant,
        skills,
    })
}

#[tokio::test]
async fn concurrent_first_skill_bodies_share_one_journal_materializer() -> Result<()> {
    use crate::conversation::{
        ContentAccess, ContentCapture, ContentPath, ContentResidencyVerifier,
    };
    let mut fixture = fixture().await?;
    let (entered_a, at_a) = tokio::sync::oneshot::channel();
    let (entered_b, at_b) = tokio::sync::oneshot::channel();
    let (release_a, resume_a) = tokio::sync::oneshot::channel();
    let (release_b, resume_b) = tokio::sync::oneshot::channel();
    let pauses = Arc::new(std::sync::Mutex::new(std::collections::VecDeque::from([
        (entered_a, resume_a),
        (entered_b, resume_b),
    ])));
    Arc::get_mut(&mut fixture.host).unwrap().publication_pause = Some(Arc::new(move || {
        let (entered, resume) = pauses.lock().unwrap().pop_front().unwrap();
        Box::pin(async move {
            entered.send(()).unwrap();
            resume.await.unwrap();
        })
    }));
    let bindings = fixture.capture_bindings().await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work_a = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::BOUNDARY_BUDGET,
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let work_b = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::BOUNDARY_BUDGET,
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io_a = PreparationIo::from_native_work(&work_a)?;
    let io_b = PreparationIo::from_native_work(&work_b)?;
    let capture = |index: usize| {
        let source = &fixture.skills[index].source;
        ContentCapture {
            source: ContentPath {
                volume: &source.root.volume,
                granted_prefix: &source.root.directory,
                path: &source.path,
                generation: &source.generation,
            },
            publisher: bindings.publisher.as_ref(),
            reader: bindings.journal_reader.as_ref(),
            operation: crate::OperationId::from_bytes([index as u8 + 41; 16]),
            path: &source.path,
            media_type: "text/markdown",
            display_name: "SKILL.md",
        }
    };
    let first = bindings.reader.capture_private_path(
        capture(0),
        Some(4096),
        ContentAccess {
            ceiling: None,
            io: Some(&io_a),
        },
    )?;
    tokio::pin!(first);
    tokio::select! {
        result = &mut first => panic!("first load completed before its pinned-transaction pause: {result:?}"),
        _ = at_a => {},
    }
    // Replacement registration retains the original source records and owner.
    let replacement = NativeSkills::new(
        fixture.filesystem.stream_provider(),
        fixture.volume.clone(),
        None,
        Vec::new(),
    )?;
    let rebound = fixture.host.clone().with_native_skills(replacement)?;
    let registered = rebound.skill_sources(&fixture.volume)?.unwrap();
    let reader = fixture.source_reader(rebound)?;
    let second = reader.capture_private_path(
        capture(1),
        Some(4096),
        ContentAccess {
            ceiling: None,
            io: Some(&io_b),
        },
    )?;
    tokio::pin!(second);
    assert!(futures::poll!(&mut second).is_pending());
    // Negative control: removing owner serialization pins both transactions
    // before either commits, reproducing the former disjoint-load race.
    if registered.materializer.enter().now_or_never().is_some() {
        tokio::select! {
            result = &mut second => panic!("second load completed before its pause: {result:?}"),
            _ = at_b => {},
        }
    }
    release_a.send(()).unwrap();
    release_b.send(()).unwrap();
    let (first, second) = futures::join!(first, second);
    for ((file, body), expected) in [(first?, "first body"), (second?, "second body")] {
        assert!(String::from_utf8_lossy(&body).contains(expected));
        let bytes = bindings
            .journal_reader
            .read(&file, 4096, None, None)?
            .await?;
        assert!(String::from_utf8_lossy(&bytes).contains(expected));
    }
    for skill in &fixture.skills {
        assert!(
            fixture
                .host
                .stat_private_path(
                    &fixture.volume,
                    &fixture.grant,
                    &skill.source.path,
                    None,
                    ContentAccess::default()
                )
                .await?
                .is_none()
        );
    }
    Ok(())
}

#[tokio::test]
async fn denied_skill_body_never_materializes_the_host_source() -> Result<()> {
    use crate::conversation::{
        ContentAccess, ContentCapture, ContentPath, ContentPublisher, ContentResidencyVerifier,
    };
    use acyclic_fs::access::{Action, Capability, Generations, Grant, ResourcePattern};
    let fixture = fixture().await?;
    let bindings = fixture.capture_bindings().await?;
    let workspace = workspace_ref(
        bindings.publisher.volume().provider().clone(),
        &bindings.publisher.volume().storage_name(&mut None)?,
    )?;
    let original = fixture.host.resolve(&workspace, None).await?.generation;
    let source = &fixture.skills[0].source;
    for (path, expires_at) in [("public/**", None), ("**", Some(0))] {
        let ceiling = Capabilities::new([Capability {
            layers: vec![vec![Grant {
                actions: [Action::List, Action::Read, Action::ReadHistory]
                    .into_iter()
                    .collect(),
                resource: ResourcePattern {
                    lineage_root: None,
                    volume: None,
                    generations: Generations::All,
                    path: path.into(),
                },
            }]],
            expires_at,
        }
        .to_scope_label()
        .map_err(|error| Error::Invalid(error.to_string()))?]);
        let result = bindings
            .reader
            .capture_private_path(
                ContentCapture {
                    source: ContentPath {
                        volume: &source.root.volume,
                        granted_prefix: &source.root.directory,
                        path: &source.path,
                        generation: &source.generation,
                    },
                    publisher: bindings.publisher.as_ref(),
                    reader: bindings.journal_reader.as_ref(),
                    operation: crate::OperationId::from_bytes([63; 16]),
                    path: "inputs/skills/denied/SKILL.md",
                    media_type: "text/markdown",
                    display_name: "SKILL.md",
                },
                Some(4096),
                ContentAccess {
                    ceiling: Some(&ceiling),
                    io: None,
                },
            )?
            .await;
        assert_eq!(
            result.unwrap_err().code(),
            crate::wire::ErrorCode::Unauthorized
        );
        assert_eq!(
            fixture.host.resolve(&workspace, None).await?.generation,
            original
        );
        assert!(
            fixture
                .host
                .stat_private_path(
                    &fixture.volume,
                    &fixture.grant,
                    &source.path,
                    None,
                    ContentAccess::default()
                )
                .await?
                .is_none()
        );
    }
    Ok(())
}
