//! Production filesystem discovery, reload, and lazy body boundaries.
#![cfg(feature = "filesystem")]

use acyclic_fs::Fs;
use acyclic_harness::{
    AgentId, Capabilities, Error, IdempotencyKey, Result,
    context::{
        ContextDiscovery, ContextDiscoveryLimits, ContextDiscoveryPolicy, ContextInput,
        ContextPipeline, ContextPlacement, ContextReloadPolicy, ContextRoot, InstructionScope,
        parse_skill_metadata,
    },
    conversation::{
        ContentGrant, ContentResidencyVerifier, VolumeClass, VolumeOperation, VolumeOwner,
        VolumeRef,
    },
    core::{AggregateKind, Authority, AuthorityIssuer},
    filesystem::{FilesystemContentVerifier, FilesystemHost},
    model::ModelContent,
    resources::ProviderRef,
};
use std::sync::{Arc, Mutex};

type TestJournal = acyclic_harness::filesystem::FilesystemExecutionJournal<
    acyclic_stream::MemoryStream,
    acyclic_fs::MemoryAuthorityBackend,
    acyclic_fs::MemoryObjectBackend,
>;

fn first_skill(
    snapshot: &acyclic_harness::context::DiscoveredContext,
) -> Result<&acyclic_harness::context::SkillMetadata> {
    snapshot
        .skills
        .first()
        .ok_or_else(|| Error::NotFound("captured skill".into()))
}

fn executor(
    snapshot: acyclic_harness::context::DiscoveredContext,
    provider: Arc<CapturingModel>,
) -> Result<acyclic_harness::executor::StockExecutor> {
    Ok(acyclic_harness::executor::StockExecutor::new(
        acyclic_harness::model::Model::new("test", "model", "1", serde_json::Value::Null)?,
        provider,
        ContextPipeline::default().with(Arc::new(snapshot.stage(ContextPlacement::Prepend)?)),
        acyclic_harness::tool::ToolRegistry::new(),
    ))
}

fn assert_lazy_request(provider: &CapturingModel) -> Result<()> {
    let requests = provider
        .0
        .lock()
        .map_err(|_| Error::Storage("model lock".into()))?;
    assert_eq!(requests.len(), 1);
    let request = std::str::from_utf8(
        requests
            .first()
            .ok_or_else(|| Error::NotFound("model request".into()))?,
    )
    .map_err(|e| Error::Invalid(e.to_string()))?;
    assert!(request.contains("Inspect files"));
    assert!(!request.contains("SECRET BODY"));
    Ok(())
}

#[derive(Default)]
struct CapturingModel(Mutex<Vec<Vec<u8>>>);

impl acyclic_harness::model::ModelProvider for CapturingModel {
    fn generate<'a>(
        &'a self,
        request: acyclic_harness::model::PreparedModelRequest,
        _dispatch: acyclic_harness::model::ModelDispatch,
    ) -> futures::stream::BoxStream<'a, Result<acyclic_harness::model::ModelEvent>> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(request.bytes().to_vec());
        Box::pin(futures::stream::iter([Ok(
            acyclic_harness::model::ModelEvent::Completed {
                metadata: serde_json::Value::Null,
            },
        )]))
    }

    fn reconcile<'a>(
        &'a self,
        _: acyclic_harness::model::ModelAttempt,
    ) -> futures::future::BoxFuture<'a, Result<Option<Vec<acyclic_harness::model::ModelEvent>>>>
    {
        Box::pin(async { Ok(None) })
    }
}

struct Fixture {
    host: Arc<FilesystemHost<acyclic_fs::MemoryAuthorityBackend, acyclic_fs::MemoryObjectBackend>>,
    issuer: AuthorityIssuer,
    volume: VolumeRef,
    writer: ContentGrant,
    reader: Arc<dyn ContentResidencyVerifier>,
}

impl Fixture {
    async fn journal_volume(&self) -> Result<(VolumeRef, acyclic_harness::core::Scope)> {
        let private = VolumeRef::new(
            self.volume.provider().clone(),
            "journal",
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([1; 16])),
        )?;
        self.host.create_volume(&private).await?;
        let scope = self.issuer.root_for_agent(
            AgentId::from_bytes([1; 16]),
            "journal",
            Capabilities::new([
                private.capability(VolumeOperation::Read)?,
                private.capability(VolumeOperation::Write)?,
                self.volume.capability(VolumeOperation::Read)?,
            ]),
        );
        Ok((private, scope))
    }

    fn journal(
        &self,
        stream: acyclic_stream::StreamClient<acyclic_stream::MemoryStream>,
        private: VolumeRef,
        scope: acyclic_harness::core::Scope,
    ) -> Result<TestJournal> {
        Ok(TestJournal::new(
            stream,
            self.host.clone(),
            private,
            self.issuer.verifier(),
            scope,
            32_768,
        )?
        .with_input_verifier(self.reader.clone()))
    }
    async fn new() -> Result<Self> {
        let provider = ProviderRef::new("discovery", "filesystem", "2")?;
        let host = Arc::new(FilesystemHost::new(Fs::memory(), provider.clone())?);
        let agent = AgentId::from_bytes([1; 16]);
        let volume = VolumeRef::new(
            provider,
            "sources",
            VolumeClass::SessionShared,
            VolumeOwner::Agent(agent),
        )?;
        let issuer = AuthorityIssuer::new(
            "discovery",
            [9; 32],
            Authority {
                kind: AggregateKind::Conversation,
                id: "test".into(),
            },
        );
        let scope = issuer.root_for_agent(
            agent,
            "owner",
            Capabilities::new([
                volume.capability(VolumeOperation::Write)?,
                volume.capability(VolumeOperation::Read)?,
                volume.directory_read_capability("")?,
            ]),
        );
        host.create_volume(&volume).await?;
        let writer =
            ContentGrant::verify(&issuer.verifier(), &scope, &volume, VolumeOperation::Write)?;
        let reader = Arc::new(FilesystemContentVerifier::new(
            host.clone(),
            issuer.verifier(),
            scope,
            32_768,
        )?);
        Ok(Self {
            host,
            issuer,
            volume,
            writer,
            reader,
        })
    }

    async fn put(&self, path: &str, bytes: &[u8], key: &str) -> Result<()> {
        self.host
            .put_content(
                &self.volume,
                &self.writer,
                path,
                bytes,
                "text/plain",
                "source.md",
                1_000_000,
                &IdempotencyKey::new(key)?,
            )
            .await?;
        Ok(())
    }

    fn declaration(&self) -> ContextDiscovery {
        let root = ContextRoot {
            volume: self.volume.clone(),
            directory: String::new(),
        };
        ContextDiscovery {
            instructions: vec![InstructionScope {
                root: root.clone(),
                active_directory: "project".into(),
            }],
            skills: vec![ContextRoot {
                directory: "skills".into(),
                ..root
            }],
            policy: ContextDiscoveryPolicy::default(),
            limits: ContextDiscoveryLimits {
                entries: 64,
                directories: 8,
                header_bytes: 256,
                instruction_bytes: 512,
            },
        }
    }
}

#[tokio::test]
async fn metadata_is_lazy_scopes_are_ordered_and_reload_is_immutable() -> Result<()> {
    let fixture = Fixture::new().await?;
    fixture.put("AGENTS.md", b"root rules", "root").await?;
    fixture
        .put("project/AGENTS.md", b"project rules", "project")
        .await?;
    let skill = b"---\nname: inspect\ndescription: Inspect the selected source\nmetadata:\n  category: review\n---\n";
    let mut large = skill.to_vec();
    large.resize(100_000, b'x');
    fixture
        .put("skills/inspect/SKILL.md", &large, "skill")
        .await?;
    let declaration = fixture.declaration();
    let captured = declaration.capture(fixture.reader.as_ref()).await?;
    assert_eq!(
        captured
            .instructions
            .iter()
            .map(|file| file.path())
            .collect::<Vec<_>>(),
        ["AGENTS.md", "project/AGENTS.md"]
    );
    assert_eq!(captured.skills.len(), 1);
    // Discovery succeeds although the complete body exceeds the bound reader.
    assert!(
        first_skill(&captured)?
            .source
            .read(fixture.reader.as_ref())
            .await
            .is_err()
    );
    let old = ContextPipeline::new(
        [Arc::new(captured.clone().stage(ContextPlacement::Prepend)?)
            as Arc<dyn acyclic_harness::context::ContextStage>],
    );
    fixture
        .put("AGENTS.md", b"changed rules", "changed-root")
        .await?;
    fixture
        .put(
            "skills/inspect/SKILL.md",
            b"---\nname: inspect\ndescription: New description\n---\nnew body",
            "changed-skill",
        )
        .await?;
    let pinned = declaration
        .for_request(
            fixture.reader.as_ref(),
            &captured,
            ContextReloadPolicy::default(),
        )
        .await?;
    assert_eq!(captured, pinned);
    let live = declaration
        .for_request(
            fixture.reader.as_ref(),
            &captured,
            ContextReloadPolicy::NextRequest,
        )
        .await?;
    assert_ne!(captured.instructions, live.instructions);
    assert_eq!(
        first_skill(&captured)?.description,
        "Inspect the selected source"
    );
    assert_eq!(first_skill(&live)?.description, "New description");
    let new = old.reload(ContextPipeline::new([
        Arc::new(live.stage(ContextPlacement::Prepend)?)
            as Arc<dyn acyclic_harness::context::ContextStage>,
    ]))?;
    assert_ne!(old.contracts(), new.contracts());
    let input = ContextInput {
        input: ModelContent::Text("inspect".into()),
        selected_context: None,
        step: 0,
        prior_messages: Vec::new(),
    };
    let old_context = old.run(&input).await?;
    let recovered: acyclic_harness::context::DiscoveredContext = serde_json::from_slice(
        &serde_json::to_vec(&captured).map_err(|e| Error::Invalid(e.to_string()))?,
    )
    .map_err(|e| Error::Invalid(e.to_string()))?;
    let restarted = ContextPipeline::new([Arc::new(recovered.stage(ContextPlacement::Prepend)?)
        as Arc<dyn acyclic_harness::context::ContextStage>]);
    assert_eq!(old_context, restarted.run(&input).await?);
    assert_ne!(old_context, new.run(&input).await?);
    // A failed replacement cannot change the old source or its contract.
    fixture
        .put(
            "skills/inspect/SKILL.md",
            b"---\nname: [invalid]\n---\n",
            "invalid",
        )
        .await?;
    assert!(declaration.capture(fixture.reader.as_ref()).await.is_err());
    assert_eq!(old_context, old.run(&input).await?);
    Ok(())
}

#[tokio::test]
async fn discovery_denies_authority_and_bounds_work_without_partial_success() -> Result<()> {
    let fixture = Fixture::new().await?;
    fixture.put("AGENTS.md", b"root", "root").await?;
    fixture.put("project/AGENTS.md", b"child", "child").await?;
    fixture
        .put(
            "skills/a/SKILL.md",
            b"---\nname: same\ndescription: one\n---\nbody",
            "a",
        )
        .await?;
    fixture
        .put(
            "skills/b/SKILL.md",
            b"---\nname: same\ndescription: two\n---\nbody",
            "b",
        )
        .await?;
    let mut declaration = fixture.declaration();
    assert!(matches!(
        declaration.capture(fixture.reader.as_ref()).await,
        Err(Error::Conflict(_))
    ));
    declaration.policy.skill_name = None;
    declaration.limits.entries = 1;
    assert!(matches!(
        declaration.capture(fixture.reader.as_ref()).await,
        Err(Error::Invalid(_))
    ));
    declaration.limits.entries = 64;
    let scope = fixture.issuer.root_for_agent(
        AgentId::from_bytes([1; 16]),
        "denied",
        Capabilities::default(),
    );
    let denied = FilesystemContentVerifier::new(
        fixture.host.clone(),
        fixture.issuer.verifier(),
        scope,
        32_768,
    )?;
    assert!(matches!(
        declaration.capture(&denied).await,
        Err(Error::Unauthorized(_))
    ));
    declaration.policy.instruction_names.clear();
    assert_eq!(declaration.capture(&denied).await?, Default::default());
    declaration
        .policy
        .instruction_names
        .push("AGENTS.md".into());
    fixture.put("AGENTS.md", &[b'x'; 513], "oversized").await?;
    assert!(matches!(
        declaration.capture(fixture.reader.as_ref()).await,
        Err(Error::Invalid(_))
    ));
    fixture.put("AGENTS.md", b"bad\0text", "nul").await?;
    assert!(matches!(
        declaration.capture(fixture.reader.as_ref()).await,
        Err(Error::Invalid(_))
    ));
    Ok(())
}

#[tokio::test]
async fn malformed_frontmatter_has_explicit_outcomes() -> Result<()> {
    let fixture = Fixture::new().await?;
    fixture
        .put(
            "skills/a/SKILL.md",
            b"---\nname: valid\ndescription: valid\n---\nbody",
            "a",
        )
        .await?;
    let mut declaration = fixture.declaration();
    declaration.instructions.clear();
    let captured = declaration.capture(fixture.reader.as_ref()).await?;
    let source = captured
        .skills
        .first()
        .ok_or_else(|| Error::NotFound("captured skill".into()))?
        .source
        .clone();
    for invalid in [
        b"no frontmatter".as_slice(),
        b"---\nname: a\ndescription: x\n",
        b"---\nname: a\nname: b\ndescription: x\n---\n",
        b"---\nname: a\ndescription: ''\n---\n",
        b"---\nname: A\ndescription: x\n---\n",
        b"---\nname: a\ndescription: \xff\n---\n",
        b"---\nname: a\ndescription: x\nmetadata: {k: 1, k: 2}\n---\n",
        b"---\nname: a\ndescription: &x value\nmetadata: *x\n---\n",
        b"---\nname: a\ndescription: !include secret\n---\n",
    ] {
        assert!(parse_skill_metadata(invalid, source.clone()).is_err());
    }
    let valid = parse_skill_metadata(
        b"---\r\nname: inspect\r\ndescription: >-\r\n  Inspect selected\r\n  files\r\n---\r\n\xff",
        source.clone(),
    )?;
    assert_eq!(valid.description, "Inspect selected files");
    let nested = format!(
        "---\nname: a\ndescription: x\nmetadata: {}0{}\n---\n",
        "[".repeat(32),
        "]".repeat(32)
    );
    assert!(parse_skill_metadata(nested.as_bytes(), source).is_ok());
    let mut forged = captured;
    forged
        .skills
        .first_mut()
        .ok_or_else(|| Error::NotFound("forged skill".into()))?
        .name = "Invalid".into();
    assert!(forged.messages().is_err());
    forged
        .skills
        .first_mut()
        .ok_or_else(|| Error::NotFound("forged skill".into()))?
        .name = "valid".into();
    forged
        .skills
        .first_mut()
        .ok_or_else(|| Error::NotFound("forged skill".into()))?
        .fields
        .insert("description".into(), serde_json::json!("overridden"));
    assert!(forged.stage(ContextPlacement::Prepend).is_err());
    Ok(())
}

#[tokio::test]
async fn admitted_frontmatter_has_no_extra_size_or_node_ceilings() -> Result<()> {
    let fixture = Fixture::new().await?;
    fixture
        .put(
            "skills/a/SKILL.md",
            b"---\nname: a\ndescription: x\n---\n",
            "a",
        )
        .await?;
    let mut declaration = fixture.declaration();
    declaration.instructions.clear();
    let captured = declaration.capture(fixture.reader.as_ref()).await?;
    let source = first_skill(&captured)?.source.clone();
    let name = "a".repeat(128);
    let description = "d".repeat(2_048);
    let payload = "x".repeat(70_000);
    let items = vec!["0"; 3_000].join(",");
    let prefix = format!(
        "---\nname: {name}\ndescription: {description}\npayload: {payload}\nitems: [{items}]\n---\n"
    );
    let metadata = parse_skill_metadata(prefix.as_bytes(), source)?;
    assert_eq!(metadata.name, name);
    assert_eq!(metadata.description, description);
    assert_eq!(
        metadata
            .fields
            .get("payload")
            .and_then(serde_json::Value::as_str),
        Some(payload.as_str())
    );
    assert_eq!(
        metadata
            .fields
            .get("items")
            .and_then(serde_json::Value::as_array)
            .map(Vec::len),
        Some(3_000)
    );
    declaration.skills.clear();
    declaration.limits.header_bytes = 65_537;
    assert!(
        declaration
            .capture(fixture.reader.as_ref())
            .await?
            .skills
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn named_sources_with_wrong_file_kind_fail_explicitly() -> Result<()> {
    let instructions = Fixture::new().await?;
    instructions
        .put("AGENTS.md/child", b"wrong kind", "directory")
        .await?;
    let mut declaration = instructions.declaration();
    declaration.instructions.clear();
    declaration.instructions.push(InstructionScope {
        root: ContextRoot {
            volume: instructions.volume.clone(),
            directory: String::new(),
        },
        active_directory: String::new(),
    });
    declaration.policy.skill_name = None;
    assert!(matches!(
        declaration.capture(instructions.reader.as_ref()).await,
        Err(Error::Invalid(_))
    ));

    let skills = Fixture::new().await?;
    skills
        .put("skills/a/SKILL.md/child", b"wrong kind", "directory")
        .await?;
    let mut declaration = skills.declaration();
    declaration.instructions.clear();
    assert!(matches!(
        declaration.capture(skills.reader.as_ref()).await,
        Err(Error::Invalid(_))
    ));
    Ok(())
}

#[tokio::test]
async fn durable_request_replay_retains_discovery_revision() -> Result<()> {
    use acyclic_harness::{OperationId, executor::TurnInput};
    use acyclic_stream::{MemoryStream, StreamClient};
    let fixture = Fixture::new().await?;
    fixture
        .put("AGENTS.md", b"original instructions", "root")
        .await?;
    fixture
        .put("project/AGENTS.md", b"project instructions", "project")
        .await?;
    fixture
        .put(
            "skills/a/SKILL.md",
            b"---\nname: inspect\ndescription: Inspect files\n---\nSECRET BODY",
            "skill",
        )
        .await?;
    let declaration = fixture.declaration();
    let snapshot = declaration.capture(fixture.reader.as_ref()).await?;
    let (private, scope) = fixture.journal_volume().await?;
    let stream = StreamClient::new(Arc::new(MemoryStream::default()));
    let journal = fixture.journal(stream.clone(), private.clone(), scope.clone())?;
    let provider = Arc::new(CapturingModel::default());
    let original_executor = executor(snapshot.clone(), provider.clone())?;
    let input = TurnInput {
        operation_id: OperationId::from_bytes([7; 16]),
        input: ModelContent::Text("inspect".into()),
        selected_context: None,
        max_steps: 1,
    };
    let first = original_executor
        .model_step(&journal, &input, 0, &[])
        .await?;
    fixture
        .put("AGENTS.md", b"replacement instructions", "replace")
        .await?;
    fixture
        .put(
            "skills/a/SKILL.md",
            b"not a valid skill anymore",
            "replace-skill",
        )
        .await?;
    assert!(declaration.capture(fixture.reader.as_ref()).await.is_err());
    let restored = serde_json::from_slice(
        &serde_json::to_vec(&snapshot).map_err(|e| Error::Invalid(e.to_string()))?,
    )
    .map_err(|e| Error::Invalid(e.to_string()))?;
    let restarted = executor(restored, provider.clone())?;
    let recovered_journal = fixture.journal(stream, private, scope)?;
    assert_eq!(
        first,
        restarted
            .model_step(&recovered_journal, &input, 0, &[])
            .await?
    );
    assert_lazy_request(provider.as_ref())?;
    assert_eq!(
        snapshot
            .skills
            .first()
            .ok_or_else(|| Error::NotFound("skill".into()))?
            .source
            .read(fixture.reader.as_ref())
            .await?
            .1,
        b"---\nname: inspect\ndescription: Inspect files\n---\nSECRET BODY"
    );
    Ok(())
}

#[tokio::test]
async fn pinned_reads_fail_closed_for_missing_generation_and_outside_root() -> Result<()> {
    let fixture = Fixture::new().await?;
    fixture
        .put(
            "skills/a/SKILL.md",
            b"---\nname: inspect\ndescription: Inspect files\n---\nbody",
            "skill",
        )
        .await?;
    let mut declaration = fixture.declaration();
    declaration.instructions.clear();
    let captured = declaration.capture(fixture.reader.as_ref()).await?;
    let mut source = first_skill(&captured)?.source.clone();
    source.generation = acyclic_harness::resources::GenerationRef::new(
        fixture.volume.provider().clone(),
        [255; 32],
        Some("missing".into()),
    )?;
    assert!(source.read(fixture.reader.as_ref()).await.is_err());
    assert!(
        fixture
            .reader
            .read_private_prefix(
                &source.root.volume,
                &source.root.directory,
                &source.path,
                &source.generation,
                256
            )
            .await
            .is_err()
    );
    source = first_skill(&captured)?.source.clone();
    source.path = "AGENTS.md".into();
    assert!(matches!(
        source.read(fixture.reader.as_ref()).await,
        Err(Error::Invalid(_))
    ));
    source.path = "skills/missing/SKILL.md".into();
    assert!(source.read(fixture.reader.as_ref()).await.is_err());
    Ok(())
}
