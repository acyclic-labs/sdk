use super::*;

pub(super) struct RecursiveState {
    pub(super) host:
        Arc<FilesystemHost<acyclic_fs::MemoryAuthorityBackend, acyclic_fs::MemoryObjectBackend>>,
    pub(super) provider: ProviderRef,
    pub(super) stream: Arc<MemoryStream>,
    pub(super) stream_provider: ProviderRef,
    pub(super) shared: VolumeRef,
    pub(super) root_agent: AgentId,
    pub(super) private: VolumeRef,
    pub(super) project: VolumeRef,
    pub(super) project_head: acyclic_harness::filesystem::WorkspaceObservation,
    pub(super) parent_project_workspace: acyclic_harness::resources::WorkspaceRef,
    pub(super) authority: Authority,
    pub(super) issuer: AuthorityIssuer,
    pub(super) grant_scope: Scope,
    pub(super) aggregate: StreamAggregate<MemoryStream>,
    pub(super) previous_file: FileRef,
    pub(super) root_attachment: FileRef,
    pub(super) root_manifest: FileRef,
    pub(super) previous_attachment: FileRef,
    pub(super) previous_project_reference: Option<FileRef>,
    pub(super) previous_shared_reference: Option<FileRef>,
}

pub(super) async fn setup() -> Result<RecursiveState> {
    let filesystem = Fs::memory();
    let provider = ProviderRef::new(
        "qualification".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let host = FilesystemHost::new(filesystem, &provider, None)?;
    let stream = Arc::new(MemoryStream::default());
    let stream_provider = ProviderRef::new(
        "qualification".into(),
        "stream".into(),
        "2".into(),
        &mut None,
    )?;

    let shared = volume(
        &provider,
        VolumeClass::SessionShared,
        "shared".into(),
        VolumeOwner::Session("session".into()),
    )?;
    host.create_volume(&shared).await?;
    let root_agent = AgentId::from_bytes([0; 16]);
    let private = volume(
        &provider,
        VolumeClass::AgentPrivate,
        "scratch".into(),
        VolumeOwner::Agent(root_agent),
    )?;
    host.create_volume(&private).await?;
    let project = volume(
        &provider,
        VolumeClass::Project,
        "project-root".into(),
        VolumeOwner::Project("project".into()),
    )?;
    let mut project_head = host.create_volume(&project).await?;
    let parent_project_workspace = project_head.workspace.clone();

    let authority = Authority {
        kind: AggregateKind::Conversation,
        id: "recursive-root".into(),
    };
    let issuer = AuthorityIssuer::new("qualification", [17; 32], authority.clone());
    let grant_scope = scope(&issuer, &private, &shared, &project)?;
    let mut aggregate = StreamAggregate::open(
        &stream,
        (
            authority.clone(),
            issuer.verifier(&mut None).expect("verifier copy"),
            SchemaRegistry::new(),
        ),
        None,
        acyclic_harness::store::default_projection_read_limits(),
        acyclic_harness::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?
    .with_content_verifier(FilesystemContentVerifier::new(
        host.clone(),
        &issuer.verifier(&mut None).expect("verifier copy"),
        &grant_scope,
        1_024,
        None,
    )?)
    .with_fork_verifier(CompositeForkVerifier::new(
        &[
            FilesystemForkVerifier::new(host.clone(), 64 * 1_024, None, None)?,
            StreamHistoryForkVerifier::new(&stream_provider, &mut None)?,
        ],
        &mut None,
    )?);
    aggregate
        .execute(
            command(
                1,
                0,
                &grant_scope,
                Action::BindConversation { agent: root_agent },
            )?,
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    let root_write = ContentGrant::verify(
        &issuer.verifier(&mut None).expect("verifier copy"),
        &grant_scope,
        &private,
        VolumeOperation::Write,
        u64::MAX,
        &mut None,
    )?;
    let previous_file = host
        .put_content(
            &private,
            &root_write,
            "messages/current.txt",
            b"root",
            "text/plain",
            "current.txt",
            1_024,
            &IdempotencyKey::new("root-message")?,
            None,
            None,
            None,
        )
        .await?;
    let root_attachment = host
        .put_content(
            &private,
            &root_write,
            "attachments/evidence.bin",
            b"root evidence",
            "application/octet-stream",
            "evidence.bin",
            1_024,
            &IdempotencyKey::new("root-attachment")?,
            None,
            None,
            None,
        )
        .await?;
    let root_manifest_bytes = serde_json::to_vec(&vec![Attachment {
        file: root_attachment.clone(),
        label: Some("root evidence".into()),
    }])
    .map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))?;
    let root_manifest = host
        .put_content(
            &private,
            &root_write,
            "attachments/manifest.json",
            &root_manifest_bytes,
            "application/vnd.acyclic.harness.attachments+json",
            "manifest.json",
            1_024,
            &IdempotencyKey::new("root-manifest")?,
            None,
            None,
            None,
        )
        .await?;
    let previous_attachment = root_attachment.clone();
    aggregate
        .execute(
            command(
                2,
                1,
                &grant_scope,
                Action::AppendConversationMessage {
                    message: Box::new(ConversationMessage {
                        id: Uuid::from_bytes([1; 16]),
                        sequence: 1,
                        kind: MessageKind::User,
                        content: previous_file.clone(),
                        attachments: ReferencedAttachments::Manifest {
                            manifest: root_manifest.clone(),
                            item_count: 1,
                        },
                        reply_to: None,
                        tool_call_id: None,
                        extensions: BTreeMap::new(),
                    }),
                },
            )?,
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    let project_write = ContentGrant::verify(
        &issuer.verifier(&mut None).expect("verifier copy"),
        &grant_scope,
        &project,
        VolumeOperation::Write,
        u64::MAX,
        &mut None,
    )?;
    let project_reference = host
        .put_content(
            &project,
            &project_write,
            "references/project.txt",
            b"project reference",
            "text/plain",
            "project.txt",
            1_024,
            &IdempotencyKey::new("root-project-reference")?,
            None,
            None,
            None,
        )
        .await?;
    project_head = host.resolve(&project_head.workspace, None).await?;
    let shared_writer = issuer
        .issue(
            "shared-writer",
            &Capabilities::new([shared.capability(VolumeOperation::Write, &mut None)?]),
            None,
            None,
            &mut None,
        )
        .expect("scope proof");
    let shared_write = ContentGrant::verify(
        &issuer.verifier(&mut None).expect("verifier copy"),
        &shared_writer,
        &shared,
        VolumeOperation::Write,
        u64::MAX,
        &mut None,
    )?;
    let shared_reference = host
        .put_content(
            &shared,
            &shared_write,
            "references/shared.txt",
            b"shared reference",
            "text/plain",
            "shared.txt",
            1_024,
            &IdempotencyKey::new("root-shared-reference")?,
            None,
            None,
            None,
        )
        .await?;
    aggregate
        .execute(
            command(
                3,
                2,
                &grant_scope,
                Action::AppendConversationMessage {
                    message: Box::new(ConversationMessage {
                        id: Uuid::from_bytes([0x11; 16]),
                        sequence: 2,
                        kind: MessageKind::User,
                        content: project_reference.clone(),
                        attachments: ReferencedAttachments::Inline {
                            items: vec![Attachment {
                                file: shared_reference.clone(),
                                label: None,
                            }],
                        },
                        reply_to: None,
                        tool_call_id: None,
                        extensions: BTreeMap::new(),
                    }),
                },
            )?,
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    let previous_project_reference = Some(project_reference);
    let previous_shared_reference = Some(shared_reference);

    Ok(RecursiveState {
        host,
        provider,
        stream,
        stream_provider,
        shared,
        root_agent,
        private,
        project,
        project_head,
        parent_project_workspace,
        authority,
        issuer,
        grant_scope,
        aggregate,
        previous_file,
        root_attachment,
        root_manifest,
        previous_attachment,
        previous_project_reference,
        previous_shared_reference,
    })
}
