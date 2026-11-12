use super::*;
use crate::{
    conversation::{ContentGrant, VolumeRef},
    core::AuthorityRoot,
    resources::ProviderRef,
};

#[tokio::test]
async fn announcement_publication_preserves_workload_and_delegated_authority() -> Result<()> {
    struct NoDispatch;
    impl TurnExecutors for NoDispatch {
        fn executor<'a>(
            &'a self,
            _: &'a AgentState,
            _: OperationId,
            _: &'a ModelContent,
            _: &'a TurnAuthority,
        ) -> BoxFuture<'a, Result<Arc<dyn Executor>>> {
            panic!("announcement publication must not dispatch an executor")
        }
    }
    for delegated in [false, true] {
        let path = AgentPath::root(&crate::agent::AgentId::from_bytes([180; 16]));
        let provider = ProviderRef::new(
            "host-documents".into(),
            "filesystem".into(),
            "2".into(),
            &mut None,
        )?;
        let private = private_volume(provider.clone(), &path.id())?;
        let journal = crate::conversation::journal_volume(provider.clone(), &path.id())?;
        let files = FilesystemHost::<
            acyclic_fs::MemoryAuthorityBackend,
            acyclic_fs::MemoryObjectBackend,
        >::new(acyclic_fs::Fs::memory(), &provider, None)?;
        files.create_volume(&private).await?;
        files.create_volume(&journal).await?;
        let audience = Authority {
            kind: AggregateKind::Conversation,
            id: path.id().to_string(),
        };
        let root = AuthorityRoot::new("host-documents", [181; 32], audience.clone());
        let grants = if delegated {
            Capabilities::new([acyclic_fs::access::Capability::whole_volume(
                None,
                &acyclic_fs::access::Action::ALL,
            )
            .to_scope_label()
            .map_err(|error| Error::Invalid(error.to_string()))?])
        } else {
            Capabilities::new([
                private.capability(VolumeOperation::Read, &mut None)?,
                private.capability(VolumeOperation::Write, &mut None)?,
            ])
        };
        let scope = root.bootstrap_scope(
            "workload",
            &grants,
            Some(crate::AgentId::from_bytes(path.id_bytes())),
            &mut None,
        )?;
        let (_producer, stopped) = futures::channel::oneshot::channel::<()>();
        let runtime = if delegated {
            let context = acyclic_auth::operation::Context::from_stopped(
                root.verifier(&mut None)?
                    .expiry(&scope)?
                    .saturating_mul(1000),
                async move {
                    let _ = stopped.await;
                    acyclic_auth::operation::Stopped::Cancelled
                },
            );
            RuntimeAuthority::admitted(
                root.admit(scope.clone(), context)?,
                OperationId::from_bytes([182; 16]),
            )
        } else {
            root.clone().into()
        };
        let state = AgentState::adopted(
            path.clone(),
            &crate::agent::ForkAdopted {
                agent: path.id(),
                parent: None,
                profile: None,
                forked_at: 0,
                conversation_tail: 0,
                checkpoint: None,
                volumes: vec![WorkspaceEntry::initial_volume(
                    crate::agent::WorkspaceRole::Private,
                    PRIVATE_VOLUME.into(),
                    private,
                )],
                brief: None,
                policy: None,
            },
        )
        .map_err(Error::from)?;
        let host = FilesystemTurnHost::new(
            Arc::new(acyclic_stream::MemoryStream::default()),
            acyclic_actors::addressing::ActorReference::new(b"host-documents", b"agents", b"owner")
                .map_err(|error| Error::Invalid(error.to_string()))?,
            files,
            runtime,
            TurnAdmission::Admitted {
                audience,
                scope: scope.clone(),
            },
            Arc::new(NoDispatch),
            TurnLimits {
                maximum_bytes: 64 * 1024,
                history_turns: 8,
            },
            Default::default(),
        );
        let admitted = host.admit(&state)?;
        let documents = host.journal_storage(&admitted, &[]).await?.files;
        assert!(
            documents
                .create("targets.json", br#"{"targets":[]}"#, 1024)
                .await?
                .is_none()
        );
        let document = documents
            .read("targets.json", 1024)
            .await?
            .ok_or_else(|| Error::NotFound("host announcement".into()))?;
        assert_eq!(document.as_ref(), br#"{"targets":[]}"#);
        let authority =
            agent_authority(&host.issuer, &host.admission, &path, &state.volumes, &[]).await?;
        assert!(authority.grants.is_subset_of(scope.capabilities()));
        if !delegated {
            for operation in [VolumeOperation::Read, VolumeOperation::Write] {
                assert!(
                    ContentGrant::verify(
                        &authority.verifier,
                        &authority.scope,
                        &journal,
                        operation,
                        &mut None
                    )
                    .is_err()
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn local_workspace_reads_and_writes_respect_role_mode_and_root() -> Result<()> {
    use crate::agent::{
        EntryId, WorkspaceInherit, WorkspaceKind, WorkspaceMode, WorkspaceRole, WorkspaceSource,
    };
    let path = AgentPath::root(&crate::agent::AgentId::from_bytes([198; 16]));
    let provider = ProviderRef::new("bounded".into(), "filesystem".into(), "1".into(), &mut None)?;
    let host = FilesystemHost::new(acyclic_fs::Fs::memory(), &provider, None)?;
    let issuer = AuthorityRoot::new(
        "bounded",
        [199; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "bounded".into(),
        },
    );
    let private = private_volume(provider.clone(), &path.id())?;
    host.create_volume(&private).await?;
    let project = VolumeRef::new(
        provider.clone(),
        "content".into(),
        VolumeClass::Project,
        VolumeOwner::Project("owner".into()),
        &mut None,
    )?;
    host.create_volume(&project).await?;
    let seed = issuer.bootstrap_scope(
        "seed",
        &Capabilities::new([project.capability(VolumeOperation::Write, &mut None)?]),
        None,
        &mut None,
    )?;
    let grant = ContentGrant::verify(
        &issuer.verifier(&mut None)?,
        &seed,
        &project,
        VolumeOperation::Write,
        &mut None,
    )?;
    let mut files = Vec::new();
    for name in ["inside/file", "outside/file"] {
        files.push(
            host.put_content(
                &project,
                &grant,
                name,
                name.as_bytes(),
                "text/plain",
                "file",
                1024,
                &crate::IdempotencyKey::new(name)?,
                None,
                Default::default(),
            )
            .await?,
        );
    }
    let mut user =
        WorkspaceEntry::initial_volume(WorkspaceRole::User, "repo".into(), project.clone());
    let mut hidden = WorkspaceEntry::initial_volume(
        WorkspaceRole::Internal,
        "approval-state".into(),
        VolumeRef::new(
            provider,
            "approvals".into(),
            VolumeClass::AgentPrivate,
            private.owner().clone(),
            &mut None,
        )?,
    );
    hidden.id = EntryId::from_bytes([200; 16]);
    let mut tree = user.clone();
    tree.id = EntryId::from_bytes([201; 16]);
    tree.name = "tree".into();
    tree.role = WorkspaceRole::Tree;
    tree.kind = WorkspaceKind::Tree;
    tree.source = WorkspaceSource::Subtree(path.clone());
    tree.mode = WorkspaceMode::ReadOnly;
    tree.inherit = WorkspaceInherit::Own;
    for rooted in [false, true] {
        user.mode = if rooted {
            WorkspaceMode::ReadWrite
        } else {
            WorkspaceMode::ReadOnly
        };
        user.root = if rooted {
            "inside".into()
        } else {
            String::new()
        };
        let volumes = vec![
            tree.clone(),
            user.clone(),
            hidden.clone(),
            WorkspaceEntry::initial_volume(
                WorkspaceRole::Private,
                "interface".into(),
                private.clone(),
            ),
        ];
        let authority = agent_authority(
            &issuer.clone().into(),
            &TurnAdmission::LocalRoot,
            &path,
            &volumes,
            &[],
        )
        .await?;
        assert!(
            ContentGrant::verify(
                &authority.verifier,
                &authority.scope,
                &project,
                VolumeOperation::Write,
                &mut None
            )
            .is_err()
        );
        assert!(
            ContentGrant::verify(
                &authority.verifier,
                &authority.scope,
                hidden.volume()?,
                VolumeOperation::Read,
                &mut None
            )
            .is_err()
        );
        let reader = FilesystemContentVerifier::new(
            host.clone(),
            &authority.verifier,
            &authority.scope,
            1024,
            None,
        )?;
        assert_eq!(
            reader.read(&files[0], 1024, None, None)?.await?.as_ref(),
            b"inside/file"
        );
        let outside = async { reader.read(&files[1], 1024, None, None)?.await };
        if rooted {
            assert!(outside.await.is_err(), "a root must not grant its sibling");
            // Retained conversation references remain exact even when a
            // still-bound volume's new root no longer contains that input.
            let resumed = agent_authority(
                &issuer.clone().into(),
                &TurnAdmission::LocalRoot,
                &path,
                &volumes,
                &files[1..],
            )
            .await?;
            let reader = FilesystemContentVerifier::new(
                host.clone(),
                &resumed.verifier,
                &resumed.scope,
                1024,
                None,
            )?;
            assert_eq!(
                reader.read(&files[1], 1024, None, None)?.await?.as_ref(),
                b"outside/file"
            );
        } else {
            assert_eq!(outside.await?.as_ref(), b"outside/file");
            // Internal bindings grant no ambient read. A retained input still
            // grants its exact version, without exposing the sibling file.
            let mut internal = volumes.clone();
            internal[1].role = WorkspaceRole::Internal;
            let retained = agent_authority(
                &issuer.clone().into(),
                &TurnAdmission::LocalRoot,
                &path,
                &internal,
                &files[..1],
            )
            .await?;
            let reader = FilesystemContentVerifier::new(
                host.clone(),
                &retained.verifier,
                &retained.scope,
                1024,
                None,
            )?;
            assert_eq!(
                reader.read(&files[0], 1024, None, None)?.await?.as_ref(),
                b"inside/file"
            );
            assert!(
                async { reader.read(&files[1], 1024, None, None)?.await }
                    .await
                    .is_err()
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn wait_selection_survives_reopening_and_reference_replacement() -> Result<()> {
    use crate::agent::{AgentId, TurnChildren};
    let mut agent = AgentPath::root(&AgentId::from_bytes([201; 16]));
    for _ in 1..128 {
        agent = agent
            .child(&AgentId::from_bytes([201; 16]))
            .map_err(Error::from)?;
    }
    let provider = ProviderRef::new("waits".into(), "filesystem".into(), "2".into(), &mut None)?;
    let volume = private_volume(provider.clone(), &agent.id())?;
    let host =
        FilesystemHost::<acyclic_fs::MemoryAuthorityBackend, acyclic_fs::MemoryObjectBackend>::new(
            acyclic_fs::Fs::memory(),
            &provider,
            None,
        )?;
    host.create_volume(&volume).await?;
    let volumes = vec![WorkspaceEntry::initial_volume(
        crate::agent::WorkspaceRole::Private,
        "ui".into(),
        volume.clone(),
    )];
    let issuer: RuntimeAuthority = AuthorityRoot::new(
        "waits",
        [202; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "waits".into(),
        },
    )
    .into();
    let authority =
        agent_authority(&issuer, &TurnAdmission::LocalRoot, &agent, &volumes, &[]).await?;
    let grant = crate::conversation::ContentGrant::verify(
        &authority.verifier,
        &authority.scope,
        &volume,
        VolumeOperation::Write,
        &mut None,
    )?;
    let files = Arc::new(AgentInternalFiles::new(host.clone(), volume, grant));
    let reopen = || FilesystemTurnChildren {
        files: files.clone(),
        stream: Arc::new(acyclic_stream::MemoryStream::default()),
        within: acyclic_actors::addressing::ActorReference::new(b"a", b"agents", b"owner")
            .expect("namespace"),
        host: host.clone(),
        issuer: issuer.clone(),
        admission: TurnAdmission::LocalRoot,
        agent: agent.clone(),
        volumes: volumes.clone(),
        maximum_bytes: 64 * 1024,
    };
    let first = agent
        .child(&AgentId::from_bytes([203; 16]))
        .map_err(Error::from)?;
    let replacement = agent
        .child(&AgentId::from_bytes([204; 16]))
        .map_err(Error::from)?;
    let operation = OperationId::from_bytes([205; 16]);
    // Repeating full paths for 32 deepest children exceeds 64 KiB even once.
    let mut references = vec![first.as_str().to_owned()];
    for byte in 0..31 {
        references.push(
            agent
                .child(&AgentId::from_bytes([byte; 16]))
                .map_err(Error::from)?
                .as_str()
                .to_owned(),
        );
    }
    let selected = reopen().select(operation, &references).await?;
    assert_eq!(selected.len(), references.len());
    assert_eq!(selected[0].1, first);
    assert_eq!(
        reopen()
            .select(operation, &[replacement.as_str().into()])
            .await?,
        selected
    );
    let fresh = reopen()
        .select(
            OperationId::from_bytes([206; 16]),
            &[replacement.as_str().into()],
        )
        .await?;
    assert_eq!(fresh[0].1, replacement);
    Ok(())
}

/// Fault injection at the original finite-read producer; all other operations
/// retain the real in-memory provider.
struct OpeningRead {
    inner: acyclic_stream::MemoryStream,
    pending: std::sync::Mutex<Option<futures::channel::oneshot::Receiver<()>>>,
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl StreamProvider for OpeningRead {
    fn context(&self) -> acyclic_stream::ProviderContext<'_> {
        self.inner.context()
    }

    async fn heads(
        &self,
        request: acyclic_stream::HeadsRequest,
    ) -> std::result::Result<acyclic_stream::Heads, acyclic_stream::StreamError> {
        self.inner.heads(request).await
    }

    async fn inspect_idempotency(
        &self,
        key: acyclic_stream::IdempotencyKey,
    ) -> std::result::Result<
        Option<acyclic_stream::IdempotencyObservation>,
        acyclic_stream::StreamError,
    > {
        self.inner.inspect_idempotency(key).await
    }

    fn write(
        &self,
        request: acyclic_stream::WriteRequest,
    ) -> BoxFuture<'_, std::result::Result<acyclic_stream::WriteOutcome, acyclic_stream::StreamError>>
    {
        self.inner.write(request)
    }

    async fn read(
        &self,
        request: ReadRequest,
    ) -> std::result::Result<acyclic_stream::RecordStream, acyclic_stream::StreamError> {
        let pending = self.pending.lock().expect("read gate").take();
        if let Some(pending) = pending {
            pending
                .await
                .map_err(|_| acyclic_stream::StreamError::Unavailable)?;
        }
        self.inner.read(request).await
    }

    async fn follow(
        &self,
        request: acyclic_stream::FollowRequest,
    ) -> std::result::Result<acyclic_stream::FollowStream, acyclic_stream::StreamError> {
        self.inner.follow(request).await
    }

    async fn read_commit(
        &self,
        id: acyclic_stream::CommitId,
    ) -> std::result::Result<acyclic_stream::CommittedEnvelope, acyclic_stream::StreamError> {
        self.inner.read_commit(id).await
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl acyclic_stream::NativeStreamProvider for OpeningRead {}

#[tokio::test]
async fn opening_history_is_cancelled_and_source_loss_is_terminal() -> Result<()> {
    use futures::FutureExt as _;

    struct NoDispatch;
    impl TurnExecutors for NoDispatch {
        fn executor<'a>(
            &'a self,
            _: &'a AgentState,
            _: OperationId,
            _: &'a ModelContent,
            _: &'a TurnAuthority,
        ) -> BoxFuture<'a, Result<Arc<dyn Executor>>> {
            panic!("an unfinished history read must not construct an executor")
        }
    }

    let path = AgentPath::root(&crate::agent::AgentId::from_bytes([196; 16]));
    let provider = ProviderRef::new("opening".into(), "filesystem".into(), "2".into(), &mut None)?;
    let volume = private_volume(provider.clone(), &path.id())?;
    let filesystem = FilesystemHost::<
        acyclic_fs::MemoryAuthorityBackend,
        acyclic_fs::MemoryObjectBackend,
    >::new(acyclic_fs::Fs::memory(), &provider, None)?;
    filesystem.create_volume(&volume).await?;
    let mut state = AgentState::adopted(
        path.clone(),
        &crate::agent::ForkAdopted {
            profile: None,
            agent: path.id(),
            parent: None,
            forked_at: 0,
            conversation_tail: 0,
            checkpoint: None,
            volumes: vec![WorkspaceEntry::initial_volume(
                crate::agent::WorkspaceRole::Private,
                PRIVATE_VOLUME.into(),
                volume,
            )],
            brief: None,
            policy: None,
        },
    )
    .map_err(Error::from)?;
    state.conversation_tail = 1;
    let issuer = AuthorityRoot::new(
        "opening",
        [197; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "opening".into(),
        },
    );
    let input = ModelContent::Text("next turn".into());
    for cancel in [true, false] {
        let (producer, pending) = futures::channel::oneshot::channel();
        let stream = Arc::new(OpeningRead {
            inner: Default::default(),
            pending: std::sync::Mutex::new(Some(pending)),
        });
        let host = FilesystemTurnHost::new(
            stream.clone(),
            acyclic_actors::addressing::ActorReference::new(b"account", b"agents", b"owner")
                .expect("actor binding"),
            filesystem.clone(),
            issuer.clone(),
            TurnAdmission::LocalRoot,
            Arc::new(NoDispatch),
            TurnLimits {
                maximum_bytes: 64 * 1024,
                history_turns: 8,
            },
            Default::default(),
        );
        let stop = acyclic_actors::guest::CancellationToken::new();
        let opening = host.open(
            &state,
            OperationId::from_bytes([198; 16]),
            0,
            1,
            &input,
            stop.clone(),
        );
        let mut opening = core::pin::pin!(opening);
        assert!(opening.as_mut().now_or_never().is_none());
        assert!(
            stream.pending.lock().expect("read gate").is_none(),
            "opening reached its original history read"
        );
        if cancel {
            stop.cancel();
            assert!(matches!(
                opening.as_mut().now_or_never(),
                Some(Err(Error::InteractionRejected(
                    crate::InteractionRejection::Cancelled
                )))
            ));
            assert!(producer.is_canceled(), "stop drops the original read");
        } else {
            drop(producer);
            assert!(matches!(
                opening.as_mut().now_or_never(),
                Some(Err(Error::Storage(_)))
            ));
        }
    }
    Ok(())
}

#[tokio::test]
async fn frozen_notice_ranges_page_without_gaining_later_records() -> Result<()> {
    use crate::agent::{AgentId, InboxRecord, IntentId, NoticePayload, NoticeSelection};
    use crate::conversation::wire::Notice;
    use crate::model::ModelContentPart;
    use acyclic_stream::{
        AppendRequest, IdempotencyKey, MemoryStream, StreamError, StreamProviderExt as _,
    };

    let stream = MemoryStream::default();
    let agent = AgentPath::root(&AgentId::from_bytes([194; 16]));
    let count = acyclic_stream::MAX_ITEMS + 1;
    let mut records = Vec::new();
    for index in 0..=count {
        if index == acyclic_stream::MAX_ITEMS / 2 {
            records.push(InboxRecord::Awaited.encode().map_err(Error::from)?);
        }
        records.push(
            InboxRecord::Notice {
                id: IntentId::try_from(format!("event-{index}")).map_err(Error::from)?,
                notice: NoticePayload::Quiet(Notice {
                    source: "events".into(),
                    kind: "source.changed".into(),
                    body: serde_json::json!({"index": index}),
                    wake: false,
                    coalesce_key: Some("source".into()),
                }),
            }
            .encode()
            .map_err(Error::from)?,
        );
    }
    let mut from = 0;
    for (page, records) in records.chunks(acyclic_stream::MAX_ITEMS).enumerate() {
        stream
            .append(AppendRequest {
                path: agent.inbox().map_err(Error::from)?,
                records: records.iter().cloned().map(Into::into).collect(),
                if_tail: Some(from),
                idempotency_key: IdempotencyKey::new(format!("notices-{page}").into_bytes())?,
            })
            .await?;
        from += records.len() as u64;
    }
    let selected = NoticeSelection {
        source: agent.clone(),
        range: 0..count as u64 + 1,
    };
    let input = ModelContent::Text("prompt".into());
    let (notices, retained) =
        selected_notices(&stream, &agent, Some(&selected), input.clone(), None).await?;
    assert_eq!(retained, Some(input));
    let [ModelContent::Part(ModelContentPart::Notices { notices })] = notices.as_slice() else {
        panic!("missing event boundary");
    };
    assert_eq!(notices.len(), 1);
    assert_eq!(notices[0].body["index"], count - 1);
    let unavailable = NoticeSelection {
        range: 0..from + 1,
        ..selected
    };
    assert!(
        selected_notices(
            &stream,
            &agent,
            Some(&unavailable),
            ModelContent::Text("prompt".into()),
            None,
        )
        .await
        .expect_err("a missing frozen suffix must refuse")
        .uncertain()
    );
    for cause in [StreamError::Unavailable, StreamError::DeadlineElapsed] {
        let error = Error::from(crate::agent::Error::Stream(cause));
        assert!(error.uncertain());
        assert!(!Error::Invalid(error.to_string()).uncertain());
    }
    Ok(())
}

#[test]
fn pure_agent_admission_has_no_mint_and_intersects_the_actual_private_owner() -> Result<()> {
    let path = AgentPath::root(&crate::agent::AgentId::from_bytes([191; 16]));
    let owner = crate::AgentId::from_bytes(path.id().to_bytes());
    let audience = Authority {
        kind: AggregateKind::Task,
        id: "admission-cost".into(),
    };
    let root = AuthorityRoot::new("admission-cost", [192; 32], audience.clone());
    let issuer = RuntimeAuthority::from(root.clone());
    let volume = VolumeRef::new(
        ProviderRef::new(
            "admission-cost".into(),
            "filesystem".into(),
            "2".into(),
            &mut None,
        )?,
        "private".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(owner),
        &mut None,
    )?;
    let volumes = [WorkspaceEntry::initial_volume(
        crate::agent::WorkspaceRole::Private,
        PRIVATE_VOLUME.into(),
        volume,
    )];
    let ceiling =
        acyclic_fs::access::Capability::whole_volume(None, &acyclic_fs::access::Action::ALL)
            .to_scope_label()
            .map_err(|_| Error::Invalid("fixture filesystem ceiling".into()))?;
    let scope = root.bootstrap_scope("caller", &Capabilities::new([ceiling]), None, &mut None)?;
    let admission = TurnAdmission::Admitted {
        audience: audience.clone(),
        scope: scope.clone(),
    };
    AdmittedAgent::verify(&issuer, &admission, &path, &volumes)?;
    for callers in [1, 100, 1000] {
        let (result, signatures) = acyclic_test_support::differential::observe(
            || (),
            |()| -> Result<()> {
                for _ in 0..callers {
                    AdmittedAgent::verify(&issuer, &admission, &path, &volumes)?;
                }
                Ok(())
            },
        );
        result?;
        let signatures: Vec<acyclic_auth::crypto::SignatureVerification> = signatures;
        assert!(
            signatures.is_empty(),
            "warm admission minted or reauthenticated a credential"
        );
    }
    // The former mint-and-discard path does real cryptographic work; removing
    // it is observed against the same issuer, grant and private binding.
    let (result, signatures) = acyclic_test_support::differential::observe(
        || (),
        |()| {
            futures::executor::block_on(async {
                AdmittedAgent::verify(&issuer, &admission, &path, &volumes)?
                    .authority(&[])
                    .await
                    .map(|_| ())
            })
        },
    );
    result?;
    let signatures: Vec<acyclic_auth::crypto::SignatureVerification> = signatures;
    assert!(!signatures.is_empty());
    let foreign = [WorkspaceEntry::initial_volume(
        crate::agent::WorkspaceRole::Private,
        PRIVATE_VOLUME.into(),
        VolumeRef::new(
            volumes[0].volume()?.provider().clone(),
            "private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(crate::AgentId::from_bytes([193; 16])),
            &mut None,
        )?,
    )];
    assert!(AdmittedAgent::verify(&issuer, &admission, &path, &foreign).is_err());

    // A valid private binding cannot authorize another agent's fresh volume.
    // Use the same owner predicate as signed volume writes, before any IO.
    let local = AdmittedAgent::verify(&issuer, &TurnAdmission::LocalRoot, &path, &volumes)?;
    let (result, signatures) = acyclic_test_support::differential::observe(
        || (),
        |()| -> Result<()> {
            local.authorize_volume(volumes[0].volume()?)?;
            let error = local
                .authorize_volume(foreign[0].volume()?)
                .expect_err("local root accepted a foreign fresh volume");
            assert!(
                error
                    .to_string()
                    .contains("only the owning agent may write")
            );
            Ok(())
        },
    );
    result?;
    let signatures: Vec<acyclic_auth::crypto::SignatureVerification> = signatures;
    assert!(
        signatures.is_empty(),
        "local ownership admission minted a scope"
    );
    let read_only = root.bootstrap_scope(
        "read-only",
        &Capabilities::new([volumes[0]
            .volume()?
            .capability(VolumeOperation::Read, &mut None)?]),
        Some(owner),
        &mut None,
    )?;
    assert!(
        AdmittedAgent::verify(
            &issuer,
            &TurnAdmission::Admitted {
                audience,
                scope: read_only
            },
            &path,
            &volumes
        )
        .is_err()
    );
    root.revoke_workload(&scope)?;
    assert!(AdmittedAgent::verify(&issuer, &admission, &path, &volumes).is_err());
    Ok(())
}
