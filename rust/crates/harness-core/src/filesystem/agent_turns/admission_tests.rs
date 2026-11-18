use super::*;
use crate::test_support::replace_policy;
use crate::{
    conversation::{ContentGrant, VolumeRef},
    core::AuthorityRoot,
    filesystem::workspace_ref,
    resources::ProviderRef,
};

#[path = "prefix_tests.rs"]
mod prefix_tests;

#[tokio::test]
async fn child_activity_verifies_under_the_admission_that_signed_it() -> Result<()> {
    use crate::executor::ExecutionJournal as _;
    let parent = AgentPath::root(&crate::agent::AgentId::from_bytes([195; 16]));
    let child = parent
        .child(&crate::agent::AgentId::from_bytes([196; 16]))
        .map_err(Error::from)?;
    let provider = ProviderRef::new(
        "activity-admission".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let parent_volume = private_volume(provider.clone(), &parent.id())?;
    let child_volume = private_volume(provider.clone(), &child.id())?;
    let journal_volume = crate::conversation::journal_volume(provider.clone(), &child.id())?;
    let host =
        FilesystemHost::<acyclic_fs::MemoryAuthorityBackend, acyclic_fs::MemoryObjectBackend>::new(
            acyclic_fs::Fs::memory(),
            &provider,
            None,
        )?;
    for volume in [&parent_volume, &child_volume, &journal_volume] {
        host.create_volume(volume).await?;
    }
    // A served tree: every agent's turns run under one admission.
    let audience = Authority {
        kind: AggregateKind::Conversation,
        id: parent.id().to_string(),
    };
    let root = AuthorityRoot::new("activity-admission", [197; 32], audience.clone());
    let broad = Capabilities::new([
        acyclic_fs::access::Capability::whole_volume(None, &acyclic_fs::access::Action::ALL)
            .to_scope_label()
            .map_err(|error| Error::Invalid(error.to_string()))?,
        crate::contract::capability::HARNESS_HOST.to_owned(),
    ]);
    let broad = root.bootstrap_scope("served", &broad, None, &mut None)?;
    let workload_grants = Capabilities::new([
        parent_volume.capability(VolumeOperation::Read, &mut None)?,
        parent_volume.capability(VolumeOperation::Write, &mut None)?,
        crate::contract::capability::HARNESS_HOST.to_owned(),
    ]);
    let scope = root.bootstrap_scope(
        "parent-workload",
        &workload_grants,
        Some(crate::AgentId::from_bytes(parent.id_bytes())),
        &mut None,
    )?;
    let admission = TurnAdmission::Admitted {
        audience: audience.clone(),
        scope: broad.clone(),
    };
    let issuer = RuntimeAuthority::from(root.clone());
    admission.verify(&issuer, &parent)?;
    let state = AgentState::adopted(
        child.clone(),
        &crate::agent::ForkAdopted {
            agent: child.id(),
            parent: Some(parent),
            profile: None,
            forked_at: 0,
            conversation_tail: 0,
            checkpoint: None,
            volumes: vec![WorkspaceEntry::initial_volume(
                crate::agent::WorkspaceRole::Private,
                PRIVATE_VOLUME.into(),
                child_volume,
            )],
            brief: None,
            policy: None,
        },
    )
    .map_err(Error::from)?;
    let stream = Arc::new(acyclic_stream::MemoryStream::default());
    let operation = OperationId::new();
    // The child's own turn signs its checkpoint under the served admission.
    query_journal(&stream, &host, &issuer, &admission, &state)
        .await?
        .with_turn(operation, operation)
        .append(
            operation,
            "start",
            &crate::executor::ExecutionEvent::Started {
                request_digest: [1; 32],
            },
            None,
        )?
        .await?;
    // The parent's wait verifies that checkpoint under the same identity.
    let journal = query_activity_journal(&stream, &host, &issuer, &admission, &state)
        .await?
        .ok_or_else(|| Error::Storage("native host lost child activity authority".into()))?
        .with_turn(operation, operation);
    assert!(
        journal
            .activity(operation, &acyclic_actors::local::SystemClock)
            .await?
            .is_some()
    );

    // A child spawned under the host's local root signs under its own
    // conversation. A later admission of the same parent must still verify
    // that checkpoint, not the admission's audience.
    let local_operation = OperationId::new();
    query_journal(&stream, &host, &issuer, &TurnAdmission::LocalRoot, &state)
        .await?
        .with_turn(local_operation, local_operation)
        .append(
            local_operation,
            "start",
            &crate::executor::ExecutionEvent::Started {
                request_digest: [2; 32],
            },
            None,
        )?
        .await?;
    let observed = query_activity_journal(&stream, &host, &issuer, &admission, &state)
        .await?
        .ok_or_else(|| Error::Storage("native host lost child activity authority".into()))?
        .with_turn(local_operation, local_operation);
    assert!(
        observed
            .activity(local_operation, &acyclic_actors::local::SystemClock)
            .await?
            .is_some(),
        "a later admission rejected the child's original checkpoint"
    );

    for (scope, expected) in [(scope, false), (broad, true)] {
        let (_producer, stopped) = futures::channel::oneshot::channel::<()>();
        let context = acyclic_auth::operation::Context::from_stopped(
            root.verifier(&mut None)?
                .expiry(&scope)?
                .saturating_mul(1000),
            async move {
                let _ = stopped.await;
                acyclic_auth::operation::Stopped::Cancelled
            },
        );
        let issuer = RuntimeAuthority::admitted(root.admit(scope, context)?, OperationId::new());
        let observed = query_activity_journal(&stream, &host, &issuer, &admission, &state).await?;
        assert_eq!(observed.is_some(), expected, "delegated ceiling changed");
        if let Some(journal) = observed {
            assert!(
                journal
                    .with_turn(operation, operation)
                    .activity(operation, &acyclic_actors::local::SystemClock)
                    .await?
                    .is_some()
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn native_exact_grants_repair_a_retained_foreign_journal_as_its_owner() -> Result<()> {
    let path = AgentPath::root(&crate::agent::AgentId::from_bytes([198; 16]));
    let other = path
        .child(&crate::agent::AgentId::from_bytes([199; 16]))
        .map_err(Error::from)?;
    let provider = ProviderRef::new(
        "journal-repair".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let private = private_volume(provider.clone(), &path.id())?;
    let journal = crate::conversation::journal_volume(provider.clone(), &path.id())?;
    let foreign = crate::conversation::journal_volume(provider.clone(), &other.id())?;
    let host =
        FilesystemHost::<acyclic_fs::MemoryAuthorityBackend, acyclic_fs::MemoryObjectBackend>::new(
            acyclic_fs::Fs::memory(),
            &provider,
            None,
        )?;
    for volume in [&private, &journal, &foreign] {
        host.create_volume(volume).await?;
    }
    // The retained journal's saved policy is stale, so admission repairs it.
    let cancellation = acyclic_fs::CancellationToken::new();
    let workspace = host
        .filesystem
        .open_workspace(
            foreign.storage_name(&mut None)?,
            acyclic_fs_work::performance::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(|failure| Error::from(failure.error))?
        .value;
    let mut stale: acyclic_fs::access::Policy =
        serde_json::from_slice(include_bytes!("../host/journal-policy.json"))
            .map_err(|error| Error::Invalid(error.to_string()))?;
    stale
        .layers
        .first_mut()
        .ok_or_else(|| Error::Invalid("journal policy has no layers".into()))?
        .retain(|rule| rule.id != "published-input");
    replace_policy(&workspace, &stale).await?;
    let audience = Authority {
        kind: AggregateKind::Conversation,
        id: path.id().to_string(),
    };
    let root = AuthorityRoot::new("journal-repair", [200; 32], audience.clone());
    // Exact grants carry no filesystem label: an agent-owned volume then
    // requires its owner as the acting writer.
    let grants = Capabilities::new([
        private.capability(VolumeOperation::Read, &mut None)?,
        private.capability(VolumeOperation::Write, &mut None)?,
        foreign.capability(VolumeOperation::Read, &mut None)?,
        foreign.capability(VolumeOperation::Write, &mut None)?,
        crate::contract::capability::HARNESS_HOST.to_owned(),
    ]);
    let admission = TurnAdmission::Admitted {
        audience,
        scope: root.bootstrap_scope("exact-host", &grants, None, &mut None)?,
    };
    let volumes = vec![WorkspaceEntry::initial_volume(
        crate::agent::WorkspaceRole::Private,
        PRIVATE_VOLUME.into(),
        private,
    )];
    let retained = FileRef::new(
        foreign.clone(),
        "inputs/retained".into(),
        "1".into(),
        crate::conversation::FileDescriptor::new([2; 32], 1, "text/plain".into())?,
        "retained".into(),
        &mut None,
    )?;
    let issuer = RuntimeAuthority::from(root);
    let before = host
        .resolve(
            &workspace_ref(provider.clone(), &foreign.storage_name(&mut None)?)?,
            None,
        )
        .await?
        .generation;
    AdmittedAgent::verify(&issuer, &admission, &path, &volumes)?
        .journal_authority(&host, std::slice::from_ref(&retained), &[])
        .await?;
    let repaired = host
        .resolve(
            &workspace_ref(provider, &foreign.storage_name(&mut None)?)?,
            None,
        )
        .await?
        .generation;
    assert_ne!(
        repaired, before,
        "the retained journal policy was not repaired"
    );
    Ok(())
}

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
            Capabilities::new([
                crate::contract::capability::HARNESS_HOST.to_owned(),
                acyclic_fs::access::Capability::whole_volume(
                    None,
                    &acyclic_fs::access::Action::ALL,
                )
                .to_scope_label()
                .map_err(|error| Error::Invalid(error.to_string()))?,
            ])
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
                maximum_bytes: Some(64 * 1024),
                history_turns: 8,
            },
            Default::default(),
        );
        let admitted = host.admit(&state)?;
        let documents = host.journal_storage(&admitted, &[], &[]).await?.files;
        assert!(
            documents
                .create("targets.json", br#"{"targets":[]}"#, Some(1024))
                .await?
                .is_none()
        );
        let document = documents
            .read("targets.json", Some(1024))
            .await?
            .ok_or_else(|| Error::NotFound("host announcement".into()))?;
        assert_eq!(document.as_ref(), br#"{"targets":[]}"#);
        let authority =
            agent_authority(&host.issuer, &host.admission, &path, &state.volumes, &[]).await?;
        assert!(within_parent(
            &scope,
            &authority.scope,
            &state.volumes,
            &journal,
            crate::AgentId::from_bytes(path.id_bytes()),
        )?);
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
async fn external_interactions_require_host_custody_even_under_broad_guest_grants() -> Result<()> {
    use crate::contract::capability::HARNESS_HOST;
    use crate::conversation::{ContentResidencyVerifier, VolumeOperation};
    let file_bytes = crate::conversation::Limits::default().file_bytes;
    for structured in [false, true] {
        let path = AgentPath::root(&crate::agent::AgentId::from_bytes([187; 16]));
        let owner = crate::AgentId::from_bytes(path.id_bytes());
        let provider =
            ProviderRef::new("custody".into(), "filesystem".into(), "1".into(), &mut None)?;
        let host = FilesystemHost::new(acyclic_fs::Fs::memory(), &provider, None)?;
        let private = private_volume(provider.clone(), &path.id())?;
        let journal = crate::conversation::journal_volume(provider, &path.id())?;
        host.create_volume(&private).await?;
        host.create_volume(&journal).await?;
        let volumes = [WorkspaceEntry::initial_volume(
            crate::agent::WorkspaceRole::Private,
            PRIVATE_VOLUME.into(),
            private.clone(),
        )];
        let audience = Authority {
            kind: AggregateKind::Conversation,
            id: "custody".into(),
        };
        let root = AuthorityRoot::new("custody", [188; 32], audience.clone());
        let mut capabilities = vec![HARNESS_HOST.to_owned()];
        for volume in [&private, &journal] {
            for operation in [VolumeOperation::Read, VolumeOperation::Write] {
                capabilities.push(volume.capability(operation, &mut None)?);
            }
        }
        if structured {
            capabilities.push(
                acyclic_fs::access::Capability::whole_volume(
                    None,
                    &acyclic_fs::access::Action::ALL,
                )
                .to_scope_label()
                .map_err(|error| Error::Invalid(error.to_string()))?,
            );
        }
        let parent = root.bootstrap_scope(
            "host",
            &Capabilities::new(capabilities),
            Some(owner),
            &mut None,
        )?;
        let admission = TurnAdmission::Admitted {
            audience: audience.clone(),
            scope: parent.clone(),
        };
        let (_producer, stopped) = futures::channel::oneshot::channel::<()>();
        let context = acyclic_auth::operation::Context::from_stopped(
            root.verifier(&mut None)?
                .expiry(&parent)?
                .saturating_mul(1000),
            async move {
                let _ = stopped.await;
                acyclic_auth::operation::Stopped::Cancelled
            },
        );
        let issuer = RuntimeAuthority::admitted(
            root.admit(parent.clone(), context)?,
            OperationId::from_bytes([191; 16]),
        );
        let admitted = AdmittedAgent::verify(&issuer, &admission, &path, &volumes)?;
        let (control_scope, signatures) = acyclic_test_support::differential::observe(
            || (),
            |()| futures::executor::block_on(admitted.control_scope(&Capabilities::default())),
        );
        let signatures: Vec<acyclic_auth::crypto::SignatureVerification> = signatures;
        assert!(
            signatures.is_empty(),
            "bound host custody repeated cryptographic work"
        );
        let control_scope = control_scope?;
        let guest = admitted.authority(&[], None).await?;
        assert!(!guest.grants.contains(HARNESS_HOST));
        let control_grant = ContentGrant::verify(
            &guest.verifier,
            &control_scope,
            &journal,
            VolumeOperation::Write,
            &mut None,
        )?;
        let private_grant = ContentGrant::verify(
            &guest.verifier,
            &guest.scope,
            &private,
            VolumeOperation::Write,
            &mut None,
        )?;
        let files = AgentInternalFiles::new(
            host.clone(),
            journal.clone(),
            control_grant.clone(),
            private.clone(),
            private_grant,
        );
        files
            .create("custody.json", b"original interaction", file_bytes)
            .await?;
        assert_eq!(
            files.read("custody.json", file_bytes).await?.as_deref(),
            Some(b"original interaction".as_slice())
        );
        let cancellation = acyclic_fs::CancellationToken::new();
        let private_workspace = host
            .filesystem
            .open_workspace(
                private.storage_name(&mut None)?,
                acyclic_fs_work::performance::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(|failure| crate::filesystem::map_error(failure.error))?
            .value;
        let private_head = private_workspace
            .head(
                acyclic_fs_work::performance::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(|failure| crate::filesystem::map_error(failure.error))?
            .value;
        let missing = private_head
            .read(
                "/.system/interactions/custody.json",
                file_bytes,
                acyclic_fs_work::performance::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .expect_err("control appeared in the authored volume");
        assert_eq!(
            crate::filesystem::map_error(missing.error).code(),
            crate::wire::ErrorCode::NotFound
        );

        // Use an ordinary path, so a filename reservation cannot satisfy the
        // confidentiality or integrity assertions instead of the actual policy.
        let control = host
            .put_content(
                &journal,
                &control_grant,
                "control.json",
                b"host data",
                "application/json",
                "control.json",
                file_bytes,
                &crate::IdempotencyKey::new("host-control")?,
                None,
                Default::default(),
            )
            .await?;
        ContentGrant::verify_read(&guest.verifier, &guest.scope, &control)?;
        let reader = FilesystemContentVerifier::new(
            host.clone(),
            &guest.verifier,
            &guest.scope,
            file_bytes,
            None,
        )?;
        let archived = host
            .put_content(
                &journal,
                &control_grant,
                "inputs/context/selected/AGENTS.md",
                b"original checkout guidance",
                "text/plain",
                "AGENTS.md",
                file_bytes,
                &crate::IdempotencyKey::new("checkout-guidance")?,
                None,
                Default::default(),
            )
            .await?;
        assert_eq!(
            reader
                .read(&archived, archived.descriptor().byte_length(), None, None)?
                .await?
                .as_ref(),
            b"original checkout guidance",
        );
        let manifest = host
            .put_content(
                &journal,
                &control_grant,
                &crate::context::ContextEpoch::path(OperationId::from_bytes([206; 16])),
                br#"{"private_source":"original","callable":{"runtime":"private"}}"#,
                "application/json",
                "manifest.json",
                Some(4096),
                &crate::IdempotencyKey::new("private-epoch-manifest")?,
                None,
                Default::default(),
            )
            .await?;
        let host_reader = FilesystemContentVerifier::new(
            host.clone(),
            &guest.verifier,
            &control_scope,
            4096,
            None,
        )?;
        let unselected = host
            .put_content(
                &journal,
                &control_grant,
                "inputs/skills/unselected/SKILL.md",
                b"unselected skill body",
                "text/markdown",
                "SKILL.md",
                file_bytes,
                &crate::IdempotencyKey::new("unselected-skill-body")?,
                None,
                Default::default(),
            )
            .await?;
        let turn = OperationId::from_bytes([207; 16]);
        let other_turn = OperationId::from_bytes([208; 16]);
        let mut turn_files = Vec::new();
        for selected in [turn, other_turn] {
            turn_files.push(
                host.put_content(
                    &journal,
                    &control_grant,
                    &format!("{TURN_INPUTS}/{selected}/0/0"),
                    b"admitted steering",
                    "text/plain",
                    "steering",
                    file_bytes,
                    &crate::IdempotencyKey::new(format!("turn-input-{selected}"))?,
                    None,
                    Default::default(),
                )
                .await?,
            );
        }
        let native: RuntimeAuthority = root.clone().into();
        let task = AdmittedAgent::verify(&native, &TurnAdmission::LocalRoot, &path, &volumes)?
            .authority(&[], Some(turn))
            .await?;
        assert!(!task.grants.contains(HARNESS_HOST));
        let task_reader =
            FilesystemContentVerifier::new(host.clone(), &task.verifier, &task.scope, 4096, None)?;
        assert_eq!(
            task_reader
                .read(&archived, 4096, None, None)?
                .await?
                .as_ref(),
            b"original checkout guidance"
        );
        assert_eq!(
            task_reader
                .read(&turn_files[0], 4096, None, None)?
                .await?
                .as_ref(),
            b"admitted steering"
        );
        for file in [&turn_files[1], &manifest, &unselected] {
            let denied = async { task_reader.read(file, 4096, None, None)?.await }
                .await
                .expect_err("a turn input route granted another turn or host controls");
            assert_eq!(denied.code(), crate::wire::ErrorCode::Unauthorized);
        }
        let listing = async {
            task_reader
                .list_private_directory(
                    &journal,
                    crate::context::ContextEpoch::INSTRUCTIONS_DIRECTORY,
                    crate::context::ContextEpoch::INSTRUCTIONS_DIRECTORY,
                    crate::filesystem::DirectoryOptions {
                        generation: None,
                        after: None,
                        maximum_entries: 1,
                        ceiling: None,
                    },
                )
                .await
        }
        .await
        .expect_err("captured instruction reads granted journal listing");
        assert_eq!(listing.code(), crate::wire::ErrorCode::Unauthorized);
        assert_eq!(
            ContentGrant::verify(
                &task.verifier,
                &task.scope,
                &journal,
                VolumeOperation::Write,
                &mut None,
            )
            .err()
            .expect("a turn input route granted journal writes")
            .code(),
            crate::wire::ErrorCode::Unauthorized
        );
        assert!(
            host_reader
                .read(&manifest, 4096, None, None)?
                .await
                .expect("host reads private epoch manifest")
                .len()
                > 0
        );
        ContentGrant::verify_read(&guest.verifier, &guest.scope, &manifest)?;
        for ceiling in [None, Some(parent.capabilities())] {
            let denied = async { reader.read(&manifest, 4096, ceiling, None)?.await }
                .await
                .expect_err("a broad guest journal grant disclosed the private epoch manifest");
            assert_eq!(denied.code(), crate::wire::ErrorCode::Unauthorized);
            let denied = async {
                reader
                    .read(&control, control.descriptor().byte_length(), ceiling, None)?
                    .await
            }
            .await
            .expect_err("guest read host data through the persisted policy");
            assert_eq!(denied.code(), crate::wire::ErrorCode::Unauthorized);
        }
        // Input references enter committed inboxes and can be inherited. The
        // journal policy admits their bytes, while the scope selects exactly
        // the original published file, never the entire journal.
        let input = host
            .put_content(
                &journal,
                &control_grant,
                "inputs/message.txt",
                b"accepted input",
                "text/plain",
                "message.txt",
                Some(4096),
                &crate::IdempotencyKey::new("accepted-input")?,
                None,
                Default::default(),
            )
            .await?;
        let skill = host
            .put_content(
                &journal,
                &control_grant,
                "inputs/skills/source/SKILL.md",
                b"optional instructions",
                "text/markdown",
                "SKILL.md",
                Some(4096),
                &crate::IdempotencyKey::new("published-skill")?,
                None,
                Default::default(),
            )
            .await?;
        assert_eq!(
            reader
                .read(&skill, 4096, None, None)?
                .await
                .expect("guest reads published skill body")
                .as_ref(),
            b"optional instructions"
        );
        let selected = root.bootstrap_scope(
            "input-reader",
            &Capabilities::new([input.read_capability(&mut None)?]),
            Some(owner),
            &mut None,
        )?;
        let selected_reader =
            FilesystemContentVerifier::new(host.clone(), &guest.verifier, &selected, 4096, None)?;
        let replacement = host
            .put_content(
                &journal,
                &control_grant,
                "inputs/message.txt",
                b"later input",
                "text/plain",
                "message.txt",
                Some(4096),
                &crate::IdempotencyKey::new("later-input")?,
                None,
                Default::default(),
            )
            .await?;
        assert_eq!(
            selected_reader
                .read(&input, 4096, None, None)?
                .await?
                .as_ref(),
            b"accepted input"
        );
        for file in [&replacement, &control] {
            let denied = async { selected_reader.read(file, 4096, None, None)?.await }
                .await
                .expect_err("an input reference granted another journal file or version");
            assert_eq!(denied.code(), crate::wire::ErrorCode::Unauthorized);
        }
        let denied = async {
            selected_reader
                .read(&input, 4096, Some(&Capabilities::default()), None)?
                .await
        }
        .await
        .expect_err("input publication bypassed the caller ceiling");
        assert_eq!(denied.code(), crate::wire::ErrorCode::Unauthorized);
        let guest_write = ContentGrant::verify(
            &guest.verifier,
            &guest.scope,
            &journal,
            VolumeOperation::Write,
            &mut None,
        )?;
        // Reopening existing storage must re-admit the kernel's current
        // policy, without rewriting any original input or granting guest writes.
        let workspace = host
            .filesystem
            .open_workspace(
                journal.storage_name(&mut None)?,
                acyclic_fs_work::performance::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(|failure| Error::from(failure.error))?
            .value;
        let mut policy: acyclic_fs::access::Policy =
            serde_json::from_slice(include_bytes!("../host/journal-policy.json"))
                .map_err(|error| Error::Invalid(error.to_string()))?;
        let layer = policy
            .layers
            .first_mut()
            .ok_or_else(|| Error::Invalid("journal policy has no layers".into()))?;
        layer.retain(|rule| rule.id != "published-input");
        layer
            .first_mut()
            .ok_or_else(|| Error::Invalid("journal policy has no host rule".into()))?
            .id = "saved-host-custody-".repeat(256);
        assert!(
            crate::contract::canonical_json_bytes(&policy)?.len()
                > include_bytes!("../host/journal-policy.json").len()
        );
        replace_policy(&workspace, &policy).await?;
        let refused = async { selected_reader.read(&input, 4096, None, None)?.await }
            .await
            .expect_err("the saved policy did not exercise input refusal");
        assert_eq!(refused.code(), crate::wire::ErrorCode::Unauthorized);
        let before = host
            .resolve(
                &workspace_ref(
                    journal.provider().clone(),
                    &journal.storage_name(&mut None)?,
                )?,
                None,
            )
            .await?;
        let refused = host
            .enforce_journal_policy(
                &journal,
                &guest.verifier,
                &guest.scope,
                true,
                || std::future::ready(Ok(guest_write.clone())),
                None,
            )?
            .await
            .expect_err("a guest replaced journal enforcement");
        assert_eq!(refused.code(), crate::wire::ErrorCode::Unauthorized);
        let reopened = FilesystemHost::new(host.filesystem.clone(), journal.provider(), None)?;
        let allowed: Vec<_> = acyclic_fs::access::Action::ALL
            .into_iter()
            .filter(|action| *action != acyclic_fs::access::Action::Admin)
            .collect();
        let limited = root.bootstrap_scope(
            "policy-without-admin",
            &Capabilities::new([
                HARNESS_HOST.to_owned(),
                journal.capability(VolumeOperation::Write, &mut None)?,
                acyclic_fs::access::Capability::whole_volume(None, &allowed)
                    .to_scope_label()
                    .map_err(|error| Error::Invalid(error.to_string()))?,
            ]),
            Some(owner),
            &mut None,
        )?;
        let limited_grant = ContentGrant::verify(
            &guest.verifier,
            &limited,
            &journal,
            VolumeOperation::Write,
            &mut None,
        )?;
        let refused = reopened
            .enforce_journal_policy(
                &journal,
                &guest.verifier,
                &control_scope,
                true,
                || std::future::ready(Ok(limited_grant)),
                None,
            )?
            .await
            .expect_err("journal admission bypassed the administrator ceiling");
        assert_eq!(refused.code(), crate::wire::ErrorCode::Unauthorized);
        assert_eq!(
            reopened
                .resolve(
                    &workspace_ref(
                        journal.provider().clone(),
                        &journal.storage_name(&mut None)?
                    )?,
                    None,
                )
                .await?
                .generation,
            before.generation
        );
        // A second admission repairs the journal after the first observes its
        // saved policy but before the first binds its publication authority.
        reopened
            .enforce_journal_policy(
                &journal,
                &guest.verifier,
                &control_scope,
                true,
                || async {
                    reopened
                        .enforce_journal_policy(
                            &journal,
                            &guest.verifier,
                            &control_scope,
                            true,
                            || std::future::ready(Ok(control_grant.clone())),
                            None,
                        )?
                        .await?;
                    Ok(control_grant.clone())
                },
                None,
            )?
            .await?;
        let reopened_admission = AdmittedAgent::verify(&issuer, &admission, &path, &volumes)?;
        let foreign_provider = ProviderRef::new(
            "another-host".into(),
            "filesystem".into(),
            "1".into(),
            &mut None,
        )?;
        let foreign_journal = crate::conversation::journal_volume(foreign_provider, &path.id())?;
        let foreign_input = FileRef::new(
            foreign_journal,
            input.path().into(),
            input.version().into(),
            input.descriptor().clone(),
            input.display_name().into(),
            &mut None,
        )?;
        let refused = reopened_admission
            .journal_authority(&reopened, &[foreign_input], &[])
            .await
            .err()
            .expect("journal admission accepted a retained source from another host");
        assert_eq!(refused.code(), crate::wire::ErrorCode::Unauthorized);
        reopened_admission
            .journal_authority(&reopened, &[input.clone()], &[])
            .await
            .expect("host repairs the owned journal policy");
        let reference = workspace_ref(
            journal.provider().clone(),
            &journal.storage_name(&mut None)?,
        )?;
        let repaired = reopened.resolve(&reference, None).await?;
        assert_ne!(repaired.generation, before.generation);
        reopened
            .enforce_journal_policy(
                &journal,
                &guest.verifier,
                &control_scope,
                true,
                || std::future::ready(Err(Error::Unauthorized("no policy writer selected".into()))),
                None,
            )?
            .await?;
        assert_eq!(
            selected_reader
                .read(&input, 4096, None, None)?
                .await?
                .as_ref(),
            b"accepted input"
        );
        reopened_admission
            .journal_authority(&reopened, &[input.clone()], &[])
            .await
            .expect("host repairs the owned journal policy");
        assert_eq!(
            reopened.resolve(&reference, None).await?.generation,
            repaired.generation
        );
        assert_eq!(
            async { reader.read(&control, 4096, None, None)?.await }
                .await
                .expect_err("policy admission exposed unrelated control data")
                .code(),
            crate::wire::ErrorCode::Unauthorized,
        );
        for path in ["forged.json", "inputs/forged.json"] {
            let denied = host
                .put_content(
                    &journal,
                    &guest_write,
                    path,
                    b"guest data",
                    "application/json",
                    "forged.json",
                    file_bytes,
                    &crate::IdempotencyKey::new(format!("guest-forgery:{path}"))?,
                    None,
                    Default::default(),
                )
                .await
                .expect_err("guest wrote the host journal");
            assert_eq!(denied.code(), crate::wire::ErrorCode::Unauthorized);
        }
        let input = host
            .put_content(
                &journal,
                &control_grant,
                "inputs/message.txt",
                b"selected input",
                "text/plain",
                "message.txt",
                file_bytes,
                &crate::IdempotencyKey::new("selected-input")?,
                None,
                Default::default(),
            )
            .await?;
        let input_scope = root.bootstrap_scope(
            "selected-input",
            &Capabilities::new([input.read_capability(&mut None)?]),
            Some(owner),
            &mut None,
        )?;
        let input_reader = FilesystemContentVerifier::new(
            host.clone(),
            &guest.verifier,
            &input_scope,
            file_bytes,
            None,
        )?;
        assert_eq!(
            input_reader
                .read(&input, input.descriptor().byte_length(), None, None)?
                .await?
                .as_ref(),
            b"selected input"
        );
        let foreign = crate::conversation::journal_volume(
            journal.provider().clone(),
            &crate::agent::AgentId::from_bytes([202; 16]),
        )?;
        host.create_volume(&foreign).await?;
        let foreign_scope = root.bootstrap_scope(
            "source-host",
            &Capabilities::new([
                HARNESS_HOST.to_owned(),
                foreign.capability(VolumeOperation::Read, &mut None)?,
                foreign.capability(VolumeOperation::Write, &mut None)?,
            ]),
            Some(crate::AgentId::from_bytes([202; 16])),
            &mut None,
        )?;
        let foreign_write = ContentGrant::verify(
            &guest.verifier,
            &foreign_scope,
            &foreign,
            VolumeOperation::Write,
            &mut None,
        )?;
        let source_files = AgentInternalFiles::new(
            host.clone(),
            foreign.clone(),
            foreign_write.clone(),
            foreign.clone(),
            foreign_write.clone(),
        );
        let shared = source_files
            .stage_announcement(
                crate::agent::AnnouncementKind::Targets,
                [203; 32],
                br#"{"source":"shared"}"#,
            )
            .await?;
        let source_input = host
            .put_content(
                &foreign,
                &foreign_write,
                "inputs/message.txt",
                b"shared original input",
                "text/plain",
                "message.txt",
                Some(4096),
                &crate::IdempotencyKey::new("shared-input")?,
                None,
                Default::default(),
            )
            .await?;
        let source_workspace = host
            .filesystem
            .open_workspace(
                foreign.storage_name(&mut None)?,
                acyclic_fs_work::performance::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(|failure| Error::from(failure.error))?
            .value;
        replace_policy(&source_workspace, &policy).await?;
        let source_reference = workspace_ref(
            foreign.provider().clone(),
            &foreign.storage_name(&mut None)?,
        )?;
        let source_before = host.resolve(&source_reference, None).await?;
        let source_volume =
            uuid::Uuid::from_bytes(source_workspace.id().volume_id().into_bytes()).to_string();
        let label = crate::core::filesystem_scope_label(parent.capabilities(), &mut None)?;
        let mut read_only: Vec<_> = parent
            .capabilities()
            .iter()
            .filter(|capability| Some(*capability) != label)
            .map(str::to_owned)
            .collect();
        read_only.push(shared.read_capability(&mut None)?);
        read_only.push(source_input.read_capability(&mut None)?);
        if structured {
            let mut capability = acyclic_fs::access::Capability::whole_volume(
                Some(
                    uuid::Uuid::from_bytes(private_workspace.id().volume_id().into_bytes())
                        .to_string(),
                ),
                &acyclic_fs::access::Action::ALL,
            );
            capability.layers[0].extend(
                acyclic_fs::access::Capability::whole_volume(
                    Some(
                        uuid::Uuid::from_bytes(workspace.id().volume_id().into_bytes()).to_string(),
                    ),
                    &acyclic_fs::access::Action::ALL,
                )
                .layers
                .into_iter()
                .flatten(),
            );
            capability.layers[0].extend(
                acyclic_fs::access::Capability::whole_volume(
                    Some(source_volume.clone()),
                    &[
                        acyclic_fs::access::Action::Read,
                        acyclic_fs::access::Action::ReadHistory,
                    ],
                )
                .layers
                .into_iter()
                .flatten(),
            );
            read_only.push(
                capability
                    .to_scope_label()
                    .map_err(|error| Error::Invalid(error.to_string()))?,
            );
        }
        let read_only = root.bootstrap_scope(
            "host-with-read-only-source",
            &Capabilities::new(read_only),
            Some(owner),
            &mut None,
        )?;
        if structured {
            host.authorize_volume_creation(&read_only, &private)?;
            host.authorize_volume_creation(&read_only, &foreign)
                .expect_err("read-only source admission allocated a fresh source volume");
            let before = host.create_volume(&foreign).await?;
            let retained = host
                .create_volume_admitted(&foreign, Some(&read_only))
                .await?;
            assert_eq!(retained, before);
            host.authorize_retained_writer(&read_only, &foreign)
                .await
                .expect_err("read-only source admission acquired an existing writer");
        }
        let (_source_producer, source_stopped) = futures::channel::oneshot::channel::<()>();
        let source_context = acyclic_auth::operation::Context::from_stopped(
            root.verifier(&mut None)?
                .expiry(&read_only)?
                .saturating_mul(1000),
            async move {
                let _ = source_stopped.await;
                acyclic_auth::operation::Stopped::Cancelled
            },
        );
        let source_issuer = RuntimeAuthority::admitted(
            root.admit(read_only.clone(), source_context)?,
            OperationId::from_bytes([204; 16]),
        );
        let read_only_admission = TurnAdmission::Admitted {
            audience: audience.clone(),
            scope: read_only.clone(),
        };
        let reader_admission =
            AdmittedAgent::verify(&source_issuer, &read_only_admission, &path, &volumes)?;
        reader_admission
            .journal_authority(&reopened, &[], std::slice::from_ref(&shared))
            .await
            .expect("read-only source admission leaves foreign policy unchanged");
        assert_eq!(
            host.resolve(&source_reference, None).await?.generation,
            source_before.generation,
            "reading a foreign journal changed its owner's policy"
        );
        let reader_authority = reader_admission
            .authority(&[shared.clone(), source_input.clone()], None)
            .await?;
        let foreign_reader = FilesystemContentVerifier::new(
            host.clone(),
            &reader_authority.verifier,
            &reader_authority.scope,
            4096,
            None,
        )?;
        assert_eq!(
            foreign_reader
                .read(&shared, 4096, None, None)?
                .await?
                .as_ref(),
            br#"{"source":"shared"}"#
        );
        assert_eq!(
            async { foreign_reader.read(&source_input, 4096, None, None)?.await }
                .await
                .expect_err("source policy admitted its unpublished input rule")
                .code(),
            crate::wire::ErrorCode::Unauthorized,
        );
        if structured {
            let label = crate::core::filesystem_scope_label(read_only.capabilities(), &mut None)?
                .expect("structured source capability");
            let mut capability = acyclic_fs::access::Capability::from_scope_label(label)
                .map_err(|error| Error::Invalid(error.to_string()))?
                .expect("typed source capability");
            for rule in capability.layers.iter_mut().flatten() {
                if rule.resource.volume.as_deref() == Some(source_volume.as_str()) {
                    rule.actions = acyclic_fs::access::Action::ALL.into_iter().collect();
                    rule.resource.generations = acyclic_fs::access::Generations::Range(
                        acyclic_fs::access::GenerationRange {
                            first: 1,
                            last: u64::MAX,
                        },
                    );
                }
            }
            let mut capabilities: Vec<_> = read_only
                .capabilities()
                .iter()
                .filter(|candidate| *candidate != label)
                .map(str::to_owned)
                .collect();
            capabilities.push(foreign.capability(VolumeOperation::Write, &mut None)?);
            capabilities.push(
                capability
                    .to_scope_label()
                    .map_err(|error| Error::Invalid(error.to_string()))?,
            );
            let administrator_scope = root.bootstrap_scope(
                "generation-bound-source-administrator",
                &Capabilities::new(capabilities),
                Some(owner),
                &mut None,
            )?;
            host.authorize_volume_creation(&administrator_scope, &foreign)
                .expect_err("a later-generation grant authorized initial storage creation");
            let before = host.create_volume(&foreign).await?;
            let retained = host
                .create_volume_admitted(&foreign, Some(&administrator_scope))
                .await?;
            assert_eq!(retained, before);
            host.authorize_retained_writer(&administrator_scope, &foreign)
                .await?;
            let (_administrator_producer, administrator_stopped) =
                futures::channel::oneshot::channel::<()>();
            let administrator_context = acyclic_auth::operation::Context::from_stopped(
                root.verifier(&mut None)?
                    .expiry(&administrator_scope)?
                    .saturating_mul(1000),
                async move {
                    let _ = administrator_stopped.await;
                    acyclic_auth::operation::Stopped::Cancelled
                },
            );
            let administrator_issuer = RuntimeAuthority::admitted(
                root.admit(administrator_scope.clone(), administrator_context)?,
                OperationId::from_bytes([205; 16]),
            );
            let administrator_admission = TurnAdmission::Admitted {
                audience: audience.clone(),
                scope: administrator_scope,
            };
            let administrator = AdmittedAgent::verify(
                &administrator_issuer,
                &administrator_admission,
                &path,
                &volumes,
            )?;
            administrator
                .journal_authority(&reopened, &[], std::slice::from_ref(&shared))
                .await
                .expect("generation-bound administrator repairs the source policy");
            assert_ne!(
                host.resolve(&source_reference, None).await?.generation,
                source_before.generation,
                "an actor-bound administrator could not update its admitted source"
            );
            assert_eq!(
                foreign_reader
                    .read(&source_input, 4096, None, None)?
                    .await?
                    .as_ref(),
                b"shared original input"
            );
        }
        assert_eq!(
            async { reader.read(&manifest, 4096, None, None)?.await }
                .await
                .expect_err("reopening published private epoch metadata")
                .code(),
            crate::wire::ErrorCode::Unauthorized,
        );
        let announcements = AgentInternalFiles::new(
            host.clone(),
            journal.clone(),
            control_grant.clone(),
            journal.clone(),
            control_grant,
        );
        let published = announcements
            .stage_announcement(
                crate::agent::AnnouncementKind::Targets,
                [189; 32],
                b"published announcement",
            )
            .await?;
        assert_eq!(
            reader
                .read(&published, published.descriptor().byte_length(), None, None)?
                .await?
                .as_ref(),
            b"published announcement"
        );
        announcements
            .stage_announcement(
                crate::agent::AnnouncementKind::Targets,
                [190; 32],
                b"later announcement",
            )
            .await?;
        assert_eq!(
            input_reader
                .read(&input, input.descriptor().byte_length(), None, None)?
                .await?
                .as_ref(),
            b"selected input"
        );
        assert_eq!(
            reader
                .read(&published, published.descriptor().byte_length(), None, None)?
                .await?
                .as_ref(),
            b"published announcement"
        );
        let missing_role = root.bootstrap_scope("guest", &guest.grants, Some(owner), &mut None)?;
        assert!(
            AdmittedAgent::verify(
                &issuer,
                &TurnAdmission::Admitted {
                    audience,
                    scope: missing_role
                },
                &path,
                &volumes
            )
            .is_err()
        );
        assert_eq!(
            files.read("custody.json", file_bytes).await?.as_deref(),
            Some(b"original interaction".as_slice())
        );
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
                Some(1024),
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
    let admitted = AdmittedAgent::verify(&issuer, &TurnAdmission::LocalRoot, &agent, &volumes)?;
    let control_volume = admitted.journal.clone();
    host.create_volume(&control_volume).await?;
    let control_scope = admitted.control_scope(&Capabilities::default()).await?;
    let authority = admitted.authority(&[], None).await?;
    let grant = crate::conversation::ContentGrant::verify(
        &authority.verifier,
        &authority.scope,
        &volume,
        VolumeOperation::Write,
        &mut None,
    )?;
    let files = Arc::new(AgentInternalFiles::new(
        host.clone(),
        control_volume.clone(),
        ContentGrant::verify(
            &authority.verifier,
            &control_scope,
            &control_volume,
            VolumeOperation::Write,
            &mut None,
        )?,
        volume,
        grant,
    ));
    let reopen = || FilesystemTurnChildren {
        clock: Arc::new(acyclic_actors::local::SystemClock),
        files: files.clone(),
        stream: Arc::new(acyclic_stream::MemoryStream::default()),
        within: acyclic_actors::addressing::ActorReference::new(b"a", b"agents", b"owner")
            .expect("namespace"),
        host: host.clone(),
        issuer: issuer.clone(),
        admission: TurnAdmission::LocalRoot,
        agent: agent.clone(),
        volumes: volumes.clone(),
        maximum_bytes: Some(64 * 1024),
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
    index_reads: std::sync::atomic::AtomicUsize,
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
        self.index_reads.fetch_add(
            request
                .paths
                .iter()
                .filter(|path| path.as_str().starts_with("harness/v1/execution-index/"))
                .count(),
            std::sync::atomic::Ordering::Relaxed,
        );
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
        if request
            .path
            .as_str()
            .starts_with("harness/v1/execution-index/")
        {
            self.index_reads
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
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
            index_reads: Default::default(),
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
                maximum_bytes: Some(64 * 1024),
                history_turns: 8,
            },
            Default::default(),
        );
        let stop = acyclic_actors::guest::CancellationToken::new();
        let opening = host.open(
            &state,
            crate::agent::TurnIdentity::new(OperationId::from_bytes([198; 16]), 0, 1),
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
    let scope = root.bootstrap_scope(
        "caller",
        &Capabilities::new([
            ceiling,
            crate::contract::capability::HARNESS_HOST.to_owned(),
        ]),
        None,
        &mut None,
    )?;
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
                    .authority(&[], None)
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

/// An admitted agent's authority is its own bindings, never more. Its
/// admission also grants a volume the agent does not bind (its parent's
/// project, as after a fork); no volume, directory, exact-file or structured
/// grant of the minted scope reaches it, while the bound fork keeps its grant.
#[test]
fn an_admitted_agents_authority_is_its_bindings_never_more() -> Result<()> {
    use acyclic_fs::access::{self, Action};
    // A grandchild fork: its parent's and grandparent's journals hold the
    // conversation it inherits.
    let path = AgentPath::root(&crate::agent::AgentId::from_bytes([193; 16]))
        .child(&crate::agent::AgentId::from_bytes([191; 16]))
        .and_then(|parent| parent.child(&crate::agent::AgentId::from_bytes([194; 16])))
        .map_err(Error::from)?;
    let owner = crate::AgentId::from_bytes(path.id().to_bytes());
    let audience = Authority {
        kind: AggregateKind::Task,
        id: "admitted-bindings".into(),
    };
    let root = AuthorityRoot::new("admitted-bindings", [195; 32], audience.clone());
    let issuer = RuntimeAuthority::from(root.clone());
    let provider = ProviderRef::new(
        "admitted-bindings".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let volume = |id: &str, class, owner| {
        VolumeRef::new(provider.clone(), id.into(), class, owner, &mut None)
    };
    let private = volume(
        "private",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(owner),
    )?;
    let fork = volume(
        "fork",
        VolumeClass::Project,
        VolumeOwner::Project("test".into()),
    )?;
    let parent = volume(
        "project",
        VolumeClass::Project,
        VolumeOwner::Project("test".into()),
    )?;
    let rooted = volume(
        "rooted",
        VolumeClass::Project,
        VolumeOwner::Project("test".into()),
    )?;
    let mut rooted_binding = WorkspaceEntry::initial_volume(
        crate::agent::WorkspaceRole::User,
        "rooted".into(),
        rooted.clone(),
    );
    rooted_binding.root = "src".into();
    let volumes = [
        WorkspaceEntry::initial_volume(
            crate::agent::WorkspaceRole::Private,
            PRIVATE_VOLUME.into(),
            private,
        ),
        WorkspaceEntry::initial_volume(
            crate::agent::WorkspaceRole::User,
            "project".into(),
            fork.clone(),
        ),
        rooted_binding,
    ];
    let parent_file = FileRef::new(
        parent.clone(),
        "seed.txt".into(),
        "1".into(),
        crate::conversation::FileDescriptor::new([3; 32], 6, "text/plain".into())?,
        "seed.txt".into(),
        &mut None,
    )?;
    let whole = access::Capability::whole_volume(None, &Action::ALL);
    let unbound = [
        parent.capability(VolumeOperation::Read, &mut None)?,
        parent.capability(VolumeOperation::Write, &mut None)?,
        parent.capability(VolumeOperation::Delete, &mut None)?,
        parent.directory_read_capability("src", &mut None)?,
        parent_file.read_capability(&mut None)?,
    ];
    let bound = fork.capability(VolumeOperation::Read, &mut None)?;
    let bound_directory = fork.directory_read_capability("src", &mut None)?;
    let unbound_directory = parent.directory_read_capability("src", &mut None)?;
    // A rooted binding keeps its own root read, never an admission's read
    // elsewhere in the same volume.
    let rooted_root = rooted.directory_read_capability("src", &mut None)?;
    let rooted_escape = rooted.directory_read_capability("secrets", &mut None)?;
    let scope = root.bootstrap_scope(
        "caller",
        &Capabilities::new(
            unbound.iter().cloned().chain([
                bound.clone(),
                bound_directory.clone(),
                rooted_root.clone(),
                rooted_escape.clone(),
                "model:generate".to_owned(),
                crate::contract::capability::HARNESS_HOST.to_owned(),
                whole
                    .to_scope_label()
                    .map_err(|_| Error::Invalid("fixture filesystem ceiling".into()))?,
            ]),
        ),
        None,
        &mut None,
    )?;
    let admission = TurnAdmission::Admitted { audience, scope };
    let minted = futures::executor::block_on(async {
        AdmittedAgent::verify(&issuer, &admission, &path, &volumes)?
            .authority(&[], None)
            .await
    })?;
    let granted = &minted.grants;
    // A directory read survives only on a volume the agent binds.
    assert!(
        !granted.contains(&unbound_directory),
        "unbound directory read inherited"
    );
    assert!(granted.contains(&bound_directory));
    assert!(granted.contains(&rooted_root));
    assert!(
        !granted.contains(&rooted_escape),
        "a read outside a rooted binding inherited"
    );
    for label in &unbound {
        assert!(!granted.contains(label), "unbound grant inherited: {label}");
    }
    assert!(granted.contains(&bound));
    assert!(granted.contains("model:generate"));
    assert!(!granted.contains(crate::contract::capability::HARNESS_HOST));

    // The structured ceiling narrows to the bound volumes, by the storage
    // reference each workspace is bound with; one bound without a reference
    // is refused, never admitted.
    let label = crate::core::filesystem_scope_label(granted, &mut None)?
        .ok_or_else(|| Error::NotFound("minted filesystem ceiling".into()))?;
    let narrowed = access::Capability::from_scope_label(label)
        .map_err(|error| Error::Invalid(error.to_string()))?
        .ok_or_else(|| Error::NotFound("minted filesystem ceiling".into()))?;
    assert!(narrowed.is_attenuation_of(&whole));
    let policy: access::Policy = serde_json::from_slice(access::INITIAL_POLICY)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let principal = access::Principal {
        kind: access::PrincipalKind::Agent,
        id: owner.to_string(),
        lineage: Vec::new(),
        groups: Default::default(),
        roles: Default::default(),
        tags: Default::default(),
        attributes: Default::default(),
    };
    let fork_reference = fork.storage_name(&mut None)?;
    let parent_reference = parent.storage_name(&mut None)?;
    let journal = |id: u8| {
        crate::conversation::journal_volume(
            provider.clone(),
            &crate::agent::AgentId::from_bytes([id; 16]),
        )?
        .storage_name(&mut None)
    };
    // 192 is a sibling of the parent 191, under the same grandparent 193.
    let (grandparent_journal, parent_journal, sibling_journal) =
        (journal(193)?, journal(191)?, journal(192)?);
    for (reference, action, allowed) in [
        (Some(fork_reference.as_str()), Action::Read, true),
        (Some(fork_reference.as_str()), Action::Write, true),
        (Some(parent_reference.as_str()), Action::Read, false),
        (Some(parent_reference.as_str()), Action::Write, false),
        // Every ancestor's journal is read, never written; a sibling's
        // journal is not reached.
        (Some(parent_journal.as_str()), Action::Read, true),
        (Some(parent_journal.as_str()), Action::Write, false),
        (Some(grandparent_journal.as_str()), Action::Read, true),
        (Some(grandparent_journal.as_str()), Action::Write, false),
        (Some(sibling_journal.as_str()), Action::Read, false),
        (None, Action::Read, false),
    ] {
        let decision = access::decide(
            &policy,
            &principal,
            action,
            access::Resource {
                config: acyclic_fs::VolumeConfig::portable(acyclic_fs::Lifecycle::Durable),
                lineage: &[],
                volume: "00000000-0000-0000-0000-000000000000",
                reference,
                generation: 1,
                head: 1,
                path: "seed.txt",
            },
            access::Context {
                now: 0,
                approvals: 0,
            },
            &narrowed,
        );
        assert_eq!(decision.allowed, allowed, "{reference:?} {action:?}");
    }
    Ok(())
}

#[tokio::test]
async fn a_local_agent_reads_what_its_journal_publishes_and_nothing_private() -> Result<()> {
    // A compacted request projects its summary through the turn's own reader.
    // The local grant set holds the journal read every admitted agent holds,
    // so the journal policy alone decides each path, as it does when hosted.
    let path = AgentPath::root(&crate::agent::AgentId::from_bytes([214; 16]));
    let owner = crate::AgentId::from_bytes(path.id_bytes());
    let provider = ProviderRef::new(
        "summaries".into(),
        "filesystem".into(),
        "1".into(),
        &mut None,
    )?;
    let host = FilesystemHost::new(acyclic_fs::Fs::memory(), &provider, None)?;
    let issuer = AuthorityRoot::new(
        "summaries",
        [215; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "summaries".into(),
        },
    );
    let private = private_volume(provider.clone(), &path.id())?;
    let journal = crate::conversation::journal_volume(provider, &path.id())?;
    host.create_volume(&private).await?;
    host.create_volume(&journal).await?;
    let volumes = [WorkspaceEntry::initial_volume(
        crate::agent::WorkspaceRole::Private,
        PRIVATE_VOLUME.into(),
        private.clone(),
    )];
    // Issued before the agent's own authority, for the negative control.
    let with_journal = issuer.bootstrap_scope(
        "journal-reader",
        &Capabilities::new([journal.capability(VolumeOperation::Read, &mut None)?]),
        Some(owner),
        &mut None,
    )?;
    let without_journal = issuer.bootstrap_scope(
        "private-reader",
        &Capabilities::new([private.capability(VolumeOperation::Read, &mut None)?]),
        Some(owner),
        &mut None,
    )?;
    let runtime: RuntimeAuthority = issuer.clone().into();
    let admitted = AdmittedAgent::verify(&runtime, &TurnAdmission::LocalRoot, &path, &volumes)?;
    // The turn's own journal stages both classes, exactly as compaction does.
    use crate::executor::ExecutionJournal as _;
    let (volume, custody) = admitted.journal_authority(&host, &[], &[]).await?;
    let writer = FilesystemExecutionJournal::new(
        Arc::new(acyclic_stream::MemoryStream::default()),
        Arc::clone(&host),
        FilesystemJournalBinding {
            volume: &volume,
            verifier: &custody.verifier,
            scope: &custody.scope,
            io: None,
        },
    )?;
    let operation = OperationId::from_bytes([216; 16]);
    let summary = writer
        .stage(
            operation,
            "summary:1:output",
            b"summary",
            "text/plain",
            None,
        )?
        .await?;
    let execution = writer
        .stage(
            operation,
            "context",
            b"execution record",
            "text/plain",
            None,
        )?
        .await?;
    assert!(summary.path().starts_with(".system/summaries/"));
    assert!(execution.path().starts_with(".system/execution/"));
    let authority =
        agent_authority(&runtime, &TurnAdmission::LocalRoot, &path, &volumes, &[]).await?;
    assert!(
        !authority
            .grants
            .contains(crate::contract::capability::HARNESS_HOST)
    );
    let reader = FilesystemContentVerifier::new(
        host.clone(),
        &authority.verifier,
        &authority.scope,
        1024,
        None,
    )?;
    assert_eq!(
        reader.read(&summary, 1024, None, None)?.await?.as_ref(),
        b"summary"
    );
    // The journal policy, not the grant, keeps execution records host-private.
    let denied = async { reader.read(&execution, 1024, None, None)?.await }
        .await
        .expect_err("an agent read its host-private execution record");
    assert_eq!(denied.code(), crate::wire::ErrorCode::Unauthorized);
    // Negative control: two scopes of the same agent from the same signer,
    // differing only in the journal read. Only the one holding it reads the
    // summary, so the grant, not the agent's identity, admits it.
    let verifier = issuer.verifier(&mut None)?;
    let with_journal =
        FilesystemContentVerifier::new(host.clone(), &verifier, &with_journal, 1024, None)?;
    assert_eq!(
        with_journal
            .read(&summary, 1024, None, None)?
            .await?
            .as_ref(),
        b"summary"
    );
    let without_journal =
        FilesystemContentVerifier::new(host.clone(), &verifier, &without_journal, 1024, None)?;
    let denied = async { without_journal.read(&summary, 1024, None, None)?.await }
        .await
        .expect_err("a scope without the journal read projected a summary");
    assert_eq!(denied.code(), crate::wire::ErrorCode::Unauthorized);
    Ok(())
}

/// Whether `derived` stays within `parent`: each label is the parent's own
/// (except host custody and the ceiling) and not of a kind the ceiling
/// withholds, an explicit label for `owner`'s bindings under that ceiling, or
/// `owner`'s own journal read under a reading ceiling. It never holds
/// `HARNESS_HOST` or a structured label.
fn within_parent(
    parent: &Scope,
    derived: &Scope,
    volumes: &[WorkspaceEntry],
    journal: &VolumeRef,
    owner: crate::AgentId,
) -> Result<bool> {
    let mut allowed = parent
        .capabilities()
        .iter()
        .filter(|label| *label != crate::contract::capability::HARNESS_HOST)
        .filter(|label| !label.starts_with(acyclic_fs::access::SCOPE_PREFIX))
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if let Some(ceiling) = crate::core::filesystem_scope_label(parent.capabilities(), &mut None)? {
        let (selected, denied) =
            crate::filesystem::ceiling_grants(ceiling, volumes, journal, &[], owner)?;
        allowed.retain(|label| !denied.iter().any(|prefix| label.starts_with(prefix)));
        allowed.extend(selected);
    }
    Ok(derived.capabilities().iter().all(|label| {
        allowed.iter().any(|held| held == label)
            && label != crate::contract::capability::HARNESS_HOST
            && !label.starts_with(acyclic_fs::access::SCOPE_PREFIX)
    }))
}

#[tokio::test]
async fn workload_scopes_never_widen_their_admission() -> Result<()> {
    use crate::contract::capability::HARNESS_HOST;
    use acyclic_fs::access::{Action, ActionSet, Capability};
    let path = AgentPath::root(&crate::agent::AgentId::from_bytes([231; 16]));
    let other = AgentPath::root(&crate::agent::AgentId::from_bytes([232; 16]));
    let owner = crate::AgentId::from_bytes(path.id_bytes());
    let provider = ProviderRef::new(
        "workload".into(),
        "filesystem".into(),
        "1".into(),
        &mut None,
    )?;
    let journal = crate::conversation::journal_volume(provider.clone(), &path.id())?;
    let other_journal = crate::conversation::journal_volume(provider.clone(), &other.id())?;
    let private = private_volume(provider.clone(), &path.id())?;
    let volumes = [WorkspaceEntry::initial_volume(
        crate::agent::WorkspaceRole::Private,
        PRIVATE_VOLUME.into(),
        private.clone(),
    )];
    let audience = Authority {
        kind: AggregateKind::Conversation,
        id: "workload".into(),
    };
    let root = AuthorityRoot::new("workload", [233; 32], audience);
    let native: RuntimeAuthority = root.clone().into();
    let verifier = root.verifier(&mut None)?;
    let workload = || crate::core::ScopeDerivation::Workload {
        journal: &journal,
        volumes: &volumes,
        external: &[],
    };
    let custody = |grants: Vec<String>, agent| {
        root.bootstrap_scope("custody", &Capabilities::new(grants), agent, &mut None)
    };
    let label = |capability: Capability| {
        capability
            .to_scope_label()
            .map_err(|error| Error::Invalid(error.to_string()))
    };
    let read = journal.capability(VolumeOperation::Read, &mut None)?;
    let write = journal.capability(VolumeOperation::Write, &mut None)?;
    // The own journal read is built from the agent's identity alone.
    let own = crate::conversation::OwnJournalRead::of(&provider, owner)?.into_label();
    assert_eq!(own, read);
    assert_ne!(
        own,
        other_journal.capability(VolumeOperation::Read, &mut None)?
    );
    let tree = label(Capability::whole_volume(None, &Action::ALL))?;
    let source_read = label(Capability::whole_volume(
        None,
        ActionSet::SourceRead.actions(),
    ))?;
    // A ceiling allowing `Read` alone withholds the list, history and fork
    // actions an explicit read label carries.
    let bare_read = label(Capability::whole_volume(None, &[Action::Read]))?;
    let mut subtree = Capability::whole_volume(None, &Action::ALL);
    subtree.layers[0][0].resource.path = "src/**".into();
    let subtree = label(subtree)?;
    // A workload derives only for its own journal's agent, and only from a
    // ceiling explicit labels can carry.
    for parent in [
        custody(
            vec![read.clone(), write.clone()],
            Some(crate::AgentId::from_bytes(other.id_bytes())),
        )?,
        custody(vec![subtree], None)?,
    ] {
        let refused = native
            .delegate(&parent, "agent", workload())
            .await
            .expect_err("a workload derived for another agent or a narrowed ceiling");
        assert_eq!(refused.code(), crate::wire::ErrorCode::Unauthorized);
    }
    // Every derivation path stays within its parent: with and without the
    // journal's labels, and under a whole, a reading and a bare-read ceiling,
    // the last beside an explicit write it must withhold.
    for (grants, agent, reads, writes) in [
        (
            vec![HARNESS_HOST.to_owned(), write.clone()],
            Some(owner),
            false,
            true,
        ),
        (
            vec![HARNESS_HOST.to_owned(), read.clone(), write.clone()],
            Some(owner),
            true,
            true,
        ),
        (vec![HARNESS_HOST.to_owned(), tree], None, true, false),
        (vec![source_read], None, true, false),
        (vec![bare_read, write.clone()], None, false, false),
    ] {
        let parent = custody(grants, agent)?;
        let derived = native.delegate(&parent, "agent", workload()).await?;
        assert_eq!(derived.agent(), Some(owner));
        assert!(
            within_parent(&parent, &derived, &volumes, &journal, owner)?,
            "a workload widened its parent: {:?}",
            derived.capabilities()
        );
        assert_eq!(derived.capabilities().contains(&read), reads);
        assert_eq!(derived.capabilities().contains(&write), writes);
        // No derivation reaches another agent's journal.
        for operation in [VolumeOperation::Read, VolumeOperation::Write] {
            let denied =
                ContentGrant::verify(&verifier, &derived, &other_journal, operation, &mut None)
                    .expect_err("a workload reached another agent's journal");
            assert_eq!(denied.code(), crate::wire::ErrorCode::Unauthorized);
        }
    }
    Ok(())
}

/// Every whole-volume ceiling built from the explicit labels' action sets,
/// and ownership last, each with the actions it allows.
fn label_ceilings() -> Vec<(Vec<acyclic_fs::access::Action>, bool)> {
    use acyclic_fs::access::{Action, ActionSet};
    let sets = [
        ActionSet::SourceRead,
        ActionSet::DestinationWrite,
        ActionSet::DestinationDelete,
    ];
    let mut ceilings: Vec<(Vec<Action>, bool)> = (1..1_usize << sets.len())
        .map(|mask| {
            let actions = sets
                .iter()
                .enumerate()
                .filter(|(bit, _)| mask & (1 << bit) != 0)
                .flat_map(|(_, set)| set.actions().iter().copied())
                .collect();
            (actions, false)
        })
        .collect();
    ceilings.push((Action::ALL.to_vec(), true));
    ceilings
}

/// One agent with its own private volume, and the workload each ceiling
/// derives for it.
struct CeilingFixture {
    root: AuthorityRoot,
    journal: VolumeRef,
    private: VolumeRef,
    volumes: [WorkspaceEntry; 1],
}

impl CeilingFixture {
    fn new(seed: u8) -> Result<Self> {
        let path = AgentPath::root(&crate::agent::AgentId::from_bytes([seed; 16]));
        let provider =
            ProviderRef::new("ceiling".into(), "filesystem".into(), "1".into(), &mut None)?;
        let private = private_volume(provider.clone(), &path.id())?;
        let audience = Authority {
            kind: AggregateKind::Conversation,
            id: "ceiling".into(),
        };
        Ok(Self {
            root: AuthorityRoot::new("ceiling", [seed; 32], audience),
            journal: crate::conversation::journal_volume(provider, &path.id())?,
            volumes: [WorkspaceEntry::initial_volume(
                crate::agent::WorkspaceRole::Private,
                PRIVATE_VOLUME.into(),
                private.clone(),
            )],
            private,
        })
    }

    async fn derive(&self, actions: &[acyclic_fs::access::Action]) -> Result<Result<Scope>> {
        self.derive_from(acyclic_fs::access::Capability::whole_volume(None, actions))
            .await
    }

    async fn derive_from(&self, ceiling: acyclic_fs::access::Capability) -> Result<Result<Scope>> {
        let ceiling = ceiling
            .to_scope_label()
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let parent =
            self.root
                .bootstrap_scope("custody", &Capabilities::new([ceiling]), None, &mut None)?;
        let native: RuntimeAuthority = self.root.clone().into();
        Ok(native
            .delegate(
                &parent,
                "agent",
                crate::core::ScopeDerivation::Workload {
                    journal: &self.journal,
                    volumes: &self.volumes,
                    external: &[],
                },
            )
            .await)
    }
}

#[tokio::test]
async fn a_ceiling_short_of_ownership_never_admits_the_raw_workspace() -> Result<()> {
    use acyclic_fs::access::ActionSet;
    let fixture = CeilingFixture::new(237)?;
    let host =
        FilesystemHost::<acyclic_fs::MemoryAuthorityBackend, acyclic_fs::MemoryObjectBackend>::new(
            acyclic_fs::Fs::memory(),
            fixture.private.provider(),
            None,
        )?;
    host.create_volume(&fixture.private).await?;
    let workspace = host
        .open(
            &workspace_ref(
                fixture.private.provider().clone(),
                &fixture.private.storage_name(&mut None)?,
            )?,
            None,
        )
        .await?;
    let verifier = fixture.root.verifier(&mut None)?;
    for (actions, owns) in label_ceilings() {
        let allows = |set: ActionSet| set.actions().iter().all(|action| actions.contains(action));
        let derived = fixture.derive(&actions).await?;
        // Read and write labels together admit the raw workspace, which
        // deletes and administers: a ceiling allowing both without owning
        // the volume has no explicit labels.
        if allows(ActionSet::SourceRead) && allows(ActionSet::DestinationWrite) && !owns {
            let refused = derived.expect_err("explicit labels widened a ceiling to ownership");
            assert_eq!(refused.code(), crate::wire::ErrorCode::Unauthorized);
            continue;
        }
        let derived = derived?;
        let raw = match crate::filesystem::WorkspaceAccess::new(
            &verifier,
            &derived,
            derived.capabilities(),
            Some(&fixture.private),
        ) {
            Ok(access) => access.require(&workspace).await.is_ok(),
            Err(_) => false,
        };
        assert_eq!(raw, owns, "raw workspace access under {actions:?}");
    }
    Ok(())
}

#[tokio::test]
async fn workload_labels_keep_every_operation_their_ceiling_allows() -> Result<()> {
    use acyclic_fs::access::ActionSet;
    let fixture = CeilingFixture::new(238)?;
    let verifier = fixture.root.verifier(&mut None)?;
    for (actions, owns) in label_ceilings() {
        let allows = |set: ActionSet| set.actions().iter().all(|action| actions.contains(action));
        let Ok(derived) = fixture.derive(&actions).await? else {
            assert!(!owns, "an owning ceiling was refused");
            continue; // Refused above, when the labels cannot carry it.
        };
        // Each operation is held exactly when the ceiling allows its label's
        // actions, independently of the others: a write-only ceiling writes.
        for (operation, set) in [
            (VolumeOperation::Read, ActionSet::SourceRead),
            (VolumeOperation::Write, ActionSet::DestinationWrite),
            (VolumeOperation::Delete, ActionSet::DestinationDelete),
        ] {
            assert_eq!(
                ContentGrant::verify(&verifier, &derived, &fixture.private, operation, &mut None)
                    .is_ok(),
                allows(set),
                "{operation:?} under {actions:?}"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn an_exact_volume_ceiling_carries_to_its_workload_unchanged() -> Result<()> {
    use acyclic_fs::access::{Action, Capability};
    let fixture = CeilingFixture::new(239)?;
    let exact = Capability::whole_volume(Some("exact".into()), &Action::ALL);
    let label = exact
        .to_scope_label()
        .map_err(|error| Error::Invalid(error.to_string()))?;
    // It reaches only the volume it names, so the workload holds the
    // parent's label and nothing more.
    let derived = fixture.derive_from(exact.clone()).await??;
    assert_eq!(derived.capabilities(), &Capabilities::new([label]));
    // Beside the host's selection by role, explicit labels can carry neither.
    let mut mixed = exact;
    mixed.layers[0].extend(
        Capability::whole_volume(None, &Action::ALL)
            .layers
            .into_iter()
            .flatten(),
    );
    let refused = fixture
        .derive_from(mixed)
        .await?
        .expect_err("a ceiling mixing exact volumes with the host's selection derived");
    assert_eq!(refused.code(), crate::wire::ErrorCode::Unauthorized);
    Ok(())
}

#[tokio::test]
async fn a_tree_admission_never_extends_to_another_agents_private_volumes() -> Result<()> {
    use crate::conversation::VolumeOperation;
    let path = AgentPath::root(&crate::agent::AgentId::from_bytes([234; 16]));
    let source = AgentPath::root(&crate::agent::AgentId::from_bytes([235; 16]));
    let provider = ProviderRef::new("tree".into(), "filesystem".into(), "1".into(), &mut None)?;
    let private = private_volume(provider.clone(), &path.id())?;
    let journal = crate::conversation::journal_volume(provider.clone(), &path.id())?;
    let source_private = private_volume(provider.clone(), &source.id())?;
    let source_journal = crate::conversation::journal_volume(provider, &source.id())?;
    // A fork binds its source's private volume beside its own.
    let volumes = [
        WorkspaceEntry::initial_volume(
            crate::agent::WorkspaceRole::Private,
            PRIVATE_VOLUME.into(),
            private.clone(),
        ),
        WorkspaceEntry::initial_volume(
            crate::agent::WorkspaceRole::Output,
            "source".into(),
            source_private.clone(),
        ),
    ];
    let audience = Authority {
        kind: AggregateKind::Conversation,
        id: "tree".into(),
    };
    let root = AuthorityRoot::new("tree", [236; 32], audience.clone());
    // The scope a served tree admits every agent under: bound to no agent,
    // with one structured label over every volume.
    let tree = acyclic_fs::access::Capability::whole_volume(None, &acyclic_fs::access::Action::ALL)
        .to_scope_label()
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let served = root.bootstrap_scope("served", &Capabilities::new([tree]), None, &mut None)?;
    let admission = TurnAdmission::Admitted {
        audience,
        scope: served,
    };
    let native: RuntimeAuthority = root.clone().into();
    let authority = AdmittedAgent::verify(&native, &admission, &path, &volumes)?
        .authority(&[], None)
        .await?;
    let verifier = root.verifier(&mut None)?;
    ContentGrant::verify(
        &verifier,
        &authority.scope,
        &private,
        VolumeOperation::Write,
        &mut None,
    )?;
    // The agent keeps only its own journal's read, issued as `OwnJournalRead`.
    assert!(
        authority
            .grants
            .contains(&journal.capability(VolumeOperation::Read, &mut None)?),
        "a workload lost its own journal's read"
    );
    for volume in [&source_private, &source_journal] {
        for operation in [VolumeOperation::Read, VolumeOperation::Write] {
            assert!(
                !authority
                    .grants
                    .contains(&volume.capability(operation, &mut None)?),
                "a workload named another private volume or its whole journal"
            );
        }
        assert!(
            !crate::core::directory_read_granted_with_work(
                &authority.grants,
                volume,
                "",
                &mut None
            )?,
            "a tree admission reached a private volume by its structured label"
        );
    }
    for volume in [&source_private, &source_journal] {
        let refused = ContentGrant::verify(
            &verifier,
            &authority.scope,
            volume,
            VolumeOperation::Read,
            &mut None,
        )
        .expect_err("a tree admission granted another agent's private volume");
        assert_eq!(refused.code(), crate::wire::ErrorCode::Unauthorized);
    }
    Ok(())
}
