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
async fn child_activity_uses_host_custody_without_rebinding_workload_identity() -> Result<()> {
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
    let audience = Authority {
        kind: AggregateKind::Conversation,
        id: parent.id().to_string(),
    };
    let root = AuthorityRoot::new("activity-admission", [197; 32], audience.clone());
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
        scope: scope.clone(),
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
    // Negative control: the former query really rejects this valid parent.
    assert!(matches!(
        query_journal(&stream, &host, &issuer, &admission, &state).await,
        Err(Error::Unauthorized(_))
    ));
    let operation = OperationId::new();
    let journal = query_activity_journal(&stream, &host, &issuer, &state)
        .await?
        .ok_or_else(|| Error::Storage("native host lost child activity authority".into()))?
        .with_turn(operation, operation);
    journal
        .append(
            operation,
            "start",
            &crate::executor::ExecutionEvent::Started {
                request_digest: [1; 32],
            },
            None,
        )?
        .await?;
    assert!(
        journal
            .activity(operation, &acyclic_actors::local::SystemClock)
            .await?
            .is_some()
    );

    let broad = Capabilities::new([
        acyclic_fs::access::Capability::whole_volume(None, &acyclic_fs::access::Action::ALL)
            .to_scope_label()
            .map_err(|error| Error::Invalid(error.to_string()))?,
        crate::contract::capability::HARNESS_HOST.to_owned(),
    ]);
    let broad = root.bootstrap_scope("unbound-host", &broad, None, &mut None)?;
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
        let observed = query_activity_journal(&stream, &host, &issuer, &state).await?;
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
                acyclic_fs::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(|failure| crate::filesystem::map_error(failure.error))?
            .value;
        let private_head = private_workspace
            .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(|failure| crate::filesystem::map_error(failure.error))?
            .value;
        let missing = private_head
            .read(
                "/.system/interactions/custody.json",
                file_bytes,
                acyclic_fs::WorkBudget::UNBOUNDED,
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
                .read(&turn_files[0], 4096, None, None)?
                .await?
                .as_ref(),
            b"admitted steering"
        );
        for file in [&turn_files[1], &manifest] {
            let denied = async { task_reader.read(file, 4096, None, None)?.await }
                .await
                .expect_err("a turn input route granted another turn or host controls");
            assert_eq!(denied.code(), crate::wire::ErrorCode::Unauthorized);
        }
        assert!(
            ContentGrant::verify(
                &task.verifier,
                &task.scope,
                &journal,
                VolumeOperation::Write,
                &mut None,
            )
            .is_err()
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
                acyclic_fs::WorkBudget::UNBOUNDED,
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
                acyclic_fs::WorkBudget::UNBOUNDED,
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
    let runtime: RuntimeAuthority = issuer.clone().into();
    let admitted = AdmittedAgent::verify(&runtime, &TurnAdmission::LocalRoot, &path, &volumes)?;
    let (_, custody) = admitted.journal_authority(&host, &[], &[]).await?;
    let grant = ContentGrant::verify(
        &custody.verifier,
        &custody.scope,
        &journal,
        VolumeOperation::Write,
        &mut None,
    )?;
    let operation = OperationId::from_bytes([216; 16]);
    let mut staged = Vec::new();
    for (class, bytes) in [("summaries", "summary"), ("execution", "execution record")] {
        staged.push(
            host.put_content(
                &journal,
                &grant,
                &format!(".system/{class}/{operation}/{}.json", "a".repeat(64)),
                bytes.as_bytes(),
                "text/plain",
                "record.json",
                Some(1024),
                &crate::IdempotencyKey::new(class)?,
                None,
                Default::default(),
            )
            .await?,
        );
    }
    let execution = staged
        .pop()
        .ok_or_else(|| Error::NotFound("execution record".into()))?;
    let summary = staged
        .pop()
        .ok_or_else(|| Error::NotFound("summary".into()))?;
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
    // Negative control: the same agent without the journal read is refused
    // the summary, so the grant, not the agent's identity, admits it.
    let narrowed = issuer.bootstrap_scope(
        "narrowed",
        &Capabilities::new([private.capability(VolumeOperation::Read, &mut None)?]),
        Some(owner),
        &mut None,
    )?;
    let narrowed =
        FilesystemContentVerifier::new(host.clone(), &authority.verifier, &narrowed, 1024, None)?;
    let denied = async { narrowed.read(&summary, 1024, None, None)?.await }
        .await
        .expect_err("a scope without the journal read projected a summary");
    assert_eq!(denied.code(), crate::wire::ErrorCode::Unauthorized);
    Ok(())
}
