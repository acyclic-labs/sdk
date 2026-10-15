use super::*;

#[tokio::test]
async fn admitted_custom_merge_candidate_reopens_without_rerunning_driver() -> Result<()> {
    use acyclic_fs::{
        ConflictKey, ConflictSide, JoinOutcome, MemoryMergeResolutionCache, MergeDriver,
        MergeDriverRegistry, MergeResolution, resolve_merge_plan,
    };
    use acyclic_harness::conversation::ContentGrant;
    use acyclic_harness::core::Reducer;
    use acyclic_harness::filesystem::{
        FilesystemWorkflowJournal, ParentProjectController, WorkspaceMutation, workspace_ref,
    };
    use acyclic_harness::workflow::{MachineCheckpoint, WorkflowAdmission};

    struct Driver(AtomicUsize);
    impl MergeDriver for Driver {
        fn fingerprint(&self) -> &[u8] {
            b"review-policy@7"
        }
        fn resolve(
            &self,
            conflict: &acyclic_fs::ConflictView,
        ) -> std::result::Result<MergeResolution, acyclic_fs::DriverError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(match conflict.key {
                ConflictKey::File(_) | ConflictKey::ContentRange { .. } => {
                    MergeResolution::Text("custom resolution".into())
                }
                _ => MergeResolution::Select(ConflictSide::Theirs),
            })
        }
    }
    let directory = workspace()?;
    let options = LocalOptions::new(directory.path().join("filesystem"));
    let stream_root = directory.path().join("streams");
    let provider = ProviderRef::new(
        "custom-merge".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let agent = AgentId::from_bytes([141; 16]);
    let project = VolumeRef::new(
        provider.clone(),
        "project".into(),
        VolumeClass::Project,
        VolumeOwner::Project("review".into()),
        &mut None,
    )?;
    let source = VolumeRef::new(
        provider.clone(),
        "source".into(),
        VolumeClass::Project,
        project.owner().clone(),
        &mut None,
    )?;
    let private = VolumeRef::new(
        provider.clone(),
        "private".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )?;
    let authority = Authority {
        kind: AggregateKind::Conversation,
        id: "custom-merge-parent".into(),
    };
    let issuer = AuthorityIssuer::new("custom-merge", [142; 32], authority.clone());
    let signed = issuer
        .issue(
            "owner",
            &Capabilities::new([
                "conversation:bind".into(),
                "fork:publish".into(),
                "project:merge".into(),
                project.capability(VolumeOperation::Read, &mut None)?,
                project.capability(VolumeOperation::Write, &mut None)?,
                source.capability(VolumeOperation::Read, &mut None)?,
                source.capability(VolumeOperation::Write, &mut None)?,
                private.capability(VolumeOperation::Read, &mut None)?,
                private.capability(VolumeOperation::Write, &mut None)?,
            ]),
            Some(agent),
            None,
            &mut None,
        )
        .expect("scope proof");
    let mut parent = Reducer::new(
        authority,
        issuer.verifier(&mut None).expect("verifier copy"),
        SchemaRegistry::new(),
    );
    parent.apply(&Command {
        operation_id: OperationId::from_bytes([143; 16]),
        idempotency_key: IdempotencyKey::new("bind")?,
        expected_revision: 0,
        scope: signed.clone(),
        causal_parent: None,
        action: Action::BindConversation { agent },
    })?;
    let driver = Arc::new(Driver(AtomicUsize::new(0)));
    let operation = OperationId::from_bytes([144; 16]);
    let admission;
    let resolution_record;
    let source_head;
    let target_head;
    {
        let host = FilesystemHost::new(
            Fs::local(options.clone())
                .await
                .map_err(|e| Error::Storage(e.to_string()))?,
            &provider,
            None,
        )?;
        host.create_volume(&project).await?;
        host.create_volume(&private).await?;
        let write = ContentGrant::verify(
            &issuer.verifier(&mut None).expect("verifier copy"),
            &signed,
            &project,
            VolumeOperation::Write,
            65_536,
            &mut None,
        )?;
        host.put_content(
            &project,
            &write,
            "shared.txt",
            b"base",
            "text/plain",
            "shared.txt",
            128,
            &IdempotencyKey::new("base")?,
            None,
            None,
        )
        .await?;
        let controller = ParentProjectController::new(
            &host,
            &parent,
            &issuer.verifier(&mut None).expect("verifier copy"),
            &signed,
            &project,
            None,
        )?;
        let base = host
            .resolve(
                &workspace_ref(provider.clone(), &project.storage_name(&mut None)?)?,
                None,
            )
            .await?;
        let fork = controller
            .fork_project(&base.generation, &source, &IdempotencyKey::new("fork")?)
            .await?;
        source_head = host
            .apply(
                &fork.workspace,
                Some(&fork.generation),
                &[WorkspaceMutation::PutFile {
                    path: "/shared.txt".into(),
                    bytes: b"source".as_slice().into(),
                }],
                &IdempotencyKey::new("source-edit")?,
            )
            .await?;
        let target = workspace_ref(provider.clone(), &project.storage_name(&mut None)?)?;
        target_head = host
            .apply(
                &target,
                None,
                &[WorkspaceMutation::PutFile {
                    path: "/shared.txt".into(),
                    bytes: b"target".as_slice().into(),
                }],
                &IdempotencyKey::new("target-edit")?,
            )
            .await?;
        let plan = controller
            .prepare_project_import(&source, &source_head, None)
            .await?;
        let JoinOutcome::Conflicted {
            conflicts,
            truncated,
        } = controller
            .apply_project_merge(&plan, operation, BTreeMap::new())
            .await?
            .0
        else {
            return Err(Error::Invalid(
                "custom merge must have a real conflict".into(),
            ));
        };
        let described = controller
            .describe_project_merge_conflicts(&plan, &conflicts, truncated)
            .await?;
        let state = json!({"policy":"review", "revision":"7", "plan":acyclic_fs::compat_wire::encode_merge_plan(&described)
            .map_err(|e| Error::Invalid(e.to_string()))?});
        admission = WorkflowAdmission {
            operation_id: operation,
            request_digest: *blake3::hash(
                &acyclic_harness::executor::json::canonical_json_bytes_bounded(
                    &state,
                    acyclic_harness::executor::json::JsonBounds {
                        maximum_bytes: 65_536,
                        maximum_items: 65_536,
                        maximum_depth: 128,
                    },
                    None,
                    &mut |_| Ok(()),
                )?,
            )
            .as_bytes(),
            initial: MachineCheckpoint {
                machine: MachineIdentity {
                    name: "test.custom-merge".into(),
                    version: "7".into(),
                    digest: [145; 32],
                },
                revision: 0,
                state,
            },
        };
        let journal = FilesystemWorkflowJournal::new(
            Arc::new(
                LocalStream::open(&stream_root, LocalStreamLimits::default())
                    .await
                    .map_err(|e| Error::Storage(e.to_string()))?,
            ),
            host.clone(),
            "custom-merge",
            acyclic_harness::filesystem::FilesystemJournalBinding {
                volume: &private,
                verifier: &issuer.verifier(&mut None).expect("verifier copy"),
                scope: &signed,
                maximum_bytes: 65_536,
                io: None,
            },
        )?;
        assert_eq!(journal.admit(admission.clone()).await?, admission);
        let mut registry = MergeDriverRegistry::new();
        registry
            .register("review", driver.clone())
            .map_err(|e| Error::Invalid(e.to_string()))?;
        registry
            .set_default("review")
            .map_err(|e| Error::Invalid(e.to_string()))?;
        let candidate = resolve_merge_plan(
            described,
            &registry,
            &mut MemoryMergeResolutionCache::default(),
            false,
        )
        .map_err(|e| Error::Invalid(e.to_string()))?;
        let input = json!(
            acyclic_fs::compat_wire::encode_merge_candidate(&candidate)
                .map_err(|e| Error::Invalid(e.to_string()))?
        );
        let state = json!({"candidate":input});
        resolution_record = WorkflowRecord {
            operation_id: OperationId::from_bytes([146; 16]),
            idempotency_key: IdempotencyKey::new("driver-result")?,
            prior: admission.initial.clone(),
            input,
            transition: MachineTransition {
                state,
                commands: Vec::new(),
                status: MachineStatus::Suspended,
            },
        };
        journal
            .commit(
                0,
                resolution_record.idempotency_key.clone(),
                resolution_record.clone(),
            )
            .await?;
    }
    let calls = driver.0.load(Ordering::SeqCst);
    assert!(calls > 0);
    for retry in 0..2 {
        let host = FilesystemHost::new(
            Fs::local(options.clone())
                .await
                .map_err(|e| Error::Storage(e.to_string()))?,
            &provider,
            None,
        )?;
        let journal = FilesystemWorkflowJournal::new(
            Arc::new(
                LocalStream::open(&stream_root, LocalStreamLimits::default())
                    .await
                    .map_err(|e| Error::Storage(e.to_string()))?,
            ),
            host.clone(),
            "custom-merge",
            acyclic_harness::filesystem::FilesystemJournalBinding {
                volume: &private,
                verifier: &issuer.verifier(&mut None).expect("verifier copy"),
                scope: &signed,
                maximum_bytes: 65_536,
                io: None,
            },
        )?;
        let retained = journal
            .admission(None)?
            .await?
            .0
            .ok_or_else(|| Error::NotFound("merge admission".into()))?;
        assert_eq!(retained, admission);
        let records = journal.replay(0, 1, None)?.await?.0;
        assert_eq!(records, vec![resolution_record.clone()]);
        let record = records
            .first()
            .ok_or_else(|| Error::NotFound("driver result".into()))?;
        let candidate = acyclic_fs::compat_wire::decode_merge_candidate(
            record.transition.state["candidate"]
                .as_str()
                .ok_or_else(|| Error::Invalid("candidate missing".into()))?,
        )
        .map_err(|e| Error::Invalid(e.to_string()))?;
        let controller = ParentProjectController::new(
            &host,
            &parent,
            &issuer.verifier(&mut None).expect("verifier copy"),
            &signed,
            &project,
            None,
        )?;
        let plan = controller
            .prepare_project_import_at(&source, &source_head, &target_head, operation)
            .await?;
        let denied = issuer
            .issue(
                "no-write",
                &Capabilities::new([source.capability(VolumeOperation::Read, &mut None)?]),
                Some(agent),
                None,
                &mut None,
            )
            .expect("scope proof");
        let unauthorized = ParentProjectController::new(
            &host,
            &parent,
            &issuer.verifier(&mut None).expect("verifier copy"),
            &denied,
            &project,
            None,
        )?;
        assert!(
            unauthorized
                .apply_project_merge_candidate(&plan, operation, &candidate)
                .await
                .is_err()
        );
        let mut changed = candidate.clone();
        changed.plan.ours = candidate.plan.base;
        assert!(
            controller
                .apply_project_merge_candidate(&plan, operation, &changed)
                .await
                .is_err()
        );
        let outcome = controller
            .apply_project_merge_candidate(&plan, operation, &candidate)
            .await?;
        assert!(if retry == 0 {
            matches!(outcome, JoinOutcome::Applied(_))
        } else {
            matches!(outcome, JoinOutcome::AlreadyApplied(_))
        });
        if retry == 0 {
            let write = ContentGrant::verify(
                &issuer.verifier(&mut None).expect("verifier copy"),
                &signed,
                &project,
                VolumeOperation::Write,
                65_536,
                &mut None,
            )?;
            host.put_content(
                &project,
                &write,
                "concurrent.txt",
                b"user edit",
                "text/plain",
                "concurrent.txt",
                128,
                &IdempotencyKey::new("concurrent")?,
                None,
                None,
            )
            .await?;
        }
        assert_eq!(
            host.read(
                &workspace_ref(provider.clone(), &project.storage_name(&mut None)?)?,
                None,
                "/concurrent.txt",
                128,
                None
            )
            .await?
            .as_ref(),
            b"user edit"
        );
        assert_eq!(
            host.read(
                &workspace_ref(provider.clone(), &project.storage_name(&mut None)?)?,
                None,
                "/shared.txt",
                128,
                None
            )
            .await?
            .as_ref(),
            b"custom resolution"
        );
        assert_eq!(
            host.resolve(
                &workspace_ref(provider.clone(), &source.storage_name(&mut None)?)?,
                None
            )
            .await?
            .generation,
            source_head
        );
    }
    assert_eq!(driver.0.load(Ordering::SeqCst), calls);
    Ok(())
}
