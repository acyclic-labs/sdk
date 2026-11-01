use super::*;

#[tokio::test]
async fn native_queries_select_original_stdout_and_keep_its_complete_spill() -> Result<()> {
    let (_directory, _state, harness, publisher) = native_fixture().await?;
    publisher
        .stage_once(
            OperationId::new(),
            "query.txt",
            b"line0\nline1\nline2\nMiXeD\nmixed\nMIXED\n",
            "text/plain",
            "query.txt",
            crate::conversation::ContentAccess::default(),
        )?
        .await
        .map(|(p, hold)| (p.file, hold))?;
    let grants = Capabilities::new(
        harness
            .signed
            .capabilities()
            .iter()
            .chain(["tool:call:shell"]),
    );
    let denied = Capabilities::new(grants.iter().filter(|grant| *grant != "tool:call:shell"));
    let context = |grants| -> Result<_> {
        let runtime = crate::runtime::Bindings {
            scope: RuntimeScope::new(grants, harness.limits.clone())?,
            ..crate::runtime::Bindings::local()
        }
        .build()?;
        runtime
            .agent_context(
                &Capabilities::default(),
                crate::runtime::ContentBindings {
                    volume: harness.journal.clone(),
                    reader: harness.client_reader()?,
                    writer: Some(Publisher::new(
                        harness.files.clone(),
                        &harness.journal,
                        &harness.verifier,
                        &harness.signed,
                        harness.limits.file_bytes,
                        None,
                    )?),
                },
            )
            .map(Arc::new)
    };
    let task = context(grants)?;
    let denied = context(denied)?;
    let mut original = crate::tools::catalog()
        .find(|tool| tool.name == "shell")
        .ok_or_else(|| Error::NotFound("stock shell".into()))?
        .bind()?;
    original.executor = publisher.tools.clone();
    let original = Arc::new(original);
    for profile in [
        crate::model::NativeToolProfile::Anthropic,
        crate::model::NativeToolProfile::Google,
    ] {
        let revisions = profile.tools()?;
        let revision = revisions
            .iter()
            .find(|revision| revision.capability == "search")
            .ok_or_else(|| Error::NotFound("native search".into()))?;
        let mut declarations = crate::tools::native::NativeDeclarations::new(profile, None)?;
        let (tool, _custody) = declarations.bind_variant(revision, original.clone())?;
        let cc = profile == crate::model::NativeToolProfile::Anthropic;
        let operation_id = OperationId::new();
        let invocation = ToolInvocation {
            operation_id,
            call_id: "native-query".into(),
            name: tool.definition.name.clone(),
            arguments: if cc {
                serde_json::json!({"pattern":"^line","path":"query.txt","output_mode":"content","-n":false,"head_limit":1,"offset":1})
            } else {
                serde_json::json!({"pattern":"mixed","dir_path":"query.txt"})
            },
        };
        // Generated file input, with output from the actual registered process
        // executor and installed rg binary; no tool transcript is a fixture.
        let result = tool
            .executor
            .execute(
                invocation.clone(),
                Some(ToolContext::new(
                    task.clone(),
                    operation_id,
                    "native-query",
                )?),
            )
            .await?;
        assert_eq!(result.value["exit_code"], 0);
        assert_eq!(result.value["omitted_bytes"], 0);
        if cc {
            assert_eq!(result.value["stdout"], "line1\n");
            let spill = result.value["spill"]
                .as_str()
                .ok_or_else(|| Error::NotFound("complete query output".into()))?;
            let bytes = {
                let state = lock_owned(StateArc::clone(&publisher.tools.state)).await;
                let (mount, relative) = state.mounts.resolve(std::path::Path::new(spill))?;
                let path = mount
                    .native_root
                    .as_ref()
                    .map_or_else(|| state.mounts.root.join(spill), |root| root.join(relative));
                std::fs::read(path).map_err(storage)?
            };
            assert_eq!(bytes, b"line0\nline1\nline2\n");
        } else {
            assert_eq!(result.value["stdout"], "4:MiXeD\n5:mixed\n6:MIXED\n");
        }
        // The original command authority still controls a cached operation.
        // A schema adapter cannot satisfy this through another executor.
        assert!(matches!(
            tool.executor
                .execute(
                    invocation,
                    Some(ToolContext::new(
                        denied.clone(),
                        operation_id,
                        "native-query"
                    )?),
                )
                .await,
            Err(Error::Unauthorized(_))
        ));
    }
    Ok(())
}

async fn native_fixture() -> Result<(
    tempfile::TempDir,
    tempfile::TempDir,
    LocalHarness,
    CheckoutPublisher,
)> {
    let directory = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
    let model = Model::new("scripted".into(), "publication".into(), "1".into())?;
    let provider = Arc::new(crate::test_support::ScriptedModelProvider::new([]));
    let harness = LocalHarness::open(
        crate::test_support::local_harness_config(directory.path(), (model, provider))?
            .state_directory(state.path()),
    )
    .await?;
    let tools = Arc::new(
        CheckoutTools::new(
            harness
                .workspace
                .as_ref()
                .ok_or_else(|| Error::NotFound("checkout".into()))?
                .clone(),
            &harness.project,
            Arc::new(acyclic_stream::SystemUnixMillisClock),
            &harness.signed,
            &harness.verifier,
        )
        .await?,
    );
    let publisher = CheckoutPublisher::new(
        tools,
        harness.files.clone(),
        &harness.project,
        &harness.verifier,
        harness.limits.file_bytes,
    );
    Ok((directory, state, harness, publisher))
}

#[tokio::test]
async fn selected_branch_publication_and_recovery_advance_the_selected_mount() -> Result<()> {
    let (_directory, _state, harness, initial) = native_fixture().await?;
    let task = Arc::new(crate::runtime::test_context::agent_context(
        RuntimeScope::new(harness.signed.capabilities().clone(), Default::default())?,
        None,
    )?);
    let root = &initial.tools.workspace;
    let stop = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(acyclic_fs::WorkBudget::UNBOUNDED, &stop)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let view = root
        .view()
        .open_related(
            root.view().workspace().clone(),
            &mut acyclic_fs::kernel::KernelAccounting::NativeOriginal {
                work: &work,
                cancellation: &stop,
            },
        )
        .await
        .map_err(storage)?;
    let mut mounted = root
        .mount_task_view(
            view,
            &*task,
            None,
            &crate::filesystem::NativeMountAuthority::admitted(
                harness.verifier.clone(),
                harness.signed.copy(&mut None)?,
            ),
        )
        .await?;
    // Compile an additional task policy through the original mount admission.
    // File creation and deletion here are permitted independently of writes.
    use acyclic_fs::access::{Action, Capability, Context, Effect, Principal, PrincipalKind};
    let principal = Principal {
        kind: PrincipalKind::Agent,
        id: "owner".into(),
        lineage: vec!["owner".into()],
        groups: Default::default(),
        roles: Default::default(),
        tags: Default::default(),
        attributes: Default::default(),
    };
    let view = |original: &crate::workspace::VolumeView<
        Option<Arc<crate::local::workspace::MountedVolume>>,
    >| {
        let mut policy = (*original.policy).clone();
        for (path, actions) in [
            ("action-only", vec![Action::Write]),
            ("no-delete", vec![Action::Write, Action::Delete]),
            ("no-create", vec![Action::Create]),
        ] {
            let mut deny = policy.layers[0][0].clone();
            deny.id = path.into();
            deny.effect = Effect::Deny;
            deny.actions = actions.into_iter().collect();
            deny.resource.path = path.into();
            policy.layers[0].push(deny);
        }
        crate::workspace::VolumeView {
            name: original.name.clone(),
            agent: original.agent.clone(),
            volume: original.volume.clone(),
            lineage: original.lineage.clone(),
            generation: original.generation,
            head: original.head,
            config: original.config,
            policy: Arc::new(policy),
            source: original.source.clone(),
        }
    };
    let mut alias = view(&mounted.mounts.named(None)?.1.view);
    alias.name = "project-alias".into();
    let mounts = Arc::new(crate::workspace::compose(
        mounted.path.clone(),
        &principal,
        &Capability::whole_volume(None, &Action::ALL),
        Context {
            now: 0,
            approvals: 0,
        },
        view(&mounted.mounts.named(None)?.1.view),
        mounted
            .mounts
            .mounts()
            .filter(|(path, _)| !path.is_empty())
            .map(|(_, mount)| view(&mount.view))
            .chain(std::iter::once(alias)),
    )?);
    // Repeated identities keep their first canonical visible binding. A
    // missing identity must refuse instead of falling back to another mount.
    assert!(std::ptr::eq(
        mounts.by_volume(&harness.project, None)?,
        mounts.named(None)?.1,
    ));
    let absent = crate::conversation::VolumeRef::new(
        harness.project.provider().clone(),
        "unmounted".into(),
        crate::conversation::VolumeClass::Project,
        crate::conversation::VolumeOwner::Project("unmounted".into()),
        &mut None,
    )?;
    assert!(matches!(
        mounts.by_volume(&absent, None),
        Err(Error::Unauthorized(_))
    ));
    Arc::get_mut(&mut mounted).unwrap().mounts = Arc::clone(&mounts);
    lock_owned(StateArc::clone(&mounted.state)).await.mounts = mounts;
    let tools = Arc::new(
        CheckoutTools::new(
            Arc::clone(&mounted),
            &harness.project,
            Arc::new(acyclic_stream::SystemUnixMillisClock),
            &harness.signed,
            &harness.verifier,
        )
        .await?,
    );
    let publisher = CheckoutPublisher::new(
        Arc::clone(&tools),
        Arc::clone(&harness.files),
        &harness.project,
        &harness.verifier,
        harness.limits.file_bytes,
    );
    use crate::conversation::{ContentAccess, ContentChange, ContentChangeResult};
    let create = [ContentChange::CreateAbsent {
        path: "action-only",
        bytes: b"created",
        media_type: "text/plain",
        display_name: "action-only",
    }];
    let operation = OperationId::new();
    let (created, _) = publisher
        .stage_changes(
            &harness.project,
            operation,
            &create,
            ContentAccess::default(),
        )?
        .await?;
    assert!(matches!(
        &created.members[0],
        ContentChangeResult::Created(_)
    ));
    let encoding_work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget {
            bytes_encoded: 64 * 1024,
            allocation_operations: 1024,
            peak_allocation_bytes: 1024 * 1024,
            ..acyclic_fs::WorkBudget::UNBOUNDED
        },
        &stop,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let (encoded, _custody) = crate::tool::value(&created, 4096, Some(&encoding_work))?;
    assert_eq!(encoded.as_array().unwrap().len(), 1);
    assert_eq!(encoded[0]["path"], "action-only");
    assert_eq!(
        publisher
            .observe_changes(&harness.project, operation, 4096, ContentAccess::default())?
            .await?
            .0,
        Some(created)
    );
    let (created, _) = publisher
        .stage_changes(
            &harness.project,
            operation,
            &create,
            ContentAccess::default(),
        )?
        .await?;
    assert_eq!(
        std::fs::read(mounted.path.join("action-only")).map_err(storage)?,
        b"created"
    );
    let update = [ContentChange::UpdateExpected {
        source: created.members[0].file().unwrap(),
        bytes: b"forbidden",
    }];
    assert!(
        publisher
            .stage_changes(
                &harness.project,
                OperationId::new(),
                &update,
                ContentAccess::default()
            )?
            .await
            .is_err()
    );
    assert_eq!(
        std::fs::read(mounted.path.join("action-only")).map_err(storage)?,
        b"created"
    );
    let deletion = [ContentChange::DeleteExpected {
        source: created.members[0].file().unwrap(),
    }];
    let operation = OperationId::new();
    let (deleted, _) = publisher
        .stage_changes(
            &harness.project,
            operation,
            &deletion,
            ContentAccess::default(),
        )?
        .await?;
    assert_eq!(
        &deleted.members,
        &[ContentChangeResult::Deleted("action-only".into())]
    );
    assert_eq!(
        publisher
            .observe_changes(&harness.project, operation, 4096, ContentAccess::default())?
            .await?
            .0,
        Some(deleted)
    );
    assert!(!mounted.path.join("action-only").exists());
    let denied = [ContentChange::CreateAbsent {
        path: "no-create",
        bytes: b"denied",
        media_type: "text/plain",
        display_name: "no-create",
    }];
    assert!(
        publisher
            .stage_changes(
                &harness.project,
                OperationId::new(),
                &denied,
                ContentAccess::default()
            )?
            .await
            .is_err()
    );
    assert!(!mounted.path.join("no-create").exists());
    let retained = [ContentChange::CreateAbsent {
        path: "no-delete",
        bytes: b"retained",
        media_type: "text/plain",
        display_name: "no-delete",
    }];
    let (retained, _) = publisher
        .stage_changes(
            &harness.project,
            OperationId::new(),
            &retained,
            ContentAccess::default(),
        )?
        .await?;
    let deletion = [ContentChange::DeleteExpected {
        source: retained.members[0].file().unwrap(),
    }];
    assert!(
        publisher
            .stage_changes(
                &harness.project,
                OperationId::new(),
                &deletion,
                ContentAccess::default()
            )?
            .await
            .is_err()
    );
    // The storage writer can author a deletion under its own wider authority.
    // Recovery through this mounted task must still apply its Delete decision.
    let operation = OperationId::new();
    publisher
        .writer_for(&harness.project, mounted.view().workspace(), None)?
        .stage_changes(
            &harness.project,
            operation,
            &deletion,
            ContentAccess::default(),
        )?
        .await?;
    assert!(
        publisher
            .observe_changes(&harness.project, operation, 4096, ContentAccess::default())?
            .await
            .is_err()
    );
    assert_eq!(
        std::fs::read(mounted.path.join("no-delete")).map_err(storage)?,
        b"retained"
    );
    let operation = OperationId::new();
    publisher
        .writer_for(&harness.project, mounted.view().workspace(), None)?
        .stage_changes(
            &harness.project,
            operation,
            &[ContentChange::CreateAbsent {
                path: "no-delete",
                bytes: b"retained",
                media_type: "text/plain",
                display_name: "no-delete",
            }],
            ContentAccess::default(),
        )?
        .await?;
    assert!(
        publisher
            .observe_changes(&harness.project, operation, 4096, ContentAccess::default())?
            .await?
            .0
            .is_some()
    );
    publisher
        .stage_once(
            OperationId::new(),
            "branch.txt",
            b"original",
            "text/plain",
            "branch.txt",
            crate::conversation::ContentAccess::default(),
        )?
        .await
        .map(|(p, hold)| (p.file, hold))?;
    let context = ToolContext::new(task, OperationId::new(), "select-branch")?;
    {
        let mut state = lock_owned(StateArc::clone(&tools.state)).await;
        tools
            .fs(
                crate::tools::parse_fs_command("checkout -b topic")?,
                &context,
                &mut state,
                &mounted,
            )
            .await?;
    }
    let selected = {
        let state = lock_owned(StateArc::clone(&tools.state)).await;
        publisher.selected_workspace(&state)?
    };
    assert_ne!(selected.path, mounted.path);
    for broken in [false, true] {
        let operation = OperationId::new();
        let bytes = if broken {
            b"counterfactual".as_slice()
        } else {
            b"selected".as_slice()
        };
        super::tests::PUBLISH_TO_INITIAL_MOUNT.set(broken);
        let staged = publisher
            .stage_once(
                operation,
                "branch.txt",
                bytes,
                "text/plain",
                "branch.txt",
                crate::conversation::ContentAccess::default(),
            )?
            .await
            .map(|(p, hold)| (p.file, hold));
        super::tests::PUBLISH_TO_INITIAL_MOUNT.set(false);
        let (file, _) = staged?;
        let visible = std::fs::read(selected.path.join("branch.txt")).map_err(storage)?;
        assert_eq!(
            visible == bytes,
            !broken,
            "publication advanced the wrong branch mount"
        );
        super::tests::PUBLISH_TO_INITIAL_MOUNT.set(broken);
        let recovered = publisher
            .observe_staged_once(
                operation,
                None,
                64,
                crate::conversation::ContentAccess::default(),
            )?
            .await
            .map(|(p, hold)| (p.map(|p| p.file), hold));
        super::tests::PUBLISH_TO_INITIAL_MOUNT.set(false);
        assert_eq!(recovered?.0, Some(file.clone()));
        let visible = std::fs::read(selected.path.join("branch.txt")).map_err(storage)?;
        assert_eq!(
            visible == bytes,
            !broken,
            "recovery advanced the wrong branch mount"
        );
        // The original observation repairs the deliberately unadvanced branch
        // without another write or generation, and retains the immutable receipt.
        assert_eq!(
            publisher
                .observe_staged_once(
                    operation,
                    None,
                    64,
                    crate::conversation::ContentAccess::default(),
                )?
                .await
                .map(|(p, hold)| (p.map(|p| p.file), hold))?
                .0,
            Some(file)
        );
        assert_eq!(
            std::fs::read(selected.path.join("branch.txt")).map_err(storage)?,
            bytes
        );
        assert_eq!(
            std::fs::read(mounted.path.join("branch.txt")).map_err(storage)?,
            b"original"
        );
    }
    Ok(())
}

#[tokio::test]
async fn file_publication_materializes_its_exact_committed_bytes() -> Result<()> {
    let (directory, _state, harness, publisher) = native_fixture().await?;
    let (file, _) = publisher
        .stage_once(
            OperationId::new(),
            "published.txt",
            b"exact bytes",
            "text/plain",
            "published.txt",
            crate::conversation::ContentAccess::default(),
        )?
        .await
        .map(|(p, hold)| (p.file, hold))?;
    assert_eq!(file.volume(), &harness.project);
    assert_eq!(
        std::fs::read(directory.path().join("published.txt")).map_err(storage)?,
        b"exact bytes"
    );
    assert!(!directory.path().join(".system").exists());
    // Private metadata stores the publication operation, whereas the returned
    // reference stores its immutable generation. Full reads authenticate bytes
    // and select that generation without assuming a literal staging version.
    let private = Publisher::new(
        harness.files.clone(),
        &harness.journal,
        &harness.verifier,
        &harness.signed,
        harness.limits.file_bytes,
        None,
    )?;
    let (published, _) = private
        .stage_once(
            OperationId::new(),
            "private.txt",
            b"private bytes",
            "text/plain",
            "private.txt",
            crate::conversation::ContentAccess::default(),
        )?
        .await
        .map(|(p, hold)| (p.file, hold))?;
    let grant = crate::conversation::ContentGrant::verify_directory_read(
        &harness.verifier,
        &harness.signed,
        &harness.journal,
        "",
        &mut None,
    )?;
    let (selected, bytes) = harness
        .files
        .read_private_path(
            &harness.journal,
            &grant,
            "private.txt",
            None,
            64,
            crate::conversation::ContentAccess::default(),
        )
        .await?;
    assert_eq!(selected, published);
    assert_eq!(bytes.as_ref(), b"private bytes");
    Ok(())
}

#[tokio::test]
async fn mounted_publication_retries_and_replacements_keep_their_volume_with_negative_control()
-> Result<()> {
    let (directory, _state, harness, publisher) = native_fixture().await?;
    let operation = OperationId::new();
    let (file, _) = publisher
        .stage_once(
            operation,
            ".volumes/scratch/note",
            b"first",
            "text/plain",
            "note",
            crate::conversation::ContentAccess::default(),
        )?
        .await
        .map(|(p, hold)| (p.file, hold))?;
    assert_eq!(file.volume(), harness.scratch());
    assert_eq!(file.path(), "note");
    // The exact workspace resolver is shared with writes. Reads need neither
    // a writer binding nor write authority, including version-token replay.
    let context_for = |grants: Capabilities, writable: bool| -> Result<_> {
        let content = crate::runtime::ContentBindings {
            volume: harness.project.clone(),
            reader: harness.client_reader()?,
            writer: if writable {
                Some(Arc::new(CheckoutPublisher::new(
                    Arc::clone(&publisher.tools),
                    Arc::clone(&harness.files),
                    &harness.project,
                    &harness.verifier,
                    harness.limits.file_bytes,
                )) as Arc<dyn ContentPublisher>)
            } else {
                None
            },
        };
        let runtime = crate::runtime::Bindings {
            targets: Some(crate::local::agents::home(publisher.tools.clone())?),
            scope: RuntimeScope::new(grants, harness.limits.clone())?,
            content: Some(content),
            ..crate::runtime::Bindings::local()
        }
        .build()?;
        runtime
            .agent_context(
                &Capabilities::default(),
                crate::runtime::ContentBindings {
                    volume: harness.journal.clone(),
                    reader: harness.client_reader()?,
                    writer: Some(Publisher::new(
                        harness.files.clone(),
                        &harness.journal,
                        &harness.verifier,
                        &harness.signed,
                        harness.limits.file_bytes,
                        None,
                    )?),
                },
            )
            .map(Arc::new)
    };
    let scratch_read = Capabilities::new([
        "tool:call:read".into(),
        harness
            .scratch()
            .capability(VolumeOperation::Read, &mut None)?,
    ]);
    let read_context = context_for(scratch_read, false)?;
    let read = crate::tool::files::read()?;
    let run = |task: Arc<crate::runtime::TaskContext>, tool: crate::tool::Tool, arguments| async move {
        let context = ToolContext::new(task, OperationId::new(), "mounted-file")?;
        let invocation = ToolInvocation {
            operation_id: context.operation_id(),
            call_id: context.call_id().into(),
            name: tool.definition.name.clone(),
            arguments,
        };
        let result = tool
            .executor
            .execute(invocation.clone(), Some(context))
            .await?;
        let projected = tool.projection.project(&invocation, &result, None)?.0;
        assert!(
            crate::executor::compile_json_schema(
                &tool.definition.projection_schema,
                "mounted tool projection",
                &mut None,
            )?
            .is_valid(&projected),
            "successful mounted tool result must satisfy its declared projection"
        );
        Ok::<_, Error>((result, projected))
    };
    let path = ".volumes/scratch/note";
    let token = serde_json::to_string(&file).map_err(storage)?;
    for version in [serde_json::Value::Null, serde_json::json!(token)] {
        let (result, projected) = run(
            read_context.clone(),
            crate::tool::files::read()?,
            serde_json::json!({"path": path, "version": version}),
        )
        .await?;
        assert_eq!(result.value["text"], "first");
        assert_eq!(
            result.value["file"],
            serde_json::to_value(&file).map_err(storage)?
        );
        assert!(projected.to_string().contains(path));
    }
    let wrong_path = run(
        read_context.clone(),
        read,
        serde_json::json!({"path": ".volumes/scratch/other", "version": token}),
    )
    .await;
    assert!(wrong_path.is_err());
    let project_context = context_for(
        Capabilities::new([
            "tool:call:read".into(),
            harness
                .project
                .capability(VolumeOperation::Read, &mut None)?,
        ]),
        false,
    )?;
    for version in [serde_json::Value::Null, serde_json::json!(token)] {
        assert!(
            run(
                project_context.clone(),
                crate::tool::files::read()?,
                serde_json::json!({"path": path, "version": version})
            )
            .await
            .is_err()
        );
    }
    let private = Publisher::new(
        harness.files.clone(),
        &harness.journal,
        &harness.verifier,
        &harness.signed,
        harness.limits.file_bytes,
        None,
    )?;
    let (other, _) = private
        .stage_once(
            OperationId::new(),
            "note",
            b"private",
            "text/plain",
            "note",
            crate::conversation::ContentAccess::default(),
        )?
        .await
        .map(|(p, hold)| (p.file, hold))?;
    let full_context = context_for(harness.signed.capabilities().clone(), true)?;
    assert!(
        run(
            full_context.clone(),
            crate::tool::files::read()?,
            serde_json::json!({
                "path": path, "version": serde_json::to_string(&other).map_err(storage)?,
            })
        )
        .await
        .is_err(),
        "a same-path token from another volume must not route authority"
    );
    let (edit_result, _) = run(
        full_context.clone(),
        crate::tool::files::edit()?,
        serde_json::json!({"path": path, "version": token,
            "old_text": "first", "new_text": "second"}),
    )
    .await?;
    assert_eq!(edit_result.value["file"]["path"], "note");
    let changed: FileRef =
        serde_json::from_value(edit_result.value["file"].clone()).map_err(storage)?;
    assert!(
        run(
            full_context.clone(),
            crate::tool::files::edit()?,
            serde_json::json!({"path": path, "version": token,
            "old_text": "first", "new_text": "stale"})
        )
        .await
        .is_err()
    );
    let (replayed, _) = run(
        read_context,
        crate::tool::files::read()?,
        serde_json::json!({"path": path, "version": token}),
    )
    .await?;
    assert_eq!(replayed.value["text"], "first");
    let project_only = Capabilities::new([
        harness
            .project
            .capability(VolumeOperation::Read, &mut None)?,
        harness
            .project
            .capability(VolumeOperation::Write, &mut None)?,
    ]);
    assert!(matches!(
        publisher
            .stage_once(
                OperationId::new(),
                ".volumes/scratch/denied",
                b"denied",
                "text/plain",
                "denied",
                crate::conversation::ContentAccess {
                    ceiling: Some(&project_only),
                    io: None
                }
            )?
            .await
            .map(|(p, hold)| (p.file, hold)),
        Err(Error::FilesystemAccessConfiguration(
            "local checkout publication"
        ))
    ));
    assert!(!directory.path().join(".volumes/scratch/denied").exists());
    assert_eq!(
        publisher
            .observe_staged_once(
                operation,
                None,
                64,
                crate::conversation::ContentAccess::default()
            )?
            .await
            .map(|(p, hold)| (p.map(|p| p.file), hold))?
            .0,
        Some(file.clone())
    );
    assert_eq!(
        publisher
            .stage_once(
                operation,
                ".volumes/scratch/note",
                b"first",
                "text/plain",
                "note",
                crate::conversation::ContentAccess::default()
            )?
            .await
            .map(|(p, hold)| (p.file, hold))?
            .0,
        file
    );
    let (edited, _) = publisher
        .stage_at(OperationId::new(), &changed, b"edited", None, None, None)?
        .await
        .map(|(p, hold)| (p.file, hold))?;
    assert_eq!(edited.volume(), harness.scratch());
    // The trained tool reaches the same atomic publication for a mounted volume.
    let (patched, _) = run(full_context, crate::tools::variants::apply_patch()?,
        serde_json::json!({"patch": "*** Begin Patch\n*** Add File: .volumes/scratch/added\n+added\n*** Update File: .volumes/scratch/note\n@@\n-edited\n+patched\n*** End Patch\n"})).await?;
    assert_eq!(patched.value["files"].as_array().unwrap().len(), 2);
    let patched_files: Vec<FileRef> =
        serde_json::from_value(patched.value["files"].clone()).map_err(storage)?;
    assert!(
        patched_files
            .iter()
            .all(|file| file.volume() == harness.scratch())
    );
    assert_eq!(patched_files[0].version(), patched_files[1].version());
    use crate::conversation::{ContentAccess, ContentChange};
    // The user's per-file byte reservation must not become a batch-receipt
    // bound. Even one byte of content has a larger authenticated receipt.
    let tiny = CheckoutPublisher::new(
        Arc::clone(&publisher.tools),
        Arc::clone(&publisher.files),
        &publisher.volume,
        &publisher.verifier,
        1,
    );
    let one_byte = [ContentChange::CreateAbsent {
        path: "one-byte",
        bytes: b"x",
        media_type: "text/plain",
        display_name: "one-byte",
    }];
    let tiny_operation = OperationId::new();
    let tiny_receipt = tiny
        .stage_changes(
            harness.scratch(),
            tiny_operation,
            &one_byte,
            ContentAccess::default(),
        )?
        .await?
        .0;
    assert_eq!(
        tiny.stage_changes(
            harness.scratch(),
            tiny_operation,
            &one_byte,
            ContentAccess::default()
        )?
        .await?
        .0,
        tiny_receipt,
    );
    let stale = [
        ContentChange::CreateAbsent {
            path: "must-stay-absent",
            bytes: b"partial",
            media_type: "text/plain",
            display_name: "partial",
        },
        ContentChange::DeleteExpected { source: &edited },
    ];
    assert!(
        publisher
            .stage_changes(
                harness.scratch(),
                OperationId::new(),
                &stale,
                ContentAccess::default()
            )?
            .await
            .is_err()
    );
    // Delete-only batches retain their exact cut even with no returned FileRef.
    let deleted = [ContentChange::DeleteExpected {
        source: &patched_files[0],
    }];
    let without_delete = Capabilities::new([
        harness
            .scratch()
            .capability(VolumeOperation::Write, &mut None)?,
        acyclic_fs::access::Capability::whole_volume(
            None,
            &[
                acyclic_fs::access::Action::Create,
                acyclic_fs::access::Action::Write,
                acyclic_fs::access::Action::MergeInto,
            ],
        )
        .to_scope_label()
        .map_err(storage)?,
    ]);
    assert!(
        publisher
            .stage_changes(
                harness.scratch(),
                OperationId::new(),
                &deleted,
                ContentAccess {
                    ceiling: Some(&without_delete),
                    io: None,
                },
            )?
            .await
            .is_err(),
        "a caller's explicit content ceiling must still refuse deletion"
    );
    let delete_operation = OperationId::new();
    let (deletion, _) = publisher
        .stage_changes(
            harness.scratch(),
            delete_operation,
            &deleted,
            ContentAccess::default(),
        )?
        .await?;
    assert!(deletion.files().next().is_none());
    let (later, _) = publisher
        .stage_once(
            OperationId::new(),
            ".volumes/scratch/later",
            b"later",
            "text/plain",
            "later",
            ContentAccess::default(),
        )?
        .await
        .map(|(p, hold)| (p.file, hold))?;
    assert_eq!(
        publisher
            .observe_changes(
                harness.scratch(),
                delete_operation,
                4096,
                ContentAccess::default()
            )?
            .await?
            .0,
        Some(deletion)
    );
    let (restored, _) = publisher
        .stage_at(
            OperationId::new(),
            &patched_files[1],
            b"edited",
            None,
            None,
            None,
        )?
        .await
        .map(|(p, hold)| (p.file, hold))?;
    assert_eq!(restored.volume(), later.volume());
    // Auxiliary volumes exist physically only while a native command owns its
    // projection. Publication must persist their bytes without leaving a mount.
    let mounts = &harness
        .workspace
        .as_ref()
        .ok_or_else(|| Error::NotFound("checkout".into()))?
        .mounts;
    assert!(!directory.path().join(".volumes/scratch").exists());
    crate::local::workspace::volumes::native_scope(mounts, async |claims| {
        assert_eq!(
            std::fs::read(directory.path().join(".volumes/scratch/note")).map_err(storage)?,
            b"edited"
        );
        assert!(!directory.path().join(".volumes/scratch/added").exists());
        assert!(
            !directory
                .path()
                .join(".volumes/scratch/must-stay-absent")
                .exists()
        );
        assert_eq!(
            std::fs::read(directory.path().join(".volumes/scratch/later")).map_err(storage)?,
            b"later"
        );
        Ok(((), claims))
    })
    .await?;
    assert!(!directory.path().join(".volumes/scratch").exists());
    let view = harness.workspace.as_ref().unwrap().view();
    let stop = acyclic_fs::CancellationToken::new();
    let primary_absent = async |path: &str| -> Result<bool> {
        let head = view
            .workspace()
            .head(acyclic_fs::WorkBudget::UNBOUNDED, &stop)
            .await
            .map_err(storage)?
            .value;
        Ok(matches!(
            head.stat(path, acyclic_fs::WorkBudget::UNBOUNDED, &stop)
                .await,
            Err(acyclic_fs::OperationFailure {
                error: acyclic_fs::WorkspaceError::NotFound,
                ..
            })
        ))
    };
    assert!(primary_absent("/.volumes/scratch/note").await?);
    assert!(primary_absent("/note").await?);
    // The former implementation selected the project writer. This deliberately
    // bypasses routing and must fail the same history-attribution oracle.
    publisher
        .writer_for(&harness.project, view.workspace(), None)?
        .stage_once(
            OperationId::new(),
            ".volumes/scratch/control",
            b"wrong history",
            "text/plain",
            "control",
            crate::conversation::ContentAccess::default(),
        )?
        .await
        .map(|(p, hold)| (p.file, hold))?;
    assert!(
        !primary_absent("/.volumes/scratch/control").await?,
        "negative control failed to expose the project-history leak"
    );
    assert!(
        publisher
            .destination(Some(&harness.journal), "nope", None)
            .is_err()
    );
    assert!(
        publisher
            .destination(Some(&harness.project), ".volumes/scratch/note", None)
            .is_err()
    );
    Ok(())
}

// A caller with project-only grants recovers its own project write: a
// publication that never committed observes as absent rather than failing
// on the scratch mount it may not reach, and a committed one is returned.
// Negative control: requiring access to every mount refuses the caller.
#[tokio::test]
async fn project_only_callers_recover_their_own_operations() -> Result<()> {
    let (_directory, _state, harness, publisher) = native_fixture().await?;
    let project_only = Capabilities::new([
        harness
            .project
            .capability(VolumeOperation::Read, &mut None)?,
        harness
            .project
            .capability(VolumeOperation::Write, &mut None)?,
    ]);
    let (publisher, project_only) = (&publisher, &project_only);
    let access = move || crate::conversation::ContentAccess {
        ceiling: Some(project_only),
        io: None,
    };
    let observe = move |operation| async move {
        publisher
            .observe_staged_once(operation, None, 1024, access())?
            .await
            .map(|(p, hold)| (p.map(|p| p.file), hold))
            .map(|(file, _)| file)
    };
    let operation = OperationId::new();
    let (written, _) = publisher
        .stage_once(
            operation,
            "result.txt",
            b"agent bytes",
            "text/plain",
            "result.txt",
            access(),
        )?
        .await
        .map(|(p, hold)| (p.file, hold))?;
    for strict in [false, true] {
        super::tests::OBSERVE_EVERY_MOUNT.set(strict);
        let missing = observe(OperationId::new()).await;
        let recovered = observe(operation).await;
        super::tests::OBSERVE_EVERY_MOUNT.set(false);
        if strict {
            assert!(matches!(
                missing,
                Err(Error::FilesystemAccessConfiguration(_) | Error::Unauthorized(_))
            ));
        } else {
            assert!(missing?.is_none());
            assert_eq!(
                recovered?.map(|file| file.version().to_owned()),
                Some(written.version().to_owned())
            );
        }
    }
    // A caller that may reach no mount is still refused.
    let none = Capabilities::new(Vec::<String>::new());
    assert!(matches!(
        publisher
            .observe_staged_once(
                OperationId::new(),
                None,
                1024,
                crate::conversation::ContentAccess {
                    ceiling: Some(&none),
                    io: None
                },
            )?
            .await
            .map(|(p, hold)| (p.map(|p| p.file), hold)),
        Err(Error::FilesystemAccessConfiguration(_) | Error::Unauthorized(_))
    ));
    Ok(())
}
