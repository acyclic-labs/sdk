use super::*;
use crate::filesystem::PreparationIo;
use acyclic_fs::access::{Action as FsAction, Effect, Policy};
use acyclic_fs::{
    CancellationToken, WorkBudget,
    model::{CheckoutMode, GenerationSelector},
};
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn reads_follow_each_policy_revision(revisions in prop::collection::vec(any::<bool>(), 1..5)) {
        tokio::runtime::Builder::new_current_thread().enable_all().build()?.block_on(async {
            let fs = Fs::memory();
            let storage = MemoryHarnessStorage::from_providers(
                AgentId::new(), 4096, fs.clone(), Arc::new(MemoryStream::default()),
            ).await?;
            let file = storage.stage(OperationId::new(), "secrets/key", b"cached", "text/plain", "key").await?;
            assert_eq!(storage.read(&file).await?.as_ref(), b"cached");
            let final_version = revisions.len() as u64 + 2;
            for (index, denied) in revisions.into_iter().enumerate() {
                policy(&fs, &storage, index as u64 + 2, denied.then_some(FsAction::Read), "secrets/**").await?;
                let read = storage.read(&file).await;
                assert_eq!(read.is_ok(), !denied, "{read:?}");
                if let Ok(bytes) = read { assert_eq!(bytes.as_ref(), b"cached"); }
                let publication = storage.content_verifier.verify_publication(&file, 4096, None)?.await;
                assert_eq!(publication.is_ok(), !denied, "local publication must recheck the current policy: {publication:?}");
            }
            // Restoring disclosure must recover the same pinned bytes, proving
            // that a denial did not succeed through eviction or data loss.
            policy(&fs, &storage, final_version, None, "secrets/**").await?;
            assert_eq!(storage.read(&file).await?.as_ref(), b"cached");
            Ok::<_, Box<dyn std::error::Error>>(())
        }).map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
}

async fn policy(
    fs: &acyclic_fs::MemoryFs,
    storage: &MemoryHarnessStorage,
    version: u64,
    deny: Option<FsAction>,
    path: &str,
) -> Result<()> {
    let mut policy: Policy = serde_json::from_slice(super::super::super::access::INITIAL_POLICY)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    policy.version = version;
    if let Some(action) = deny {
        let mut deny = policy.layers[0][0].clone();
        deny.id = "hide-cached-access".into();
        deny.effect = Effect::Deny;
        deny.actions = [action].into_iter().collect();
        deny.resource.path = path.into();
        policy.layers[0].push(deny);
    }
    publish_volume_policy(fs, storage.volume(), &policy).await
}

async fn publish_volume_policy(
    fs: &acyclic_fs::MemoryFs,
    volume: &VolumeRef,
    policy: &Policy,
) -> Result<()> {
    let cancel = CancellationToken::new();
    let workspace = fs
        .open_workspace(
            volume.storage_name(&mut None)?,
            WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await
        .map_err(|failure| super::super::super::map_error(failure.error))?
        .value;
    let mut checkout = workspace
        .checkout(
            GenerationSelector::Head,
            CheckoutMode::tracking_transaction(),
            WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await
        .map_err(|failure| super::super::super::map_error(failure.error))?
        .value;
    checkout
        .write_access_policy(
            &serde_json::to_vec(policy).map_err(|error| Error::Invalid(error.to_string()))?,
            acyclic_fs::NamedAttributeWriteMode::Replace,
            WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await
        .map_err(|failure| super::super::super::map_error(failure.error.into()))?;
    checkout
        .commit::<acyclic_fs::FsError, _, _>(
            acyclic_fs::OperationId::new(),
            None,
            None,
            None,
            acyclic_fs::PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &cancel,
            |_| {
                std::future::ready(Ok(acyclic_fs::OperationReceipt {
                    value: (),
                    work: acyclic_fs::WorkCounters::UNCHARGED,
                }))
            },
        )
        .await
        .map_err(|failure| super::super::super::map_error(failure.error.into()))?;
    Ok(())
}

#[tokio::test]
#[cfg(not(target_arch = "wasm32"))]
async fn filesystem_roles_require_signed_scope_and_caller_ceiling() -> Result<()> {
    for structured in [false, true] {
        let fs = Fs::memory();
        let agent = AgentId::new();
        let storage = MemoryHarnessStorage::from_providers(
            agent,
            4096,
            fs.clone(),
            Arc::new(MemoryStream::default()),
        )
        .await?;
        let file = storage
            .stage(
                OperationId::new(),
                "ordinary",
                b"original",
                "text/plain",
                "file",
            )
            .await?;
        let structured = structured
            .then(|| {
                acyclic_fs::access::Capability::whole_volume(None, &FsAction::ALL).to_scope_label()
            })
            .transpose()
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let inherited = VolumeRef::new(
            storage.volume.provider().clone(),
            "role-fork".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
            &mut None,
        )?;
        let inherited_capabilities = [
            inherited.capability(VolumeOperation::Read, &mut None)?,
            inherited.capability(VolumeOperation::Write, &mut None)?,
        ];
        let guest_capabilities = Capabilities::new(
            storage
                .scope
                .capabilities()
                .iter()
                .chain(structured.as_deref())
                .chain(inherited_capabilities.iter().map(String::as_str)),
        );
        let capabilities = Capabilities::new(guest_capabilities.iter().chain(["role:control"]));
        let owner = storage.issuer.bootstrap_scope(
            "control-owner",
            &capabilities,
            Some(agent),
            &mut None,
        )?;
        let guest = storage
            .issuer
            .attenuate(&owner, "guest", &guest_capabilities)?;
        let verifier = storage.issuer.verifier(&mut None)?;
        let read = |scope| {
            ContentGrant::verify(
                &verifier,
                scope,
                &storage.volume,
                VolumeOperation::Read,
                &mut None,
            )
        };
        let owner_read = read(&owner)?;
        let guest_read = read(&guest)?;
        let directory_read = |scope| {
            ContentGrant::verify_directory_read(&verifier, scope, &storage.volume, "", &mut None)
        };
        let owner_directory = directory_read(&owner)?;
        let guest_directory = directory_read(&guest)?;
        // The project root supplies selection authority; the original graph
        // proves its descendant is a distinct data-policy owner.
        let cancel = CancellationToken::new();
        let root = fs
            .open_workspace(
                storage.volume.storage_name(&mut None)?,
                WorkBudget::UNBOUNDED,
                &cancel,
            )
            .await
            .map_err(|failure| super::super::super::map_error(failure.error))?
            .value;
        let lineage = acyclic_fs::StreamGitCompatStore::new(acyclic_fs::StreamAuthorityStore::new(
            fs.stream_provider(),
        ));
        let graph = acyclic_fs::WorkspaceGraph::new(lineage.clone());
        graph
            .register_root(&root, WorkBudget::UNBOUNDED, &cancel)
            .await
            .map_err(|failure| Error::Storage(failure.error.to_string()))?;
        let child = graph
            .fork(
                &root,
                &inherited.storage_name(&mut None)?,
                acyclic_fs::IdempotencyKey::new(),
                WorkBudget::UNBOUNDED,
                &cancel,
            )
            .await
            .map_err(|failure| Error::Storage(failure.error.to_string()))?
            .value;
        let foreign_host = FilesystemHost::new(fs.clone(), storage.volume.provider(), None)?
            .with_project_families(lineage)?;
        let mut restricted: Policy =
            serde_json::from_slice(super::super::super::access::INITIAL_POLICY)
                .map_err(|error| Error::Invalid(error.to_string()))?;
        restricted.version += 1;
        restricted.layers[0][0].conditions.role = Some("control".into());
        publish_volume_policy(&fs, &inherited, &restricted).await?;
        let selected = child
            .head(WorkBudget::UNBOUNDED, &cancel)
            .await
            .map_err(|failure| super::super::super::map_error(failure.error))?
            .value;
        let source_generation = foreign_host.generation_ref_id(selected.id(), None)?;
        let budget = FINITE_CONTENT_BUDGET;
        let native = acyclic_fs::NativeMaterializationWork::new(budget, &cancel)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let owned = PreparationIo::from_budget(budget, &cancel)?;
        let original = PreparationIo::from_native_work(&native)?;
        for io in [None, Some(&owned)] {
            for path in ["ordinary", "absent"] {
                let selected = foreign_host
                    .select_private_generation(
                        &storage.volume,
                        &owner_directory,
                        path,
                        Some(&source_generation),
                        crate::conversation::ContentAccess {
                            ceiling: Some(owner.capabilities()),
                            io,
                        },
                    )
                    .await?;
                assert_eq!(selected, source_generation);
                let stat = foreign_host
                    .stat_private_path(
                        &storage.volume,
                        &owner_directory,
                        path,
                        Some(&selected),
                        crate::conversation::ContentAccess {
                            ceiling: Some(owner.capabilities()),
                            io,
                        },
                    )
                    .await?;
                assert_eq!(stat.is_some(), path == "ordinary");
                for (grant, ceiling) in [
                    (&owner_directory, guest.capabilities()),
                    (&guest_directory, owner.capabilities()),
                ] {
                    let access = crate::conversation::ContentAccess {
                        ceiling: Some(ceiling),
                        io,
                    };
                    let error = foreign_host
                        .select_private_generation(
                            &storage.volume,
                            grant,
                            path,
                            Some(&source_generation),
                            access,
                        )
                        .await
                        .expect_err("inherited generation bypassed caller ceiling");
                    assert_eq!(error.code(), crate::wire::ErrorCode::Unauthorized);
                    let error = foreign_host
                        .stat_private_path(
                            &storage.volume,
                            grant,
                            path,
                            Some(&selected),
                            crate::conversation::ContentAccess {
                                ceiling: Some(ceiling),
                                io,
                            },
                        )
                        .await
                        .expect_err("inherited metadata bypassed caller ceiling");
                    assert_eq!(error.code(), crate::wire::ErrorCode::Unauthorized);
                }
            }
        }
        let error = foreign_host
            .select_private_generation(
                &storage.volume,
                &owner_directory,
                "ordinary",
                Some(&source_generation),
                crate::conversation::ContentAccess {
                    ceiling: Some(owner.capabilities()),
                    io: Some(&original),
                },
            )
            .await
            .expect_err(
                "native foreign-project reads bypassed their original unsupported boundary",
            );
        assert_eq!(error.code(), crate::wire::ErrorCode::Unsupported);
        publish_volume_policy(&fs, storage.volume(), &restricted).await?;
        for io in [None, Some(&owned), Some(&original)] {
            assert_eq!(
                storage
                    .host
                    .read_private_path(
                        &storage.volume,
                        &owner_directory,
                        "ordinary",
                        None,
                        4096,
                        crate::conversation::ContentAccess {
                            ceiling: Some(owner.capabilities()),
                            io,
                        },
                    )
                    .await?
                    .1
                    .as_ref(),
                b"original"
            );
            assert_eq!(
                storage
                    .host
                    .read_content(&file, &owner_read, 4096, io)
                    .await?
                    .as_ref(),
                b"original"
            );
            assert!(matches!(
                storage.host.read_content(&file, &guest_read, 4096, io).await,
                Err(error) if error.code() == crate::wire::ErrorCode::Unauthorized
            ));
            // Removing the claim from a caller ceiling restricts an authenticated
            // owner; adding it to an unprivileged caller never fabricates a role.
            for (grant, ceiling) in [
                (&owner_directory, guest.capabilities()),
                (&guest_directory, owner.capabilities()),
            ] {
                let error = storage
                    .host
                    .read_private_path(
                        &storage.volume,
                        grant,
                        "ordinary",
                        None,
                        4096,
                        crate::conversation::ContentAccess {
                            ceiling: Some(ceiling),
                            io,
                        },
                    )
                    .await
                    .expect_err("a caller ceiling cannot supply an authenticated role");
                assert_eq!(
                    error.code(),
                    crate::wire::ErrorCode::Unauthorized,
                    "{error}"
                );
            }
        }
        restricted.version += 1;
        restricted.layers[0][0].conditions.role = None;
        publish_volume_policy(&fs, storage.volume(), &restricted).await?;
        assert_eq!(
            storage
                .host
                .read_content(&file, &guest_read, 4096, None)
                .await?
                .as_ref(),
            b"original"
        );
        // A deny-role is not safe with attenuatable claims. Both removing it in
        // the signed scope and omitting it from a caller ceiling must refuse at
        // the persisted-policy boundary, rather than disclose the same file.
        restricted.version += 1;
        let mut deny = restricted.layers[0][0].clone();
        deny.id = "restricted-role".into();
        deny.effect = Effect::Deny;
        deny.actions = [FsAction::Read].into_iter().collect();
        deny.conditions.role = Some("control".into());
        restricted.layers[0].push(deny);
        publish_volume_policy(&fs, storage.volume(), &restricted).await?;
        for io in [None, Some(&owned), Some(&original)] {
            for (grant, ceiling) in [
                (&owner_directory, None),
                (&guest_directory, None),
                (&owner_directory, Some(guest.capabilities())),
            ] {
                let error = storage
                    .host
                    .read_private_path(
                        &storage.volume,
                        grant,
                        "ordinary",
                        None,
                        4096,
                        crate::conversation::ContentAccess { ceiling, io },
                    )
                    .await
                    .expect_err("role-conditioned denies need complete role facts");
                assert_eq!(error.code(), crate::wire::ErrorCode::Unsupported, "{error}");
            }
        }
    }
    Ok(())
}

mod context {
    extern crate self as acyclic_harness;
    include!("../../../../../harness/tests/support/content_context.rs");
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn cached_directory_replies_pin_data_and_recheck_current_disclosure(
        revisions in prop::collection::vec(any::<bool>(), 1..5), narrowed in any::<bool>(),
    ) {
        tokio::runtime::Builder::new_current_thread().enable_all().build()?.block_on(async {
            let fs = Fs::memory();
            let storage = MemoryHarnessStorage::from_providers(
                AgentId::new(), 4096, fs.clone(), Arc::new(MemoryStream::default()),
            ).await?;
            for name in ["one", "two"] {
                storage.stage(OperationId::new(), &format!("notes/{name}"), b"data", "text/plain", name).await?;
            }
            let tool = storage.list_files_tool(Limits { file_bytes: Some(4096), ..Limits::default() })?;
            let invocation = ToolInvocation {
                operation_id: OperationId::new(), call_id: "cached-list".into(), name: "acyclic.list_files".into(),
                arguments: json!({"path":"notes", "maximum_entries":8}),
            };
            let original = tool.executor.execute(invocation.clone(), None).await?;
            assert_eq!(original.value["entries"].as_array().unwrap().len(), 2);
            storage.stage(OperationId::new(), "notes/three", b"later", "text/plain", "three").await?;
            let grants = if narrowed {
                use acyclic_fs::access::{Capability, Grant, Generations, ResourcePattern};
                Capabilities::new([Capability {
                    layers: vec![vec!["notes", "notes/one"].into_iter().map(|path| Grant {
                        actions: [FsAction::List, FsAction::ReadHistory].into_iter().collect(),
                        resource: ResourcePattern { lineage_root: None, volume: None,
                            generations: Generations::All, path: path.into() },
                    }).collect()], expires_at: None,
                }.to_scope_label().map_err(|error| Error::Invalid(error.to_string()))?])
            } else { Capabilities::new([storage.read_capability.clone()]) };
            let task = context::content_context(storage.content_verifier.clone(), None, grants).await?;
            let context = crate::runtime::ToolContext::new(task, invocation.operation_id, &invocation.call_id)?;
            for (index, denied) in revisions.into_iter().enumerate() {
                policy(&fs, &storage, index as u64 + 2, denied.then_some(FsAction::List), "notes/two").await?;
                let replay = tool.executor.execute(invocation.clone(), Some(context.clone())).await;
                let reconciled = tool.executor.reconcile(invocation.clone(), Some(context.clone())).await;
                let allowed = !denied && !narrowed;
                assert_eq!(replay.is_ok(), allowed, "{replay:?}");
                assert_eq!(reconciled.is_ok(), allowed, "{reconciled:?}");
                if allowed {
                    assert_eq!(replay?, original);
                    assert_eq!(reconciled?, Some(original.clone()));
                }
            }
            Ok::<_, Box<dyn std::error::Error>>(())
        }).map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
}

#[tokio::test]
async fn local_publication_evidence_rechecks_current_disclosure_policy() -> Result<()> {
    let fs = Fs::memory();
    let storage = MemoryHarnessStorage::from_providers(
        AgentId::new(),
        4096,
        fs.clone(),
        Arc::new(MemoryStream::default()),
    )
    .await?;
    let file = storage
        .stage(
            OperationId::new(),
            "secrets/key",
            b"verified",
            "text/plain",
            "key",
        )
        .await?;
    storage
        .content_verifier
        .verify_publication(&file, 4096, None)?
        .await?;
    policy(&fs, &storage, 2, Some(FsAction::Read), "secrets/**").await?;
    assert!(
        storage
            .content_verifier
            .verify_publication(&file, 4096, None)?
            .await
            .is_err(),
        "local publication evidence must not bypass a later disclosure denial"
    );
    policy(&fs, &storage, 3, None, "secrets/**").await?;
    storage
        .content_verifier
        .verify_publication(&file, 4096, None)?
        .await?;
    Ok(())
}

#[tokio::test]
async fn summary_bodies_are_readable_while_execution_state_stays_private() -> Result<()> {
    let fs = Fs::memory();
    let storage = MemoryHarnessStorage::from_providers(
        AgentId::new(),
        None,
        fs.clone(),
        Arc::new(MemoryStream::default()),
    )
    .await?;
    let journal = storage.journal();
    let operation = OperationId::new();
    let summary = journal
        .stage(
            operation,
            "summary:0:output",
            b"retained facts",
            "text/plain",
            None,
        )?
        .await?;
    let marker = journal
        .stage(
            operation,
            "backstop:1:marker",
            b"evicted history",
            "text/plain",
            None,
        )?
        .await?;
    let private = journal
        .stage(
            operation,
            "summary:0:source",
            b"private context",
            "text/plain",
            None,
        )?
        .await?;
    let policy: Policy = serde_json::from_slice(include_bytes!("../../host/journal-policy.json"))
        .map_err(|error| Error::Invalid(error.to_string()))?;
    publish_volume_policy(&fs, summary.volume(), &policy).await?;
    for (file, expected) in [
        (&summary, b"retained facts".as_slice()),
        (&marker, b"evicted history".as_slice()),
    ] {
        let bytes = storage
            .content_reader()
            .read(file, file.descriptor().byte_length(), None, None)?
            .await?;
        assert_eq!(bytes.as_ref(), expected);
        assert!(
            !journal.authenticate_summary(file, None)?.await?,
            "readable bytes alone must not authenticate a summary"
        );
    }
    assert_eq!(
        storage
            .content_reader()
            .read(&private, private.descriptor().byte_length(), None, None)?
            .await
            .unwrap_err()
            .code(),
        crate::wire::ErrorCode::Unauthorized
    );
    Ok(())
}
