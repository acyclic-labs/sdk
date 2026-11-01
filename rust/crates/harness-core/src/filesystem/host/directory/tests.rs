use super::*;
use crate::core::{AggregateKind, Authority, AuthorityRoot};
use crate::{AgentId, conversation::VolumeOwner};
use acyclic_fs::access::{Capability, Generations, Grant, ResourcePattern};
use proptest::prelude::*;

#[tokio::test]
async fn inherited_directory_intersects_current_root_and_data_policy()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let (host, volume, grant, writer) = fixture([true; 8], VolumeClass::AgentPrivate).await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let reference = bounded_io::content_workspace(&host.provider, &volume, None)?;
    let root = host.open(&reference, None).await?;
    let store = acyclic_fs::StreamGitCompatStore::new(acyclic_fs::StreamAuthorityStore::new(
        host.filesystem.stream_provider(),
    ));
    let graph = acyclic_fs::WorkspaceGraph::new(store.clone());
    graph.register_root(&root).await?;
    let child = graph
        .fork(&root, "directory-branch", acyclic_fs::IdempotencyKey::new())
        .await?;
    let pinned = child
        .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value;
    let generation = host.generation_ref_id(pinned.id(), None)?;
    let repository = acyclic_fs::GitCompatRepository::new(root.id(), store.clone());
    repository
        .register_branch_workspace("directory-branch", child.id(), None, true)
        .await?;
    let mut host = host.with_project_families(store)?;
    let options = DirectoryOptions {
        generation: Some(&generation),
        after: None,
        maximum_entries: 8,
        ceiling: None,
    };
    let (_, before) = host
        .list_private_directory(&volume, &grant, "rows", options)
        .await?;
    assert_eq!(before.entries.len(), 8);
    let (current, _) = host
        .list_private_directory(
            &volume,
            &grant,
            "rows",
            DirectoryOptions {
                generation: None,
                ..options
            },
        )
        .await?;
    assert_eq!(current, generation);
    host.put_content(
        &volume,
        &writer,
        "rows/02",
        b"branch row",
        "text/plain",
        "02",
        4096,
        &IdempotencyKey::new("selected-branch-write")?,
        None,
        crate::conversation::ContentAccess::default(),
    )
    .await?;
    assert_eq!(
        host.read(&reference, None, "/rows/02", 4096, None)
            .await?
            .as_ref(),
        b"branch row"
    );
    assert_eq!(
        host.read(&reference, Some(&generation), "/rows/02", 4096, None)
            .await?
            .as_ref(),
        b"row"
    );
    assert_eq!(
        root.read(
            "/rows/02",
            4096,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancellation
        )
        .await?
        .value
        .as_ref(),
        b"row",
        "a branch publication must not change the original root"
    );
    assert_eq!(
        host.stat(&reference, None, "/rows/02").await?.logical_bytes,
        Some(10)
    );
    assert_eq!(
        host.stat(&reference, Some(&generation), "/rows/02")
            .await?
            .logical_bytes,
        Some(3)
    );
    host.put_content(
        &volume,
        &writer,
        "branch-only",
        b"branch",
        "text/plain",
        "branch-only",
        4096,
        &IdempotencyKey::new("selected-branch-directory-entry")?,
        None,
        crate::conversation::ContentAccess::default(),
    )
    .await?;
    let current_page = host.list(&reference, None, "/", 8).await?;
    assert!(
        current_page
            .entries
            .iter()
            .any(|entry| entry.name.as_bytes() == b"branch-only")
    );
    let pinned_page = host.list(&reference, Some(&generation), "/", 8).await?;
    assert!(
        !pinned_page
            .entries
            .iter()
            .any(|entry| entry.name.as_bytes() == b"branch-only")
    );
    assert!(
        host.stat(&reference, Some(&generation), "/branch-only")
            .await
            .is_err()
    );
    // Internal approvals/effects keep the original private-volume owner even
    // while user data is selected from a descendant.
    {
        let child_before = child
            .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value
            .id();
        let files = crate::filesystem::agent_internal::AgentInternalFiles::new(
            host.clone(),
            volume.clone(),
            writer.clone(),
        );
        assert_eq!(
            files
                .create("branch-owner/action.json", b"first", 4096)
                .await?,
            None
        );
        assert_eq!(
            files
                .read("branch-owner/action.json", 4096)
                .await?
                .as_deref(),
            Some(b"first".as_slice())
        );
        assert_eq!(files.list("branch-owner").await?, ["action.json"]);
        assert_eq!(
            files
                .create("branch-owner/action.json", b"later", 4096)
                .await?
                .as_deref(),
            Some(b"first".as_slice())
        );
        assert_eq!(
            child
                .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
                .await?
                .value
                .id(),
            child_before
        );
    }
    let range_grant = ContentGrant::verify_directory_read(
        &writer.copy_verifier(&mut None)?,
        writer.policy_scope(),
        &volume,
        "rows",
        &mut None,
    )?;
    for pinned in [None, Some(&generation)] {
        let (bytes, _, length) = host
            .read_private_range(
                &volume,
                &range_grant,
                "rows/02",
                pinned,
                acyclic_fs::ByteRange {
                    offset: 1,
                    length: 2,
                },
                crate::conversation::ContentAccess::default(),
            )
            .await?;
        assert_eq!(bytes.as_ref(), if pinned.is_some() { b"ow" } else { b"ra" });
        assert_eq!(length, if pinned.is_some() { 3 } else { 10 });
    }
    let before_policy_change = child
        .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value
        .id();
    let policy_root = root.clone();
    Arc::get_mut(&mut host).unwrap().publication_pause = Some(Arc::new(move || {
        let policy_root = policy_root.clone();
        Box::pin(async move {
            let mut policy: acyclic_fs::access::Policy =
                serde_json::from_slice(access::INITIAL_POLICY).unwrap();
            policy.version = 2;
            write_policy(&policy_root, &policy).await.unwrap();
        })
    }));
    let stale = host
        .put_content(
            &volume,
            &writer,
            "rows/02",
            b"stale",
            "text/plain",
            "02",
            4096,
            &IdempotencyKey::new("root-policy-changed-during-branch-write")?,
            None,
            crate::conversation::ContentAccess::default(),
        )
        .await;
    assert!(
        matches!(stale, Err(Error::Conflict(_))),
        "a changed root policy must fence the descendant commit: {stale:?}"
    );
    Arc::get_mut(&mut host).unwrap().publication_pause = None;
    assert_eq!(
        child
            .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value
            .id(),
        before_policy_change
    );
    for (workspace, hidden) in [(&child, "rows/00"), (&root, "rows/01")] {
        let mut policy: acyclic_fs::access::Policy =
            serde_json::from_slice(access::INITIAL_POLICY)?;
        policy.version = 2;
        let mut deny = policy.layers[0][0].clone();
        deny.id = "inherited-list-denial".into();
        deny.effect = acyclic_fs::access::Effect::Deny;
        deny.actions = [Action::List, Action::Read, Action::Write]
            .into_iter()
            .collect();
        deny.resource.path = hidden.into();
        policy.layers[0].push(deny);
        write_policy(workspace, &policy).await?;
        for pinned in [None, Some(&generation)] {
            let denied = host
                .read_private_range(
                    &volume,
                    &range_grant,
                    hidden,
                    pinned,
                    acyclic_fs::ByteRange {
                        offset: 0,
                        length: 1,
                    },
                    crate::conversation::ContentAccess::default(),
                )
                .await
                .expect_err("root and descendant policy must both authorize saved ranges");
            assert_eq!(denied.code(), crate::wire::ErrorCode::Unauthorized);
        }
        let (_, page) = host
            .list_private_directory(&volume, &grant, "rows", options)
            .await?;
        assert!(!page.entries.iter().any(|entry| entry.name.as_bytes() == hidden.rsplit('/').next().unwrap().as_bytes()));
        assert!(
            !page
                .entries
                .iter()
                .any(|entry| entry.name.as_bytes() == b"00"),
            "a root policy update must not restore names hidden by the data owner"
        );
        let (_, current) = host
            .list_private_directory(
                &volume,
                &grant,
                "rows",
                DirectoryOptions {
                    generation: None,
                    ..options
                },
            )
            .await?;
        assert_eq!(current.entries.len(), page.entries.len());
    }
    let before_refusal = child
        .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value;
    for (path, key) in [
        ("rows/00", "child-denied-write"),
        ("rows/01", "root-denied-write"),
    ] {
        assert!(
            matches!(
                host.put_content(
                    &volume,
                    &writer,
                    path,
                    b"denied",
                    "text/plain",
                    "row",
                    4096,
                    &IdempotencyKey::new(key)?,
                    None,
                    crate::conversation::ContentAccess::default()
                )
                .await,
                Err(Error::Unauthorized(_))
            ),
            "both current policy owners must authorize publication"
        );
    }
    assert_eq!(
        child
            .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value
            .id(),
        before_refusal.id()
    );
    repository
        .register_branch_workspace("main", root.id(), None, true)
        .await?;
    assert_eq!(
        host.stat(&reference, None, "/rows/02").await?.logical_bytes,
        Some(3)
    );
    assert!(host.stat(&reference, None, "/branch-only").await.is_err());
    assert!(
        !host
            .list(&reference, None, "/", 8)
            .await?
            .entries
            .iter()
            .any(|entry| entry.name.as_bytes() == b"branch-only")
    );
    let (_, current) = host
        .list_private_directory(
            &volume,
            &grant,
            "rows",
            DirectoryOptions {
                generation: None,
                ..options
            },
        )
        .await?;
    assert_eq!(
        current.entries.len(),
        7,
        "current reads must follow the durable branch switch"
    );
    let (selected, page) = host
        .list_private_directory(&volume, &grant, "rows", options)
        .await?;
    assert_eq!(
        selected, generation,
        "policy changes must not replace the pinned data cut"
    );
    assert_eq!(page.entries.len(), 6);
    Ok(())
}

#[tokio::test]
async fn descendant_restore_retains_the_original_repository_policy_cut()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let (host, volume, _, writer) = fixture([true; 8], VolumeClass::Project).await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let reference = bounded_io::content_workspace(&host.provider, &volume, None)?;
    let root = host.open(&reference, None).await?;
    let store = acyclic_fs::StreamGitCompatStore::new(acyclic_fs::StreamAuthorityStore::new(
        host.filesystem.stream_provider(),
    ));
    let graph = acyclic_fs::WorkspaceGraph::new(store.clone());
    graph.register_root(&root).await?;
    let access = crate::filesystem::WorkspaceAccess::new(
        &writer.copy_verifier(&mut None)?,
        writer.policy_scope(),
        writer.policy_scope().capabilities(),
        Some(&volume),
    )?;
    let command_workspace = |selected| crate::tools::git::Workspace {
        access: &access,
        filesystem: &host.filesystem,
        store: &store,
        view: crate::tools::git::View::Volume(selected),
        root: std::path::Path::new("/"),
        repository: root.id(),
        lease: None,
        #[cfg(not(target_arch = "wasm32"))]
        checkout: None,
        reserved: &[],
        author: "test",
        authored_at_seconds: 1,
        request: None,
    };
    crate::tools::git::run_volume_with_guard(
        command_workspace(&root),
        None,
        &["commit".into(), "-m".into(), "base".into()],
        || async { access.require(&root).await },
    )
    .await?;
    let child = graph
        .fork(&root, "restore-branch", acyclic_fs::IdempotencyKey::new())
        .await?;
    acyclic_fs::GitCompatRepository::new(root.id(), store.clone())
        .register_branch_workspace("restore-branch", child.id(), None, true)
        .await?;
    let mut transaction = child
        .begin_transaction(
            acyclic_fs::IdempotencyKey::new(),
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    transaction
        .write(
            "/rows/02",
            bytes::Bytes::from_static(b"changed"),
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    transaction
        .commit::<acyclic_fs::WorkspaceError, _, _>(
            None,
            acyclic_fs::PublicationPermit::Unrestricted,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancellation,
            |_| async {
                Ok(acyclic_fs::OperationReceipt {
                    value: (),
                    work: WorkCounters::UNCHARGED,
                })
            },
        )
        .await?;
    let before = child
        .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value
        .id();
    let changed = std::sync::atomic::AtomicBool::new(false);
    let arguments = [
        "restore".into(),
        "--source".into(),
        "main".into(),
        "rows/02".into(),
    ];
    let refused = crate::tools::git::run_volume_with_guard(
        command_workspace(&child),
        Some(
            root.capture_policy_cut(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
                .await?
                .value,
        ),
        &arguments,
        || async {
            access.require(&root).await?;
            if !changed.swap(true, std::sync::atomic::Ordering::SeqCst) {
                let mut policy: acyclic_fs::access::Policy =
                    serde_json::from_slice(access::INITIAL_POLICY)?;
                policy.version = 2;
                write_policy(&root, &policy)
                    .await
                    .map_err(|error| Error::Storage(error.to_string()))?;
            }
            Ok(())
        },
    )
    .await;
    assert!(
        matches!(refused, Err(Error::Conflict(_))),
        "stale root cut published a restore"
    );
    assert_eq!(
        child
            .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value
            .id(),
        before
    );
    // A new explicit command captures the new cut and can make progress.
    crate::tools::git::run_volume_with_guard(
        command_workspace(&child),
        Some(
            root.capture_policy_cut(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
                .await?
                .value,
        ),
        &arguments,
        || async { access.require(&root).await },
    )
    .await?;
    assert_eq!(
        child
            .read(
                "/rows/02",
                4096,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &cancellation
            )
            .await?
            .value
            .as_ref(),
        b"row"
    );
    Ok(())
}

async fn write_policy(
    workspace: &Workspace<acyclic_fs::MemoryAuthorityBackend, acyclic_fs::MemoryObjectBackend>,
    policy: &acyclic_fs::access::Policy,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let cancellation = acyclic_fs::CancellationToken::new();
    let mut checkout = workspace
        .checkout(
            acyclic_fs::model::GenerationSelector::Head,
            acyclic_fs::model::CheckoutMode::tracking_transaction(),
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    checkout
        .write_access_policy(
            &serde_json::to_vec(&policy)?,
            acyclic_fs::NamedAttributeWriteMode::Replace,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    let outcome = checkout
        .commit::<acyclic_fs::FsError, _, _>(
            acyclic_fs::OperationId::new(),
            None,
            None,
            None,
            acyclic_fs::PublicationPermit::Unrestricted,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancellation,
            |_| {
                std::future::ready(Ok(acyclic_fs::OperationReceipt {
                    value: (),
                    work: WorkCounters::UNCHARGED,
                }))
            },
        )
        .await?
        .value;
    assert!(matches!(
        outcome,
        acyclic_fs::LivePublicationObservation::Committed { .. }
    ));
    Ok(())
}

async fn fixture(
    mask: [bool; 8],
    class: VolumeClass,
) -> Result<(
    Arc<FilesystemHost<acyclic_fs::MemoryAuthorityBackend, acyclic_fs::MemoryObjectBackend>>,
    VolumeRef,
    ContentGrant,
    ContentGrant,
)> {
    let provider = ProviderRef::new(
        "native-list".into(),
        "filesystem".into(),
        "1".into(),
        &mut None,
    )?;
    let host = FilesystemHost::new(Fs::memory(), &provider, None)?;
    let volume = VolumeRef::new(
        provider,
        "project".into(),
        class,
        if class == VolumeClass::AgentPrivate {
            VolumeOwner::Agent(AgentId::from_bytes([9; 16]))
        } else {
            VolumeOwner::Project("owner".into())
        },
        &mut None,
    )?;
    host.create_volume(&volume).await?;
    let issuer = AuthorityRoot::new(
        "native-list",
        [7; 32],
        Authority {
            kind: AggregateKind::Task,
            id: "list".into(),
        },
    );
    let agent = Some(AgentId::from_bytes([9; 16]));
    let legacy = volume.capability(VolumeOperation::Read, &mut None)?;
    let scope = issuer.bootstrap_scope(
        "writer",
        &crate::Capabilities::new([
            legacy.clone(),
            volume.capability(VolumeOperation::Write, &mut None)?,
        ]),
        agent,
        &mut None,
    )?;
    let verifier = issuer.verifier(&mut None)?;
    let writer = ContentGrant::verify(
        &verifier,
        &scope,
        &volume,
        VolumeOperation::Write,
        4096,
        &mut None,
    )?;
    let grant = |path: String| Grant {
        actions: [Action::List, Action::ReadHistory].into_iter().collect(),
        resource: ResourcePattern {
            lineage_root: None,
            volume: None,
            generations: Generations::All,
            path,
        },
    };
    let mut grants = vec![grant("rows".into())];
    for (index, keep) in mask.into_iter().enumerate() {
        let name = format!("{index:02}");
        let path = format!("rows/{name}");
        host.put_content(
            &volume,
            &writer,
            &path,
            b"row",
            "text/plain",
            &name,
            4096,
            &IdempotencyKey::new(format!("row-{index}"))?,
            None,
            crate::conversation::ContentAccess::default(),
        )
        .await?;
        if keep {
            grants.push(grant(path));
        }
    }
    let scope = issuer.bootstrap_scope(
        "reader",
        &crate::Capabilities::new([
            legacy,
            Capability {
                layers: vec![grants],
                expires_at: None,
            }
            .to_scope_label()
            .map_err(|error| Error::Invalid(error.to_string()))?,
        ]),
        agent,
        &mut None,
    )?;
    let read = ContentGrant::verify_directory_read(&verifier, &scope, &volume, "rows", &mut None)?;
    Ok((host, volume, read, writer))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn native_directory_pages_release_custody_and_honor_each_guard(
        mask in any::<[bool; 8]>(), caller_mask in any::<[bool; 8]>(), page_size in 1u32..5,
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        runtime.block_on(async {
            let (host, volume, grant, _) = fixture(mask, VolumeClass::Project).await?;
            let caller = crate::Capabilities::new([Capability {
                layers: vec![std::iter::once("rows".into())
                    .chain(caller_mask.into_iter().enumerate().filter(|(_, keep)| *keep)
                        .map(|(index, _)| format!("rows/{index:02}")))
                    .map(|path| Grant {
                        actions: [Action::List, Action::ReadHistory].into_iter().collect(),
                        resource: ResourcePattern { lineage_root: None, volume: None,
                            generations: Generations::All, path },
                    }).collect()],
                expires_at: None,
            }.to_scope_label().map_err(|error| Error::Invalid(error.to_string()))?]);
            let cancellation = acyclic_fs::CancellationToken::new();
            let work = acyclic_fs::NativeMaterializationWork::new(
                acyclic_fs::performance::BOUNDARY_BUDGET, &cancellation,
            ).map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            // SDK receipts reconcile deferred proof-control retirement.
            let _ = work.receipt();
            let baseline = work.live_allocation_bytes();
            let mut refuse_at = 0;
            let mut guard_count = 0;
            loop {
                let mut calls = 0;
                let result = host.list_private_directory_guarded_with_native_work(DirectoryListing {
                    volume: &volume, grant: &grant, ceiling: Some(&caller), path: "rows", generation: None,
                    after: None, maximum_entries: page_size,
                }, &work, || {
                    calls += 1;
                    std::future::ready(if calls == refuse_at { Err(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Unauthorized, reason: "original owner revoked",
                    }) } else { Ok(()) })
                }).await;
                if refuse_at == 0 { guard_count = calls; }
                assert_eq!(result.is_ok(), refuse_at == 0, "guard {refuse_at}: {result:?}");
                if let Ok(((generation, page), custody)) = result {
                    assert_eq!(custody.bytes(), owned_bytes(&page)? + generation.logical_owned_bytes()?);
                    assert!(!page.entries.is_empty() || !page.has_more);
                    assert!(page.entries.len() <= page_size as usize);
                    for entry in &page.entries {
                        let name = std::str::from_utf8(entry.name.as_bytes()).unwrap();
                        let index: usize = name.parse().unwrap();
                        assert!(mask[index] && caller_mask[index]);
                    }
                    drop(generation);
                    drop(page);
                    drop(custody);
                }
                let _ = work.receipt();
                assert_eq!(work.live_allocation_bytes(), baseline);
                refuse_at += 1;
                if refuse_at > guard_count { break; }
            }
            Ok::<_, Error>(())
        }).map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
}
