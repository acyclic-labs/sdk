use super::*;
use acyclic_fs::WorkBudget;

#[test]
fn native_work_json_preserves_full_u64_values() {
    let work = acyclic_fs::WorkCounters {
        authority_records_read: 9_007_199_254_740_993,
        bytes_copied: u64::MAX,
        materializations: u64::MAX,
        ..Default::default()
    };
    let encoded = work_json(&work).expect("work counters serialize");
    let value: serde_json::Value = serde_json::from_str(&encoded).expect("valid work JSON");
    let object = value.as_object().expect("work counters object");
    let canonical = serde_json::to_value(acyclic_fs::WorkCounters::default())
        .expect("canonical work counters serialize");
    let mut encoded_keys = object.keys().collect::<Vec<_>>();
    let mut canonical_keys = canonical
        .as_object()
        .expect("canonical work counters object")
        .keys()
        .collect::<Vec<_>>();
    encoded_keys.sort_unstable();
    canonical_keys.sort_unstable();
    assert_eq!(
        encoded_keys, canonical_keys,
        "every canonical counter must be present"
    );
    assert!(object.values().all(serde_json::Value::is_string));
    assert_eq!(
        value.get("authorityRecordsRead"),
        Some(&serde_json::Value::String("9007199254740993".to_owned()))
    );
    assert_eq!(
        value.get("bytesCopied"),
        Some(&serde_json::Value::String(u64::MAX.to_string()))
    );
    assert_eq!(
        value.get("materializations"),
        Some(&serde_json::Value::String(u64::MAX.to_string()))
    );
}

#[test]
#[allow(clippy::expect_used, reason = "fixed test fixture must deserialize")]
fn git_json_boundary_rejects_deserialized_capture_authority() {
    let tree = serde_json::json!({
        "kind": "exact",
        "workspace_id": vec![0u8; 16],
        "generation": vec![0u8; 32],
    });
    let result: GitFilesystemResult = serde_json::from_value(serde_json::json!({
        "Captured": {
            "tree": tree,
            "tracked_paths": [],
            "proof": {
                "fork_parent": tree,
                "initial_generation": vec![0u8; 32],
                "operation_id": "00000000-0000-0000-0000-000000000000",
            },
        },
    }))
    .expect("deserialize externally supplied proof");
    assert!(reject_unverified_git_capture(&result).is_err());
    let unfiltered = GitFilesystemResult::Captured {
        tree: GitTreeRef::exact(
            WorkspaceId::from_bytes([0; 16]),
            GenerationId::new(Digest::from_bytes([0; 32])),
        ),
        tracked_paths: Default::default(),
        proof: None,
    };
    assert!(reject_unverified_git_capture(&unfiltered).is_ok());
}

fn test_config() -> VolumeConfig {
    VolumeConfig {
        profile: FilesystemProfile::Portable,
        concurrency: ConcurrencyMode::Optimistic,
        lifecycle: Lifecycle::Durable,
        case_sensitivity: CaseSensitivity::Sensitive,
        unicode: UnicodePolicy::Preserve,
        symbolic_links: true,
        hard_links: true,
        sparse_files: true,
        limits: VolumeLimits::default(),
    }
}

#[test]
fn string_paths_and_directory_cursors_use_native_profile() -> Result<()> {
    let mut config = test_config();
    config.profile = FilesystemProfile::Windows;
    let path = native_path("/ÃƒÆ’Ã‚Â©.txt", config)?;
    let cursor = native_name("ÃƒÆ’Ã‚Â©.txt", config)?;
    assert_eq!(path.components(), &[cursor]);
    assert_eq!(
        path.components().first().map(LogicalName::encoding),
        Some(NameEncoding::WindowsUtf16Le)
    );
    Ok(())
}

#[test]
fn capability_identity_matches_build_target() {
    let facts = native_capabilities();
    assert_eq!(facts.version, env!("CARGO_PKG_VERSION"));
    assert_eq!(facts.platform, std::env::consts::OS);
    assert_eq!(facts.architecture, std::env::consts::ARCH);
    assert!(facts.local);
    assert!(facts.native_watch);
}

#[tokio::test]
async fn native_owner_exposes_and_clears_shared_object_acceleration()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let fs = NativeFs::open(
        root.path().to_string_lossy().into_owned(),
        NativeObjectCacheOptions {
            maximum_entries: NapiU32(8),
            maximum_bytes: bigint(1024),
            maximum_in_flight: NapiU32(2),
            maximum_waiters_per_object: NapiU32(2),
        },
    )
    .await?;
    assert_eq!(fs.object_cache_stats()?.resident_entries.get_u64().1, 0);
    fs.clear_object_cache()?;
    assert_eq!(fs.object_cache_stats()?.resident_bytes.get_u64().1, 0);
    Ok(())
}

#[tokio::test]
async fn native_owner_rejects_zero_or_lossy_object_cache_bounds()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    assert!(
        NativeFs::open(
            root.path().to_string_lossy().into_owned(),
            NativeObjectCacheOptions {
                maximum_entries: NapiU32(0),
                maximum_bytes: bigint(1024),
                maximum_in_flight: NapiU32(2),
                maximum_waiters_per_object: NapiU32(2),
            },
        )
        .await
        .is_err()
    );
    assert!(
        NativeFs::open(
            root.path().to_string_lossy().into_owned(),
            NativeObjectCacheOptions {
                maximum_entries: NapiU32(8),
                maximum_bytes: BigInt {
                    sign_bit: false,
                    words: vec![0, 1],
                },
                maximum_in_flight: NapiU32(2),
                maximum_waiters_per_object: NapiU32(2),
            },
        )
        .await
        .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn native_owner_exposes_both_generation_fenced_speculation_engines()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let fs = NativeFs::open(
        root.path().to_string_lossy().into_owned(),
        NativeObjectCacheOptions {
            maximum_entries: NapiU32(8),
            maximum_bytes: bigint(1024),
            maximum_in_flight: NapiU32(2),
            maximum_waiters_per_object: NapiU32(2),
        },
    )
    .await?;
    let volume_id = VolumeId::new();
    let generation_id = acyclic_fs::GenerationId::new(Digest::from_bytes([7_u8; 32]));
    let speculation = fs.create_speculation(
        Buffer::from(volume_id.into_bytes().to_vec()),
        Buffer::from(generation_id.digest().into_bytes().to_vec()),
        NativeSpeculationOptions {
            residency: NativeResidencySpeculationOptions {
                maximum_active_operations: NapiU32(2),
                maximum_active_bytes: bigint(1024),
                outcome_window: NapiU32(8),
                traffic_window: NapiU32(8),
                speculative_cost_basis_points: NapiU32(10_000),
                minimum_usefulness_samples: NapiU32(2),
                minimum_usefulness_basis_points: NapiU32(1),
            },
            promotion: NativePromotionSpeculationOptions {
                maximum_active_operations: NapiU32(2),
                maximum_active_bytes: bigint(1024),
                maximum_active_cost_units: bigint(1024),
                maximum_residency_facts: NapiU32(4),
                maximum_destinations: NapiU32(4),
                maximum_accepted_tiers: NapiU32(4),
                outcome_window: NapiU32(8),
                minimum_usefulness_samples: NapiU32(2),
                minimum_usefulness_basis_points: NapiU32(1),
            },
        },
    )?;
    let operation_id = OperationId::new();
    let object_id = ObjectId {
        kind: acyclic_fs::ObjectKind::Blob,
        digest: Digest::from_bytes([9_u8; 32]),
    };
    let admission = speculation
        .observe(NativeResidencyObservation {
            operation_id: Buffer::from(operation_id.into_bytes().to_vec()),
            volume_id: Buffer::from(volume_id.into_bytes().to_vec()),
            generation_id: Buffer::from(generation_id.digest().into_bytes().to_vec()),
            foreground_bytes: bigint(1024),
            object_id: encode_object_id(object_id),
            maximum_bytes: bigint(16),
            reason: "sequential-range".to_owned(),
        })
        .await?;
    assert!(matches!(admission.status, NativeResidencyStatus::Admitted));

    let promotion = speculation
        .plan_promotion(
            Buffer::from(operation_id.into_bytes().to_vec()),
            vec!["node-local".to_owned()],
            vec![NativeObjectResidency {
                object_id: encode_object_id(object_id),
                location_id: Buffer::from([1_u8; 16].to_vec()),
                tier: "durable-origin".to_owned(),
                source_priority: NapiU32(0),
            }],
            vec![NativePromotionDestination {
                location_id: Buffer::from([2_u8; 16].to_vec()),
                tier: "node-local".to_owned(),
                writable: true,
                maximum_object_bytes: bigint(1024),
                priority: NapiU32(0),
                cost_units_per_byte: bigint(1),
            }],
        )
        .await?;
    assert!(matches!(promotion.status, NativePromotionStatus::Planned));
    speculation
        .finish_promotion(Buffer::from(operation_id.into_bytes().to_vec()), true)
        .await?;
    speculation
        .finish_residency(Buffer::from(operation_id.into_bytes().to_vec()), true)
        .await?;
    let metrics = speculation.metrics_json().await?;
    assert!(metrics.contains("\"residency\""));
    assert!(metrics.contains("\"promotion\""));
    Ok(())
}

async fn exercise_native_arbitrary_shape_transaction(
    transaction: &NativeWorkspaceTransaction,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    transaction.create_directory("/shapes".to_owned()).await?;
    transaction
        .write(
            "/shapes/source".to_owned(),
            Buffer::from(b"abcdef".to_vec()),
        )
        .await?;
    transaction
        .write(
            "/shapes/destination".to_owned(),
            Buffer::from(b"......".to_vec()),
        )
        .await?;
    transaction
        .create_symbolic_link(
            "/shapes/symlink".to_owned(),
            Buffer::from(b"source".to_vec()),
        )
        .await?;
    transaction
        .hard_link("/shapes/source".to_owned(), "/shapes/hard-link".to_owned())
        .await?;
    transaction
        .write_range(
            "/shapes/source".to_owned(),
            bigint(1),
            Buffer::from(b"Z".to_vec()),
        )
        .await?;
    transaction
        .zero_range(
            "/shapes/source".to_owned(),
            bigint(2),
            bigint(2),
            false,
            false,
        )
        .await?;
    transaction
        .preallocate("/shapes/source".to_owned(), bigint(0), bigint(6), true)
        .await?;
    transaction
        .clone_range(
            "/shapes/source".to_owned(),
            bigint(0),
            "/shapes/destination".to_owned(),
            bigint(1),
            bigint(5),
        )
        .await?;
    transaction
        .resize("/shapes/destination".to_owned(), bigint(8))
        .await?;
    Ok(())
}

async fn assert_native_arbitrary_shape_files(
    workspace: &NativeWorkspace,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    for path in ["/shapes/source", "/shapes/hard-link"] {
        assert_eq!(
            workspace.read(path.to_owned(), bigint(8)).await?.as_ref(),
            b"aZ\0\0ef"
        );
    }
    assert_eq!(
        workspace
            .read("/shapes/destination".to_owned(), bigint(8))
            .await?
            .as_ref(),
        b".aZ\0\0e\0\0"
    );
    Ok(())
}

async fn assert_native_bounded_reads(
    workspace: &NativeWorkspace,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    assert_eq!(
        workspace
            .read_range("/output/status".to_owned(), bigint(1), bigint(3))
            .await?
            .as_ref(),
        b"ead"
    );
    let source_stat = workspace.stat("/shapes/source".to_owned()).await?;
    let hard_link_stat = workspace.stat("/shapes/hard-link".to_owned()).await?;
    assert_eq!(bigint_u64(&source_stat.link_count, "linkCount")?, 2);
    assert_eq!(
        source_stat.file_id.as_ref(),
        hard_link_stat.file_id.as_ref()
    );
    assert!(matches!(source_stat.kind, FileKind::Regular));
    assert_eq!(
        workspace
            .read_symbolic_link("/shapes/symlink".to_owned())
            .await?
            .as_ref(),
        b"source"
    );
    let listing = workspace.sync().await?;
    let first_page = listing
        .list_directory("/shapes".to_owned(), None, NapiU32(1))
        .await?;
    assert!(first_page.has_more);
    let Some(first_entry) = first_page.entries.first() else {
        unreachable!("a page reporting has_more must contain at least one entry");
    };
    assert_eq!(
        workspace
            .write("/shapes/late".to_owned(), Buffer::from(vec![1]))
            .await?
            .status,
        "committed"
    );
    let remaining_page = listing
        .list_directory(
            "/shapes".to_owned(),
            Some(NativeWorkspaceName {
                encoding: first_entry.name.encoding.clone(),
                bytes: Buffer::from(first_entry.name.bytes.as_ref().to_vec()),
            }),
            NapiU32(16),
        )
        .await?;
    assert_eq!(remaining_page.entries.len(), 3);
    let extents = workspace
        .plan_extents(
            "/shapes/source".to_owned(),
            bigint(0),
            bigint(6),
            NapiU32(8),
        )
        .await?;
    assert!(extents.spans.iter().any(|span| span.kind == "content"));
    assert!(
        extents
            .spans
            .iter()
            .any(|span| span.kind == "allocated-zero")
    );
    Ok(())
}

async fn assert_native_transaction_rebase(
    workspace: &NativeWorkspace,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let first = workspace
        .begin_transaction(Some(Buffer::from(vec![31; 16])))
        .await?;
    let disjoint = workspace
        .begin_transaction(Some(Buffer::from(vec![32; 16])))
        .await?;
    first
        .write("/native-race-a".to_owned(), Buffer::from(vec![1]))
        .await?;
    disjoint
        .write("/native-race-b".to_owned(), Buffer::from(vec![2]))
        .await?;
    assert_eq!(first.commit(None).await?.status, "committed");
    assert_eq!(disjoint.commit(None).await?.status, "conflict");
    let safe = disjoint.rebase(NapiU32(16)).await?;
    assert_eq!(safe.status, "rebased");
    assert!(safe.conflicts.is_empty());
    assert_eq!(disjoint.commit(None).await?.status, "committed");

    let winner = workspace
        .begin_transaction(Some(Buffer::from(vec![33; 16])))
        .await?;
    let loser = workspace
        .begin_transaction(Some(Buffer::from(vec![34; 16])))
        .await?;
    winner
        .write("/native-race-a".to_owned(), Buffer::from(vec![3]))
        .await?;
    loser
        .write("/native-race-a".to_owned(), Buffer::from(vec![4]))
        .await?;
    assert_eq!(winner.commit(None).await?.status, "committed");
    assert_eq!(loser.commit(None).await?.status, "conflict");
    let conflict = loser.rebase(NapiU32(16)).await?;
    assert_eq!(conflict.status, "conflicted");
    assert!(!conflict.conflicts.is_empty());
    assert!(!conflict.truncated);
    Ok(())
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one end-to-end lifecycle verifies binary data, exact forks, and reopen durability"
)]
async fn native_named_workspace_is_durable_binary_and_fork_exact()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let fs = NativeFs::open(
        root.path().to_string_lossy().into_owned(),
        NativeObjectCacheOptions {
            maximum_entries: NapiU32(8),
            maximum_bytes: bigint(1024 * 1024),
            maximum_in_flight: NapiU32(2),
            maximum_waiters_per_object: NapiU32(2),
        },
    )
    .await?;
    let workspace = fs.create_workspace("main".to_owned()).await?;
    assert_eq!(workspace.name(), "main");
    let committed = workspace
        .write("/value.bin".to_owned(), Buffer::from(vec![0, 1, 2, 255]))
        .await?;
    assert_eq!(committed.status, "committed");
    assert_eq!(
        workspace
            .read("/value.bin".to_owned(), bigint(16))
            .await?
            .as_ref(),
        &[0, 1, 2, 255]
    );
    let exact = workspace.sync().await?;
    workspace.checkpoint("before-output".to_owned()).await?;
    exact.pin("input-generation".to_owned()).await?;
    let transaction = workspace
        .begin_transaction(Some(Buffer::from(vec![7; 16])))
        .await?;
    transaction
        .create_dir_all("/output/nested".to_owned())
        .await?;
    transaction
        .copy("/value.bin".to_owned(), "/output/nested/copied".to_owned())
        .await?;
    transaction
        .rename(
            "/output/nested/copied".to_owned(),
            "/output/result".to_owned(),
        )
        .await?;
    transaction
        .write("/output/status".to_owned(), Buffer::from(b"ready".to_vec()))
        .await?;
    exercise_native_arbitrary_shape_transaction(&transaction).await?;
    assert_eq!(transaction.commit(None).await?.status, "committed");
    assert_eq!(
        workspace
            .read("/output/status".to_owned(), bigint(5))
            .await?
            .as_ref(),
        b"ready"
    );
    Box::pin(assert_native_arbitrary_shape_files(&workspace)).await?;
    Box::pin(assert_native_bounded_reads(&workspace)).await?;
    Box::pin(assert_native_transaction_rebase(&workspace)).await?;
    assert!(
        exact
            .read("/output/status".to_owned(), bigint(5))
            .await
            .is_err()
    );
    let exact_fork = workspace
        .fork_at(
            "exact-agent".to_owned(),
            &exact,
            NativeForkOptions {
                paths: None,
                idempotency_key: None,
            },
        )
        .await?;
    assert!(
        exact_fork
            .read("/output/status".to_owned(), bigint(5))
            .await
            .is_err()
    );
    let fork = workspace.fork("agent".to_owned(), None).await?;
    assert_eq!(
        fork.read("/value.bin".to_owned(), bigint(16))
            .await?
            .as_ref(),
        &[0, 1, 2, 255]
    );
    drop(fs);

    let reopened = NativeFs::open(
        root.path().to_string_lossy().into_owned(),
        NativeObjectCacheOptions {
            maximum_entries: NapiU32(8),
            maximum_bytes: bigint(1024 * 1024),
            maximum_in_flight: NapiU32(2),
            maximum_waiters_per_object: NapiU32(2),
        },
    )
    .await?
    .open_workspace("main".to_owned())
    .await?;
    assert_eq!(
        reopened
            .read("/value.bin".to_owned(), bigint(16))
            .await?
            .as_ref(),
        &[0, 1, 2, 255]
    );
    Ok(())
}

#[tokio::test]
async fn native_attached_workspace_rescans_and_seals_one_source()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let source = tempfile::tempdir()?;
    std::fs::write(source.path().join("value.bin"), [1_u8, 2, 3])?;
    let fs = NativeFs::open(
        root.path().to_string_lossy().into_owned(),
        NativeObjectCacheOptions {
            maximum_entries: NapiU32(8),
            maximum_bytes: bigint(1024 * 1024),
            maximum_in_flight: NapiU32(2),
            maximum_waiters_per_object: NapiU32(2),
        },
    )
    .await?;
    let workspace = fs
        .attach_directory(
            "attached".to_owned(),
            source.path().to_string_lossy().into_owned(),
            NativeSourceOptions {
                mode: "tracking".to_owned(),
                maximum_paths: NapiU32(128),
                maximum_extent_spans: NapiU32(128),
                maximum_queued_changes: NapiU32(128),
                excluded_paths: None,
            },
        )
        .await?;
    assert_eq!(workspace.source_state().await.status, "clean");
    assert_eq!(
        workspace
            .read("/value.bin".to_owned(), bigint(8))
            .await?
            .as_ref(),
        &[1, 2, 3]
    );
    std::fs::write(source.path().join("value.bin"), [4_u8, 5])?;
    assert_eq!(workspace.rescan_source().await?.status, "clean");
    assert_eq!(
        workspace
            .read("/value.bin".to_owned(), bigint(8))
            .await?
            .as_ref(),
        &[4, 5]
    );
    let sealed = workspace.seal().await?;
    assert_eq!(
        sealed
            .read("/value.bin".to_owned(), bigint(8))
            .await?
            .as_ref(),
        &[4, 5]
    );
    assert_eq!(workspace.source_state().await.status, "sealed");
    Ok(())
}

#[tokio::test]
async fn native_workspace_change_sets_compose_and_join_atomically()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let fs = NativeFs::open(
        root.path().to_string_lossy().into_owned(),
        NativeObjectCacheOptions {
            maximum_entries: NapiU32(32),
            maximum_bytes: bigint(1024 * 1024),
            maximum_in_flight: NapiU32(4),
            maximum_waiters_per_object: NapiU32(4),
        },
    )
    .await?;
    let main = fs.create_workspace("main".to_owned()).await?;
    main.write("/base".to_owned(), Buffer::from(vec![1_u8]))
        .await?;
    let agent = main.fork("agent".to_owned(), None).await?;
    let base = agent.sync().await?;
    agent
        .write("/first".to_owned(), Buffer::from(vec![2_u8]))
        .await?;
    let middle = agent.sync().await?;
    agent
        .write("/second".to_owned(), Buffer::from(vec![3_u8]))
        .await?;
    let end = agent.sync().await?;
    let first = agent.diff(&base, &middle, NapiU32(32)).await?;
    let second = agent.diff(&middle, &end, NapiU32(32)).await?;
    let composed = first.compose(&second, NapiU32(32)).await?;
    assert_eq!(composed.from().id().as_ref(), base.id().as_ref());
    assert_eq!(composed.to().id().as_ref(), end.id().as_ref());
    assert!(!composed.changes()?.files.is_empty());

    let plan = agent
        .join_into(
            &main,
            NativeJoinOptions {
                source_generation: None,
                target_generation: None,
                history: "merge".to_owned(),
                maximum_generations: NapiU32(64),
                maximum_changes: NapiU32(64),
                maximum_conflicts: NapiU32(16),
            },
        )
        .await?;
    let result = plan.apply(plan.target_head(), None).await?;
    assert_eq!(result.status, "applied");
    assert_eq!(
        main.read("/second".to_owned(), bigint(8)).await?.as_ref(),
        &[3]
    );
    Ok(())
}

#[tokio::test]
async fn napi_operation_windows_share_the_native_authority_fence()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let fs = NativeFs::open(
        root.path().to_string_lossy().into_owned(),
        NativeObjectCacheOptions {
            maximum_entries: NapiU32(32),
            maximum_bytes: bigint(1024 * 1024),
            maximum_in_flight: NapiU32(4),
            maximum_waiters_per_object: NapiU32(4),
        },
    )
    .await?;
    let workspace = fs.create_workspace("leased".to_owned()).await?;
    let coordinator = fs.operation_windows();
    let parent = workspace
        .inner
        .head(
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis()
        .try_into()?;
    let lease = coordinator
        .inner
        .begin(workspace.inner.id(), parent.id(), "tool", now, u64::MAX)
        .await?;
    let mut transaction = workspace
        .inner
        .begin_transaction(
            IdempotencyKey::from_bytes([0x71; 16]),
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    transaction.write_text("/accepted", "yes").await?;
    assert!(matches!(
        transaction
            .commit_with_permit(lease.publication_permit())
            .await?,
        TransactionCommit::Committed(_)
    ));

    let parent = workspace
        .inner
        .head(
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    let expired = coordinator
        .inner
        .begin(
            workspace.inner.id(),
            parent.id(),
            "expired",
            now,
            now.saturating_add(1),
        )
        .await?;
    assert!(matches!(
        coordinator
            .inner
            .finish(&expired, now.saturating_add(2))
            .await?,
        OperationWindowFinish::AlreadyClosed
    ));
    let mut rejected = workspace
        .inner
        .begin_transaction(
            IdempotencyKey::from_bytes([0x72; 16]),
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    rejected.write_text("/rejected", "no").await?;
    assert!(matches!(
        rejected
            .commit_with_permit(expired.publication_permit())
            .await?,
        TransactionCommit::Fenced
    ));
    Ok(())
}

#[tokio::test]
async fn shared_publication_fence_blocks_napi_mutation_and_reconcile_recovers()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let source_root = tempfile::tempdir()?;
    let cancellation = CancellationToken::new();
    let fs = LocalFs::local(LocalOptions::new(root.path())).await?;
    let volume = fs
        .create_volume(test_config(), WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value;
    let checkout = volume
        .checkout(
            GenerationSelector::Head,
            CheckoutMode {
                access: AccessMode::ReadWrite,
                consistency: ConsistencyMode::Pinned,
                mutations: MutationMode::PrivateOverlay,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    let shared = Arc::new(SharedCheckout::new(checkout));
    let native = NativeCheckout {
        inner: Arc::clone(&shared),
        config: test_config(),
        cancellation: CancellationToken::new(),
        acquisition_work: acyclic_fs::WorkCounters::default(),
    };
    let unresolved = OperationId::new();
    Box::pin(shared.lock())
        .await
        .retain_operation_id(unresolved)?;
    assert!(
        native
            .create_file("/blocked.txt".to_owned(), Buffer::from(b"blocked".to_vec()))
            .await
            .is_err()
    );
    assert!(!Box::pin(shared.lock()).await.has_pending_mutations());

    let watcher = NativeWatcher {
        inner: std::sync::Mutex::new(FsNativeWatch::open_with_profile(
            source_root.path(),
            test_config().profile,
            NativeWatchOptions {
                limits: test_config().limits,
                maximum_queued_changes: 64,
                recursive: true,
            },
        )?),
        pending: std::sync::Mutex::new(None),
        pending_reconcile: std::sync::Mutex::new(None),
        operation: tokio::sync::Mutex::new(()),
        checkout: Arc::clone(&shared),
        source_root: source_root.path().to_path_buf(),
        cancellation: CancellationToken::new(),
    };
    assert!(
        Box::pin(watcher.reconcile(NapiU32(64), NapiU32(64)))
            .await
            .is_err()
    );
    Box::pin(shared.lock())
        .await
        .clear_retained_operation(unresolved);
    Box::pin(watcher.reconcile(NapiU32(64), NapiU32(64))).await?;
    Ok(())
}
