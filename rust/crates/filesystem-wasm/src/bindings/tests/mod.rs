use super::*;

#[cfg(all(test, target_arch = "wasm32"))]
pub(crate) fn test_workspace_extent_plan_js() -> Result<JsValue, JsValue> {
    BrowserWorkspaceExtentPlan {
        spans: vec![BrowserWorkspaceExtentSpan {
            offset: 9_007_199_254_740_993,
            length: 1,
            source_end: 9_007_199_254_740_994,
            kind: "content",
        }],
    }
    .into_js()
    .map(Into::into)
    .map_err(|error| JsValue::from_str(&error.to_string()))
}

#[cfg(all(test, target_arch = "wasm32"))]
pub(in crate::bindings) fn test_browser_work() -> BrowserWorkCounters {
    browser_work(acyclic_fs::WorkCounters {
        bytes_copied: 17,
        source_path_components: 3,
        source_entries_visited: 2,
        ..acyclic_fs::WorkCounters::default()
    })
}

#[cfg(all(test, target_arch = "wasm32"))]
pub(crate) fn test_checkout_mutation_result_js() -> Result<JsValue, JsValue> {
    BrowserMutationResult {
        file_id: Some(vec![7; 16].into()),
        work: test_browser_work(),
    }
    .into_js()
    .map(Into::into)
    .map_err(|error| JsValue::from_str(&error.to_string()))
}

#[cfg(all(test, target_arch = "wasm32"))]
pub(crate) fn test_checkout_metadata_result_js() -> Result<JsValue, JsValue> {
    BrowserMetadataResult {
        canonical_bytes: vec![0, 255, 1],
        work: test_browser_work(),
    }
    .into_js()
    .map(Into::into)
    .map_err(|error| JsValue::from_str(&error.to_string()))
}

#[cfg(all(test, target_arch = "wasm32"))]
pub(crate) fn test_checkout_export_manifest_result_js() -> Result<JsValue, JsValue> {
    BrowserExportManifestResult {
        manifest_bytes: vec![9, 8, 7],
        objects: vec![vec![1, 2].into(), vec![3, 4].into()],
        work: test_browser_work(),
    }
    .into_js()
    .map(Into::into)
    .map_err(|error| JsValue::from_str(&error.to_string()))
}

#[cfg(all(test, target_arch = "wasm32"))]
pub(crate) fn test_checkout_commit_result_js() -> Result<JsValue, JsValue> {
    BrowserCommitResult {
        status: "committed",
        generation_id: Some(vec![5; 32].into()),
        epoch: Some(9_007_199_254_740_993),
        sequence: None,
        committed_fingerprint: Some(vec![6; 32].into()),
        work: test_browser_work(),
    }
    .into_js()
    .map(Into::into)
    .map_err(|error| JsValue::from_str(&error.to_string()))
}

#[cfg(all(test, target_arch = "wasm32"))]
pub(crate) fn test_transfer_and_residency_results_js() -> Result<JsValue, JsValue> {
    GenerationTransferBatchResult {
        first_object: 9_007_199_254_740_993,
        next_object: Some(9_007_199_254_740_994),
        objects: vec![vec![1, 2, 3].into()],
        work: test_browser_work(),
    }
    .into_js()
    .map(Into::into)
    .map_err(|error| JsValue::from_str(&error.to_string()))
}

#[cfg(all(test, target_arch = "wasm32"))]
pub(crate) fn test_speculation_results_js() -> Result<JsValue, JsValue> {
    BrowserSpeculationPreemption {
        residency_operation_ids: vec![vec![4; 16].into()],
        promotion_operation_ids: vec![vec![5; 16].into()],
    }
    .into_js()
    .map(Into::into)
    .map_err(|error| JsValue::from_str(&error.to_string()))
}

#[cfg(all(test, target_arch = "wasm32"))]
pub(crate) fn test_oversized_transaction_array_rejected() -> Result<(), JsValue> {
    let operations = js_sys::Array::new();
    operations.set_length(2_049);
    decode_authored_transactions(
        operations.into(),
        FilesystemProfile::Posix,
        VolumeLimits::default(),
    )
    .map(|_| ())
}

#[cfg(all(test, target_arch = "wasm32"))]
pub(crate) fn test_transaction_rebase_js(offset: u64) -> Result<JsValue, JsValue> {
    ts(BrowserTransactionRebase {
        status: BrowserTransactionRebaseStatus::Conflicted,
        generation_id: None,
        conflicts: vec![browser_transaction_conflict(RebaseConflict {
            region: DependencyRegion::ContentRange {
                file_id: FileId::from_bytes([7; 16]),
                offset,
                length: u64::MAX,
            },
            usage: DependencyUse::ObservationAndMutation,
            expected: DependencyState::Absent,
            actual: DependencyState::Absent,
        })],
        truncated: false,
    })
    .map(Into::into)
}

use super::*;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

fn test_browser_fs() -> Result<BrowserFs, JsValue> {
    let engine = memory_engine(
        1024,
        64 * 1024 * 1024,
        ObjectCacheOptions {
            maximum_entries: 8,
            maximum_bytes: 1024,
            maximum_in_flight: 2,
            maximum_waiters_per_object: 2,
        },
    )?;
    Ok(BrowserFs {
        engine: Some(engine),
        capabilities: Capabilities {
            version: env!("CARGO_PKG_VERSION"),
            platform: "browser",
            architecture: "wasm32",
            authority: "memory",
            immutable_objects: "memory",
            native_mount: "none",
            writable_native_mount: false,
            native_watch: false,
            native_watch_backend: "none".to_owned(),
            native_watch_persistent_restart: false,
            native_watch_root_identity_fencing: false,
            provider_process_io_observable: false,
        },
    })
}

pub(in crate::bindings) const fn test_speculation_options() -> BrowserSpeculationOptions {
    BrowserSpeculationOptions {
        residency: BrowserResidencySpeculationOptions {
            maximum_active_operations: 2,
            maximum_active_bytes: 1024,
            outcome_window: 8,
            traffic_window: 8,
            speculative_cost_basis_points: 10_000,
            minimum_usefulness_samples: 2,
            minimum_usefulness_basis_points: 1,
        },
        promotion: BrowserPromotionSpeculationOptions {
            maximum_active_operations: 2,
            maximum_active_bytes: 1024,
            maximum_active_cost_units: 1024,
            maximum_residency_facts: 4,
            maximum_destinations: 4,
            maximum_accepted_tiers: 4,
            outcome_window: 8,
            minimum_usefulness_samples: 2,
            minimum_usefulness_basis_points: 1,
        },
    }
}

fn observe_residency(
    speculation: &BrowserSpeculation,
    operation_id: OperationId,
    volume_id: VolumeId,
    generation_id: acyclic_fs::GenerationId,
    object_id: ObjectId,
) -> Result<(), JsValue> {
    let observation = BrowserResidencyObservation {
        operation_id: operation_id.into_bytes().to_vec(),
        volume_id: volume_id.into_bytes().to_vec(),
        generation_id: generation_id.digest().into_bytes().to_vec(),
        foreground_bytes: 1024,
        object_id: encode_object_id(object_id),
        maximum_bytes: 16,
        reason: "sequential-range".to_owned(),
    };
    speculation.observe(serde_wasm_bindgen::to_value(&observation)?)?;
    Ok(())
}

fn plan_promotion(
    speculation: &BrowserSpeculation,
    operation_id: OperationId,
    object_id: ObjectId,
) -> Result<(), JsValue> {
    let request = BrowserPromotionRequest {
        operation_id: operation_id.into_bytes().to_vec(),
        accepted_tiers: vec!["node-local".to_owned()],
        residency: vec![BrowserObjectResidency {
            object_id: encode_object_id(object_id),
            location_id: vec![5; 16],
            tier: "durable-origin".to_owned(),
            source_priority: 0,
        }],
        destinations: vec![BrowserPromotionDestination {
            location_id: vec![6; 16],
            tier: "node-local".to_owned(),
            writable: true,
            maximum_object_bytes: 1024,
            priority: 0,
            cost_units_per_byte: 1,
        }],
    };
    speculation.plan_promotion(serde_wasm_bindgen::to_value(&request)?)?;
    Ok(())
}

#[wasm_bindgen_test]
fn browser_owner_exposes_both_generation_fenced_speculation_engines() -> Result<(), JsValue> {
    let fs = test_browser_fs()?;
    let volume_id = VolumeId::from_bytes([1; 16]);
    let generation_id = acyclic_fs::GenerationId::new(Digest::from_bytes([2; 32]));
    let operation_id = OperationId::from_bytes([3; 16]);
    let object_id = ObjectId {
        kind: acyclic_fs::ObjectKind::Blob,
        digest: Digest::from_bytes([4; 32]),
    };
    let speculation = fs.create_speculation(
        &volume_id.into_bytes(),
        &generation_id.digest().into_bytes(),
        serde_wasm_bindgen::to_value(&test_speculation_options())?,
    )?;
    observe_residency(
        &speculation,
        operation_id,
        volume_id,
        generation_id,
        object_id,
    )?;
    plan_promotion(&speculation, operation_id, object_id)?;
    speculation.finish_promotion(&operation_id.into_bytes(), true)?;
    speculation.finish_residency(&operation_id.into_bytes(), true)?;
    speculation.metrics()?;
    Ok(())
}

#[wasm_bindgen_test(async)]
async fn named_workspace_round_trips_binary_data_and_exact_forks() -> Result<(), JsValue> {
    let fs = test_browser_fs()?;
    let workspace = fs
        .create_workspace(
            "main".to_owned(),
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    let payload = vec![0, 255, 1, 0, 128];
    let exact = Box::pin(prepare_workspace(&workspace, &payload)).await?;
    Box::pin(mutate_workspace(&workspace, &exact)).await
}

#[wasm_bindgen_test(async)]
async fn workspace_fork_retains_independent_binary_data() -> Result<(), JsValue> {
    let fs = test_browser_fs()?;
    let workspace = fs
        .create_workspace(
            "main".to_owned(),
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    let payload = vec![0, 255, 1, 0, 128];
    workspace
        .write("/binary".to_owned(), payload.clone())
        .await?;
    Box::pin(verify_workspace_fork(&workspace, &payload)).await
}

#[wasm_bindgen_test(async)]
async fn workspace_fork_preserves_post_transaction_tree() -> Result<(), JsValue> {
    let fs = test_browser_fs()?;
    let workspace = fs
        .create_workspace(
            "main".to_owned(),
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    let payload = vec![0, 255, 1, 0, 128];
    Box::pin(build_transaction_tree(&workspace, &payload)).await?;
    Box::pin(verify_transaction_tree_fork(&workspace, &payload)).await
}

#[wasm_bindgen_test(async)]
async fn transaction_results_preserve_commits_and_conflicting_rebases() -> Result<(), JsValue> {
    let fs = test_browser_fs()?;
    let workspace = fs
        .create_workspace(
            "transactions".to_owned(),
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    workspace.write("/file".to_owned(), vec![1]).await?;
    let mut winner = workspace.begin_transaction(Some(vec![31; 16])).await?;
    let mut loser = workspace.begin_transaction(Some(vec![32; 16])).await?;
    winner.write("/file".to_owned(), vec![2]).await?;
    loser.write("/file".to_owned(), vec![3]).await?;
    let committed: JsValue = winner.commit(None).await?.into();
    assert_eq!(
        js_sys::Reflect::get(&committed, &"status".into())?
            .as_string()
            .as_deref(),
        Some("committed")
    );
    let generation = js_sys::Reflect::get(&committed, &"generationId".into())?
        .dyn_into::<js_sys::Uint8Array>()?;
    assert_eq!(generation.length(), 32);
    let conflict: JsValue = loser.commit(None).await?.into();
    assert_eq!(
        js_sys::Reflect::get(&conflict, &"status".into())?
            .as_string()
            .as_deref(),
        Some("conflict")
    );
    let rebased: JsValue = loser.rebase(JsValue::from(16)).await?.into();
    assert_eq!(
        js_sys::Reflect::get(&rebased, &"status".into())?
            .as_string()
            .as_deref(),
        Some("conflicted")
    );
    assert!(js_sys::Reflect::get(&rebased, &"generationId".into())?.is_undefined());
    let conflicts =
        js_sys::Reflect::get(&rebased, &"conflicts".into())?.dyn_into::<js_sys::Array>()?;
    assert!(conflicts.length() > 0);
    assert_eq!(workspace.read("/file".to_owned(), 1).await?, vec![2]);
    Ok(())
}

async fn prepare_workspace(
    workspace: &BrowserWorkspace,
    payload: &[u8],
) -> Result<BrowserGeneration, JsValue> {
    assert_eq!(workspace.name(), "main");
    assert_eq!(workspace.id().len(), 16);
    let initial = workspace.head().await?;
    assert_eq!(initial.len(), 32);

    workspace
        .write("/binary".to_owned(), payload.to_vec())
        .await?;
    assert_eq!(workspace.read("/binary".to_owned(), 5).await?, payload);
    let exact = workspace.sync().await?;
    workspace.checkpoint("before-output".to_owned()).await?;
    exact.pin("input-generation".to_owned()).await?;
    Ok(exact)
}

async fn mutate_workspace(
    workspace: &BrowserWorkspace,
    exact: &BrowserGeneration,
) -> Result<(), JsValue> {
    let mut transaction = workspace.begin_transaction(Some(vec![9; 16])).await?;
    transaction
        .create_dir_all("/output/nested".to_owned())
        .await?;
    transaction
        .copy("/binary".to_owned(), "/output/nested/copied".to_owned())
        .await?;
    transaction
        .rename(
            "/output/nested/copied".to_owned(),
            "/output/result".to_owned(),
        )
        .await?;
    transaction
        .write("/output/status".to_owned(), b"ready".to_vec())
        .await?;
    transaction.commit(None).await?;
    assert_eq!(
        workspace.read("/output/status".to_owned(), 5).await?,
        b"ready"
    );
    assert!(exact.read("/output/status".to_owned(), 5).await.is_err());
    let exact_fork = workspace
        .fork_at(
            "exact-fork".to_owned(),
            exact,
            serde_wasm_bindgen::to_value(&BrowserForkOptions {
                paths: None,
                idempotency_key: None,
            })?,
        )
        .await?;
    assert!(
        exact_fork
            .read("/output/status".to_owned(), 5)
            .await
            .is_err()
    );
    Ok(())
}

async fn build_transaction_tree(
    workspace: &BrowserWorkspace,
    payload: &[u8],
) -> Result<(), JsValue> {
    Box::pin(workspace.write("/binary".to_owned(), payload.to_vec())).await?;
    let mut transaction = Box::pin(workspace.begin_transaction(Some(vec![10; 16]))).await?;
    Box::pin(transaction.create_dir_all("/output/nested".to_owned())).await?;
    Box::pin(transaction.copy("/binary".to_owned(), "/output/nested/copied".to_owned())).await?;
    Box::pin(transaction.rename(
        "/output/nested/copied".to_owned(),
        "/output/result".to_owned(),
    ))
    .await?;
    Box::pin(transaction.write("/output/status".to_owned(), b"ready".to_vec())).await?;
    Box::pin(transaction.commit(None)).await.map(|_| ())
}

async fn verify_transaction_tree_fork(
    workspace: &BrowserWorkspace,
    payload: &[u8],
) -> Result<(), JsValue> {
    let fork = Box::pin(workspace.fork("mutated-fork".to_owned(), None)).await?;
    assert_eq!(
        Box::pin(fork.read("/output/result".to_owned(), 5)).await?,
        payload
    );
    assert_eq!(
        Box::pin(fork.read("/output/status".to_owned(), 5)).await?,
        b"ready"
    );
    Box::pin(workspace.remove("/output/status".to_owned())).await?;
    assert!(
        Box::pin(workspace.read("/output/status".to_owned(), 5))
            .await
            .is_err()
    );
    assert_eq!(
        Box::pin(fork.read("/output/status".to_owned(), 5)).await?,
        b"ready"
    );
    Ok(())
}

async fn verify_workspace_fork(
    workspace: &BrowserWorkspace,
    payload: &[u8],
) -> Result<(), JsValue> {
    let fork = workspace.fork("fork".to_owned(), None).await?;
    assert_ne!(fork.id(), workspace.id());
    let fork_head = fork
        .head(
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    let workspace_head = workspace.head().await?;
    assert_ne!(fork_head, workspace_head);
    let fork_payload = fork
        .read(
            "/binary".to_owned(),
            5,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    assert_eq!(fork_payload, payload);

    workspace.remove("/binary".to_owned()).await?;
    let workspace_payload = workspace.read("/binary".to_owned(), 5).await;
    assert!(workspace_payload.is_err());
    let retained_payload = fork
        .read(
            "/binary".to_owned(),
            5,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    assert_eq!(retained_payload, payload);
    Ok(())
}
