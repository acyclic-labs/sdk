//! These regressions consume the real original Root admission and FS creators.
use super::*;
use crate::pi_fork::tests::Fixture;

fn native_runtime() -> pi_native::PiNativeHostRuntime {
    pi_native::PiNativeHostRuntime {
        version: pi_native::PI_NATIVE_VERSION.to_owned(),
        executable: std::env::current_exe().unwrap(), executable_prefix: Vec::new(),
        extension: PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("pi/index.ts").canonicalize().unwrap(),
        provider_extension: None,
        environment: BTreeMap::from([("PATH".to_owned(), std::env::var("PATH").unwrap())]),
    }
}

fn session(fixture: &Fixture) -> PiSession {
    let pi_root::PiRootIntent::Project { context_id, root_id, paths } = &fixture.root_request.root else {
        panic!("actual project Root fixture");
    };
    let mut binding = fixture.root_request.binding.clone();
    binding.scratch.allocation = PiScratchAllocation::Created(fixture.root_allocation.scratch.clone());
    let state = AdapterState {
        task_binding: PluginTaskBinding::Pi(Box::new(binding)), version: ADAPTER_STATE_VERSION,
        root_session_id: "fork-root".to_owned(), root_agent_id: "fork-root".to_owned(),
        active: true, root_context_id: *context_id, root_id: *root_id, pending_root_registration: false,
        roots: fixture.root_allocation.roots.iter().map(|(key, creation)| (key.clone(), RootBinding {
            root_id: decode_fixed(key).unwrap(), path: PathBuf::from(&paths[key]),
            allocation: RootAllocation::Created(creation.clone()),
        })).collect(),
        ..AdapterState::default()
    };
    let directory = fixture.resources.session_directory(&state.root_session_id);
    let mut slots = StateSlots::read(&directory).unwrap().0;
    slots.save(&directory, &state, Survives::PowerLoss).unwrap();
    PiSession { directory, state, slots, runtime: fixture.runtime.clone(), filesystem: fixture.filesystem.clone(),
        stream: fixture.stream.clone(), resources: fixture.resources.clone(), roots: BTreeMap::new(),
        native: None, native_jobs: BTreeMap::new() }
}

fn arguments(session: &PiSession, fixture: &Fixture) -> Value {
    let caller = fixture.owner.policy().root_task;
    let original = pi_fork::original_operation(caller, caller, &fixture.commit.sdk_operation);
    let operations = pi_fork::PiForkOperations::new(original);
    let key = fixture.root_request.binding.project_volumes.keys().next().unwrap();
    let paths = pi_native::planned_native_paths(&fixture.resources, "fork-root",
        acyclic_harness::runtime::task_id_for_admission(operations.child), Some(key)).unwrap();
    let cut = fixture.commit.launch.source.clone();
    let native_session_id = uuid::Uuid::new_v4().to_string();
    let session_bytes = format!("{}\n", json!({"type":"session","version":3,"id":native_session_id,
        "timestamp":cut.captured_at,"cwd":paths.cwd}));
    json!({"operation":fixture.commit.sdk_operation,"id":fixture.commit.alias,
        "prompt":"--version\nliteral '-e' must reach standard input", "provider":"actual-provider","model":"actual-model",
        "context":"scratch","cwd":session.state.roots[key].path,"cut":cut,
        "native_session_id":native_session_id,"session_bytes":session_bytes})
}

#[tokio::test]
async fn preparing_native_seed_is_pure_single_root_and_has_distinct_empty_private_intents() {
    let fixture = Fixture::new_with_native_runtime(Some(native_runtime())).await;
    let session = session(&fixture);
    let args = arguments(&session, &fixture);
    let caller = fixture.owner.policy().root_task;
    let work = NativeMaterializationWork::new(pi_root_provider::allocation_budget(), &CancellationToken::new()).unwrap();
    let (commit, _commit_custody) = session.spawn_native_commit(caller, &args, &fixture.owner, &work).unwrap();
    let (again, _again_custody) = session.spawn_native_commit(caller, &args, &fixture.owner, &work).unwrap();
    assert!(_commit_custody.work().shares_ledger(&work));
    assert!(_again_custody.work().shares_ledger(&work));
    let commit = commit;
    let again = again;
    assert!(same_spawn_commit(&commit, &again));
    assert_eq!(commit.alias, commit.launch.agent_alias);
    assert_eq!(commit.child.native_session_id, args["native_session_id"].as_str().unwrap());
    assert_eq!(commit.child.project_volumes.len(), 1);
    assert_eq!(commit.launch.invocation.prompt, args["prompt"].as_str().unwrap());
    assert_eq!(commit.launch.source.mode, pi_native::PiNativeSourceMode::Scratch);
    assert_eq!(commit.launch.source.entries_json, "[]");
    assert!(matches!(commit.child.scratch.allocation, PiScratchAllocation::Preparing));
    let result = commit.child.results.as_ref().unwrap();
    assert!(matches!(result.allocation, PiScratchAllocation::Preparing));
    assert_ne!(result.volume, commit.child.scratch.volume);
    assert_eq!(result.volume.owner(), &VolumeOwner::Agent(commit.child.agent_id));
    assert_eq!(commit.child.scratch.volume.owner(), result.volume.owner());
    assert_eq!(result.volume.provider(), fixture.owner.policy().journal.provider());
    assert!(!commit.launch.invocation.environment.contains_key("ACYCLIC_PI_CALLER"));
    for volume in commit.child.project_volumes.values().chain([&commit.child.scratch.volume, &result.volume]) {
        assert!(fixture.resources.fs.open_workspace(volume.storage_name().unwrap()).await.is_err());
    }
    assert!(session.state.pending.is_empty());
    assert!(session.native.is_none());
    assert!(session.native_jobs.is_empty());
}

#[tokio::test]
async fn original_creator_catalog_barriers_and_duplicate_observation_do_not_dispatch_native_again() {
    let mut fixture = Fixture::new_with_native_runtime(Some(native_runtime())).await;
    let mut session = session(&fixture);
    let args = arguments(&session, &fixture);
    let caller = fixture.owner.policy().root_task;
    let intent_work = NativeMaterializationWork::new(pi_root_provider::allocation_budget(), &CancellationToken::new()).unwrap();
    let (commit, _commit_custody) = session.spawn_native_commit(caller, &args, &fixture.owner, &intent_work).unwrap();
    assert!(_commit_custody.work().shares_ledger(&intent_work));
    let commit = commit;
    fixture.commit = commit.clone();
    let plan = fixture.plan().await;
    let operations = pi_fork::PiForkOperations::new(plan.original);
    #[derive(Serialize)]
    struct SourceReply<'a> { root_key: &'a str, snapshot: LazySnapshotRef }
    let source_work = NativeMaterializationWork::new(pi_root_provider::allocation_budget(), &CancellationToken::new()).unwrap();
    let source_baseline = source_work.live_allocation_bytes();
    let root_key = commit.child.project_volumes.keys().next().unwrap();
    let (source_value, source_custody) = pi_root_provider::guarded_pi_value(
        &SourceReply { root_key, snapshot: plan.frozen }, fixture.owner.policy().maximum_payload_bytes, &source_work).unwrap();
    let observed: SpawnRootSourceAcknowledgement = serde_json::from_value(source_value).unwrap();
    assert_eq!(observed.root_key, *root_key);
    assert_eq!(observed.snapshot, plan.frozen);
    let source_owned = source_work.live_allocation_bytes();
    assert!(source_owned > source_baseline);
    fixture.coordinator().admit_preparation(&fixture.context, &fixture.journal,
        fixture.fence.clone(), &plan).await.unwrap();
    let reservation = session.spawn_reservation(&commit, operations).unwrap();
    session.spawn_save_pending(vec![reservation], &commit, operations).unwrap();
    assert_eq!(source_work.live_allocation_bytes(), source_owned);
    let (preparing, _) = load_state(&session.directory).unwrap();
    assert_eq!(preparing.pending.len(), 1);
    assert!(preparing.routes.is_empty());
    assert_eq!(preparing.pending[0].agent_id.as_deref(), Some(commit.alias.as_str()));
    assert_eq!(preparing.pending[0].fork_key, operations.fork.into_bytes());
    assert!(fixture.resources.fs.open_workspace(commit.child.scratch.volume.storage_name().unwrap()).await.is_err());
    let fork_request: acyclic_harness::fork::ForkRequest = serde_json::from_value(plan.request.payload.clone()).unwrap();
    assert!(fork_request.selections.iter().any(|selection| matches!(&selection.revision,
        acyclic_harness::fork::ResourceRevision::PrivateVolume { volume, paths, .. }
            if volume == &plan.source.private_volume && paths.is_empty())));
    let prepared = fixture.coordinator().advance(&fixture.context, &fixture.journal, fixture.fence.clone(),
        &plan, fixture.source.clone(), pi_root_provider::allocation_budget(), CancellationToken::new()).await.unwrap().unwrap();
    let results = prepared.binding.results.as_ref().unwrap();
    let PiScratchAllocation::Created(result_creation) = &results.allocation else { panic!("actual third creator"); };
    let PiScratchAllocation::Created(scratch_creation) = &prepared.binding.scratch.allocation else { panic!("actual scratch creator"); };
    assert_ne!(result_creation.workspace_id, scratch_creation.workspace_id);
    assert_eq!(prepared.report.child_result_volume.as_ref(), Some(&results.volume));
    assert_eq!(prepared.report.child_result_generation.as_ref(), Some(&result_creation.generation));
    assert_eq!(fixture.resources.fs.open_workspace(results.volume.storage_name().unwrap()).await.unwrap().id().into_bytes(),
        result_creation.workspace_id);
    let workspace = prepared.workspace.workspace().id();
    let source_cut = prepared.frozen_source;
    let native_cut = prepared.native_lazy;
    assert_eq!(prepared.workspace.read("/observed.txt", 64).await.unwrap().as_ref(), b"original");
    session.spawn_save_created(&commit, operations, &prepared).unwrap();
    // Actual decoded source metadata stays in original custody across creator
    // await and both PowerLoss catalog barriers, without resetting work.
    assert_eq!(source_work.live_allocation_bytes(), source_owned);
    let source_spent = source_work.receipt();
    drop(observed);
    assert_eq!(source_work.live_allocation_bytes(), source_owned);
    drop(source_custody);
    assert_eq!(source_work.live_allocation_bytes(), source_baseline);
    assert_eq!(source_work.receipt(), source_spent);
    let (created, _) = load_state(&session.directory).unwrap();
    assert!(created.pending.is_empty());
    let route = &created.routes[&commit.alias];
    assert_eq!(route.agent_id, commit.alias);
    assert_eq!(route.turn_id, commit.sdk_operation);
    assert_eq!(route.context_id, operations.fork.into_bytes());
    assert_eq!(route.roots.values().next().unwrap().repository_workspace_id, workspace.into_bytes());
    assert!(session.native.is_none());
    assert!(session.native_jobs.is_empty());
    let generations = session.slots.last_generation;
    let query_work = NativeMaterializationWork::new(pi_root_provider::allocation_budget(), &CancellationToken::new()).unwrap();
    let (reply, custody) = session.spawn_commit(caller, &args, &fixture.owner, &query_work).await.unwrap();
    let reply_work = custody.work().clone();
    assert!(reply_work.shares_ledger(&query_work));
    let returned_prefix = reply_work.receipt();
    query_work.charge(acyclic_fs::WorkCounters { items_examined: 1, ..Default::default() }).unwrap();
    assert_eq!(reply_work.receipt().items_examined, returned_prefix.items_examined + 1);
    assert_eq!(reply_work.receipt(), query_work.receipt());
    let reply_baseline = reply_work.live_allocation_bytes() - custody.bytes();
    assert!(reply_work.live_allocation_bytes() > reply_baseline);
    assert!(reply_work.receipt().bytes_encoded > 0);
    assert!(reply_work.receipt().allocation_operations > 0);
    assert_eq!(reply["child"]["task"], json!(commit.child.task_id));
    assert_eq!(reply["fork_operation"], json!(operations.fork));
    assert_eq!(reply["native_operation"], json!(operations.native));
    assert_eq!(reply["state"], "pending");
    assert_eq!(reply["uncertain"], false);
    assert_eq!(session.slots.last_generation, generations);
    // The funded readonly host carries original owner custody, but no model driver is started.
    assert!(session.native.is_some());
    assert!(session.native_jobs.is_empty());
    assert_eq!(prepared.frozen_source, source_cut);
    assert_eq!(prepared.native_lazy, native_cut);
    // Reply custody reaches the final observer; dropping the reply first
    // releases owned memory without refunding this query's finite work.
    let final_work = reply_work.receipt();
    drop(reply);
    assert!(reply_work.live_allocation_bytes() > reply_baseline);
    drop(custody);
    assert_eq!(reply_work.live_allocation_bytes(), reply_baseline);
    assert_eq!(reply_work.receipt(), final_work);
    let mut changed = args.clone();
    changed["prompt"] = json!("changed original literal instruction");
    assert!(session.spawn_commit(caller, &changed, &fixture.owner, &query_work).await.is_err());
    let mut changed = args.clone();
    changed["native_session_id"] = json!(uuid::Uuid::new_v4().to_string());
    assert!(session.spawn_commit(caller, &changed, &fixture.owner, &query_work).await.is_err());
    assert_eq!(session.slots.last_generation, generations);
    assert!(session.native_jobs.is_empty());
    query_work.cancellation().cancel();
    assert!(matches!(session.spawn_commit(caller, &args, &fixture.owner, &query_work).await,
        Err(acyclic_harness::Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, .. })));
}

#[tokio::test]
async fn selected_workspace_never_substitutes_foreign_native_cut_or_reserved_alias() {
    let fixture = Fixture::new_with_native_runtime(Some(native_runtime())).await;
    let session = session(&fixture);
    let args = arguments(&session, &fixture);
    let caller = fixture.owner.policy().root_task;
    let oversized_alias = "a".repeat(129);
    let work = NativeMaterializationWork::new(pi_root_provider::allocation_budget(), &CancellationToken::new()).unwrap();
    for id in ["root", "fork-root", "slash/alias", "", oversized_alias.as_str()] {
        let mut changed = args.clone();
        changed["id"] = json!(id);
        assert!(matches!(session.spawn_native_commit(caller, &changed, &fixture.owner, &work),
            Err(acyclic_harness::Error::Invalid(_))));
    }
    let mut changed = args.clone();
    changed["source"] = json!("root");
    changed["cut"]["parentSessionId"] = json!("foreign-native-session");
    assert!(session.spawn_native_commit(caller, &changed, &fixture.owner, &work).is_err());
    let mut changed = args.clone();
    changed["context"] = json!("native_cut");
    assert!(session.spawn_native_commit(caller, &changed, &fixture.owner, &work).is_err());
    let mut changed = args.clone();
    changed["session_bytes"] = json!(format!("{}{{}}\n", args["session_bytes"].as_str().unwrap()));
    assert!(session.spawn_native_commit(caller, &changed, &fixture.owner, &work).is_err());
    let mut changed = args;
    changed["root"] = json!(".");
    assert!(session.spawn_native_commit(caller, &changed, &fixture.owner, &work).is_err());
    assert!(session.state.pending.is_empty());
    assert!(session.state.routes.is_empty());
}
