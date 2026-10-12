use super::*;
use crate::{Digest, WorkspaceContextRoot, WorkspaceContextState, WorkspaceName, WorkspaceRootId};

pub(super) fn context_paths(namespace: &Namespace, id: WorkspaceContextId) -> RecordPaths<'_> {
    RecordKey::Context(id)
        .paths(namespace)
        .expect("record paths")
}

pub(super) fn children_paths(namespace: &Namespace, id: WorkspaceContextId) -> RecordPaths<'_> {
    RecordKey::Children(id)
        .paths(namespace)
        .expect("record paths")
}

pub(super) fn count_paths(namespace: &Namespace, id: WorkspaceContextId) -> RecordPaths<'_> {
    RecordKey::ChildCount(id)
        .paths(namespace)
        .expect("record paths")
}

pub(super) fn workspace() -> WorkspaceId {
    WorkspaceId::derive(
        [4; 16],
        &WorkspaceName::new(
            "durable",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .expect("valid workspace")
        .value,
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .expect("unbounded workspace identity")
    .value
}

pub(super) fn workspace_context(
    directory: &Path,
    context_id: WorkspaceContextId,
    revision: u64,
    state: WorkspaceContextState,
) -> WorkspaceContext {
    let root_id = WorkspaceRootId::from_bytes([9; 16]);
    WorkspaceContext {
        version: 1,
        revision,
        context_id,
        parent_context_id: None,
        roots: [(
            root_id,
            WorkspaceContextRoot {
                root_id,
                source_path: directory.to_path_buf(),
                workspace_id: workspace(),
                workspace_name: "durable".to_owned(),
                parent_workspace_id: None,
                mount_path: None,
            },
        )]
        .into_iter()
        .collect(),
        state,
    }
}

/// One non-empty overlay node and its address.
pub(super) fn overlay_node(path: &str) -> (LazyOverlayId, LazyOverlay) {
    let node: LazyOverlay = serde_json::from_value(serde_json::json!({
        "Node": {
            "path": path,
            "priority": 0,
            "change": {"Tombstone": {"removed": null}},
            "left": vec![0_u8; 32],
            "right": vec![0_u8; 32]
        }
    }))
    .expect("overlay node");
    let id = LazyOverlayId::from_bytes(content_address(&node).expect("overlay address"));
    (id, node)
}

pub(super) fn materialization_journal(operation_id: OperationId) -> MaterializationJournal {
    MaterializationJournal {
        #[cfg(not(target_arch = "wasm32"))]
        custody: None,
        version: 2,
        revision: 1,
        root_identity: None,
        preparation_identity: None,
        preparation_progress: None,
        work: crate::WorkCounters::UNCHARGED,
        plan: crate::MaterializationPlan {
            operation_id,
            intent: crate::MaterializationIntent::GenerationTransition {
                from: crate::GenerationId::new(Digest::from_bytes([1; 32])),
                to: crate::GenerationId::new(Digest::from_bytes([2; 32])),
            },
            edits: Vec::new(),
        },
        preimages: Vec::new(),
        phase: crate::MaterializationPhase::Prepared,
        applied: 0,
        restored: 0,
    }
}

/// Canonical root binding without original fork evidence.
pub(super) fn lazy_state(workspace_id: WorkspaceId, revision: u64) -> LazyWorkspaceState {
    let shadows = [3_u8; 32];
    serde_json::from_value(serde_json::json!({
        "schema_version": 10,
        "revision": revision,
        "workspace_id": workspace_id,
        "parent_workspace_id": null,
        "fork_origin": null,
        "completed_selected_import": null,
        "pending_write": null,
        "completed_write": null,
        "source": crate::demand::SourceReference { identity: [1; 16], epoch: 0 },
        "overlay": LazyOverlayId::from_bytes([2; 32]),
        "shadows": shadows,
        "pending_remove": null,
    }))
    .expect("lazy binding fixture")
}

pub(super) fn is_ownership_conflict<T>(result: &Result<T, LocalCoreStateStoreError>) -> bool {
    matches!(result, Err(LocalCoreStateStoreError::OwnershipConflict(_)))
}

#[test]
pub(super) fn ownership_child_acts_on_its_namespace() -> Result<(), Box<dyn std::error::Error>> {
    let Some(root) = std::env::var_os("ACYCLIC_CORE_STATE_CHILD_ROOT") else {
        return Ok(());
    };
    if let Some(ready) = std::env::var_os("ACYCLIC_CORE_STATE_CHILD_READY") {
        let _owner = LocalCoreStateStore::open_owned(&root)?;
        std::fs::write(ready, b"ready")?;
        loop {
            std::thread::park();
        }
    }
    if !is_ownership_conflict(&LocalCoreStateStore::new(&root).namespace()) {
        return Err("child admitted a shared store to an owned namespace".into());
    }
    if !is_ownership_conflict(&LocalCoreStateStore::open_owned(&root)) {
        return Err("child opened a second owner".into());
    }
    Ok(())
}

pub(super) fn ownership_child(root: &Path) -> Result<std::process::Command, std::io::Error> {
    let mut child = std::process::Command::new(std::env::current_exe()?);
    child
        .args([
            "--exact",
            "core_state::tests::ownership_child_acts_on_its_namespace",
            "--nocapture",
        ])
        .env("ACYCLIC_CORE_STATE_CHILD_ROOT", root);
    Ok(child)
}

/// Rewrites the log's only frame with `edit` and a matching checksum.
pub(super) fn rewrite_intact_frame(
    root: &Path,
    edit: impl FnOnce(&mut serde_json::Value),
) -> Vec<u8> {
    let log = {
        let store = LocalCoreStateStore::new(root);
        let namespace = store.namespace().expect("admitted namespace");
        &namespace
            .record(CORE_LOG_FAMILY, &[0; 16])
            .expect("record paths")
            .current
    };
    let bytes = std::fs::read(&log).expect("read core log");
    let line = bytes.strip_suffix(b"\n").expect("one complete frame");
    let mut frame: serde_json::Value = serde_json::from_slice(&line[65..]).expect("frame json");
    edit(&mut frame);
    let frame = serde_json::to_vec(&frame).expect("encode frame");
    let mut rewritten = blake3::hash(&frame).to_hex().as_bytes().to_vec();
    rewritten.push(b' ');
    rewritten.extend_from_slice(&frame);
    rewritten.push(b'\n');
    std::fs::write(&log, &rewritten).expect("rewrite core log");
    rewritten
}

pub(super) fn crash_lineage() -> WorkspaceLineageRecord {
    WorkspaceLineageRecord {
        version: 1,
        revision: 1,
        ready: true,
        workspace_id: CRASH_WORKSPACE,
        workspace_name: "crash-child".to_owned(),
        parent_workspace_id: Some(workspace()),
        parent_workspace_name: Some("durable".to_owned()),
        fork_generation: crate::GenerationId::new(Digest::from_bytes([5; 32])),
        initial_generation: crate::GenerationId::new(Digest::from_bytes([6; 32])),
    }
}

/// Every logged record the crash script changes.
#[derive(Debug, PartialEq)]
pub(super) struct CoreSnapshot {
    pub(super) contexts: Vec<WorkspaceContext>,
    pub(super) children: Option<ContextChildren>,
    pub(super) child_count: Option<ContextChildCount>,
    pub(super) index_ready: bool,
    pub(super) lineage: Option<WorkspaceLineageRecord>,
    pub(super) lazy: Option<LazyWorkspaceState>,
}

/// Recovers the namespace through `store` and reads every scripted record.
pub(super) fn core_snapshot(store: &LocalCoreStateStore) -> CoreSnapshot {
    let namespace = store.namespace().expect("admitted namespace");
    namespace
        .with_log(|log| {
            Ok(CoreSnapshot {
                contexts: load_all_contexts(&namespace, log)?,
                children: log.get::<ContextChildren>(&namespace, CRASH_PARENT)?,
                child_count: log.get::<ContextChildCount>(&namespace, CRASH_PARENT)?,
                index_ready: context_children_index_ready(&namespace, log)?,
                lineage: log.get::<WorkspaceLineageRecord>(&namespace, CRASH_WORKSPACE)?,
                lazy: log.get::<LazyWorkspaceState>(&namespace, CRASH_WORKSPACE)?,
            })
        })
        .expect("recovered snapshot")
}

/// Runs one commit of the crash script: index, register a parent, register its child with the child's lineage and binding, freeze the child, advance the binding, discard the child.
pub(super) fn crash_step(store: &LocalCoreStateStore, step: usize) {
    let source = std::env::temp_dir();
    let parent = workspace_context(&source, CRASH_PARENT, 1, WorkspaceContextState::Active);
    let mut child = workspace_context(&source, CRASH_CHILD, 1, WorkspaceContextState::Active);
    child.parent_context_id = Some(CRASH_PARENT);
    let committed = futures::executor::block_on(async {
        match step {
            0 => WorkspaceContextStore::list(store).await.map(|_| true),
            1 => {
                store
                    .compare_and_swap_many(BTreeMap::from([(CRASH_PARENT, 0)]), vec![parent], false)
                    .await
            }
            2 => {
                store
                    .compare_and_swap_many(
                        BTreeMap::from([(CRASH_CHILD, 0), (CRASH_PARENT, 1)]),
                        vec![child],
                        false,
                    )
                    .await
            }
            3 => {
                WorkspaceLineageStore::compare_and_swap(store, CRASH_WORKSPACE, 0, crash_lineage())
                    .await
            }
            4 => {
                store
                    .compare_and_swap_lazy_workspace(
                        CRASH_WORKSPACE,
                        0,
                        lazy_state(CRASH_WORKSPACE, 1),
                    )
                    .await
            }
            5 => {
                child.revision = 2;
                child.state = WorkspaceContextState::Frozen;
                store
                    .compare_and_swap_many(BTreeMap::from([(CRASH_CHILD, 1)]), vec![child], false)
                    .await
            }
            6 => {
                store
                    .compare_and_swap_lazy_workspace(
                        CRASH_WORKSPACE,
                        1,
                        lazy_state(CRASH_WORKSPACE, 2),
                    )
                    .await
            }
            _ => WorkspaceContextStore::discard_subtree(store, CRASH_PARENT, CRASH_CHILD, 8)
                .await
                .map(|outcome| matches!(outcome, WorkspaceContextDiscardOutcome::Discarded(_))),
        }
    })
    .expect("crash script step");
    assert!(committed, "crash script step {step} was rejected");
}

pub(super) fn crash_store(root: &Path, owned: bool) -> LocalCoreStateStore {
    if owned {
        LocalCoreStateStore::open_owned(root).expect("owner")
    } else {
        LocalCoreStateStore::new(root)
    }
}

#[test]
pub(super) fn core_crash_child_runs_until_its_crash_point() -> Result<(), Box<dyn std::error::Error>>
{
    let Some(root) = std::env::var_os("ACYCLIC_CORE_STATE_CRASH_ROOT") else {
        return Ok(());
    };
    let acknowledged = std::env::var_os("ACYCLIC_CORE_STATE_CRASH_ACKNOWLEDGED")
        .ok_or("crash child acknowledgement path is absent")?;
    let store = crash_store(
        Path::new(&root),
        std::env::var_os("ACYCLIC_CORE_STATE_CRASH_OWNED").is_some(),
    );
    for group in CRASH_GROUPS {
        if group.len() == 1 {
            crash_step(&store, group.start);
        } else {
            let (deferred, durability) = store.defer_durability();
            for step in group.clone() {
                crash_step(&deferred, step);
            }
            futures::executor::block_on(durability.commit())?;
        }
        std::fs::write(&acknowledged, group.end.to_string())?;
    }
    Ok(())
}

/// Simulates losing every write not yet flushed: the log keeps its durable prefix and half of the next frame, and every record a durable frame names reads back torn.
pub(super) fn lose_unflushed_writes(root: &Path, flushed: usize) {
    let store = LocalCoreStateStore::new(root);
    let namespace = store.namespace().expect("admitted namespace");
    let log = &namespace
        .record(CORE_LOG_FAMILY, &[0; 16])
        .expect("record paths")
        .current;
    let mut bytes = std::fs::read(&log).expect("read core log");
    let torn = flushed + (bytes.len().saturating_sub(flushed)) / 2;
    bytes.truncate(torn);
    std::fs::write(&log, &bytes).expect("tear unflushed frames");
    for change in replayable_changes(bytes.get(..flushed).unwrap_or(&bytes)).expect("decodable log")
    {
        for record in change.0 {
            let key = record.key;
            let paths = key.paths(&namespace).expect("record paths");
            std::fs::create_dir_all(&paths.directory).expect("record directory");
            std::fs::write(paths.current, b"{\"torn").expect("tear unflushed record");
        }
    }
}

pub(super) fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("copy directory");
    for entry in std::fs::read_dir(from).expect("read copied directory") {
        let entry = entry.expect("copied entry");
        let target = to.join(entry.file_name());
        if entry.file_type().expect("copied entry type").is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).expect("copy file");
        }
    }
}

/// Kills a writer at every crash point of every commit, then recovers both what the killed process left and what a power loss would leave.
pub(super) fn assert_core_commits_are_crash_atomic(owned: bool) {
    let directory = tempfile::tempdir().expect("temporary directory");
    let reference_store = crash_store(&directory.path().join("reference"), owned);
    let mut snapshots = vec![core_snapshot(&reference_store)];
    for step in 0..CRASH_GROUPS[CRASH_GROUPS.len() - 1].end {
        crash_step(&reference_store, step);
        snapshots.push(core_snapshot(&reference_store));
    }
    drop(reference_store);

    let executable = std::env::current_exe().expect("test executable");
    let mut crashes = 0;
    for point in 1.. {
        assert!(point < 1_000, "crash script never completed");
        let run = directory.path().join(format!("run-{point}"));
        let root = run.join("state");
        let acknowledged = run.join("acknowledged");
        let report = run.join("crash-point");
        std::fs::create_dir_all(&run).expect("run directory");
        let mut child = std::process::Command::new(&executable);
        child
            .args([
                "--exact",
                "core_state::tests::core_crash_child_runs_until_its_crash_point",
                "--nocapture",
            ])
            .env("ACYCLIC_CORE_STATE_CRASH_ROOT", &root)
            .env("ACYCLIC_CORE_STATE_CRASH_ACKNOWLEDGED", &acknowledged)
            .env("ACYCLIC_CORE_STATE_CRASH_POINT", point.to_string())
            .env("ACYCLIC_CORE_STATE_CRASH_REPORT", &report)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        if owned {
            child.env("ACYCLIC_CORE_STATE_CRASH_OWNED", "1");
        }
        let status =
            acyclic_native_runtime::process_status(&mut child, std::time::Duration::from_secs(120))
                .expect("crash child");
        if status.success() {
            assert!(
                !report.exists(),
                "a crash child reported success after aborting"
            );
            assert_eq!(
                core_snapshot(&crash_store(&root, owned)),
                snapshots[snapshots.len() - 1]
            );
            break;
        }
        crashes += 1;
        let report = std::fs::read_to_string(&report).expect("crash point report");
        let (label, flushed) = report.split_once('\n').expect("crash point fields");
        let flushed = flushed.parse().expect("durable log length");
        let done: usize = std::fs::read_to_string(&acknowledged)
            .map_or(0, |count| count.parse().expect("acknowledged count"));
        let in_flight = CRASH_GROUPS
            .iter()
            .find(|group| group.start == done)
            .expect("in-flight group");
        let lost = run.join("power-loss");
        copy_tree(&root, &lost);
        lose_unflushed_writes(&lost, flushed);
        for (recovered, outcome) in [(&root, "kill"), (&lost, "power loss")] {
            let snapshot = core_snapshot(&crash_store(recovered, !owned));
            assert!(
                snapshots[done..=in_flight.end].contains(&snapshot),
                "{outcome} at point {point} ({label}) after {done} commits exposed {snapshot:?}"
            );
        }
    }
    assert!(crashes > 16, "crash points were not exercised");
}

/// A treap node over `left` and `right`, as mutations build them.
pub(super) fn linked_overlay(
    path: &str,
    left: LazyOverlayId,
    right: LazyOverlayId,
) -> (LazyOverlayId, LazyOverlay) {
    let node: LazyOverlay = serde_json::from_value(serde_json::json!({
        "Node": {
            "path": path,
            "priority": 0,
            "change": {"Tombstone": {"removed": null}},
            "left": left,
            "right": right
        }
    }))
    .expect("overlay node");
    let id = LazyOverlayId::from_bytes(content_address(&node).expect("overlay address"));
    (id, node)
}

pub(super) async fn origin_parent_fixture(store: &LocalCoreStateStore) -> LazyWorkspaceState {
    let empty = LazyOverlayId::from_bytes(content_address(&LazyOverlay::Empty).expect("empty"));
    let (overlay, node) = linked_overlay("/original", empty, empty);
    store
        .put_lazy_overlay(overlay, node)
        .await
        .expect("original overlay");
    let empty_shadows = crate::LazyShadowId::from_bytes(
        content_address(&crate::LazyShadow::Empty).expect("empty shadows"),
    );
    let shadow = crate::LazyShadow::Node {
        file_id: crate::FileId::from_bytes([0xa1; 16]),
        priority: 0,
        record: vec![1, 2, 3],
        metadata: Box::new(crate::WorkspaceMetadata::from_engine(
            crate::kernel::FileMetadata::default(),
        )),
        left: empty_shadows,
        right: empty_shadows,
    };
    let shadows = crate::LazyShadowId::from_bytes(content_address(&shadow).expect("shadow"));
    store
        .put_lazy_shadow(shadows, shadow)
        .await
        .expect("original shadows");
    let parent = LazyWorkspaceState {
        overlay,
        shadows,
        ..lazy_state(WorkspaceId::from_bytes([0xa0; 16]), 1)
    };
    assert!(
        store
            .compare_and_swap_lazy_workspace(parent.workspace_id, 0, parent.clone(),)
            .await
            .expect("parent binding")
    );
    parent
}

pub(super) fn prepared_origin_fixture(parent: &LazyWorkspaceState) -> LazyWorkspaceState {
    let snapshot = crate::LazySnapshotRef::new(
        parent.workspace_id,
        crate::GenerationId::new(Digest::from_bytes([0xa2; 32])),
        parent.source,
        parent.overlay,
        parent.shadows,
    );
    LazyWorkspaceState {
        parent_workspace_id: Some(parent.workspace_id),
        fork_origin: Some(crate::LazyForkOrigin {
            operation_id: OperationId::from_bytes([0xa3; 16]),
            snapshot,
            config: crate::model::VolumeConfig::portable(crate::model::Lifecycle::Ephemeral),
            phase: crate::LazyForkPhase::Prepared,
        }),
        source: parent.source,
        overlay: parent.overlay,
        shadows: parent.shadows,
        ..lazy_state(WorkspaceId::from_bytes([0xa4; 16]), 1)
    }
}

pub(super) fn advanced_origin_parent(parent: &LazyWorkspaceState) -> LazyWorkspaceState {
    LazyWorkspaceState {
        revision: parent.revision + 1,
        source: crate::demand::SourceReference {
            epoch: parent.source.epoch + 1,
            ..parent.source
        },
        overlay: LazyOverlayId::from_bytes(content_address(&LazyOverlay::Empty).expect("empty")),
        shadows: crate::LazyShadowId::from_bytes(
            content_address(&crate::LazyShadow::Empty).expect("empty shadows"),
        ),
        ..parent.clone()
    }
}

mod contexts;
mod journal;
mod log;
mod native;
mod nodes;
mod ownership;

/// Decodes the log's valid prefix: frames of the first frame's epoch in
pub(super) fn replayable_changes(bytes: &[u8]) -> Result<Vec<Change>, serde_json::Error> {
    let mut changes = Vec::new();
    let mut epoch = None;
    for line in bytes.split_inclusive(|byte| *byte == b'\n') {
        let Some(line) = line.strip_suffix(b"\n") else {
            break;
        };
        let Some((checksum, frame)) = line.split_first_chunk::<64>() else {
            break;
        };
        let Some(frame) = frame.strip_prefix(b" ") else {
            break;
        };
        if blake3::hash(frame).to_hex().as_bytes() != checksum {
            break;
        }
        let frame = serde_json::from_slice::<LogFrame<Change>>(frame)?;
        if *epoch.get_or_insert(frame.epoch) != frame.epoch
            || frame.sequence != changes.len() as u64
        {
            break;
        }
        changes.push(frame.change);
    }
    Ok(changes)
}
