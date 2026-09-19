//! Black-box invariants for immutable merge inputs, publication, and workspace lineage.
#![cfg(feature = "memory")]
#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::too_many_lines,
    reason = "test fixtures use explicit fatal assertions for impossible setup failures"
)]

use acyclic_fs::{
    AppendOutcome, ApplyOptions, AsyncAuthorityStore, AttributeRule, AuthorityId,
    CachedMergeResolution, CancellationToken, ConflictKey, ConflictKind, ConflictSide,
    ConflictValue, Digest, DrivenJoinError, Epoch, FenceOutcome, FileId, ForkOptions, Fs,
    GenerationId, Head, IdempotencyKey, JoinOutcome, MemoryMergeResolutionCache, MergeDriver,
    MergeDriverMode, MergeDriverRegistry, MergePlan, MergePlanResolutionError, MergeResolution,
    MergeResolutionCache, OperationId, ProposedCommit, ResolutionKey, StreamAuthorityStore,
    WorkBudget, WorkspaceGraph, WorkspaceId, WorkspaceLineageError, WorkspaceLineageRecord,
    WorkspaceLineageStore, resolve_merge_plan,
};
use acyclic_stream::MemoryStream;
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use thiserror::Error;

fn generation(byte: u8) -> GenerationId {
    GenerationId::new(Digest::from_bytes([byte; 32]))
}

fn file(byte: u8) -> FileId {
    FileId::from_bytes([byte; 16])
}

fn conflict(key: ConflictKey, kind: ConflictKind, path: &str) -> acyclic_fs::ConflictView {
    acyclic_fs::ConflictView {
        key,
        path: Some(path.to_owned()),
        kind,
        base: ConflictValue::Text("base".to_owned()),
        ours: ConflictValue::Text("ours".to_owned()),
        theirs: ConflictValue::Text("theirs".to_owned()),
    }
}

#[test]
fn resolution_keys_separate_every_generation_driver_and_conflict_dimension() {
    let id = file(7);
    let keys = vec![
        ConflictKey::File(id),
        ConflictKey::Binding {
            directory_id: id,
            name: b"name".to_vec(),
        },
        ConflictKey::Binding {
            directory_id: file(8),
            name: b"name".to_vec(),
        },
        ConflictKey::Binding {
            directory_id: id,
            name: b"other".to_vec(),
        },
        ConflictKey::ContentRange {
            file_id: id,
            offset: 0,
            length: 4,
        },
        ConflictKey::ContentRange {
            file_id: id,
            offset: 1,
            length: 4,
        },
        ConflictKey::ContentRange {
            file_id: id,
            offset: 0,
            length: 5,
        },
        ConflictKey::Metadata(id),
        ConflictKey::HardLinks(id),
        ConflictKey::Directory(id),
        ConflictKey::Rename {
            file_id: id,
            from: "from".to_owned(),
            to: "to".to_owned(),
        },
        ConflictKey::Rename {
            file_id: id,
            from: "other".to_owned(),
            to: "to".to_owned(),
        },
        ConflictKey::Rename {
            file_id: id,
            from: "from".to_owned(),
            to: "other".to_owned(),
        },
        ConflictKey::SpecialPayload(id),
    ];
    let resolution_keys = keys
        .iter()
        .map(|key| ResolutionKey::new(generation(1), generation(2), generation(3), key, b"v1"))
        .collect::<BTreeSet<_>>();
    assert_eq!(resolution_keys.len(), keys.len());

    let reference = &keys[0];
    let dimensions = [
        ResolutionKey::new(
            generation(1),
            generation(2),
            generation(3),
            reference,
            b"v1",
        ),
        ResolutionKey::new(
            generation(9),
            generation(2),
            generation(3),
            reference,
            b"v1",
        ),
        ResolutionKey::new(
            generation(1),
            generation(9),
            generation(3),
            reference,
            b"v1",
        ),
        ResolutionKey::new(
            generation(1),
            generation(2),
            generation(9),
            reference,
            b"v1",
        ),
        ResolutionKey::new(generation(1), generation(2), generation(3), reference, b""),
        ResolutionKey::new(generation(1), generation(2), generation(3), reference, b"v"),
        ResolutionKey::new(
            generation(1),
            generation(2),
            generation(3),
            reference,
            b"v1\0",
        ),
    ];
    assert_eq!(dimensions.into_iter().collect::<BTreeSet<_>>().len(), 7);
}

struct CountingDriver {
    calls: AtomicUsize,
    fingerprint: &'static [u8],
    mode: MergeDriverMode,
}

impl CountingDriver {
    const fn new(fingerprint: &'static [u8], mode: MergeDriverMode) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            fingerprint,
            mode,
        }
    }
}

impl MergeDriver for CountingDriver {
    fn fingerprint(&self) -> &[u8] {
        self.fingerprint
    }

    fn mode(&self) -> MergeDriverMode {
        self.mode
    }

    fn resolve(
        &self,
        _conflict: &acyclic_fs::ConflictView,
    ) -> Result<MergeResolution, acyclic_fs::DriverError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(MergeResolution::Select(ConflictSide::Ours))
    }
}

fn one_conflict_plan() -> MergePlan {
    MergePlan {
        base: generation(1),
        ours: generation(2),
        theirs: generation(3),
        conflicts: vec![conflict(
            ConflictKey::ContentRange {
                file_id: file(1),
                offset: 0,
                length: 4,
            },
            ConflictKind::Text,
            "file.txt",
        )],
        truncated: false,
    }
}

#[test]
fn replanning_reuses_exact_effectful_entries_and_reruns_only_deterministic_misses() {
    let plan = one_conflict_plan();
    let effectful = Arc::new(CountingDriver::new(
        b"effectful-v1",
        MergeDriverMode::Effectful,
    ));
    let mut registry = MergeDriverRegistry::new();
    registry
        .register("effectful", effectful.clone())
        .expect("register effectful driver");
    registry
        .set_default("effectful")
        .expect("set effectful default");
    let mut cache = MemoryMergeResolutionCache::default();
    assert_eq!(
        resolve_merge_plan(plan.clone(), &registry, &mut cache, true),
        Err(MergePlanResolutionError::StaleEffectful)
    );
    assert_eq!(effectful.calls.load(Ordering::SeqCst), 0);

    let key = ResolutionKey::new(
        plan.base,
        plan.ours,
        plan.theirs,
        &plan.conflicts[0].key,
        effectful.fingerprint(),
    );
    cache.insert(
        key,
        CachedMergeResolution {
            mode: MergeDriverMode::Effectful,
            resolution: MergeResolution::Select(ConflictSide::Theirs),
        },
    );
    let reused = resolve_merge_plan(plan.clone(), &registry, &mut cache, true)
        .expect("reuse exact effectful decision");
    assert_eq!(effectful.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        reused.resolutions.get(&plan.conflicts[0].key),
        Some(&MergeResolution::Select(ConflictSide::Theirs))
    );

    let deterministic = Arc::new(CountingDriver::new(
        b"deterministic-v2",
        MergeDriverMode::Deterministic,
    ));
    let mut deterministic_registry = MergeDriverRegistry::new();
    deterministic_registry
        .register("deterministic", deterministic.clone())
        .expect("register deterministic driver");
    deterministic_registry
        .set_rules(vec![AttributeRule {
            pattern: "*.txt".to_owned(),
            driver: "deterministic".to_owned(),
        }])
        .expect("set deterministic rule");
    resolve_merge_plan(plan, &deterministic_registry, &mut cache, true)
        .expect("rerun deterministic miss");
    assert_eq!(deterministic.calls.load(Ordering::SeqCst), 1);
}

#[test]
fn typed_conflict_categories_and_keys_survive_immutable_planning() {
    let categories = [
        (ConflictKey::File(file(1)), ConflictKind::Record),
        (
            ConflictKey::ContentRange {
                file_id: file(2),
                offset: 4,
                length: 8,
            },
            ConflictKind::Binary,
        ),
        (ConflictKey::Metadata(file(3)), ConflictKind::Metadata),
        (
            ConflictKey::Binding {
                directory_id: file(4),
                name: b"entry".to_vec(),
            },
            ConflictKind::Binding,
        ),
        (
            ConflictKey::Rename {
                file_id: file(5),
                from: "old".to_owned(),
                to: "new".to_owned(),
            },
            ConflictKind::Rename,
        ),
        (ConflictKey::Directory(file(6)), ConflictKind::Directory),
        (ConflictKey::HardLinks(file(7)), ConflictKind::HardLink),
        (ConflictKey::SpecialPayload(file(8)), ConflictKind::Special),
        (
            ConflictKey::SpecialPayload(file(9)),
            ConflictKind::SymbolicLink,
        ),
    ];
    let plan = MergePlan {
        base: generation(1),
        ours: generation(2),
        theirs: generation(3),
        conflicts: categories
            .iter()
            .enumerate()
            .map(|(index, (key, kind))| {
                let (base, ours, theirs) = match kind {
                    ConflictKind::Metadata => (
                        ConflictValue::Metadata(vec![0]),
                        ConflictValue::Metadata(vec![1]),
                        ConflictValue::Metadata(vec![2]),
                    ),
                    ConflictKind::Binding | ConflictKind::Rename => (
                        ConflictValue::Binding(None),
                        ConflictValue::Binding(Some(file(0xa1))),
                        ConflictValue::Binding(Some(file(0xa2))),
                    ),
                    ConflictKind::Record | ConflictKind::Directory | ConflictKind::HardLink => (
                        ConflictValue::Record {
                            generation: generation(1),
                            file_id: file(0xb0),
                        },
                        ConflictValue::Record {
                            generation: generation(2),
                            file_id: file(0xb1),
                        },
                        ConflictValue::Record {
                            generation: generation(3),
                            file_id: file(0xb2),
                        },
                    ),
                    ConflictKind::Text => (
                        ConflictValue::Text("base".to_owned()),
                        ConflictValue::Text("ours".to_owned()),
                        ConflictValue::Text("theirs".to_owned()),
                    ),
                    _ => (
                        ConflictValue::Binary(vec![0]),
                        ConflictValue::Binary(vec![1]),
                        ConflictValue::Binary(vec![2]),
                    ),
                };
                acyclic_fs::ConflictView {
                    key: key.clone(),
                    path: Some(format!("path-{index}")),
                    kind: *kind,
                    base,
                    ours,
                    theirs,
                }
            })
            .collect(),
        truncated: false,
    };
    let driver = Arc::new(CountingDriver::new(
        b"all-categories-v1",
        MergeDriverMode::Deterministic,
    ));
    let mut registry = MergeDriverRegistry::new();
    registry.register("all", driver).expect("register driver");
    registry.set_default("all").expect("set default");
    let candidate = resolve_merge_plan(
        plan.clone(),
        &registry,
        &mut MemoryMergeResolutionCache::default(),
        false,
    )
    .expect("resolve typed plan");
    assert_eq!(candidate.plan, plan);
    assert_eq!(candidate.resolutions.len(), categories.len());
    for (key, _) in categories {
        assert_eq!(
            candidate.resolutions.get(&key),
            Some(&MergeResolution::Select(ConflictSide::Ours))
        );
    }
}

#[derive(Clone, Default)]
struct SharedLineageStore(Arc<Mutex<BTreeMap<WorkspaceId, WorkspaceLineageRecord>>>);

#[derive(Debug, Error)]
#[error("lineage test store unavailable")]
struct LineageStoreError;

impl WorkspaceLineageStore for SharedLineageStore {
    type Error = LineageStoreError;

    async fn load(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Option<WorkspaceLineageRecord>, Self::Error> {
        self.0
            .lock()
            .map_err(|_| LineageStoreError)
            .map(|records| records.get(&workspace_id).cloned())
    }

    async fn compare_and_swap(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: WorkspaceLineageRecord,
    ) -> Result<bool, Self::Error> {
        let mut records = self.0.lock().map_err(|_| LineageStoreError)?;
        if records
            .get(&workspace_id)
            .map_or(0, |record| record.revision)
            != expected_revision
        {
            return Ok(false);
        }
        records.insert(workspace_id, replacement);
        Ok(true)
    }
}

#[tokio::test]
async fn recursive_lineage_and_direct_parent_authorization_survive_restart()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let store = SharedLineageStore::default();
    let graph = WorkspaceGraph::new(store.clone());
    let root = fs.create_workspace("graph-root").await?;
    graph.register_root(&root).await?;
    let child = graph
        .fork(&root, "graph-child", IdempotencyKey::from_bytes([1; 16]))
        .await?;
    let grandchild = graph
        .fork(
            &child,
            "graph-grandchild",
            IdempotencyKey::from_bytes([2; 16]),
        )
        .await?;
    drop(graph);

    let restarted = WorkspaceGraph::new(store);
    let ancestors = restarted.ancestors(grandchild.id(), 3).await?;
    assert_eq!(
        ancestors
            .iter()
            .map(|record| record.workspace_id)
            .collect::<Vec<_>>(),
        vec![child.id(), root.id()]
    );
    restarted
        .authorize_join(grandchild.id(), child.id())
        .await?;
    assert!(matches!(
        restarted.authorize_join(grandchild.id(), root.id()).await,
        Err(WorkspaceLineageError::UnauthorizedJoin)
    ));
    assert!(matches!(
        restarted.ancestors(grandchild.id(), 1).await,
        Err(WorkspaceLineageError::TraversalLimit)
    ));
    Ok(())
}

#[tokio::test]
async fn merge_plans_are_immutable_stale_safe_and_publication_is_idempotent()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let target = fs.create_workspace("immutable-target").await?;
    target.write_text("/base", "base").await?;
    let source = target
        .fork(
            "immutable-source",
            ForkOptions::from_generation(target.head().await?, IdempotencyKey::from_bytes([3; 16])),
        )
        .await?;
    source.write_text("/source-before-plan", "one").await?;
    let immutable_plan = source.join_into(&target).plan().await?;
    source.write_text("/source-after-plan", "two").await?;
    assert!(matches!(
        immutable_plan
            .apply(ApplyOptions {
                if_target: immutable_plan.target_head(),
                idempotency_key: IdempotencyKey::from_bytes([4; 16]),
            })
            .await?,
        JoinOutcome::Applied(_)
    ));
    assert_eq!(target.read("/source-before-plan", 16).await?, b"one"[..]);
    assert!(target.read("/source-after-plan", 16).await.is_err());

    let stale_plan = source.join_into(&target).plan().await?;
    target.write_text("/target-race", "winner").await?;
    assert!(matches!(
        stale_plan
            .apply(ApplyOptions {
                if_target: stale_plan.target_head(),
                idempotency_key: IdempotencyKey::from_bytes([5; 16]),
            })
            .await?,
        JoinOutcome::StaleTarget(_)
    ));
    assert!(target.read("/source-after-plan", 16).await.is_err());

    let current_plan = source.join_into(&target).plan().await?;
    let options = ApplyOptions {
        if_target: current_plan.target_head(),
        idempotency_key: IdempotencyKey::from_bytes([6; 16]),
    };
    assert!(matches!(
        current_plan.apply(options).await?,
        JoinOutcome::Applied(_)
    ));
    assert!(matches!(
        current_plan.apply(options).await?,
        JoinOutcome::AlreadyApplied(_)
    ));
    assert_eq!(target.read("/source-after-plan", 16).await?, b"two"[..]);
    Ok(())
}

#[tokio::test]
async fn forged_merge_candidate_is_rejected_before_publication()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let target = fs.create_workspace("candidate-target").await?;
    target.write_text("/shared", "base").await?;
    let source = target
        .fork(
            "candidate-source",
            ForkOptions::from_generation(target.head().await?, IdempotencyKey::from_bytes([6; 16])),
        )
        .await?;
    source.write_text("/shared", "source").await?;
    target.write_text("/shared", "target").await?;
    let plan = source.join_into(&target).plan().await?;
    let JoinOutcome::Conflicted {
        conflicts,
        truncated,
    } = plan
        .apply(ApplyOptions {
            if_target: plan.target_head(),
            idempotency_key: IdempotencyKey::from_bytes([7; 16]),
        })
        .await?
    else {
        return Err("expected a semantic conflict".into());
    };
    let typed = plan.describe_conflicts(&conflicts, truncated).await?;
    let mut registry = MergeDriverRegistry::new();
    registry
        .register(
            "select",
            Arc::new(CountingDriver::new(
                b"candidate-v1",
                MergeDriverMode::Deterministic,
            )),
        )
        .expect("register candidate driver");
    registry.set_default("select").expect("set default");
    let mut candidate = resolve_merge_plan(
        typed,
        &registry,
        &mut MemoryMergeResolutionCache::default(),
        false,
    )?;
    candidate.plan.ours = generation(0xff);
    assert!(matches!(
        plan.apply_candidate(
            ApplyOptions {
                if_target: plan.target_head(),
                idempotency_key: IdempotencyKey::from_bytes([8; 16]),
            },
            &candidate,
        )
        .await,
        Err(DrivenJoinError::StaleCandidate)
    ));
    assert_eq!(target.read("/shared", 16).await?, b"target"[..]);
    Ok(())
}

#[tokio::test]
async fn concurrent_stale_cas_and_epoch_fencing_preserve_exactly_once_replay()
-> Result<(), Box<dyn std::error::Error>> {
    let store = StreamAuthorityStore::new(Arc::new(MemoryStream::default()));
    let authority = AuthorityId::from_bytes([0x51; 16]);
    let cancellation = CancellationToken::new();
    store
        .create_authority(
            authority,
            Epoch::GENESIS,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    let proposal = |operation, fingerprint, payload| ProposedCommit {
        operation_id: OperationId::from_bytes([operation; 16]),
        fingerprint: Digest::from_bytes([fingerprint; 32]),
        payload: Bytes::from_static(payload),
    };
    let left = proposal(1, 1, b"left");
    let right = proposal(2, 2, b"right");
    let expected = Head::genesis(Epoch::GENESIS);
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let left_barrier = barrier.clone();
    let right_barrier = barrier;
    let (left_outcome, right_outcome) = tokio::join!(
        async {
            left_barrier.wait().await;
            store
                .compare_and_append(
                    authority,
                    Epoch::GENESIS,
                    expected,
                    left.clone(),
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await
        },
        async {
            right_barrier.wait().await;
            store
                .compare_and_append(
                    authority,
                    Epoch::GENESIS,
                    expected,
                    right.clone(),
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await
        }
    );
    let outcomes = [left_outcome?.value, right_outcome?.value];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, AppendOutcome::Committed(_)))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, AppendOutcome::Conflict { .. }))
            .count(),
        1
    );
    let (committed, committed_proposal) = match &outcomes {
        [AppendOutcome::Committed(commit), _] => (commit.clone(), left),
        [_, AppendOutcome::Committed(commit)] => (commit.clone(), right),
        _ => return Err("concurrent CAS did not select exactly one winner".into()),
    };
    let head = Head {
        epoch: committed.epoch,
        sequence: committed.sequence,
        digest: committed.digest,
    };
    assert!(matches!(
        store
            .fence(authority, head, WorkBudget::UNBOUNDED, &cancellation,)
            .await?
            .value,
        FenceOutcome::Advanced(_)
    ));
    assert!(matches!(
        store
            .compare_and_append(
                authority,
                Epoch::GENESIS,
                expected,
                committed_proposal,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value,
        AppendOutcome::AlreadyCommitted(replayed) if replayed == committed
    ));
    assert!(matches!(
        store
            .compare_and_append(
                authority,
                Epoch::GENESIS,
                expected,
                proposal(3, 3, b"late"),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value,
        AppendOutcome::Fenced { .. }
    ));
    Ok(())
}
