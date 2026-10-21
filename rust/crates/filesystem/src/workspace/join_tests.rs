use super::*;
use acyclic_test_support::{AllocationInfo, measure};
use proptest::prelude::*;
use std::cell::{Cell, RefCell};

type A = crate::memory::MemoryAuthorityStore;
type O = crate::memory::MemoryObjectStore;
thread_local! {
    static REDUNDANT: Cell<bool> = const { Cell::new(false) };
    static EVENTS: RefCell<Vec<(u8, u64)>> = const { RefCell::new(Vec::new()) };
}
pub(super) fn before_charge(
    accounting: &KernelAccounting<'_>,
    delta: crate::WorkCounters,
) -> Result<(), WorkspaceError> {
    if REDUNDANT.with(Cell::get) {
        accounting.admit(delta)?;
    }
    Ok(())
}
pub(super) fn observed(tag: u8, amount: u64) {
    EVENTS.with_borrow_mut(|events| {
        if events.capacity() != 0 {
            events.push((tag, amount));
        }
    });
}

fn generation(fs: &crate::Fs<A, O>, workspace: u8, version: u8) -> Generation<A, O> {
    let id = WorkspaceId::from_bytes([workspace; 16]);
    Generation {
        workspace: Workspace {
            name: WorkspaceName::new("join", &WorkBudget::UNBOUNDED, &CancellationToken::new())
                .unwrap()
                .value,
            id,
            volume: Volume {
                fs: fs.clone(),
                id: id.volume_id(),
                config: crate::model::VolumeConfig::portable(crate::model::Lifecycle::Ephemeral),
            },
            #[cfg(feature = "native-watch")]
            source: None,
        },
        id: GenerationId::new(crate::Digest::from_bytes([version; 32])),
        publication_closure: None,
    }
}

fn fixture() -> (
    JoinBuilder<A, O>,
    Generation<A, O>,
    Generation<A, O>,
    JoinPlan<A, O>,
    JoinApplication<A, O>,
) {
    // Every side owns independent stores, names, generations and accounting.
    let fs = crate::Fs::new(
        A::default(),
        O::default(),
        crate::EmbeddedCapabilities::MEMORY,
    );
    let source = generation(&fs, 1, 2);
    let target = generation(&fs, 3, 4);
    let builder = JoinBuilder {
        source: source.workspace.clone(),
        target: target.workspace.clone(),
        history: JoinHistory::Rebase,
        git_metadata: true,
        maximum_generations: 5,
        maximum_changes: 6,
        maximum_conflicts: 7,
    };
    let plan = JoinPlan {
        source_head: source.clone(),
        target_head: target.clone(),
        target_authority_head: crate::Head::genesis(crate::Epoch::GENESIS),
        base: crate::facade::WorkspaceCommonAncestor {
            id: GenerationId::new(crate::Digest::from_bytes([8; 32])),
            volume_id: source.workspace.id.volume_id(),
        },
        history: JoinHistory::Rebase,
        git_metadata: true,
        maximum_generations: 5,
        maximum_changes: 6,
        maximum_conflicts: 7,
    };
    let application = JoinApplication {
        generation: generation(&fs, 3, 9),
        resolutions_digest: crate::Digest::from_bytes([10; 32]),
    };
    (builder, source, target, plan, application)
}

// Frozen pre-PR admission/effect/completion sequence; never calls candidate charging.
fn old_effect<T>(
    accounting: &mut KernelAccounting<'_>,
    delta: crate::WorkCounters,
    tag: u8,
    amount: u64,
    effect: impl FnOnce() -> T,
) -> Result<T, WorkspaceError> {
    accounting.admit(delta)?;
    let value = effect();
    observed(tag, amount);
    accounting.record_completed(delta)?;
    Ok(value)
}

enum Output {
    Endpoints((Generation<A, O>, Generation<A, O>)),
    Plan(JoinPlan<A, O>),
    Witness(crate::JoinCommitWitness),
}
fn generation_bytes(value: &Generation<A, O>) -> Vec<u8> {
    assert!(value.publication_closure.is_none());
    serde_json::to_vec(&(
        value.id,
        value.workspace.id,
        value.workspace.name.as_str(),
        value.workspace.volume.id,
        value.workspace.volume.config,
    ))
    .unwrap()
}
fn output_bytes(value: Output) -> Vec<u8> {
    match value {
        Output::Endpoints((source, target)) => {
            serde_json::to_vec(&(generation_bytes(&source), generation_bytes(&target))).unwrap()
        }
        Output::Plan(plan) => serde_json::to_vec(&(
            generation_bytes(&plan.source_head),
            generation_bytes(&plan.target_head),
            plan.target_authority_head,
            plan.base.id,
            plan.base.volume_id,
            plan.history,
            plan.git_metadata,
            plan.maximum_generations,
            plan.maximum_changes,
            plan.maximum_conflicts,
        ))
        .unwrap(),
        Output::Witness(witness) => serde_json::to_vec(&witness).unwrap(),
    }
}

fn operation(
    old: bool,
    kind: u8,
    accounting: &mut KernelAccounting<'_>,
    inputs: &(
        JoinBuilder<A, O>,
        Generation<A, O>,
        Generation<A, O>,
        JoinPlan<A, O>,
        JoinApplication<A, O>,
    ),
) -> Result<Output, WorkspaceError> {
    let (builder, source, target, plan, application) = inputs;
    let key = IdempotencyKey::from_bytes([11; 16]);
    match (old, kind) {
        (false, 0) => copy_join_generations(&source, &target, accounting).map(Output::Endpoints),
        (true, 0) => {
            let bytes = (2 * std::mem::size_of::<Generation<A, O>>()) as u64;
            old_effect(accounting, copy_work(bytes), 0, bytes, || {
                (source.clone(), target.clone())
            })
            .map(Output::Endpoints)
        }
        (false, 1) => JoinBuilder {
            source: builder.source.clone(),
            target: builder.target.clone(),
            history: builder.history,
            git_metadata: builder.git_metadata,
            maximum_generations: builder.maximum_generations,
            maximum_changes: builder.maximum_changes,
            maximum_conflicts: builder.maximum_conflicts,
        }
        .retain_plan(
            source.clone(),
            target.clone(),
            plan.target_authority_head,
            plan.base,
            accounting,
        )
        .map(Output::Plan),
        (true, 1) => {
            let bytes = std::mem::size_of::<JoinPlan<A, O>>() as u64;
            old_effect(accounting, copy_work(bytes), 0, bytes, || {
                let mut source_head = source.clone();
                let mut target_head = target.clone();
                source_head.workspace = builder.source.clone();
                target_head.workspace = builder.target.clone();
                JoinPlan {
                    source_head,
                    target_head,
                    target_authority_head: plan.target_authority_head,
                    base: plan.base,
                    history: builder.history,
                    git_metadata: builder.git_metadata,
                    maximum_generations: builder.maximum_generations,
                    maximum_changes: builder.maximum_changes,
                    maximum_conflicts: builder.maximum_conflicts,
                }
            })
            .map(Output::Plan)
        }
        (false, _) => plan
            .commit_witness_accounted(&application, key, accounting)
            .map(Output::Witness),
        (true, _) => {
            let incompatible = old_effect(accounting, item_work(1), 1, 1, || {
                application.generation.workspace.id() != plan.target_head.workspace.id()
                    || application.generation.id() == plan.target_head.id()
            })?;
            if incompatible {
                return Err(WorkspaceError::IncompatibleWorkspace);
            }
            let bytes = std::mem::size_of::<crate::JoinCommitWitness>() as u64;
            old_effect(accounting, copy_work(bytes), 0, bytes, || {
                crate::JoinCommitWitness {
                    source_workspace: plan.source_head.workspace.id().volume_id(),
                    source_generation: plan.source_head.id,
                    target_workspace: plan.target_head.workspace.id().volume_id(),
                    expected_target: plan.target_head.id,
                    base_workspace: plan.base.volume_id,
                    base_generation: plan.base.id,
                    history: plan.history,
                    maximum_generations: plan.maximum_generations,
                    maximum_changes: plan.maximum_changes,
                    maximum_conflicts: plan.maximum_conflicts,
                    resolutions_digest: application.resolutions_digest,
                    expected_head: plan.target_authority_head,
                    operation_id: key.operation_id(),
                    result_generation: application.generation.id,
                }
            })
            .map(Output::Witness)
        }
    }
}

fn run(
    old: bool,
    kind: u8,
    mode: u8,
    limit: WorkBudget,
    failure: u8,
    redundant: bool,
) -> (
    Result<Vec<u8>, String>,
    crate::WorkCounters,
    u64,
    AllocationInfo,
    (usize, usize),
) {
    let root_cancel = CancellationToken::new();
    let caller = CancellationToken::new();
    let root = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &root_cancel).unwrap();
    let mut work = root.clone();
    // Fund invocation setup before applying the operation's resource limits.
    if mode == 3 {
        work = work.invocation_view().unwrap().0;
    }
    for _ in 0..mode.min(2) {
        work = work.native_volume_work(limit).unwrap();
    }
    if mode == 0 {
        // Only work fields are bounded; standing ledger metadata remains funded.
        root.state(|state| {
            state.budget.bytes_copied = limit.bytes_copied;
            state.budget.items_examined = limit.items_examined;
        });
    }
    let inputs = fixture();
    let before = root.receipt();
    let mut accounting = KernelAccounting::native(&work, &caller);
    accounting.check().unwrap();
    match failure {
        1 => caller.cancel(),
        2 => root_cancel.cancel(),
        3 => root.state(|state| state.poison_native_original_work()),
        _ => (),
    }
    // Cancellation may synchronously retire standing invocation metadata.
    // Reconcile that fixture transition before observing this allocation-free span.
    let _ = root.receipt();
    let live = root.live_allocation_bytes();
    EVENTS.with_borrow_mut(|events| *events = Vec::with_capacity(2));
    REDUNDANT.with(|flag| flag.set(redundant));
    let mut captured = None;
    let heap = measure(|| {
        captured = Some(crate::materializer::count_resource_locks(|| {
            root.state
                .count_locks(|| operation(old, kind, &mut accounting, &inputs))
        }))
    });
    REDUNDANT.with(|flag| flag.set(false));
    let ((result, leaf), resources) = captured.unwrap();
    let result = result
        .map(output_bytes)
        .map_err(|error| format!("{error:?}"));
    for event in EVENTS.with_borrow_mut(std::mem::take) {
        acyclic_test_support::differential::record(event);
    }
    let receipt = root.receipt();
    assert_eq!(root.live_allocation_bytes(), live);
    if result.is_ok() {
        let bytes = match kind {
            0 => 2 * std::mem::size_of::<Generation<A, O>>(),
            1 => std::mem::size_of::<JoinPlan<A, O>>(),
            _ => std::mem::size_of::<crate::JoinCommitWitness>(),
        } as u64;
        assert_eq!(
            receipt,
            before
                .checked_add(crate::WorkCounters {
                    bytes_copied: bytes,
                    items_examined: u64::from(kind == 2),
                    ..crate::WorkCounters::UNCHARGED
                })
                .unwrap()
        );
    }
    (result, receipt, live, heap, (leaf, resources))
}

fn compare(
    kind: u8,
    mode: u8,
    limit: WorkBudget,
    failure: u8,
    redundant: bool,
) -> Result<(), &'static str> {
    let [(old, old_trace), (new, new_trace)] =
        acyclic_test_support::differential::pair::<_, _, (u8, u64)>(
            || (),
            |(), baseline| run(baseline, kind, mode, limit, failure, !baseline && redundant),
        );
    if old.0 != new.0 || old.1 != new.1 || old.2 != new.2 || old_trace != new_trace {
        return Err("full output/error/work/storage/trace equality");
    }
    if old.3 != new.3 || new.3 != AllocationInfo::default() {
        return Err("exact zero allocation");
    }
    if new.0.is_ok() {
        let phases = if kind == 2 { 2 } else { 1 };
        // Each old phase admits once and completes with two leaf visits.
        // Candidate charge is one leaf visit and two resource visits per scope.
        if old.4 != (3 * phases, 4 * phases * usize::from(mode.min(2)))
            || new.4 != (phases, 2 * phases * usize::from(mode.min(2)))
        {
            return Err("exact lock counts");
        }
    }
    Ok(())
}

#[test]
fn all_join_effects_match_independent_baselines_and_redundant_admission_fails_the_pins() {
    for kind in 0..3 {
        for mode in 0..4 {
            compare(kind, mode, WorkBudget::UNBOUNDED, 0, false).unwrap();
            assert_eq!(
                compare(kind, mode, WorkBudget::UNBOUNDED, 0, true),
                Err("exact lock counts")
            );
        }
    }
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]
    #[test]
    fn generated_join_limits_preserve_exact_errors_full_receipts_and_actual_effects(kind in 0_u8..3, mode in 0_u8..4, copies in 0_u64..2049, items in 0_u64..3, failure in 0_u8..4) {
        compare(kind, mode, WorkBudget { bytes_copied: copies, items_examined: items, ..WorkBudget::UNBOUNDED }, failure, false).map_err(proptest::test_runner::TestCaseError::fail)?;
    }
}
