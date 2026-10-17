//! Reducer admission and canonical snapshot cost (see `docs/observability.md`).

use acyclic_harness::core::{
    Action, AggregateKind, Authority, AuthorityIssuer, Command, LifecycleState, Reducer,
    SchemaRegistry,
};
use acyclic_harness::{Capabilities, IdempotencyKey, OperationId};
use divan::Bencher;
use divan::counter::ItemsCount;

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
    let _tracing = std::env::var_os("HARNESS_BENCH_TRACING")
        .map(|_| tracing::subscriber::set_default(tracing_subscriber::registry()));
    let span = tracing::info_span!(
        "harness.bench",
        outcome = tracing::field::Empty,
        error.kind = tracing::field::Empty
    );
    let _entered = span.enter();
    divan::main();
}

#[track_caller]
fn failed<T>(error: impl std::fmt::Debug) -> T {
    unreachable!("benchmark setup or operation failed: {error:?}");
}

fn authority() -> Authority {
    Authority {
        kind: AggregateKind::Conversation,
        id: "conversation-1".into(),
    }
}

/// A reducer and `events` valid lifecycle commands (Pending, then Active/Waiting).
fn workload(events: u64) -> (Reducer, Vec<Command>) {
    let issuer = AuthorityIssuer::new("bench", [7; 32], authority());
    let scope = issuer
        .issue(
            "root",
            &Capabilities::new(["lifecycle:manage"]),
            None,
            None,
            &mut None,
        )
        .unwrap_or_else(failed);
    let reducer = Reducer::new(
        authority(),
        issuer.verifier(&mut None).unwrap_or_else(failed),
        SchemaRegistry::new(),
    );
    let commands = (0..events)
        .map(|revision| Command {
            operation_id: OperationId::from_bytes(u128::from(revision + 1).to_be_bytes()),
            idempotency_key: IdempotencyKey(format!("key-{revision}")),
            expected_revision: revision,
            scope: scope.clone(),
            causal_parent: None,
            action: Action::TransitionLifecycle {
                to: if revision % 2 == 0 {
                    LifecycleState::Active
                } else {
                    LifecycleState::Waiting
                },
                reason: None,
            },
        })
        .collect();
    (reducer, commands)
}

#[divan::bench(args = [100, 1000])]
fn reducer_apply(bencher: Bencher, events: u64) {
    bencher
        .counter(ItemsCount::new(
            usize::try_from(events).unwrap_or_else(failed),
        ))
        .with_inputs(|| workload(events))
        .bench_local_values(|(mut reducer, commands)| {
            for command in commands {
                reducer.apply(&command).unwrap_or_else(failed);
            }
            reducer
        });
}

/// Canonical JSON digest over every committed event.
#[divan::bench(args = [1, 4, 100, 1000])]
fn reducer_snapshot(bencher: Bencher, events: u64) {
    let (mut reducer, commands) = workload(events);
    for command in commands {
        reducer.apply(&command).unwrap_or_else(failed);
    }
    bencher
        .counter(ItemsCount::new(
            usize::try_from(events).unwrap_or_else(failed),
        ))
        .bench_local(|| {
            reducer
                .snapshot(usize::MAX, &mut None)
                .unwrap_or_else(failed)
        });
}

#[divan::bench(args = [0, 65536])]
fn reducer_plan(bencher: Bencher, reason_bytes: usize) {
    let (reducer, mut commands) = workload(1);
    let mut command = commands
        .pop()
        .unwrap_or_else(|| failed("missing lifecycle command"));
    command.action = Action::TransitionLifecycle {
        to: LifecycleState::Active,
        reason: Some("x".repeat(reason_bytes)),
    };
    bencher.bench_local(|| reducer.plan(&command).unwrap_or_else(failed));
}

#[divan::bench(args = [0, 65536])]
fn command_hash_floor(bencher: Bencher, reason_bytes: usize) {
    let (_, mut commands) = workload(1);
    let mut command = commands
        .pop()
        .unwrap_or_else(|| failed("missing lifecycle command"));
    command.action = Action::TransitionLifecycle {
        to: LifecycleState::Active,
        reason: Some("x".repeat(reason_bytes)),
    };
    let mut value = serde_json::to_value(&command).unwrap_or_else(failed);
    value.sort_all_objects();
    let bytes = serde_json::to_vec(&value).unwrap_or_else(failed);
    eprintln!(
        "canonical command ({reason_bytes} reason bytes): {} bytes",
        bytes.len()
    );
    bencher
        .counter(divan::counter::BytesCount::new(bytes.len()))
        .bench_local(|| blake3::hash(divan::black_box(&bytes)));
}

#[cfg(feature = "harness-accounting")]
#[divan::bench(args = [1, 4])]
fn reducer_snapshot_accounted(bencher: Bencher, events: u64) {
    use acyclic_harness::filesystem::PreparationIo;
    use acyclic_harness::store::HistoryWork;
    let (mut reducer, commands) = workload(events);
    for command in commands {
        reducer.apply(&command).unwrap_or_else(failed);
    }
    bencher
        .with_inputs(|| {
            HistoryWork::new(
                PreparationIo::from_budget(
                    acyclic_fs::WorkBudget {
                        allocation_operations: 1_000_000,
                        peak_allocation_bytes: 64 * 1024 * 1024,
                        bytes_copied: 64 * 1024 * 1024,
                        bytes_encoded: 64 * 1024 * 1024,
                        bytes_hashed: 64 * 1024 * 1024,
                        items_examined: 1_000_000,
                        ..Default::default()
                    },
                    &acyclic_fs::CancellationToken::default(),
                )
                .unwrap_or_else(failed),
            )
        })
        .bench_local_values(|mut work| {
            let snapshot = reducer
                .snapshot(usize::MAX, &mut Some(&mut work))
                .unwrap_or_else(failed);
            (snapshot, work)
        });
}

#[divan::bench(args = [1, 4, 100, 1000])]
fn snapshot_hash_floor(bencher: Bencher, events: u64) {
    let (mut reducer, commands) = workload(events);
    for command in commands {
        reducer.apply(&command).unwrap_or_else(failed);
    }
    let mut snapshot = serde_json::to_value(
        reducer
            .snapshot(usize::MAX, &mut None)
            .unwrap_or_else(failed)
            .0,
    )
    .unwrap_or_else(failed);
    let fields = snapshot
        .as_object_mut()
        .unwrap_or_else(|| failed("snapshot must be an object"));
    let mut value = serde_json::Value::Array(
        [
            "format_version",
            "authority",
            "revision",
            "events",
            "projection",
        ]
        .map(|key| fields.remove(key).unwrap_or_else(|| failed(key)))
        .into(),
    );
    value.sort_all_objects();
    let bytes = serde_json::to_vec(&value).unwrap_or_else(failed);
    eprintln!(
        "canonical snapshot ({events} events): {} bytes",
        bytes.len()
    );
    bencher
        .counter(divan::counter::BytesCount::new(bytes.len()))
        .bench_local(|| blake3::hash(divan::black_box(&bytes)));
}

#[divan::bench(args = [1, 4, 16, 64, 256, 1024])]
fn reducer_restore(bencher: Bencher, events: u64) {
    let (mut reducer, commands) = workload(events);
    for command in commands {
        reducer.apply(&command).unwrap_or_else(failed);
    }
    let snapshot = reducer
        .snapshot(usize::MAX, &mut None)
        .unwrap_or_else(failed)
        .0;
    let issuer = AuthorityIssuer::new("bench", [7; 32], authority());
    bencher
        .counter(ItemsCount::new(
            usize::try_from(events).unwrap_or_else(failed),
        ))
        .with_inputs(|| {
            (
                snapshot.copy(&mut None).unwrap_or_else(failed),
                issuer.verifier(&mut None).unwrap_or_else(failed),
            )
        })
        .bench_local_values(|(snapshot, verifier)| {
            Reducer::restore(snapshot, verifier, SchemaRegistry::new(), &mut None)
                .unwrap_or_else(failed)
        });
}

#[cfg(feature = "harness-accounting")]
#[divan::bench(args = [1, 4])]
fn reducer_restore_accounted(bencher: Bencher, events: u64) {
    use acyclic_harness::filesystem::PreparationIo;
    use acyclic_harness::store::HistoryWork;
    let (mut reducer, commands) = workload(events);
    for command in commands {
        reducer.apply(&command).unwrap_or_else(failed);
    }
    let snapshot = reducer
        .snapshot(usize::MAX, &mut None)
        .unwrap_or_else(failed)
        .0;
    let issuer = AuthorityIssuer::new("bench", [7; 32], authority());
    bencher
        .with_inputs(|| {
            (
                snapshot.copy(&mut None).unwrap_or_else(failed),
                issuer.verifier(&mut None).unwrap_or_else(failed),
                HistoryWork::new(
                    PreparationIo::from_budget(
                        acyclic_fs::WorkBudget {
                            allocation_operations: 1_000_000,
                            peak_allocation_bytes: 64 * 1024 * 1024,
                            bytes_copied: 64 * 1024 * 1024,
                            bytes_encoded: 64 * 1024 * 1024,
                            bytes_hashed: 64 * 1024 * 1024,
                            items_examined: 1_000_000,
                            ..Default::default()
                        },
                        &acyclic_fs::CancellationToken::default(),
                    )
                    .unwrap_or_else(failed),
                ),
            )
        })
        .bench_local_values(|(snapshot, verifier, mut work)| {
            let reducer = Reducer::restore(
                snapshot,
                verifier,
                SchemaRegistry::new(),
                &mut Some(&mut work),
            )
            .unwrap_or_else(failed);
            (reducer, work)
        });
}
