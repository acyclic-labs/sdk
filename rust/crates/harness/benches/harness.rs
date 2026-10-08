//! Reducer admission and canonical snapshot cost (see `docs/observability.md`).
#![allow(clippy::unwrap_used, reason = "benchmarks abort on setup failure")]

use acyclic_harness::core::{
    Action, AggregateKind, Authority, AuthorityIssuer, Command, LifecycleState, Reducer,
    SchemaRegistry,
};
use acyclic_harness::{Capabilities, IdempotencyKey, OperationId};
use divan::Bencher;
use divan::counter::ItemsCount;

fn main() {
    divan::main();
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
    let scope = issuer.root("root", Capabilities::new(["lifecycle:manage"]));
    let reducer = Reducer::new(authority(), issuer.verifier(), SchemaRegistry::new());
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
        .counter(ItemsCount::new(usize::try_from(events).unwrap()))
        .with_inputs(|| workload(events))
        .bench_local_values(|(mut reducer, commands)| {
            for command in commands {
                reducer.apply(command).unwrap();
            }
            reducer
        });
}

/// Canonical JSON digest over every committed event.
#[divan::bench(args = [100, 1000])]
fn reducer_snapshot(bencher: Bencher, events: u64) {
    let (mut reducer, commands) = workload(events);
    for command in commands {
        reducer.apply(command).unwrap();
    }
    bencher
        .counter(ItemsCount::new(usize::try_from(events).unwrap()))
        .bench_local(|| reducer.snapshot().unwrap());
}
