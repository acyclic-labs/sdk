//! R-grade FileClient protocol; disk persistence is an abstract environment.
//! The real client, TaskVolume and MemoryStream locks are selected by the two
//! crate-local cfgs. One mutable client is raced only by durability advancement.
mod fixture;
mod observation;
mod provider;
use super::confirmation::Barrier;
use acyclic_test_support::differential::oracle::Oracle;
use observation::Event;
use std::{
    cell::Cell,
    sync::{Arc, Mutex},
};

// Compile-time bindings: the actual component shims must select Loom locks.
const _: fn(acyclic_stream::query_sync::SyncMutex<()>) -> loom::sync::Mutex<()> = |lock| lock;
const _: fn(crate::sync::Mutex<()>) -> acyclic_stream::query_sync::Mutex<()> = |lock| lock;

#[derive(Clone, Copy, Debug)]
struct Input {
    barrier: Barrier,
    concurrent: bool,
}
#[derive(Clone, Copy, Debug)]
pub(super) enum Fault {
    Skip(Barrier),
    Refuse,
}
loom::thread_local! {
    static FAULT: Cell<Option<Fault>> = Cell::new(None);
}
pub(super) fn fault() -> Option<Fault> {
    FAULT.with(Cell::get)
}

fn oracle() -> Oracle<Input, Fault, Event> {
    Oracle::declare(|input: &Input, fault: Option<&Fault>, see| {
        // This mutex carries immutable observations OUT of Loom only. Neither
        // actor reads it to decide protocol behavior; it is not a provider lock.
        let observations = Arc::new(Mutex::new(Vec::new()));
        let sink = observations.clone();
        let input = *input;
        let fault = fault.copied();
        // Default builder: no preemption, permutation, iteration or time cap.
        loom::model(move || {
            let event = fixture::run(input, fault);
            sink.lock().unwrap().push(event);
        });
        let observations = Arc::try_unwrap(observations).unwrap().into_inner().unwrap();
        assert!(!observations.is_empty(), "Loom selected zero executions");
        eprintln!(
            "FILE_CLIENT_MODEL {input:?} fault={fault:?} executions={}",
            observations.len()
        );
        for event in observations {
            see.see(event)?;
        }
        Ok(())
    })
    .invariant("prefix-before-effect", observation::safety)
    .invariant("confirmed-intent-progress", observation::progress)
    // Controls use the valid publisher-after-attempt schedule. There is no
    // racing actor then: adding a spawn would introduce no relevant ordering.
    // Each stage has a zero-preemption missing-gate witness and a progress
    // witness, through the SAME entry, observations and verdict as the model.
    .control(
        "prefix-before-effect",
        Input {
            barrier: Barrier::Intent,
            concurrent: false,
        },
        Fault::Skip(Barrier::Intent),
    )
    .control(
        "prefix-before-effect",
        Input {
            barrier: Barrier::Target,
            concurrent: false,
        },
        Fault::Skip(Barrier::Target),
    )
    .control(
        "prefix-before-effect",
        Input {
            barrier: Barrier::Checkpoint,
            concurrent: false,
        },
        Fault::Skip(Barrier::Checkpoint),
    )
    .control(
        "confirmed-intent-progress",
        Input {
            barrier: Barrier::Checkpoint,
            concurrent: false,
        },
        Fault::Refuse,
    )
    .build()
}

#[test]
fn intent_precedes_target_publication() {
    oracle().check(&Input {
        barrier: Barrier::Intent,
        concurrent: true,
    });
}
#[test]
fn target_precedes_checkpoint_publication() {
    oracle().check(&Input {
        barrier: Barrier::Target,
        concurrent: true,
    });
}
#[test]
fn checkpoint_precedes_retirement() {
    oracle().check(&Input {
        barrier: Barrier::Checkpoint,
        concurrent: true,
    });
}
