//! Commit vs collect vs a failed append over the real proof slot and epoch counter.
//!
//! The collector here protects only what the store protects: published
//! history, held checkout bases and objects admitted while it runs. Retiring
//! the volume removes its history from the live set, as deleting a workspace
//! does. After every interleaving, a reusable proof must claim exactly the
//! closure a full walk of the store would find.

use super::*;
use crate::foundation::{DurableCommit, Epoch, OperationId, Sequence};
use crate::Digest;
use bytes::Bytes;
use loom::sync::RwLock;
use std::collections::BTreeSet;

const G0: u8 = 0;
const G1: u8 = 1;
const GARBAGE: u8 = 9;

fn closure(generation: u8) -> BTreeSet<u8> {
    match generation {
        G0 => [1, 2].into(),
        _ => [1, 2, 3].into(),
    }
}

#[derive(Debug)]
struct Proved {
    generation: u8,
    claimed: BTreeSet<u8>,
}

#[derive(Default)]
struct Store {
    objects: BTreeSet<u8>,
    sweeps: u64,
    swept: Vec<(u8, u64)>,
    admitted: Option<BTreeSet<u8>>,
    history: Vec<u8>,
    holds: Vec<u8>,
}

struct World {
    epochs: Epochs,
    slot: Arc<ProofSlot<Proved>>,
    gate: RwLock<()>,
    store: Mutex<Store>,
}

#[derive(Clone, Copy, PartialEq)]
enum Append {
    Commits,
    // Fails before its objects are flushed, so none of them is written or admitted.
    Fails,
}

#[derive(Clone, Copy, PartialEq)]
enum Broken {
    No,
    ReuseAcrossEpochs,
    CacheUnpublished,
}

fn committed() -> AppendOutcome {
    AppendOutcome::Committed(DurableCommit {
        epoch: Epoch::GENESIS,
        sequence: Sequence::new(1),
        operation_id: OperationId::from_bytes([1; 16]),
        fingerprint: Digest::ZERO,
        previous_digest: Digest::ZERO,
        digest: Digest::ZERO,
        payload: Bytes::new(),
        settled_at_micros: None,
    })
}

fn current(world: &World, broken: Broken) -> Option<Arc<Proved>> {
    if broken == Broken::ReuseAcrossEpochs {
        world.slot.current_in_any_epoch()
    } else {
        world.slot.current(Some(&world.epochs))
    }
}

/// A checkout on G0 commits G1, then closes its checkout.
fn commit(world: &World, append: Append, broken: Broken) {
    let proven_at = world.store.lock().unwrap().sweeps;
    let began = CollectionEpoch::read(Some(&world.epochs));
    let previous = current(world, broken).filter(|previous| previous.generation == G0);
    let (listed, claimed) = if let Some(previous) = previous {
        let mut claimed = previous.claimed.clone();
        claimed.insert(3);
        (BTreeSet::from([3]), claimed)
    } else {
        // The full walk reads every parent object; a missing one fails the proof.
        if !closure(G0).is_subset(&world.store.lock().unwrap().objects) {
            return;
        }
        (closure(G1), closure(G1))
    };
    let pending = Unpublished::new(
        Arc::clone(&world.slot),
        began,
        Proved {
            generation: G1,
            claimed,
        },
    );
    if append == Append::Fails {
        if broken == Broken::CacheUnpublished {
            pending.install_unpublished();
        }
        world.store.lock().unwrap().holds.clear();
        return;
    }
    {
        let _admitted = world.gate.read().unwrap();
        let mut store = world.store.lock().unwrap();
        let removed = proven_at < store.sweeps
            && store
                .swept
                .iter()
                .any(|(object, sweep)| *sweep > proven_at && listed.contains(object));
        if removed {
            store.holds.clear();
            return;
        }
        if let Some(admitted) = &mut store.admitted {
            admitted.extend(listed.iter().copied());
        }
        store.objects.insert(3);
        store.history.push(G1);
        store.holds = vec![G1];
        drop(store);
        pending.publish(&committed());
    }
    world.store.lock().unwrap().holds.clear();
}

fn collect(world: &World, retire: bool) {
    {
        let _begun = world.gate.write().unwrap();
        world.epochs.begin();
        let mut store = world.store.lock().unwrap();
        store.admitted = Some(BTreeSet::new());
        if retire {
            store.history.clear();
        }
    }
    let marked = {
        let store = world.store.lock().unwrap();
        store
            .history
            .iter()
            .chain(&store.holds)
            .flat_map(|generation| closure(*generation))
            .collect::<BTreeSet<_>>()
    };
    let _sweep = world.gate.write().unwrap();
    let mut store = world.store.lock().unwrap();
    let candidates = store.objects.clone();
    for object in candidates {
        let admitted = store
            .admitted
            .as_ref()
            .is_some_and(|admitted| admitted.contains(&object));
        if !marked.contains(&object) && !admitted {
            store.objects.remove(&object);
            store.sweeps += 1;
            let sweep = store.sweeps;
            store.swept.push((object, sweep));
        }
    }
    store.admitted = None;
}

fn model(append: Append, retire: bool, broken: Broken) {
    loom::model(move || {
        let world = Arc::new(World {
            epochs: Epochs::default(),
            slot: Arc::new(ProofSlot::default()),
            gate: RwLock::new(()),
            store: Mutex::new(Store {
                objects: [1, 2, GARBAGE].into(),
                history: vec![G0],
                holds: vec![G0],
                ..Store::default()
            }),
        });
        Unpublished::new(
            Arc::clone(&world.slot),
            CollectionEpoch::read(Some(&world.epochs)),
            Proved {
                generation: G0,
                claimed: closure(G0),
            },
        )
        .publish(&committed());
        let committer = {
            let world = Arc::clone(&world);
            loom::thread::spawn(move || commit(&world, append, broken))
        };
        let collector = {
            let world = Arc::clone(&world);
            loom::thread::spawn(move || collect(&world, retire))
        };
        committer.join().unwrap();
        collector.join().unwrap();
        // A later checkout reuses whatever the slot still vouches for.
        if let Some(proof) = current(&world, broken) {
            let store = world.store.lock().unwrap();
            assert_eq!(proof.claimed, closure(proof.generation));
            assert!(
                proof.claimed.is_subset(&store.objects),
                "reused proof of {} claims swept objects",
                proof.generation
            );
            assert!(retire || store.history.contains(&proof.generation));
        }
    });
}

#[test]
fn committed_proof_stays_live_through_a_collection() {
    model(Append::Commits, false, Broken::No);
}

#[test]
fn committed_proof_of_a_retired_volume_is_never_reused() {
    model(Append::Commits, true, Broken::No);
}

#[test]
fn failed_append_never_becomes_reusable() {
    model(Append::Fails, false, Broken::No);
}

#[test]
fn negative_control_detects_reuse_across_a_collection_epoch() {
    let failure = std::panic::catch_unwind(|| model(Append::Commits, true, Broken::ReuseAcrossEpochs))
        .expect_err("a proof reused after a later collection began must be detected");
    drop(failure);
}

#[test]
fn negative_control_detects_caching_an_unpublished_generation() {
    let failure = std::panic::catch_unwind(|| model(Append::Fails, false, Broken::CacheUnpublished))
        .expect_err("a proof of a generation whose append failed must be detected");
    drop(failure);
}
