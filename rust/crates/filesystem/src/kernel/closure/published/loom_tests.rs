//! Commit vs collect vs a failed append over the real proof slot and epoch counter.
//!
//! The collector here protects only what the store protects: published
//! history, held checkout bases and objects admitted while it runs. Retiring
//! the volume removes its history from the live set, as deleting a workspace
//! does. After every interleaving, a reusable proof must claim exactly the
//! closure a full walk of the store would find.

use super::test_support::outcome;
use super::*;
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
        pending.publish(&outcome(1, false));
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
        .publish(&outcome(0, false));
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
    let failure =
        std::panic::catch_unwind(|| model(Append::Commits, true, Broken::ReuseAcrossEpochs))
            .expect_err("a proof reused after a later collection began must be detected");
    drop(failure);
}

#[test]
fn negative_control_detects_caching_an_unpublished_generation() {
    let failure =
        std::panic::catch_unwind(|| model(Append::Fails, false, Broken::CacheUnpublished))
            .expect_err("a proof of a generation whose append failed must be detected");
    drop(failure);
}

fn ordered_publications<const ORDERED: bool>(retry: bool) {
    loom::model(move || {
        // Lock order: proof slot only. Both actors run the production install.
        let slot = Arc::new(ProofSlot::default());
        let ticket =
            |sequence| Unpublished::new(slot.clone(), CollectionEpoch::read(None), sequence);
        let old = ticket(1u64);
        let delayed = ticket(1);
        let new = ticket(2);
        let publish = move |ticket: Unpublished<u64>, sequence, retry| {
            if ORDERED {
                ticket.publish(&outcome(sequence, retry));
            } else {
                ticket.install::<false, true>(Sequence::new(sequence));
            }
        };
        let first = loom::thread::spawn(move || publish(delayed, 1, false));
        let second = loom::thread::spawn(move || {
            publish(new, 2, false);
            publish(old, 1, retry);
        });
        first.join().unwrap();
        second.join().unwrap();
        assert_eq!(
            *slot.current(None).unwrap(),
            2,
            "proof slot regressed durable sequence"
        );
    });
}

#[test]
fn delayed_publication_and_retry_keep_the_newest_durable_proof() {
    ordered_publications::<true>(false);
    ordered_publications::<true>(true);
}

#[test]
#[should_panic(expected = "proof slot regressed durable sequence")]
fn negative_control_detects_unconditional_install() {
    ordered_publications::<false>(true);
}

fn epoch_refresh<const REFRESH: bool>() {
    loom::model(move || {
        // Lock order: proof slot only. Epoch reads are the production atomic.
        let epochs = Epochs::default();
        let slot = Arc::new(ProofSlot::default());
        let old_epoch = CollectionEpoch::read(Some(&epochs));
        Unpublished::new(slot.clone(), old_epoch, 0).publish(&outcome(1, false));
        let delayed = Unpublished::new(slot.clone(), old_epoch, 0);
        epochs.begin();
        assert!(slot.current(Some(&epochs)).is_none());
        let fresh = Unpublished::new(slot.clone(), CollectionEpoch::read(Some(&epochs)), 1);
        let expected = fresh.proof.clone();
        let old = loom::thread::spawn(move || delayed.publish(&outcome(1, false)));
        let retry = loom::thread::spawn(move || {
            if REFRESH {
                fresh.publish(&outcome(1, true));
            } else {
                fresh.install::<true, false>(Sequence::new(1));
            }
        });
        old.join().unwrap();
        retry.join().unwrap();
        let current = slot
            .current(Some(&epochs))
            .expect("retry lost the fresh epoch proof");
        assert!(Arc::ptr_eq(&current, &expected));
        Unpublished::new(slot.clone(), CollectionEpoch::read(Some(&epochs)), 2)
            .publish(&outcome(1, true));
        assert!(Arc::ptr_eq(
            &slot.current(Some(&epochs)).unwrap(),
            &expected
        ));
    });
}

#[test]
fn cross_epoch_retry_survives_delayed_publication_and_exact_ties() {
    epoch_refresh::<true>();
}

#[test]
#[should_panic(expected = "retry lost the fresh epoch proof")]
fn negative_control_detects_sequence_only_install() {
    epoch_refresh::<false>();
}
