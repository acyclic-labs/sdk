//! Refresh protocol only: a reader, the peer's monotone index, and a captured tail.
use super::{Generation, advanced, complete};
use crate::sync::SyncMutex as Mutex;
use std::sync::Arc;

const _: fn(Mutex<()>) -> loom::sync::Mutex<()> = |lock| lock;

fn observe<const RESUME: bool>(index: &Mutex<Generation>, captured_tail: u64) -> Generation {
    let mut expected = *index.lock().unwrap();
    loop {
        let mut current = index.lock().unwrap();
        let peer_advanced = *current != expected;
        if peer_advanced {
            expected = advanced(*current, &expected).unwrap();
        } else if expected.position < captured_tail {
            // Abstract one validated accepted record; no provider or payload IO.
            current.position += 1;
            current.commit = [current.position as u8; 32];
            expected = *current;
        }
        drop(current);
        if complete::<RESUME>(expected.position, captured_tail, peer_advanced) {
            // Both head and snapshot capture the generation under this lock
            // after refresh. Later peer progress cannot repair this saved value.
            return *index.lock().unwrap();
        }
    }
}

fn check<const RESUME: bool>(captured_tail: u64, peer_through: u64) {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = None;
    builder.max_permutations = None;
    builder.max_duration = None;
    builder.check(move || {
        // Lock order: index only; critical sections contain no await or IO.
        // Stable volume/epoch, positions 1 -> 2 -> 3, position-derived stamps.
        let index = Arc::new(Mutex::new(Generation {
            volume: [0; 32],
            epoch: uuid::Uuid::nil(),
            position: 1,
            commit: [1; 32],
        }));
        let peer = loom::thread::spawn({
            let index = index.clone();
            move || {
                for next in 2..=peer_through {
                    let mut current = index.lock().unwrap();
                    if current.position < next {
                        current.position = next;
                        current.commit = [next as u8; 32];
                    }
                }
            }
        });
        let observed = observe::<RESUME>(&index, captured_tail);
        assert!(
            observed.position >= captured_tail,
            "refresh returned before its entry accepted tail"
        );
        assert_eq!(observed.commit, [observed.position as u8; 32]);
        peer.join().unwrap();
    });
}

#[test]
fn refresh_protocol_reaches_the_captured_tail() {
    // The peer either stops below the tail or reaches it. A completed first
    // publication (tail 2) and both publications (tail 3) cover entry sampling.
    for (tail, peer_through) in [(2, 2), (3, 2), (3, 3)] {
        check::<true>(tail, peer_through);
    }
}

#[test]
#[should_panic(expected = "refresh returned before its entry accepted tail")]
fn negative_control_detects_the_advanced_early_return() {
    check::<false>(3, 2);
}
