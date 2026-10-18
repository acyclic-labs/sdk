//! The real pool take and release paths, with the shard lock replaced by Loom.
use super::*;
use loom::thread;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone, Copy)]
enum Fault {
    None,
    Accounting,
    Ownership,
}

/// A worker takes a buffer for its next read while a consumer drops the last
/// view of an earlier result from the same shard. Whichever runs first, the
/// earlier storage ends up in exactly one place (reused by the worker, or
/// idle in the shard), and the shard's byte count matches what it holds.
fn recycle(fault: Fault) {
    let reused = std::sync::Arc::new(AtomicUsize::new(0));
    let fresh = std::sync::Arc::new(AtomicUsize::new(0));
    let (seen_reused, seen_fresh) = (reused.clone(), fresh.clone());
    loom::model(move || {
        let shard: &'static Shard = Box::leak(Box::new(Shard::new()));
        {
            let mut idle = shard.idle();
            idle.broken_accounting = matches!(fault, Fault::Accounting);
            idle.broken_ownership = matches!(fault, Fault::Ownership);
        }
        // An earlier result, now held only by its consumer.
        let earlier = read_buffer(POOL_PAGE, shard).unwrap();
        let storage = earlier.as_ptr();
        let held = read_result(earlier, POOL_PAGE, shard);
        let consumer = thread::spawn(move || drop(held));
        let next = read_buffer(POOL_PAGE, shard).unwrap();
        consumer.join().unwrap();
        assert!(next.is_empty());
        assert_eq!(next.capacity(), POOL_PAGE);
        let took_earlier = next.as_ptr() == storage;
        if took_earlier {
            seen_reused.fetch_add(1, Ordering::Relaxed);
        } else {
            seen_fresh.fetch_add(1, Ordering::Relaxed);
        }
        let idle = shard.idle();
        let buffers: Vec<&Vec<u8>> = idle
            .classes
            .iter()
            .flat_map(|(_, buffers)| buffers)
            .collect();
        let holders = usize::from(took_earlier)
            + buffers
                .iter()
                .filter(|buffer| buffer.as_ptr() == storage)
                .count();
        assert_eq!(holders, 1, "pool ownership lost");
        assert_eq!(
            idle.bytes,
            buffers
                .iter()
                .map(|buffer| buffer.capacity())
                .sum::<usize>(),
            "pool accounting lost"
        );
    });
    // Both orders of take and release were explored.
    assert_ne!(
        reused.load(Ordering::Relaxed),
        0,
        "release-first order not explored"
    );
    assert_ne!(
        fresh.load(Ordering::Relaxed),
        0,
        "take-first order not explored"
    );
}

#[test]
fn a_released_buffer_is_owned_once_and_counted_exactly() {
    recycle(Fault::None);
}

#[test]
#[should_panic(expected = "pool accounting lost")]
fn a_take_that_keeps_the_idle_count_is_detected() {
    recycle(Fault::Accounting);
}

#[test]
#[should_panic(expected = "pool ownership lost")]
fn a_release_that_drops_the_buffer_is_detected() {
    recycle(Fault::Ownership);
}
