use super::OwnerCount;
use loom::sync::Arc;
use loom::thread;

/// The last two owners release on different threads: in every interleaving
/// exactly one of them is told it was last.
#[test]
fn concurrent_final_releases_elect_exactly_one_last_owner() {
    loom::model(|| {
        let count = Arc::new(OwnerCount::default());
        count.acquire();
        count.acquire();
        let other = Arc::clone(&count);
        let worker = thread::spawn(move || other.release());
        let mine = count.release();
        let theirs = worker.join().unwrap();
        assert!(mine ^ theirs, "last owners elected: {mine} and {theirs}");
        assert!(count.idle());
    });
}

/// Negative control: deciding by the `Arc` strong count read before the
/// owner's own `Arc` drops (the previous rule) lets both final owners see
/// the other and neither clean up.
#[test]
fn strong_count_before_drop_can_elect_no_last_owner() {
    static NEITHER: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    loom::model(|| {
        let health = Arc::new(());
        let other = Arc::clone(&health);
        let worker = thread::spawn(move || {
            let last = Arc::strong_count(&other) == 1;
            drop(other);
            last
        });
        let mine = Arc::strong_count(&health) == 1;
        drop(health);
        let theirs = worker.join().unwrap();
        if !mine && !theirs {
            NEITHER.store(true, std::sync::atomic::Ordering::Relaxed);
        }
    });
    assert!(
        NEITHER.load(std::sync::atomic::Ordering::Relaxed),
        "loom found no interleaving where both final owners skip cleanup"
    );
}
