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
