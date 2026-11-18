//! The invalidation queue's deferred state: one taker owns the deferred items.

use super::{InvalidationQueue, KernelCacheItem};
use loom::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};

#[test]
fn deferred_items_are_taken_once() {
    loom::model(|| {
        let queue = Arc::new(Mutex::new(InvalidationQueue::default()));
        queue
            .lock()
            .unwrap()
            .deferred
            .push(KernelCacheItem::Inode(10));
        let taken = Arc::new(AtomicUsize::new(0));
        let threads = [0u8, 1].map(|_| {
            let queue = Arc::clone(&queue);
            let taken = Arc::clone(&taken);
            loom::thread::spawn(move || {
                let mut queue = queue.lock().unwrap();
                let items = queue.take_deferred().unwrap();
                taken.fetch_add(items.len(), Ordering::SeqCst);
                queue.finish(None);
            })
        });
        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(taken.load(Ordering::SeqCst), 1);
    });
}

// The queue is the production type; the model only drives its deferred state.
