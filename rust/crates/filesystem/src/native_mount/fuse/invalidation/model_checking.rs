use super::*;
use loom::sync::{Arc, Condvar, Mutex};
use std::sync::LockResult;

fn healthy<T>(result: LockResult<T>) -> T {
    result.unwrap_or_else(|error| std::panic::resume_unwind(Box::new(error.to_string())))
}

#[test]
fn refused_tail_precedes_concurrent_callbacks_and_keeps_waiters_pending() {
    crate::initialize_test_native_io();
    loom::model(|| {
        let work = Work::new(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .unwrap_or_else(|(error, _)| std::panic::resume_unwind(Box::new(error.to_string())));
        let clock = std::sync::atomic::AtomicU64::new(0);
        let older = ViewStamp::record(&clock);
        let newer = ViewStamp::record(&clock);
        let mut deferred = fuse_vec(2, &work)
            .unwrap_or_else(|error| std::panic::resume_unwind(Box::new(error.to_string())));
        deferred.extend([KernelCacheItem::Inode(10), KernelCacheItem::Inode(11)]);
        let shared = Arc::new((
            Mutex::new(InvalidationQueue {
                notified: Some(older),
                deferred,
                ..Default::default()
            }),
            Condvar::new(),
        ));
        let other = Arc::clone(&shared);
        let invalidator = loom::thread::spawn(move || {
            let mut items = healthy(other.0.lock()).take_deferred();
            let mut delivered = vec![items.next()];
            {
                let mut queue = healthy(other.0.lock());
                queue.retain_pending_tail(items);
                queue.finish(Some(older));
                // Refusal leaves the next item owned by the original pass.
                assert_eq!(queue.processed, None);
                assert!(queue.is_pending(Some(older)));
            }
            while delivered.len() != 3 {
                let mut queue = healthy(other.0.lock());
                while !queue.is_pending(queue.notified) {
                    queue = healthy(other.1.wait(queue));
                }
                let through = queue.notified;
                let items = queue.take_deferred();
                drop(queue);
                delivered.extend(items.map(Some));
                let mut queue = healthy(other.0.lock());
                queue.finish(through);
                if delivered.len() != 3 {
                    assert!(queue.is_pending(Some(newer)));
                }
            }
            assert_eq!(
                delivered,
                vec![
                    Some(KernelCacheItem::Inode(10)),
                    Some(KernelCacheItem::Inode(11)),
                    Some(KernelCacheItem::Inode(12)),
                ]
            );
        });
        {
            let mut queue = healthy(shared.0.lock());
            fuse_reserve(&mut queue.deferred, 1, &work)
                .unwrap_or_else(|error| std::panic::resume_unwind(Box::new(error.to_string())));
            queue.deferred.push(KernelCacheItem::Inode(12));
            queue.notified = Some(newer);
            shared.1.notify_all();
        }
        assert!(invalidator.join().is_ok());
        let queue = healthy(shared.0.lock());
        assert!(!queue.is_pending(Some(newer)));
        assert_eq!(queue.processed, Some(newer));
    });
}
