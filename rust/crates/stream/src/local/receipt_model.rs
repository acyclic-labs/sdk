use super::*;
use crate::verification::{Fault, check, fault_is, set_fault};

pub(crate) fn verify(broken: bool) {
    check(move || {
        let stream = super::query_model::fixture(false)?;
        let path = StreamPath::new("approval")?;
        // Each actor has already flushed its own snapshot (1 or 2). Only the
        // real receipt publication is raced; journal I/O adds no interleavings
        // to this boundary and is covered by the crash-cut tests.
        let newer = loom::thread::spawn({
            let stream = stream.clone();
            let path = path.clone();
            move || {
                stream
                    .receipt_for_flushed_snapshot(path, 2, 2, DeviceFlushed)
                    .unwrap()
                    .unwrap()
            }
        });
        if broken {
            set_fault(Fault::MixedReceiptCut);
        }
        let older = stream.receipt_for_flushed_snapshot(path.clone(), 1, 1, DeviceFlushed)?;
        newer.join().unwrap();
        assert!(older.is_some(), "flushed snapshot lost its receipt");
        assert_eq!(
            stream.inner.durable_tails.lock().unwrap().get(&path),
            Some(&2)
        );
        assert!(
            stream
                .receipt_for_flushed_snapshot(path, 1, 2, DeviceFlushed)?
                .is_none()
        );
        assert_eq!(fault_is(Fault::MixedReceiptCut), broken);
        Ok(())
    });
}

pub(crate) fn verify_barrier(broken: bool) {
    check(move || {
        use crate::verification::block_on;
        let stream = super::query_model::fixture(false)?;
        let path = StreamPath::new("approval")?;
        block_on(deferring_durability(stream.append(AppendRequest {
            path: path.clone(),
            records: vec![crate::OwnedBytes::from_static(b"approval")],
            if_tail: None,
            idempotency_key: None,
        })))?;
        if broken {
            set_fault(Fault::BarrierAsFlushed);
        }
        {
            let mut journal = stream.inner.journal.lock().unwrap();
            stream
                .inner
                .publish_synced(&mut journal, 1, SyncCompletion::Ordered);
        }
        assert_eq!(
            block_on(stream.watermarks(path.clone()))?.durable,
            0,
            "barrier certified an unflushed record"
        );
        block_on(deferring_durability(stream.append(AppendRequest {
            path: path.clone(),
            records: vec![crate::OwnedBytes::from_static(b"next")],
            if_tail: None,
            idempotency_key: None,
        })))?;
        // A native ordering operation owns the device outside the model. Race
        // its real completion publication against the full-flush wait: the
        // waiter must wake, see that ordering is insufficient, and lead a flush.
        stream.inner.journal.lock().unwrap().syncing = true;
        let ordering = loom::thread::spawn({
            let stream = stream.clone();
            move || {
                let mut journal = stream.inner.journal.lock().unwrap();
                journal.syncing = false;
                stream
                    .inner
                    .publish_synced(&mut journal, 2, SyncCompletion::Ordered);
            }
        });
        let flushed = block_on(stream.await_flushed(2))?;
        ordering.join().unwrap();
        let receipt = stream
            .receipt_for_flushed_snapshot(path.clone(), 2, 2, flushed)?
            .unwrap();
        assert_eq!((receipt.path(), receipt.through()), (&path, 2));
        assert_eq!(stream.inner.synced.borrow().flushed, Some(2));
        Ok(())
    });
}
