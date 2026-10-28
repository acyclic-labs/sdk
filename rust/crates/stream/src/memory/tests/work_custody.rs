use super::*;
use crate::test_meter::Meter;
use std::sync::atomic::Ordering;

#[test]
fn the_original_work_ledger_funds_its_complete_shared_allocation()
-> Result<(), Box<dyn std::error::Error>> {
    let meter = Arc::new(Meter::default());
    let mut output = None;
    let physical = acyclic_test_support::measure(|| {
        output = Some(Work::new(meter.original()));
    });
    let work = output.ok_or("work constructor was not called")??;
    assert_eq!(physical.count_current, 1);
    assert_eq!(
        meter.held.load(Ordering::SeqCst),
        u64::try_from(physical.bytes_current)?,
        "shared work omitted its original physical allocation"
    );
    let clone = Arc::clone(&work);
    drop(work);
    assert_eq!(
        meter.held.load(Ordering::SeqCst),
        u64::try_from(physical.bytes_current)?
    );
    drop(clone);
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    meter.assert_refunded();
    Ok(())
}

#[test]
fn native_commit_admits_original_context_before_its_unpolled_frame()
-> Result<(), Box<dyn std::error::Error>> {
    let provider = MemoryStream::default();
    for limit in [0, 1, u64::MAX] {
        let meter = Arc::new(Meter::default());
        meter.limit.store(limit, Ordering::SeqCst);
        let request = CommitRequest {
            deadline_unix_millis: None,
            conditions: Vec::new(),
            mutations: vec![CommitMutation::Append {
                path: StreamPath::new("prebox")?,
                records: vec![crate::OwnedBytes::from_static(b"value").into()],
            }],
            idempotency_key: IdempotencyKey::new("prebox")?,
        };
        if limit != u64::MAX {
            let future = provider.native_commit(request, None, meter.original());
            assert!(
                matches!(future, crate::NativeProviderFuture::Ready(Some(Err(ref error)))
                if error.kind() == std::io::ErrorKind::OutOfMemory)
            );
            assert_eq!(meter.allocations.load(Ordering::SeqCst), limit);
            drop(future);
        } else {
            let mut future = None;
            let physical = acyclic_test_support::measure(|| {
                future = Some(provider.native_commit(request, None, meter.original()));
            });
            let future = future.ok_or("native constructor was not called")?;
            assert!(matches!(future, crate::NativeProviderFuture::Funded { .. }));
            assert_eq!(
                physical.count_current, 2,
                "original context and sole future frame"
            );
            assert_eq!(meter.allocations.load(Ordering::SeqCst), 2);
            assert_eq!(
                meter.held.load(Ordering::SeqCst),
                u64::try_from(physical.bytes_current)?
            );
            drop(future);
        }
        assert_eq!(meter.held.load(Ordering::SeqCst), 0);
        assert_eq!(meter.write_operations.load(Ordering::SeqCst), 0);
        meter.assert_refunded();
    }
    Ok(())
}
