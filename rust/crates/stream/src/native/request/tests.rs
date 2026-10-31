use super::*;
use crate::{Attribute, IdempotencyKey, OwnedBytes, RecordHeaders, StreamPath, test_meter::Meter};
use std::sync::atomic::Ordering;

#[test]
fn retained_request_accounts_each_control_allocation_and_shares_payloads()
-> Result<(), Box<dyn std::error::Error>> {
    let path = StreamPath::new("retained/source")?;
    let body = OwnedBytes::from(vec![19; 128 * 1024]);
    let request = CommitRequest {
        deadline_unix_millis: Some(42),
        conditions: vec![CommitCondition::Tail {
            path: path.clone(),
            expected: 1,
        }],
        mutations: vec![
            CommitMutation::Append {
                path: path.clone(),
                records: vec![AppendRecord {
                    value: body.clone(),
                    headers: RecordHeaders::new(
                        vec![OwnedBytes::from_static(b"original")],
                        vec![Attribute {
                            name: "authority".into(),
                            value: OwnedBytes::from_static(b"retained"),
                        }],
                    )?,
                }],
            },
            CommitMutation::Fork {
                exclude: vec![StreamPath::new("private")?],
                source: path,
                destination: StreamPath::new("retained/child")?,
                at_tail: 1,
                records: vec![body.clone().into()],
            },
        ],
        idempotency_key: IdempotencyKey::new(OwnedBytes::from_static(b"original-intent"))?,
    };
    let meter = Arc::new(Meter::default());
    let original = meter.original();
    let mut retained = None;
    let physical = acyclic_test_support::measure(|| {
        retained = Some(request.try_clone_native(&original));
    });
    let retained = retained.unwrap()?;
    assert_eq!(retained.value(), &request);
    assert_eq!(
        physical.count_current,
        meter.allocations.load(Ordering::SeqCst) as i64
    );
    assert_eq!(
        physical.bytes_current,
        meter.held.load(Ordering::SeqCst) as i64
    );
    assert!(physical.bytes_current < body.len() as i64);
    let CommitMutation::Append { records, .. } = &retained.value().mutations[0] else {
        unreachable!()
    };
    assert!(std::ptr::eq(records[0].value.as_ptr(), body.as_ptr()));
    assert!(std::ptr::eq(
        records[0].headers.keys().as_ptr(),
        match &request.mutations[0] {
            CommitMutation::Append { records, .. } => records[0].headers.keys().as_ptr(),
            _ => unreachable!(),
        }
    ));
    let allocations = meter.allocations.load(Ordering::SeqCst);
    let control_bytes = usize::try_from(physical.bytes_current)?;
    drop(retained);
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    meter.assert_refunded();

    // The former aggregate reservation around Clone reports one allocation
    // while several vectors are physically copied. Keep that actual fragment
    // as a control for the independent allocator observation above.
    let former = Arc::new(Meter::default());
    let mut copied = None;
    let mut claim = None;
    let physical = acyclic_test_support::measure(|| {
        claim = Some(acyclic_native_runtime::ReadAllocation::new(
            &former.original(),
            control_bytes,
        ));
        copied = Some(request.clone());
    });
    assert_ne!(
        physical.count_current,
        former.allocations.load(Ordering::SeqCst) as i64
    );
    drop(copied);
    drop(claim);
    assert_eq!(former.held.load(Ordering::SeqCst), 0);

    for allowed in 0..allocations {
        let denied = Arc::new(Meter::default());
        denied.limit.store(allowed, Ordering::SeqCst);
        assert!(request.try_clone_native(&denied.original()).is_err());
        assert_eq!(denied.held.load(Ordering::SeqCst), 0);
        denied.assert_refunded();
    }
    let cancelled = Arc::new(Meter::local());
    cancelled.cancel_on_copy.store(true, Ordering::SeqCst);
    assert!(request.try_clone_native(&cancelled.original()).is_err());
    assert_eq!(cancelled.held.load(Ordering::SeqCst), 0);
    cancelled.assert_refunded();
    Ok(())
}
