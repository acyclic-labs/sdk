use super::*;
use crate::memory::RetainedLink;
use acyclic_native_runtime::ReadAllocation;
use std::cell::Cell;
use std::sync::atomic::Ordering;

thread_local! {
    static RELEASED: Cell<u64> = const { Cell::new(0) };
    static EARLY_RELEASE: Cell<bool> = const { Cell::new(false) };
}

fn observe_release(bytes: u64) {
    let released = RELEASED.get() + bytes;
    RELEASED.set(released);
    let observed = acyclic_test_support::current_allocations().expect("active allocation scope");
    if observed.bytes_total as i64 - observed.bytes_current < released as i64 {
        EARLY_RELEASE.set(true);
    }
}

fn release_order<const RAW_DROP: bool>(length: usize) {
    let meter = Arc::new(crate::test_meter::Meter {
        release_observer: Some(observe_release),
        ..Default::default()
    });
    let original = meter.original();
    let header = std::mem::size_of::<RetainedFact>() + 2 * std::mem::size_of::<usize>();
    RELEASED.set(0);
    EARLY_RELEASE.set(false);
    let measured = acyclic_test_support::measure(|| {
        let mut root: Option<RetainedFactRef> = None;
        let mut first = None;
        for index in 0..length {
            let charge = ReadAllocation::new(&original, header).expect("original header charge");
            let mut fact = RetainedFact::reserved(Some(charge));
            let data = fact.get_mut().expect("exclusive reservation");
            data.previous = root.take();
            let charge = if data.previous.is_some() {
                data._header.take()
            } else {
                None
            };
            if let Some(previous) = &fact.previous {
                previous
                    .next
                    .set(RetainedLink {
                        fact: fact.downgrade(),
                        _allocation: charge,
                    })
                    .unwrap_or_else(|_| unreachable!("one successor"));
            }
            if index == 0 && !RAW_DROP {
                first = Some(fact.clone());
            }
            root = Some(fact);
        }
        // Exercise the encoder's weak-upgrade ownership after the tail and
        // first snapshot owners disappear, without another allocation.
        let upgraded = first
            .as_ref()
            .and_then(|first| first.next.get())
            .and_then(|link| link.fact.upgrade());
        if RAW_DROP {
            // The former ordinary Arc destructor releases the inline charge
            // before its implicit weak reference retires the allocation.
            drop(root.take().expect("one fact").0.take());
        }
        drop(root);
        drop(first);
        drop(upgraded);
    });
    assert_eq!(measured.count_total, length as u64);
    assert_eq!(measured.bytes_total, (header * length) as u64);
    assert_eq!(measured.count_current, 0);
    assert_eq!(RELEASED.get(), (header * length) as u64);
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    meter.assert_refunded();
    assert!(
        !EARLY_RELEASE.get(),
        "header claim was released before allocation retirement"
    );
}

#[test]
fn original_header_release_follows_physical_allocation_retirement() {
    for length in [1, 16, 256] {
        release_order::<false>(length);
    }
}

#[test]
#[should_panic(expected = "header claim was released before allocation retirement")]
fn negative_control_ordinary_arc_drop_refunds_before_retirement() {
    release_order::<true>(1);
}
