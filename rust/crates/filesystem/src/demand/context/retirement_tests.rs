use super::*;
use crate::{NativeMaterializationWork, WorkBudget};

#[test]
fn final_capture_packet_retires_before_returning_its_counter() {
    crate::initialize_test_native_io();
    let cancellation = CancellationToken::new();
    let original = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
    let baseline = original.live_allocation_bytes();
    let capture = CaptureTransientCustody::new_native(&original).unwrap();
    capture.claim(9);
    let packet = Arc::clone(&capture);
    let late = Arc::downgrade(&capture);
    let controls = std::thread::scope(|threads| {
        let (started, observed) = std::sync::mpsc::sync_channel(0);
        let (finished, retired) = std::sync::mpsc::sync_channel(0);
        let retire = threads.spawn(move || {
            started.send(()).unwrap();
            finished.send(capture.retire()).unwrap();
        });
        observed.recv().unwrap();
        // A source may still upgrade its original weak custody while an
        // accepted packet owns it. Both owners must precede retirement.
        let late_packet = late.upgrade().unwrap();
        late_packet.claim(3);
        packet.release(2);
        drop(packet);
        assert!(matches!(
            retired.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));
        cancellation.cancel();
        // A temporary Weak upgrade in demand accounting can release the final
        // owner while the original leaf is still locked.
        let (bytes, controls) = original.with_original_leaf(|_| {
            drop(late_packet);
            retired.recv().unwrap()
        });
        assert_eq!(bytes, 10);
        retire.join().unwrap();
        controls
    });
    assert!(late.upgrade().is_none());
    // Both control guards remain funded until the observer is outside the leaf.
    assert_eq!(
        original.live_allocation_bytes(),
        baseline + CaptureTransientCustody::allocation_bytes(),
    );
    drop(controls);
    assert_eq!(original.live_allocation_bytes(), baseline);
}

#[test]
fn capture_retirement_admits_all_control_storage_before_allocation() {
    crate::initialize_test_native_io();
    for admitted_headers in 0..=2 {
        let original = NativeMaterializationWork::new(
            WorkBudget {
                allocation_operations: 1 + admitted_headers,
                ..WorkBudget::UNBOUNDED
            },
            &CancellationToken::new(),
        )
        .unwrap();
        let baseline = original.live_allocation_bytes();
        let mut output = None;
        let actual = acyclic_test_support::measure(|| {
            output = Some(CaptureTransientCustody::new_native(&original));
        });
        let output = output.unwrap();
        if admitted_headers == 2 {
            let capture = output.unwrap();
            assert_eq!(actual.count_current, 2);
            let physical_bytes = u64::try_from(actual.bytes_current).unwrap();
            assert_eq!(physical_bytes, CaptureTransientCustody::allocation_bytes());
            assert_eq!(original.live_allocation_bytes() - baseline, physical_bytes);
            let (bytes, controls) = capture.retire();
            assert_eq!(bytes, 0);
            drop(controls);
        } else {
            assert!(output.is_err());
            assert_eq!(actual.count_current, 0);
        }
        assert_eq!(original.live_allocation_bytes(), baseline);
    }
}
