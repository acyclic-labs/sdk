use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Credit {
    Operations,
    Bytes,
    Codec,
    AllocationReservation,
    Allocation,
}

fn preserves_unwind(credit: Credit) -> io::Result<()> {
    if crate::delivery::isolated()? {
        return Ok(());
    }
    for callback_panics in [false, true] {
        for caller_panics in [false, true] {
            let returned = Arc::new(AtomicU64::new(0));
            let mut ledger = ReadBudgetMeter::new(8, 8);
            ledger.credit_returned = Some(Box::new({
                let returned = Arc::clone(&returned);
                move |actual| {
                    if actual == credit {
                        returned.fetch_add(1, Ordering::AcqRel);
                        if callback_panics {
                            panic!("original credit return failure: {actual:?}");
                        }
                    }
                }
            }));
            let observed = Arc::new(ledger);
            let meter: Arc<dyn NativeReadMeter> = observed.clone();
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match credit {
                Credit::Operations => {
                    let file = tempfile::tempfile().unwrap();
                    let mut reservation = NativeOperationReservation::new(
                        Arc::clone(&meter),
                        NativeOperationKind::Read,
                        2,
                    )
                    .unwrap();
                    let (metadata, recorded) = reservation.attempt(|| file.metadata());
                    assert_eq!(metadata.unwrap().len(), 0);
                    recorded.unwrap();
                    if caller_panics {
                        panic!("original credit owner unwind");
                    }
                }
                Credit::Bytes => {
                    let _reservation = NativeByteReservation::new(
                        Arc::clone(&meter),
                        NativeOperationKind::Read,
                        8,
                    )
                    .unwrap();
                    if caller_panics {
                        panic!("original credit owner unwind");
                    }
                }
                Credit::Codec => {
                    let mut reservation = NativeCodecReservation::new(
                        Arc::clone(&meter),
                        NativeCodecWork {
                            copied: 8,
                            ..NativeCodecWork::default()
                        },
                    )
                    .unwrap();
                    let mut copied = [0; 3];
                    copied.copy_from_slice(b"abc");
                    reservation
                        .record(NativeCodecWork {
                            copied: copied.len() as u64,
                            ..NativeCodecWork::default()
                        })
                        .unwrap();
                    assert_eq!(&copied, b"abc");
                    if caller_panics {
                        panic!("original credit owner unwind");
                    }
                }
                Credit::AllocationReservation => {
                    let _reservation = ReadAllocationReservation::new(&meter, 8).unwrap();
                    if caller_panics {
                        panic!("original credit owner unwind");
                    }
                }
                Credit::Allocation => {
                    let mut buffer = crate::meter::ReadBuffer::<u8>::new(8, &meter).unwrap();
                    buffer.data.extend_from_slice(b"original");
                    assert_eq!(buffer.data.len(), 8);
                    if caller_panics {
                        panic!("original credit owner unwind");
                    }
                }
            }));
            assert_eq!(result.is_err(), caller_panics || callback_panics);
            if caller_panics {
                assert_eq!(
                    result.err().and_then(|p| p.downcast::<&str>().ok()),
                    Some(Box::new("original credit owner unwind")),
                    "credit return replaced the original owner unwind"
                );
            }
            assert_eq!(
                returned.load(Ordering::Acquire),
                1,
                "original credit returned more than once"
            );
            assert_eq!(
                observed.used_operations.load(Ordering::Acquire),
                u64::from(credit == Credit::Operations)
            );
            assert_eq!(observed.used_bytes.load(Ordering::Acquire), 0);
            assert_eq!(
                observed.used_allocations.load(Ordering::Acquire),
                u64::from(credit == Credit::Allocation)
            );
            assert_eq!(observed.memory.load(Ordering::Acquire), 0);
            assert_eq!(
                observed.copied.load(Ordering::Acquire),
                if credit == Credit::Codec { 3 } else { 0 }
            );
        }
    }
    Ok(())
}

#[test]
fn operations_preserve_original_unwind() -> io::Result<()> {
    preserves_unwind(Credit::Operations)
}
#[test]
fn bytes_preserve_original_unwind() -> io::Result<()> {
    preserves_unwind(Credit::Bytes)
}
#[test]
fn codec_preserves_original_unwind() -> io::Result<()> {
    preserves_unwind(Credit::Codec)
}
#[test]
fn allocation_reservation_preserves_original_unwind() -> io::Result<()> {
    preserves_unwind(Credit::AllocationReservation)
}
#[test]
fn allocation_preserves_original_unwind() -> io::Result<()> {
    preserves_unwind(Credit::Allocation)
}
