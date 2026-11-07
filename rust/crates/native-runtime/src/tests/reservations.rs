use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Credit {
    Operations,
    Bytes,
    Codec,
    AllocationReservation,
    Allocation,
}

fn exercise_credit(
    credit: Credit,
    meter: &Arc<dyn NativeReadMeter>,
    caller_panics: bool,
) -> io::Result<()> {
    let unwind = || {
        if caller_panics {
            std::panic::panic_any("original credit owner unwind");
        }
    };
    match credit {
        Credit::Operations => {
            let file = tempfile::tempfile()?;
            let mut reservation =
                NativeOperationReservation::new(Arc::clone(meter), NativeOperationKind::Read, 2)?;
            let (metadata, recorded) = reservation.attempt(|| file.metadata());
            assert_eq!(metadata?.len(), 0);
            recorded?;
            unwind();
        }
        Credit::Bytes => {
            let _reservation =
                NativeByteReservation::new(Arc::clone(meter), NativeOperationKind::Read, 8)?;
            unwind();
        }
        Credit::Codec => {
            let mut reservation = NativeCodecReservation::new(
                Arc::clone(meter),
                NativeCodecWork {
                    copied: 8,
                    ..NativeCodecWork::default()
                },
            )?;
            let mut copied = [0; 3];
            copied.copy_from_slice(b"abc");
            reservation.record(NativeCodecWork {
                copied: copied.len() as u64,
                ..NativeCodecWork::default()
            })?;
            assert_eq!(&copied, b"abc");
            unwind();
        }
        Credit::AllocationReservation => {
            let _reservation = ReadAllocationReservation::new(meter, 8)?;
            unwind();
        }
        Credit::Allocation => {
            let mut buffer = crate::meter::ReadBuffer::<u8>::new(8, meter)?;
            buffer.data.extend_from_slice(b"original");
            assert_eq!(buffer.data.len(), 8);
            unwind();
        }
    }
    Ok(())
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
                            std::panic::panic_any(format!(
                                "original credit return failure: {actual:?}"
                            ));
                        }
                    }
                }
            }));
            let observed = Arc::new(ledger);
            let meter: Arc<dyn NativeReadMeter> = observed.clone();
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                exercise_credit(credit, &meter, caller_panics)
            }));
            let panic = match result {
                Ok(result) => {
                    result?;
                    None
                }
                Err(payload) => Some(payload),
            };
            assert_eq!(panic.is_some(), caller_panics || callback_panics);
            if caller_panics {
                assert_eq!(
                    panic.and_then(|p| p.downcast::<&str>().ok()),
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

#[test]
fn shrinking_allocation_commits_custody_before_refund_callback() -> io::Result<()> {
    type Shrink = fn(&mut ReadAllocation, usize) -> io::Result<()>;
    let shrink: [(&str, Shrink); 3] = [
        ("release", |allocation, retained| {
            allocation.release(8 - retained)
        }),
        ("resize_retained", ReadAllocation::resize_retained),
        ("reconcile_capacity", ReadAllocation::reconcile_capacity),
    ];
    let mut violations = Vec::new();
    for (name, shrink) in shrink {
        for retained in [0, 4] {
            let returned = Arc::new(AtomicU64::new(0));
            let mut ledger = ReadBudgetMeter::new(8, 8);
            ledger.credit_returned = Some(Box::new({
                let returned = Arc::clone(&returned);
                move |credit| {
                    if credit == Credit::Allocation && returned.fetch_add(1, Ordering::AcqRel) == 0
                    {
                        std::panic::panic_any("original allocation shrink callback");
                    }
                }
            }));
            let observed = Arc::new(ledger);
            let meter: Arc<dyn NativeReadMeter> = observed.clone();
            let mut allocation = ReadAllocation::new(&meter, 8)?;
            // Retire actual backing before returning its admitted credit. Any
            // remaining claim conservatively outlives that physical retirement.
            let backing = Vec::<u8>::with_capacity(8);
            drop(backing);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                shrink(&mut allocation, retained)
            }));
            assert_eq!(
                result.err().and_then(|p| p.downcast::<&str>().ok()),
                Some(Box::new("original allocation shrink callback"))
            );
            let custody = allocation.bytes();
            let charged = observed.memory.load(Ordering::Acquire);
            let retired = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                drop(allocation);
            }));
            let final_charge = observed.memory.load(Ordering::Acquire);
            let returns = returned.load(Ordering::Acquire);
            if custody != retained as u64
                || charged != retained as u64
                || final_charge != 0
                || retired.is_err()
                || returns != 2
            {
                violations.push((name, retained, custody, charged, final_charge, returns));
            }
        }
    }
    assert!(
        violations.is_empty(),
        "allocation retained ownership precedes original refund callback: {violations:?}"
    );
    Ok(())
}
