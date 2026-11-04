use super::*;
use std::mem::size_of;

#[test]
fn boxed_codec_slots_are_admitted_before_allocation_and_released_after_drop() -> Result<()> {
    for native in [false, true] {
        check_boxed_slot::<{ size_of::<crate::conversation::FileRef>() }>(native)?;
    }
    Ok(())
}

fn check_boxed_slot<const N: usize>(native: bool) -> Result<()> {
    use crate::test_support::accounted::accounted;
    let observe = |maximum, funded| {
        accounted(native, maximum, false, |io| -> Result<(u64, u64)> {
            let mut history = io.history_io();
            let mut work = Some(&mut history as &mut dyn crate::store::HistoryIo);
            let mut quota = CodecQuota::new(&mut work, 0)?;
            let mut value = None;
            let allocation = acyclic_test_support::measure(|| {
                value = Some(if funded {
                    quota.boxed([7_u8; N])
                } else {
                    Ok(Box::new([7_u8; N]))
                });
            });
            let value = value.expect("box allocation attempted")?;
            assert_eq!(value.as_ref(), &[7_u8; N]);
            let held = io.owned_bytes()?;
            drop(value);
            drop(quota);
            assert_eq!(io.owned_bytes()?, 0);
            Ok((held, allocation.bytes_max))
        })
        .map_err(|(message, _)| Error::Invalid(message))
    };
    let positive = observe(1_048_576, true)?;
    let (held, physical) = positive.0?;
    assert_eq!(held, N as u64);
    assert_eq!(physical, N as u64);
    let negative = observe(1_048_576, false)?.0?;
    assert!(
        negative.0 < negative.1,
        "unfunded boxes escaped the same ownership oracle"
    );
    let boundary = if native { positive.3 } else { positive.1 }.peak_allocation_bytes;
    assert!(
        observe(boundary - 1, true)?.0.is_err(),
        "heap admission ran after allocation"
    );
    assert!(observe(boundary, true)?.0.is_ok());
    assert!(
        observe(boundary - 1, false)?.0.is_ok(),
        "negative control did not bypass admission"
    );
    Ok(())
}

#[test]
fn borrowed_index_nodes_keep_original_credit_on_refusal_and_cancelled_retirement() -> Result<()> {
    use crate::test_support::accounted::accounted;
    use acyclic_collections::FixedIndex;

    for native in [false, true] {
        let observe = |maximum| {
            accounted(native, maximum, false, |io| -> Result<()> {
                let mut history = io.history_io();
                let mut observer = Some(&mut history as &mut dyn HistoryIo);
                let original = BorrowedIndexWork::new(&mut observer);
                let mut result = Ok(());
                let physical = acyclic_test_support::measure(|| {
                    result = (|| {
                        let mut index = FixedIndex::<usize, _, 8>::default();
                        index.get_or_try_insert_with([0; 8], &mut &original, |_| Ok(7))?;
                        let retained = io.owned_bytes()?;
                        assert_eq!(
                            retained,
                            acyclic_test_support::current_allocations()
                                .unwrap()
                                .bytes_current as u64,
                            "borrowed index backing must retain its original credit"
                        );
                        assert!(
                            index
                                .get_or_try_insert_with([1; 8], &mut &original, |_| {
                                    Err(Error::Conflict("constructor refused".into()))
                                })
                                .is_err()
                        );
                        assert_eq!(io.owned_bytes()?, retained);
                        assert_eq!(index.get([0; 8], &mut &original)?, Some(&7));
                        assert_eq!(index.get([1; 8], &mut &original)?, None);
                        index.get_or_try_insert_with([2; 8], &mut &original, |_| Ok(9))?;
                        assert_eq!(
                            io.owned_bytes()?,
                            acyclic_test_support::current_allocations()
                                .unwrap()
                                .bytes_current as u64,
                            "every live node must keep its original byte claim"
                        );
                        io.cancellation().cancel();
                        assert!(index.get([0; 8], &mut &original).is_err());
                        assert!(
                            index
                                .get_or_try_insert_with([3; 8], &mut &original, |_| {
                                    panic!("cancelled constructor")
                                })
                                .is_err()
                        );
                        drop(index);
                        assert_eq!(
                            io.owned_bytes()?,
                            0,
                            "cancelled retirement must release custody"
                        );
                        Ok(())
                    })();
                });
                if result.is_ok() {
                    assert_eq!(physical.bytes_current, 0);
                }
                result
            })
            .map_err(|(message, _)| Error::Invalid(message))
        };
        let positive = observe(1_048_576)?;
        positive.0?;
        let boundary = if native { positive.3 } else { positive.1 }.peak_allocation_bytes;
        assert!(
            observe(boundary - 1)?.0.is_err(),
            "node claim must precede allocation"
        );
        assert!(observe(boundary)?.0.is_ok());
    }
    Ok(())
}
