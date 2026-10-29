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
