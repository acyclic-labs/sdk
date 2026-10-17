use super::*;
use proptest::prelude::*;

fn prefix_matches(
    data: &[u8],
    expected: &[u8],
    success: bool,
) -> proptest::test_runner::TestCaseResult {
    prop_assert!(data.len() <= expected.len());
    if success {
        prop_assert_eq!(data.len(), expected.len());
    }
    prop_assert_eq!(data, &expected[..data.len()]);
    Ok(())
}

fn open_source(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt as _;
        options.custom_flags(windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OVERLAPPED);
    }
    options.open(path)
}

#[test]
fn owned_read_matches_source_prefix_and_retires_custody() -> Result<(), Box<dyn std::error::Error>>
{
    initialize_native_io_services()?;
    let _inline = InlineBlocking::enter();
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("owned-prefix");
    let mut runner = proptest::test_runner::TestRunner::new(ProptestConfig {
        cases: 128,
        source_file: Some(file!()),
        ..ProptestConfig::default()
    });
    // Property: the owned API publishes exactly the available bounded prefix,
    // or preserves actual read charges and retires all custody on failure.
    runner.run(
        &(
            prop::collection::vec(any::<u8>(), 0..96),
            0_usize..128,
            0_usize..128,
            any::<bool>(),
            any::<bool>(),
        ),
        |(body, offset, maximum, exact, post_cancel)| {
            std::fs::write(&path, &body).unwrap();
            let file = NativeFile::from_file(open_source(&path).unwrap(), None)
                .map_err(|(error, _)| error)
                .unwrap();
            let mut policy = ReadBudgetMeter::new(256, 256);
            policy.cancel_after_read = post_cancel;
            let meter = Arc::new(policy);
            let available = body.len().saturating_sub(offset).min(maximum);
            let result = complete(
                file.read_bounded_async(offset as u64, maximum, meter.clone(), exact)
                    .unwrap(),
            );
            let charged = meter.read.load(Ordering::Acquire) as usize;
            if post_cancel && available != 0 {
                prop_assert!((1..=available).contains(&charged));
            } else {
                prop_assert_eq!(charged, available);
            }
            match result {
                Ok(chunk) => {
                    prop_assert!(!post_cancel || available == 0);
                    prop_assert!(!exact || available == maximum);
                    let expected = body.get(offset..offset + available).unwrap_or_default();
                    prefix_matches(&chunk.0, expected, true)?;
                    // Deliberately broken publication includes an unwritten tail.
                    let mut broken = chunk.0.to_vec();
                    broken.push(0x55);
                    prop_assert!(prefix_matches(&broken, expected, true).is_err());
                    drop(chunk);
                }
                Err(error) => prop_assert_eq!(
                    error.kind(),
                    if post_cancel && available != 0 {
                        io::ErrorKind::Interrupted
                    } else {
                        prop_assert!(exact && available != maximum);
                        io::ErrorKind::UnexpectedEof
                    }
                ),
            }
            prop_assert_eq!(meter.memory.load(Ordering::Acquire), 0);
            Ok(())
        },
    )?;
    Ok(())
}

#[test]
fn spare_read_prefix_matches_source_and_rejects_overpublication()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("spare-prefix");
    let mut runner = proptest::test_runner::TestRunner::new(ProptestConfig {
        cases: 128,
        source_file: Some(file!()),
        ..ProptestConfig::default()
    });
    // Property: success and every failure expose only bytes actually written by
    // the kernel, regardless of excess capacity, EOF, exactness or cancellation.
    runner.run(
        &(
            prop::collection::vec(any::<u8>(), 0..96),
            0_usize..128,
            0_usize..128,
            0_usize..9,
            any::<bool>(),
            any::<bool>(),
            any::<bool>(),
        ),
        |(body, offset, maximum, excess, exact, pre_cancel, post_cancel)| {
            std::fs::write(&path, &body).unwrap();
            let file = open_source(&path).unwrap();
            let mut data = Vec::with_capacity(maximum + excess);
            let mut meter = ReadBudgetMeter::new(8, 256);
            meter.cancel_after_read = post_cancel;
            meter.cancelled.store(pre_cancel, Ordering::Release);
            let available = if pre_cancel {
                0
            } else {
                body.len().saturating_sub(offset).min(maximum)
            };
            let result = crate::file::read_bounded_spare(
                &file,
                offset as u64,
                &mut data,
                maximum,
                exact,
                &meter,
            );
            let success = result.is_ok();
            let charged = meter.read.load(Ordering::Acquire) as usize;
            prop_assert_eq!(charged, data.len());
            if post_cancel && available != 0 {
                prop_assert!((1..=available).contains(&charged));
            } else {
                prop_assert_eq!(charged, available);
            }
            if pre_cancel || (post_cancel && available != 0) {
                prop_assert_eq!(result.unwrap_err().kind(), io::ErrorKind::Interrupted);
            } else if exact && available != maximum {
                prop_assert_eq!(result.unwrap_err().kind(), io::ErrorKind::UnexpectedEof);
            } else {
                prop_assert_eq!(result.unwrap(), available);
            }
            let expected_bytes = body.get(offset..offset + charged).unwrap_or_default();
            prefix_matches(&data, expected_bytes, success)?;
            // Broken publication includes one unwritten tail byte. Initialize
            // its sentinel so the negative control itself never invokes UB.
            data.resize(expected_bytes.len() + 1, 0x55);
            prop_assert!(prefix_matches(&data, expected_bytes, success).is_err());
            Ok(())
        },
    )?;
    Ok(())
}

#[test]
fn spare_reads_miri_initialized_prefixes() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("miri-spare");
    std::fs::write(&path, b"actual")?;
    let file = open_source(&path)?;
    for (offset, maximum, exact, cancelled, after_read, available) in [
        (0, 0, false, false, false, 0),
        (0, 3, true, false, false, 3),
        (0, 8, false, false, false, 6),
        (0, 8, true, false, false, 6),
        (9, 4, false, false, false, 0),
        (0, 4, false, true, false, 0),
        (0, 4, false, false, true, 4),
    ] {
        let mut data = Vec::with_capacity(16);
        let mut meter = ReadBudgetMeter::new(8, 64);
        meter.cancelled.store(cancelled, Ordering::Release);
        meter.cancel_after_read = after_read;
        let result =
            crate::file::read_bounded_spare(&file, offset, &mut data, maximum, exact, &meter);
        let charged = meter.read.load(Ordering::Acquire) as usize;
        assert_eq!(charged, data.len());
        if after_read {
            // Cancellation follows the first positive OS completion, which
            // can legally initialize less than the requested prefix.
            assert!((1..=available).contains(&charged));
        } else {
            assert_eq!(charged, available);
        }
        assert_eq!(
            data,
            b"actual"
                .get(offset as usize..offset as usize + data.len())
                .unwrap_or_default()
        );
        assert_eq!(
            result.is_err(),
            cancelled || after_read || (exact && available != maximum)
        );
    }
    let mut data = Vec::with_capacity(3);
    let meter = ReadBudgetMeter::new(8, 64);
    assert!(crate::file::read_bounded_spare(&file, 0, &mut data, 4, true, &meter).is_err());
    assert!(data.is_empty());
    Ok(())
}

proptest! {
    // Property: only the exact reserved footprint can become an allocation;
    // mismatches retire the untouched reservation, while cancellation cannot
    // discard a previously admitted allocation's completed custody.
    #[test]
    fn reserved_header_requires_exact_footprint(
        reserved in 1_usize..257, additional in 1_usize..257, cancelled in any::<bool>(),
    ) {
      for actual in [reserved, reserved + additional] {
        let meter = Arc::new(ReadBudgetMeter::new(0, 0));
        let observer: Arc<dyn NativeReadMeter> = meter.clone();
        let reservation = ReadAllocationReservation::new(&observer, reserved as u64).unwrap();
        meter.cancelled.store(cancelled, Ordering::Release);
        let result = reservation.allocate_exact(actual);
        prop_assert_eq!(result.is_ok(), reserved == actual);
        if let Ok(allocation) = result {
            prop_assert_eq!(allocation.bytes(), reserved as u64);
            prop_assert_eq!(meter.memory.load(Ordering::Acquire), reserved as u64);
            drop(allocation);
        }
        prop_assert_eq!(meter.memory.load(Ordering::Acquire), 0);
        prop_assert_eq!(meter.used_allocations.load(Ordering::Acquire), u64::from(reserved == actual));
      }
    }
}
