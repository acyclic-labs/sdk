use super::*;
use proptest::prelude::*;

fn read_range<F: std::borrow::Borrow<NativeFile>>(
    file: F,
    offset: usize,
    length: usize,
    maximum: usize,
    api: u8,
    reject: bool,
    meter: Arc<ReadBudgetMeter>,
) -> (Result<Vec<u8>, io::ErrorKind>, u64) {
    let mut range = AsyncRangeReader::new(file, offset as u64, length as u64);
    let accept = |result: io::Result<(Vec<u8>, usize)>| {
        let (mut bytes, filled) = result?;
        if reject {
            return Err(io::ErrorKind::Interrupted.into());
        }
        bytes.truncate(filled);
        Ok(bytes)
    };
    let result = match api {
        0 => complete(range.read_prepared(vec![0x55; maximum], maximum, meter, true, accept)),
        1 => complete(range.read_spare(
            (Vec::with_capacity(maximum), ()),
            maximum,
            meter,
            true,
            |result| accept(result.map(|((bytes, ()), filled)| (bytes, filled))),
        )),
        _ => complete(range.read(maximum, meter, true)).map(|chunk| chunk.0.to_vec()),
    };
    (result.map_err(|error| error.kind()), range.remaining())
}

fn cursor_matches(
    result: &Result<Vec<u8>, io::ErrorKind>,
    remaining: u64,
    length: usize,
) -> proptest::test_runner::TestCaseResult {
    prop_assert_eq!(
        remaining.checked_add(result.as_ref().map_or(0, |bytes| bytes.len() as u64)),
        Some(length as u64)
    );
    Ok(())
}

#[test]
fn borrowed_ranges_match_owned_bytes_cursor_charges_and_retirement()
-> Result<(), Box<dyn std::error::Error>> {
    initialize_native_io_services()?;
    let _inline = InlineBlocking::enter();
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("borrowed-range");
    let mut runner = proptest::test_runner::TestRunner::new(ProptestConfig {
        cases: 64,
        source_file: Some(file!()),
        ..ProptestConfig::default()
    });
    // All three read APIs have identical outcomes, charges and retirement with
    // borrowed and owned files, including EOF, cancellation and consumer refusal.
    runner.run(
        &(
            prop::collection::vec(any::<u8>(), 0..96),
            0_usize..128,
            0_usize..128,
            0_usize..128,
            any::<bool>(),
            any::<bool>(),
        ),
        |(body, offset, length, maximum, cancelled, reject)| {
            std::fs::write(&path, &body).unwrap();
            let file = Arc::new(bounded_write_file(&path, false).unwrap());
            for api in 0..3 {
                let make_meter = || {
                    let meter = Arc::new(ReadBudgetMeter::new(16, 256));
                    meter.cancelled.store(cancelled, Ordering::Release);
                    meter
                };
                let owned_meter = make_meter();
                let borrowed_meter = make_meter();
                let owned = read_range(
                    file.clone(),
                    offset,
                    length,
                    maximum,
                    api,
                    reject,
                    owned_meter.clone(),
                );
                let borrowed = read_range(
                    file.as_ref(),
                    offset,
                    length,
                    maximum,
                    api,
                    reject,
                    borrowed_meter.clone(),
                );
                prop_assert_eq!(&borrowed, &owned);
                cursor_matches(&borrowed.0, borrowed.1, length)?;
                // Cursor-only control: retaining an extra byte must fail the same
                // cursor invariant, even when the read itself fails.
                prop_assert!(
                    cursor_matches(&borrowed.0, borrowed.1.saturating_add(1), length).is_err()
                );
                let requested = length.min(maximum);
                let available = body.len().saturating_sub(offset).min(requested);
                if !cancelled && (api == 2 || !reject) && available == requested {
                    prop_assert_eq!(
                        borrowed.0.unwrap(),
                        body.get(offset..offset + available).unwrap_or_default()
                    );
                } else {
                    prop_assert!(borrowed.0.is_err());
                }
                for counter in [
                    |meter: &ReadBudgetMeter| meter.read.load(Ordering::Acquire),
                    |meter: &ReadBudgetMeter| meter.used_operations.load(Ordering::Acquire),
                    |meter: &ReadBudgetMeter| meter.used_bytes.load(Ordering::Acquire),
                    |meter: &ReadBudgetMeter| meter.used_allocations.load(Ordering::Acquire),
                ] {
                    prop_assert_eq!(counter(&owned_meter), counter(&borrowed_meter));
                }
                prop_assert_eq!(
                    borrowed_meter.read.load(Ordering::Acquire),
                    if cancelled { 0 } else { available as u64 }
                );
                prop_assert_eq!(borrowed_meter.memory.load(Ordering::Acquire), 0);
                prop_assert_eq!(owned_meter.memory.load(Ordering::Acquire), 0);
                prop_assert_eq!(Arc::strong_count(&file), 1);
            }
            Ok(())
        },
    )?;
    Ok(())
}
