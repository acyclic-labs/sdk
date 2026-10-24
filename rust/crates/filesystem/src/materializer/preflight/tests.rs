use super::*;
use proptest::prelude::*;
use std::io::Read;

struct ShortRead<'a> {
    bytes: &'a [u8],
    maximum: usize,
}

impl Read for ShortRead<'_> {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        let count = output.len().min(self.maximum).min(self.bytes.len());
        output[..count].copy_from_slice(&self.bytes[..count]);
        self.bytes = &self.bytes[count..];
        Ok(count)
    }
}

proptest! {
    // Property: arbitrary short reads compare exactly the requested prefix,
    // including corruption and EOF, with exact physical read/byte charges and
    // no extra allocation. The independent oracle is slice prefix equality.
    #[test]
    fn host_comparison_preserves_prefix_equality_and_work(
        expected in prop::collection::vec(any::<u8>(), 0..257),
        maximum in 1usize..33,
        variant in 0u8..4,
        position in any::<u16>(),
    ) {
        let mut actual = expected.clone();
        match variant {
            1 if !actual.is_empty() => {
                let index = usize::from(position) % actual.len();
                actual[index] ^= 0xff;
            }
            2 => actual.truncate(usize::from(position).min(actual.len())),
            3 => actual.push(0),
            _ => {}
        }
        let work = NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
        ).unwrap();
        let before = work.receipt();
        let mut reader = ShortRead { bytes: &actual, maximum };
        let equal = host_range_equals(&mut reader, &expected, &work).unwrap();
        prop_assert_eq!(equal, actual.starts_with(&expected));
        let after = work.receipt();
        let read = expected.len().min(actual.len());
        let eof = actual.len() < expected.len();
        prop_assert_eq!(after.backend_read_operations - before.backend_read_operations,
            read.div_ceil(maximum) as u64 + u64::from(eof));
        prop_assert_eq!(after.source_bytes_read - before.source_bytes_read, read as u64);
        let comparisons = if eof { 0 } else {
            expected.iter().zip(&actual).position(|(a, b)| a != b)
                .unwrap_or(expected.len()) as u64 + 1
        };
        prop_assert_eq!(after.items_examined - before.items_examined, comparisons);
        prop_assert_eq!(after.allocation_operations, before.allocation_operations);
        prop_assert_eq!(after.peak_allocation_bytes, before.peak_allocation_bytes);
    }
}

#[test]
fn the_full_host_range_detects_a_difference_in_the_last_byte() {
    let expected = vec![7; HOST_COMPARE_BYTES];
    let mut actual = expected.clone();
    let work = NativeMaterializationWork::new(
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .unwrap();
    assert!(host_range_equals(&mut actual.as_slice(), &expected, &work).unwrap());
    *actual.last_mut().unwrap() ^= 0xff;
    assert!(!host_range_equals(&mut actual.as_slice(), &expected, &work).unwrap());
}
