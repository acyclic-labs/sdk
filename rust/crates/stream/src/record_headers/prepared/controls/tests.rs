use super::*;
use crate::test_meter::Meter;
use std::sync::atomic::Ordering;

struct Case {
    keys: Vec<Range<usize>>,
    attributes: Vec<(Range<usize>, Range<usize>)>,
    maximum: usize,
}

fn expected(bytes: &[u8], case: &Case) -> Option<RecordHeaders> {
    let keys = case
        .keys
        .iter()
        .map(|range| {
            bytes
                .get(range.clone())
                .map(|bytes| OwnedBytes::from(bytes.to_vec()))
        })
        .collect::<Option<Vec<_>>>()?;
    let attributes = case
        .attributes
        .iter()
        .map(|(name, value)| {
            Some(Attribute {
                name: std::str::from_utf8(bytes.get(name.clone())?)
                    .ok()?
                    .to_owned(),
                value: bytes.get(value.clone())?.to_vec().into(),
            })
        })
        .collect::<Option<Vec<_>>>()?;
    let headers = RecordHeaders::new(keys, attributes).ok()?;
    (headers.payload_bytes() <= case.maximum).then_some(headers)
}

fn cases() -> [Case; 11] {
    [
        Case {
            keys: vec![0..1, 0..1],
            attributes: vec![],
            maximum: crate::MAX_RECORD_BYTES,
        },
        Case {
            keys: vec![1..2, 0..1],
            attributes: vec![],
            maximum: crate::MAX_RECORD_BYTES,
        },
        Case {
            keys: vec![],
            attributes: vec![(0..1, 1..2), (0..1, 1..2)],
            maximum: crate::MAX_RECORD_BYTES,
        },
        Case {
            keys: vec![],
            attributes: vec![(3..4, 0..1)],
            maximum: crate::MAX_RECORD_BYTES,
        },
        Case {
            keys: vec![],
            attributes: vec![(8..9, 0..1)],
            maximum: crate::MAX_RECORD_BYTES,
        },
        Case {
            keys: vec![],
            attributes: vec![(0..0, 0..1)],
            maximum: crate::MAX_RECORD_BYTES,
        },
        Case {
            keys: std::iter::once(0..10).collect(),
            attributes: vec![],
            maximum: crate::MAX_RECORD_BYTES,
        },
        Case {
            keys: std::iter::once(0..2).collect(),
            attributes: vec![],
            maximum: 1,
        },
        Case {
            keys: vec![0..0, 0..1, 0..2],
            attributes: vec![(0..1, 2..4), (1..2, 4..8)],
            maximum: crate::MAX_RECORD_BYTES,
        },
        Case {
            keys: vec![],
            attributes: vec![(2..4, 4..8)],
            maximum: crate::MAX_RECORD_BYTES,
        },
        Case {
            keys: vec![],
            attributes: vec![],
            maximum: 0,
        },
    ]
}

#[test]
fn prepared_page_ranges_preserve_the_ordinary_header_contract_without_late_allocation()
-> Result<(), Box<dyn std::error::Error>> {
    let bytes = OwnedBytes::from_static("abλ🙂\0".as_bytes());
    for case in cases() {
        let expected = expected(&bytes, &case);
        let meter = Arc::new(Meter::default());
        let prepared = PreparedHeaderControls::new(&meter.original(), case.maximum)?;
        let bytes = bytes.clone();
        meter.cancelled.store(true, Ordering::SeqCst);
        meter.deny_allocation.store(true, Ordering::SeqCst);
        meter.deny_codec.store(true, Ordering::SeqCst);
        let mut result = None;
        let allocation = acyclic_test_support::measure(|| {
            result = Some(prepared.finish(bytes, &case.keys, &case.attributes));
        });
        assert_eq!(
            allocation.count_max, 0,
            "page range validation allocated after issuance"
        );
        assert_eq!(
            result.ok_or("missing header result")?.ok(),
            expected,
            "prepared page ranges changed the canonical header contract"
        );
        meter.assert_refunded();
    }
    Ok(())
}
