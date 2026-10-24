use super::*;
use crate::test_meter::Meter;
use proptest::prelude::*;
use std::sync::atomic::Ordering;

fn complete(
    keys: Vec<OwnedBytes>,
    attributes: Vec<Attribute>,
) -> Result<(), Box<dyn std::error::Error>> {
    let headers = RecordHeaders::new(keys, attributes)?;
    let meter = Arc::new(Meter::default());
    let plan = PreparedHeaders::new(&meter.original(), headers.payload_bytes())?;
    let allocations = meter.allocations.load(Ordering::SeqCst);
    meter.cancelled.store(true, Ordering::SeqCst);
    meter.deny_allocation.store(true, Ordering::SeqCst);
    meter.deny_codec.store(true, Ordering::SeqCst);
    let output = plan.finish(&headers)?;
    assert_eq!(output, headers);
    assert_eq!(output.attributes().to_vec(), headers.attributes().to_vec());
    assert_eq!(
        meter.allocations.load(Ordering::SeqCst),
        allocations,
        "prepared header completion allocated after issuance"
    );
    for (position, attribute) in output.attributes().iter().enumerate() {
        assert_eq!(
            output
                .attributes()
                .binary_search_by(|value| value.name.cmp(attribute.name)),
            Ok(position)
        );
    }
    drop(output);
    meter.assert_refunded();
    Ok(())
}

#[test]
fn prepared_headers_cover_empty_payloads_unicode_names_and_the_full_payload_bound()
-> Result<(), Box<dyn std::error::Error>> {
    complete(vec![], vec![])?;
    complete(vec![OwnedBytes::new()], vec![])?;
    complete(
        vec![],
        vec![Attribute {
            name: "λ🙂".into(),
            value: OwnedBytes::new(),
        }],
    )?;
    complete(
        vec![],
        vec![Attribute {
            name: "λ🙂".into(),
            value: vec![1; crate::MAX_RECORD_BYTES - "λ🙂".len()].into(),
        }],
    )?;
    Ok(())
}

proptest! {
    #[test]
    fn arbitrary_prepared_header_shapes_keep_exact_utf8_names_and_original_custody(
        keys in prop::collection::btree_set(prop::collection::vec(any::<u8>(), 0..32), 0..=MAX_RECORD_KEYS),
        attributes in prop::collection::btree_map("[a-zλ🙂]{1,12}", prop::collection::vec(any::<u8>(), 0..32), 0..=MAX_RECORD_ATTRIBUTES),
    ) {
        let keys = keys.into_iter().map(OwnedBytes::from).collect();
        let attributes = attributes.into_iter().map(|(name, value)| Attribute { name, value: value.into() }).collect();
        prop_assert!(complete(keys, attributes).is_ok());
    }
}
