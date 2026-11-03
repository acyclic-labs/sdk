//! Contract checks shared by native writer captures and fuzzing.
use super::*;
use std::collections::HashMap;

/// Check the deployed record and materialization receipt admission functions.
pub fn check(bytes: &[u8]) {
    let resource = serde_json::from_slice::<crate::resources::ResourceRef>(bytes);
    let admitted = decode_materialized(bytes);
    assert_eq!(
        admitted.is_ok(),
        resource
            .as_ref()
            .is_ok_and(|resource| resource.kind() == crate::resources::ResourceKind::Generation),
        "skill receipt admission"
    );
    if let (Ok(admitted), Ok(resource)) = (admitted, resource) {
        assert_eq!(admitted.as_resource(), &resource, "skill receipt identity");
        let mut wrong = serde_json::to_value(resource).unwrap();
        wrong["kind"] = "workspace".into();
        assert!(
            decode_materialized(&serde_json::to_vec(&wrong).unwrap()).is_err(),
            "foreign skill receipt kind admitted"
        );
    }

    let reference = match serde_json::from_slice::<RetainedSource>(bytes) {
        Ok(RetainedSource::Batch(records)) => records,
        Ok(record) => HashMap::from([(
            blake3::hash(&serde_json::to_vec(&record).unwrap()).to_string(),
            record,
        )]),
        Err(_) => {
            assert!(
                retained::decode_records(bytes, "absent").is_err(),
                "malformed skill record admitted"
            );
            return;
        }
    };
    let selected = reference.keys().min().map(String::as_str).unwrap_or("");
    check_selection(bytes, selected, &reference);
    check_selection(bytes, "absent", &reference);

    // Derive single-record, changed-sibling and nested-batch inputs from the
    // original writer record. These are mutations, not captured wire fixtures.
    if let Some(record) = reference.get(selected) {
        check_selection(
            &serde_json::to_vec(record).unwrap(),
            selected,
            &HashMap::from([(selected.to_owned(), record)]),
        );
        let mut changed = serde_json::to_value(&reference).unwrap();
        changed.as_object_mut().unwrap().insert(
            "changed-sibling".into(),
            serde_json::to_value(record).unwrap(),
        );
        let changed = serde_json::json!({ "Batch": changed });
        assert!(
            retained::decode_records(&serde_json::to_vec(&changed).unwrap(), selected).is_err(),
            "changed skill sibling admitted"
        );
        let nested = serde_json::json!({ "Batch": { (selected): { "Batch": {} } } });
        assert!(
            retained::decode_records(&serde_json::to_vec(&nested).unwrap(), selected).is_err(),
            "nested skill batch admitted"
        );
    }
}

fn check_selection<T: std::borrow::Borrow<RetainedSource> + serde::Serialize>(
    bytes: &[u8],
    selected: &str,
    reference: &HashMap<String, T>,
) {
    let expected = reference.contains_key(selected)
        && reference.iter().all(|(key, record)| {
            !matches!(record.borrow(), RetainedSource::Batch(_))
                && blake3::hash(&serde_json::to_vec(record).unwrap())
                    .to_hex()
                    .as_str()
                    == key
        });
    let admitted = retained::decode_records(bytes, selected);
    assert_eq!(admitted.is_ok(), expected, "skill batch admission");
    if let Ok(admitted) = admitted {
        assert_eq!(
            serde_json::to_value(admitted).unwrap(),
            serde_json::to_value(reference).unwrap(),
            "skill batch identity"
        );
    }
}
