//! Contract checks shared by native writer captures and fuzzing.
use super::*;
use std::collections::BTreeMap;

/// Check retained records and the shared typed-generation admission contract.
pub fn check(bytes: &[u8]) {
    let resource = serde_json::from_slice::<crate::resources::ResourceRef>(bytes);
    let admitted = serde_json::from_slice::<crate::resources::GenerationRef>(bytes);
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
            matches!(
                serde_json::from_slice::<crate::resources::GenerationRef>(
                    &serde_json::to_vec(&wrong).unwrap()
                ),
                Err(error) if error.to_string().contains("typed resource kind mismatch")
            ),
            "foreign skill receipt kind admitted or refusal changed"
        );
    }

    let reference = match serde_json::from_slice::<RetainedSource>(bytes) {
        Ok(RetainedSource::Batch(records)) => records,
        Ok(record) => BTreeMap::from([(
            blake3::hash(&serde_json::to_vec(&record).unwrap()).to_string(),
            record,
        )]),
        Err(_) => {
            assert!(
                matches!(
                    retained::decode_records(bytes, "absent"),
                    Err(Error::Storage(reason)) if !reason.is_empty()
                ),
                "malformed skill record admitted or refusal changed"
            );
            return;
        }
    };
    let selected = reference.keys().min().map(String::as_str).unwrap_or("");
    check_selection(bytes, selected, &reference);
    check_selection(bytes, "absent", &reference);
    // The indexed storage envelope versions the same source identity codec.
    let mut envelope = serde_json::json!({
        "version": 1,
        "sources": serde_json::from_slice::<serde_json::Value>(bytes).unwrap(),
    });
    assert_eq!(
        retained::decode_stream_records(&serde_json::to_vec(&envelope).unwrap(), selected).is_ok(),
        retained::decode_records(bytes, selected).is_ok(),
        "indexed skill records changed source admission",
    );
    envelope["version"] = 2.into();
    assert!(
        matches!(
            retained::decode_stream_records(&serde_json::to_vec(&envelope).unwrap(), selected),
            Err(Error::Unsupported(_)),
        ),
        "foreign skill record format admitted"
    );

    // Derive single-record, changed-sibling and nested-batch inputs from the
    // original writer record. These are mutations, not captured wire fixtures.
    if let Some(record) = reference.get(selected)
        && !matches!(record, RetainedSource::Batch(_))
    {
        let single = serde_json::to_vec(record).unwrap();
        let selected = blake3::hash(&single).to_string();
        check_selection(
            &single,
            &selected,
            &BTreeMap::from([(selected.clone(), record)]),
        );
        // Build one defect at a time from an admitted original record. A fuzzed
        // bundle may have several unrelated defects with unordered diagnostics.
        let changed = serde_json::json!({ "Batch": {
            (&selected): record,
            "changed-sibling": record,
        } });
        assert!(
            matches!(
                retained::decode_records(&serde_json::to_vec(&changed).unwrap(), &selected),
                Err(Error::Conflict(reason)) if reason == "retained skill source changed"
            ),
            "changed skill sibling admitted or refusal changed"
        );
        let absent = serde_json::json!({ "Batch": {} });
        assert!(
            matches!(
                retained::decode_records(&serde_json::to_vec(&absent).unwrap(), &selected),
                Err(Error::Conflict(reason)) if reason == "retained skill source is absent"
            ),
            "missing skill source admitted or refusal changed"
        );
        let nested = serde_json::json!({ "Batch": { (&selected): { "Batch": {} } } });
        assert!(
            matches!(
                retained::decode_records(&serde_json::to_vec(&nested).unwrap(), &selected),
                Err(Error::Invalid(reason)) if reason == "nested retained skill batch"
            ),
            "nested skill batch admitted or refusal changed"
        );
    }
}

fn check_selection<T: std::borrow::Borrow<RetainedSource> + serde::Serialize>(
    bytes: &[u8],
    selected: &str,
    reference: &BTreeMap<String, T>,
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
