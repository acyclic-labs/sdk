use serde_json::{Value, json};

pub fn check(bytes: &[u8]) {
    let Ok(value) = serde_json::from_slice::<Value>(bytes) else {
        return;
    };
    let decoded: Value = serde_json::from_slice(&serde_json::to_vec(&value).unwrap()).unwrap();
    assert_eq!(value, decoded);
    let parts = match value {
        Value::Array(parts) => parts,
        value => vec![value],
    };
    let expected = json!({"messages":[{"content":parts}]});
    let mut inserted = parts.clone();
    let slot = bytes.len() % (parts.len() + 1);
    inserted.insert(slot, json!({"kind":"text", "text":""}));
    assert!(
        !crate::matchers::model_requests(
            &[expected.clone()],
            &[json!({"messages":[{"content":inserted}]})]
        )
        .is_ok()
    );
    if !parts.is_empty() {
        let mut removed = parts;
        removed.remove(slot % removed.len());
        assert!(
            !crate::matchers::model_requests(
                &[expected],
                &[json!({"messages":[{"content":removed}]})]
            )
            .is_ok()
        );
    }
}
