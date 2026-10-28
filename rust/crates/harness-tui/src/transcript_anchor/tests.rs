use super::*;
use proptest::prelude::*;

const PARTS: [TranscriptPart; 3] = [
    TranscriptPart::Reasoning,
    TranscriptPart::Answer,
    TranscriptPart::Tools,
];

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    #[test]
    fn source_anchors_round_trip_without_losing_large_offsets(line in any::<usize>(), row in any::<usize>(), part in 0usize..3) {
        let anchor = TranscriptAnchor { origin: Some(TranscriptOrigin {file: [41;32], line}),part:PARTS[part], position:RowPosition {line,row}};
        let bytes = serde_json::to_vec(&anchor).unwrap();
        prop_assert_eq!(serde_json::from_slice::<TranscriptAnchor>(&bytes).unwrap(),anchor);
        let mut value = serde_json::to_value(anchor).unwrap();
        value.get_mut("position").unwrap().as_object_mut().unwrap().insert("unknown".into(), serde_json::json!(true));
        prop_assert!(serde_json::from_value::<TranscriptAnchor>(value).is_err());
    }
}

#[test]
fn persisted_anchor_requires_source_provenance() {
    let anchor = TranscriptAnchor {
        part: TranscriptPart::Answer,
        position: RowPosition { line: 50, row: 2 },
        origin: Some(TranscriptOrigin {
            file: [19; 32],
            line: 7,
        }),
    };
    let mut encoded = serde_json::to_value(anchor).unwrap();
    assert_eq!(
        serde_json::from_value::<TranscriptAnchor>(encoded.clone()).unwrap(),
        anchor
    );
    encoded.as_object_mut().unwrap().remove("origin");
    assert!(serde_json::from_value::<TranscriptAnchor>(encoded.clone()).is_err());
    encoded
        .as_object_mut()
        .unwrap()
        .insert("origin".into(), serde_json::Value::Null);
    assert!(serde_json::from_value::<TranscriptAnchor>(encoded).is_err());
}
