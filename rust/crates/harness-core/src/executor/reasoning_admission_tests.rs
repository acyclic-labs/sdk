use super::*;
use crate::model::{ReasoningBlock, ReasoningKind, ReasoningOrigin, ReasoningOutcome};
use proptest::prelude::*;

fn event(bytes: Vec<u8>) -> ModelEvent {
    ModelEvent::ReasoningBlock {
        block: ReasoningBlock {
            kind: ReasoningKind::Encrypted,
            text: None,
            origin: ReasoningOrigin {
                provider: "responses".into(),
                model: "fixture".into(),
                credential_scope: Some([7; 32]),
            },
            continuity: bytes,
            outcome: ReasoningOutcome::PreservedOpaque,
        },
    }
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn continuation_bytes_round_trip_in_compact_text(bytes in prop::collection::vec(any::<u8>(), 0..8192)) {
        let event = event(bytes.clone());
        let wire = serde_json::to_value(&event).unwrap();
        let encoded = wire["block"]["continuity"].as_str().unwrap();
        prop_assert_eq!(encoded.len(), (bytes.len() * 4).div_ceil(3));
        prop_assert_eq!(serde_json::from_value::<ModelEvent>(wire.clone()).unwrap(), event);
        let ModelEvent::ReasoningBlock { block } = event else { unreachable!() };
        let native = crate::model::NativePart { origin: block.origin, continuity: bytes.clone() };
        let native_wire = serde_json::to_value(&native).unwrap();
        prop_assert_eq!(native_wire["continuity"].as_str(), Some(encoded));
        prop_assert_eq!(serde_json::from_value::<crate::model::NativePart>(native_wire).unwrap(), native);
        let mut obsolete = wire;
        obsolete["block"]["continuity"] = serde_json::json!(bytes);
        prop_assert!(serde_json::from_value::<ModelEvent>(obsolete).is_err());
    }
    #[test]
    fn only_complete_replayable_reasoning_envelopes_are_admitted(bytes in prop::collection::vec(any::<u8>(), 0..256), offset in 0u64..3) {
        let event = event(bytes);
        let length = serde_json::to_vec(&event).unwrap().len() as u64;
        let maximum = length + offset - 1;
        let mut admission = ModelEventAdmission::default();
        let limits = Limits { render_bytes:maximum, file_bytes:maximum, ..Limits::default() };
        let result = admission.observe(&event, limits, None);
        prop_assert_eq!(result.is_ok(), maximum >= length);
        prop_assert_eq!(admission.count, usize::from(result.is_ok()));
    }
}
#[test]
fn block_only_admission_negative_control_accepts_unreadable_envelope() {
    let event = event(b"opaque".to_vec());
    let maximum = serde_json::to_vec(&event).unwrap().len() as u64 - 1;
    let limits = Limits {
        render_bytes: maximum,
        file_bytes: maximum,
        ..Limits::default()
    };
    let ModelEvent::ReasoningBlock { block } = &event else {
        unreachable!()
    };
    assert!(block.validate_limits(limits, &mut None).is_ok());
    assert!(
        ModelEventAdmission::default()
            .observe(&event, limits, None)
            .is_err()
    );
}
