use super::*;
use proptest::prelude::*;

#[path = "../../fuzz/transcript_anchor.rs"]
mod oracle;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    #[test]
    fn source_anchors_round_trip_without_losing_large_offsets(
        line in any::<usize>(),
        row in any::<usize>(),
        source_line in any::<usize>(),
        file in any::<[u8; 32]>(),
        part in prop_oneof![Just(TranscriptPart::Reasoning), Just(TranscriptPart::Answer), Just(TranscriptPart::Tools)],
    ) {
        let anchor = TranscriptAnchor {
            origin: Some(TranscriptOrigin { file, line: source_line }),
            part,
            position: RowPosition { line, row },
        };
        prop_assert_eq!(oracle::check(&anchor), Ok(()));
    }
}
