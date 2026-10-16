//! Independent timer admission invariants shared with the parser fuzzer.
use acyclic_actors::timers::TimerRecord;

/// Check record invariants without invoking production admission or codecs.
pub fn check(record: &TimerRecord) {
    assert!(record.deadline_unix_ms > 0, "positive timer deadline");
    for identity in [&record.task_id, &record.operation_id] {
        assert_eq!(identity.len(), 36, "canonical timer identity");
        for (index, byte) in identity.bytes().enumerate() {
            assert!(
                if [8, 13, 18, 23].contains(&index) {
                    byte == b'-'
                } else {
                    byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
                },
                "canonical timer identity"
            );
        }
    }
}
