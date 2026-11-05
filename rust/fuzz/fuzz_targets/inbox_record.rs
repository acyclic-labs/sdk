//! Fuzz the agent record decoders and their canonical re-encoding.
#![no_main]

use acyclic_harness_core::agent::{AgentPath, AgentRecord, Closed, InboxRecord, StatusRecord};
use libfuzzer_sys::fuzz_target;

/// Any accepted record re-encodes canonically to a fixed point.
fn round_trip<R: AgentRecord + PartialEq + std::fmt::Debug>(bytes: &[u8]) {
    let Ok(record) = R::decode(bytes) else {
        return;
    };
    let Ok(canonical) = record.encode() else {
        panic!("accepted record does not re-encode");
    };
    let Ok(again) = R::decode(&canonical) else {
        panic!("canonical encoding does not decode");
    };
    assert_eq!(again, record);
    assert_eq!(again.encode().ok().as_deref(), Some(canonical.as_slice()));
}

fuzz_target!(|data: &[u8]| {
    let Some((selector, bytes)) = data.split_first() else {
        return;
    };
    match selector % 4 {
        0 => round_trip::<InboxRecord>(bytes),
        1 => round_trip::<StatusRecord>(bytes),
        2 => round_trip::<Closed>(bytes),
        _ => {
            if let Ok(text) = std::str::from_utf8(bytes)
                && let Ok(path) = AgentPath::try_from(text.to_owned())
            {
                assert_eq!(path.as_str(), text);
                if let Some(parent) = path.parent() {
                    assert_eq!(parent.child(&path.id()), path);
                    assert_eq!(parent.depth() + 1, path.depth());
                }
            }
        }
    }
});
