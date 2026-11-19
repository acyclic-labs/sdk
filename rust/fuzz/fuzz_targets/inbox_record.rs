//! Fuzz the bounded agent record decoders and their canonical re-encoding.
#![no_main]

use acyclic_harness_core::agent::{
    AgentPath, AgentRecord, Closed, Error, InboxRecord, MAX_RECORD_BYTES, StatusRecord,
};
use libfuzzer_sys::fuzz_target;

/// Any accepted record re-encodes canonically, within bounds, to a fixed point.
fn round_trip<R: AgentRecord + PartialEq + std::fmt::Debug>(bytes: &[u8]) -> Result<(), Error> {
    let Ok(record) = R::decode(bytes) else {
        return Ok(());
    };
    let canonical = record.encode()?;
    assert!(canonical.len() <= MAX_RECORD_BYTES);
    let again = R::decode(&canonical)?;
    assert_eq!(again, record);
    assert_eq!(again.encode()?, canonical);
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let Some((selector, bytes)) = data.split_first() else {
        return;
    };
    let result = match selector % 4 {
        0 => round_trip::<InboxRecord>(bytes),
        1 => round_trip::<StatusRecord>(bytes),
        2 => round_trip::<Closed>(bytes),
        _ => {
            if let Ok(text) = std::str::from_utf8(bytes)
                && let Ok(path) = AgentPath::try_from(text.to_owned())
            {
                assert_eq!(path.as_str(), text);
                if let Some(parent) = path.parent() {
                    assert_eq!(parent.child(&path.id()).ok().as_ref(), Some(&path));
                    assert_eq!(parent.depth() + 1, path.depth());
                }
            }
            Ok(())
        }
    };
    assert!(
        result.is_ok(),
        "accepted record must round-trip: {result:?}"
    );
});
