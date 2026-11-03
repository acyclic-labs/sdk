//! Exercise the original observation and assignment admission boundaries.
#![no_main]

use acyclic_harness_core::experiments::{Observation, plan::PairAssignment};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|bytes: &[u8]| {
    if let Ok(row) = Observation::decode(bytes) {
        let canonical = serde_json::to_vec(&row).expect("admitted observation encodes");
        let restored = Observation::decode(&canonical).expect("canonical observation admits");
        assert_eq!(
            serde_json::to_vec(&restored).expect("restored observation encodes"),
            canonical
        );
        assert!(restored.difference(&row).is_err(), "one arm is not a pair");
        // A streaming decoder may advance to another row; the single-object
        // entrance must refuse the same surplus instead of ignoring it.
        let mut surplus = canonical;
        surplus.extend_from_slice(b" null");
        assert!(Observation::decode(&surplus).is_err());
    }
    if let Ok(assignment) = serde_json::from_slice::<PairAssignment>(bytes) {
        let canonical = serde_json::to_string(&assignment).expect("admitted assignment encodes");
        assert_eq!(
            serde_json::from_str::<PairAssignment>(&canonical)
                .expect("canonical assignment admits"),
            assignment
        );
        let duplicate = format!("{{\"version\":1,{}", &canonical[1..]);
        assert!(serde_json::from_str::<PairAssignment>(&duplicate).is_err());
    }
});
