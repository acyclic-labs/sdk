#![no_main]

#[path = "../../harness/tests/e2e/swarm_case.rs"]
mod swarm_case;

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    let Ok(case) = serde_json::from_slice::<swarm_case::SwarmCase>(bytes) else {
        return;
    };
    let encoded = serde_json::to_vec(&case).unwrap();
    let decoded: swarm_case::SwarmCase = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(case, decoded);
    if case.validate().is_ok() {
        for (phase, layout) in case.layouts.iter().enumerate() {
            assert!(layout.selected().all(|id| id < case.agents));
            assert!(layout.selected().count() <= 64);
            assert!(case.through(phase) >= (phase as u64 + 1) * 2);
        }
    }
});
