#![no_main]

pub use acyclic_fuzz_support as fuzz_support;

#[path = "../../harness/tests/e2e/swarm_case.rs"]
mod swarm_case;

fuzz_support::fuzz_target!(|bytes: &[u8], fuzz_input| {
    let Some(case) =
        fuzz_support::FuzzInput::parse(fuzz_input, serde_json::from_slice::<swarm_case::SwarmCase>)
    else {
        return;
    };
    let encoded = serde_json::to_vec(&case).unwrap();
    let _: swarm_case::SwarmCase =
        fuzz_support::round_trip(&case, serde_json::from_slice(&encoded));

    if case.validate().is_ok() {
        for (phase, layout) in case.layouts.iter().enumerate() {
            assert!(layout.selected().all(|id| id < case.agents));
            assert!(layout.selected().count() <= 64);
            for id in layout.selected().chain([case.agents]) {
                assert_eq!(
                    layout.rendered(id),
                    (layout.visible[0]..layout.visible[1]).contains(&id)
                );
            }
            assert!(case.through(phase) >= (phase as u64 + 1) * 2);
        }
    }
});
