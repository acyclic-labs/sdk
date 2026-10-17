use super::*;
use proptest::prelude::*;

proptest! {
    // Property: encoding normalized configuration preserves every field and
    // its admission outcome, including invalid commands and deadline bounds.
    #[test]
    fn normalized_rules_round_trip_without_changing_validation(
        event in 0usize..4,
        executable in ".{0,48}",
        argv in prop::collection::vec(".{0,24}", 0..5),
        shell in prop::option::of(prop_oneof![Just("sh".to_owned()), Just("unknown".to_owned())]),
        timeout in any::<u64>(),
        block in any::<bool>(),
        exec in any::<bool>(),
        matcher in ".{0,24}",
    ) {
        let hook = HookSpec {
            run: if exec {
                HookRun::Executable { executable, argv }
            } else {
                HookRun::Shell { command: executable, shell }
            },
            timeout_ms: timeout,
            failure: if block { HookFailure::Block } else { HookFailure::Report },
        };
        let valid = hook.validate().is_ok();
        let rule = HookRule {
            on: [HookEvent::SessionStart, HookEvent::PreToolUse, HookEvent::PostToolUse, HookEvent::Stop][event],
            matcher,
            hooks: vec![hook],
        };
        let encoded = serde_json::to_vec(&rule).unwrap();
        let decoded: HookRule = serde_json::from_slice(&encoded).unwrap();
        prop_assert_eq!(&rule, &decoded);
        prop_assert_eq!(decoded.hooks[0].validate().is_ok(), valid);
        prop_assert_eq!(serde_json::to_vec(&decoded).unwrap(), encoded);
    }
}
