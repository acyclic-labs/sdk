#![no_main]

use acyclic_actors::guest::{
    AdmittedCaller, CallerCapability, Checkpoint, GuestEvent, GuestEventResult, RuntimeHandle,
};
use libfuzzer_sys::fuzz_target;

fn valid_identity(value: &str) -> bool {
    !value.is_empty() && value.trim() == value && !value.chars().any(char::is_control)
}

fuzz_target!(|input: &[u8]| {
    if let Ok(result) = serde_json::from_slice::<GuestEventResult>(input) {
        let encoded = serde_json::to_vec(&result).expect("event result must encode");
        assert_eq!(
            serde_json::from_slice::<GuestEventResult>(&encoded).expect("result round trip"),
            result
        );
    }
    if let Ok(caller) = serde_json::from_slice::<AdmittedCaller>(input) {
        if let Some(capability) = &caller.capability {
            let valid = capability.version != 0 && valid_identity(&capability.handle);
            assert_eq!(
                CallerCapability::new(capability.version, capability.handle.clone()).is_ok(),
                valid,
                "caller capability admission"
            );
            assert_eq!(
                RuntimeHandle::new(capability.version, capability.handle.clone()).is_ok(),
                valid,
                "runtime handle admission"
            );
        }
        if let Ok(expiry) = caller.expires_at_ms.parse() {
            let expected = expiry != 0
                && [
                    &caller.account_id,
                    &caller.org_id,
                    &caller.principal,
                    &caller.credential_id,
                    &caller.actor_id,
                ]
                .into_iter()
                .all(|value| valid_identity(value));
            let constructed = AdmittedCaller::new(
                caller.account_id.clone(),
                caller.org_id.clone(),
                caller.principal.clone(),
                caller.credential_id.clone(),
                caller.actor_id.clone(),
                expiry,
            );
            assert_eq!(
                constructed.is_ok(),
                expected,
                "admitted caller construction"
            );
            if let Ok(constructed) = constructed {
                assert_eq!(constructed.principal, caller.principal);
                assert_eq!(constructed.expires_at_ms, expiry.to_string());
            }
        }
        let encoded = serde_json::to_vec(&caller).expect("caller projection must encode");
        assert_eq!(
            serde_json::from_slice::<AdmittedCaller>(&encoded).expect("caller round trip"),
            caller
        );
    }
    if let Ok(event) = serde_json::from_slice::<GuestEvent>(input) {
        let encoded = serde_json::to_vec(&event).expect("accepted event must encode");
        assert_eq!(
            serde_json::from_slice::<GuestEvent>(&encoded).expect("event round trip"),
            event
        );
    }
    if let Ok(checkpoint) = serde_json::from_slice::<Checkpoint>(input) {
        let encoded = serde_json::to_vec(&checkpoint).expect("accepted checkpoint must encode");
        assert_eq!(
            serde_json::from_slice::<Checkpoint>(&encoded).expect("checkpoint round trip"),
            checkpoint
        );
    }
});
