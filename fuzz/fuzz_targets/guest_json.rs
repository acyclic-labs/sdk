#![no_main]

use acyclic_actors::guest::{
    AdmittedCaller, CallerCapability, Checkpoint, GuestEvent, GuestEventResult, RuntimeHandle,
};
use libfuzzer_sys::fuzz_target;

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
            let _ = CallerCapability::new(capability.version, capability.handle.clone());
            let _ = RuntimeHandle::new(capability.version, capability.handle.clone());
        }
        if let Ok(expiry) = caller.expires_at_ms.parse() {
            if let Ok(constructed) = AdmittedCaller::new(
                caller.account_id.clone(),
                caller.org_id.clone(),
                caller.principal.clone(),
                caller.credential_id.clone(),
                caller.actor_id.clone(),
                expiry,
            ) {
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
