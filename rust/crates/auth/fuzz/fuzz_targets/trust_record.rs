#![no_main]
//! Arbitrary record sequences never panic and never roll back a served floor.
use acyclic_auth::{
    trust::{Applied, Follower, Issuer},
    verify::Trust,
};
use ed25519_dalek::SigningKey;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|bytes: &[u8]| {
    let issuer = Issuer::new("test", SigningKey::from_bytes(&[1; 32]));
    let follower = Follower::new("test", issuer.anchor());
    let (_, genesis) = issuer
        .genesis("account", SigningKey::from_bytes(&[2; 32]), 100)
        .unwrap();
    for record in &genesis {
        assert_eq!(follower.apply(record, 100), Ok(Applied::Changed));
    }
    follower.resume();
    follower.serve("account");
    for record in bytes.split(|byte| *byte == 0xA5) {
        let before = follower
            .account("account")
            .map_or(0, |state| state.minimum_key_epoch);
        let _ = follower.apply(record, 100);
        // No record sequence lowers a served epoch floor.
        if let Ok(after) = follower.account("account") {
            assert!(after.minimum_key_epoch >= before);
        }
    }
});
