#![no_main]
//! Arbitrary record sequences never panic, never roll back a served floor,
//! and replay agrees with following: custody restored from the accepted
//! records holds the follower's frontier.
use acyclic_auth::{
    trust::{Applied, EnvironmentFrontier, Follower, Issuer, KeyringFrontier},
    verify::Trust,
};
use ed25519_dalek::SigningKey;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|bytes: &[u8]| {
    let issuer = Issuer::new("test", SigningKey::from_bytes(&[1; 32]));
    let anchor = issuer.anchor();
    let follower = Follower::new("test", anchor.clone());
    let (mut root, genesis) = issuer
        .genesis("account", SigningKey::from_bytes(&[2; 32]), 100)
        .unwrap();
    for record in &genesis {
        assert_eq!(follower.apply(record, 100), Ok(Applied::Changed));
    }
    follower.resume();
    follower.serve("account");
    let mut accepted = genesis.clone();
    let mut environment = Vec::new();
    for (index, record) in bytes.split(|byte| *byte == 0xA5).enumerate() {
        // Interleave custody's own records, so replay sees every signer.
        let own = match index % 4 {
            0 => root
                .revoke(
                    &[([u8::try_from(index % 256).unwrap_or_default(); 32], 200)],
                    100,
                )
                .ok(),
            1 => root.revoke_keys(&[("fuzz", 200)], 100).ok(),
            2 => root
                .raise_minimum_epoch(root.minimum_key_epoch() + 1, 100)
                .ok(),
            _ => None,
        };
        for record in own.iter().map(Vec::as_slice).chain([record]) {
            let before = follower
                .account("account")
                .map_or(0, |state| state.minimum_key_epoch);
            let applied = follower.apply(record, 100);
            if applied == Ok(Applied::Changed) {
                if EnvironmentFrontier::resume("test", anchor.clone(), &[record]).is_ok() {
                    environment.push(record.to_vec());
                } else {
                    accepted.push(record.to_vec());
                }
            }
            // No record sequence lowers a served epoch floor.
            if let Ok(after) = follower.account("account") {
                assert!(after.minimum_key_epoch >= before);
            }
        }
    }
    if environment.is_empty() {
        let records = accepted.iter().map(Vec::as_slice).collect::<Vec<_>>();
        let replayed = KeyringFrontier::resume("account", &anchor, &records)
            .expect("records a follower accepted replay");
        assert_eq!(replayed.minimum_key_epoch(), root.minimum_key_epoch());
    }
});
