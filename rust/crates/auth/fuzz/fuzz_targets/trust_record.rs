#![no_main]
//! Arbitrary record sequences never panic, never roll back a served floor,
//! and replay agrees with following: custody restored from the accepted
//! records holds the follower's frontier.
use acyclic_auth::{
    contract::{TrustRecord, TrustRecordKind, TrustRecordProto},
    trust::{Applied, Follower, Issuer, KeyringFrontier},
    verify::Trust,
};
use ed25519_dalek::SigningKey;
use libfuzzer_sys::fuzz_target;
use prost::Message as _;

#[path = "../../tests/common/reanchor.rs"]
mod reanchor_check;

fuzz_target!(|bytes: &[u8]| {
    reanchor_check::check(bytes).unwrap();
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
    let mut environment_changed = false;
    let mut served_floor = 1;
    // A signed wire record may itself contain the sequence separator.
    for (index, record) in std::iter::once(bytes)
        .chain(bytes.split(|byte| *byte == 0xA5))
        .enumerate()
    {
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
            let applied = follower.apply(record, 100);
            if applied == Ok(Applied::Changed) {
                // The canonical decoder selects the original scoped duplicate
                // check. Probing the wrong scope would fence this live follower
                // and silently stop the subsequent served-floor assertions.
                let decoded =
                    TrustRecord::try_from(TrustRecordProto::decode(record).unwrap()).unwrap();
                match decoded.record {
                    TrustRecordKind::Environment(_) => {
                        assert_eq!(
                            follower.apply_environment(record, 100),
                            Ok(Applied::Duplicate)
                        );
                        environment_changed = true;
                    }
                    _ => {
                        assert_eq!(
                            follower.apply_account("account", record, 100),
                            Ok(Applied::Duplicate)
                        );
                        accepted.push(record.to_vec());
                    }
                }
            }
            // No record sequence lowers a served epoch floor.
            if let Ok(after) = follower.account("account") {
                assert!(after.minimum_key_epoch >= served_floor);
                served_floor = after.minimum_key_epoch;
            }
        }
    }
    if !environment_changed {
        let records = accepted.iter().map(Vec::as_slice).collect::<Vec<_>>();
        let replayed = KeyringFrontier::resume("account", &anchor, &records)
            .expect("records a follower accepted replay");
        assert!(replayed.minimum_key_epoch() >= served_floor);
        if let Ok(current) = follower.account("account") {
            assert_eq!(replayed.minimum_key_epoch(), current.minimum_key_epoch);
        }
    }
});
