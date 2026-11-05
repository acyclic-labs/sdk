//! Shared production trust-record oracle for corpus replay and fuzzing.
use acyclic_auth::{
    contract::{RevocationPublication, TrustRecord, TrustRecordKind, TrustRecordProto},
    crypto::BlockId,
    trust::{AnchorKeys, Applied, FenceCause, Follower, FollowerState, Issuer, KeyringFrontier},
    verify::{Trust as _, VerifyError},
};
use ed25519_dalek::SigningKey;
use prost::Message as _;
use std::sync::Arc;

/// Re-anchoring a refused record must agree with fresh current-key admission.
fn check_reanchor(bytes: &[u8]) -> Result<(), String> {
    check_publication(bytes, |owner, keys| {
        owner.send_replace(AnchorKeys::new(keys.to_vec()));
        Ok(())
    })
}

fn check_publication(
    bytes: &[u8],
    publish: impl FnOnce(
        &tokio::sync::watch::Sender<AnchorKeys>,
        Arc<[BlockId]>,
    ) -> Result<(), VerifyError>,
) -> Result<(), String> {
    let keys = Issuer::new("test", SigningKey::from_bytes(&[1; 32])).anchor();
    let placeholder = Issuer::new("test", SigningKey::from_bytes(&[9; 32])).anchor();
    let (owner, watched) = tokio::sync::watch::channel(AnchorKeys::new(placeholder));
    let recovering = Follower::new("test", watched);
    if let Err(cause) = recovering.apply_environment(bytes, 100) {
        if *recovering.state().borrow() != FollowerState::Fenced(FenceCause::Refused)
            || recovering.environment_refusal() != Some(cause)
            || recovering.environment_keys("test", 100).err() != Some(VerifyError::Unavailable)
        {
            return Err(
                "refused environment must retain its cause behind an unavailable fence".into(),
            );
        }
        publish(&owner, keys.clone().into()).map_err(|error| error.to_string())?;
        let fresh = Follower::new("test", AnchorKeys::fixed(keys));
        if recovering.apply_environment(bytes, 100) != fresh.apply_environment(bytes, 100) {
            return Err("reanchored frontier differs from a fresh current-anchor frontier".into());
        }
    } else {
        recovering.resume();
        let established = recovering
            .environment_initialized()
            .map_err(|error| error.to_string())?;
        let accepted = recovering.environment_keys("test", 100).unwrap().to_vec();
        publish(&owner, keys.into()).map_err(|error| error.to_string())?;
        if established && *recovering.environment_keys("test", 100).unwrap() != accepted {
            return Err("anchor publication replaced an authenticated signed frontier".into());
        }
    }
    Ok(())
}

pub fn check(bytes: &[u8]) {
    let issuer = Issuer::new("test", SigningKey::from_bytes(&[1; 32]));
    let anchor = issuer.anchor();
    let follower = Follower::new("test", AnchorKeys::fixed(anchor.clone()));
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
                // Exercise actual cross-scope admission on an independent
                // follower: its refusal must not fence the live floor oracle.
                let scoped = Follower::new("test", AnchorKeys::fixed(anchor.clone()));
                match &decoded.record {
                    TrustRecordKind::Environment(_) => assert!(
                        scoped.apply_account("account", record, 100).is_err(),
                        "environment record must refuse account scope"
                    ),
                    _ => assert!(
                        scoped.apply_environment(record, 100).is_err(),
                        "account record must refuse environment scope"
                    ),
                }
                let account = match decoded.record {
                    TrustRecordKind::Environment(_) => {
                        assert_eq!(
                            follower.apply_environment(record, 100),
                            Ok(Applied::Duplicate)
                        );
                        environment_changed = true;
                        None
                    }
                    TrustRecordKind::Keyring(signed) => Some(signed.snapshot.account_id),
                    TrustRecordKind::Revocation(signed) => Some(match signed.publication {
                        RevocationPublication::Delta(delta) => delta.account_id,
                        RevocationPublication::Checkpoint(checkpoint) => checkpoint.account_id,
                    }),
                };
                if let Some(account) = account {
                    assert_eq!(
                        follower.apply_account(&account, record, 100),
                        Ok(Applied::Duplicate)
                    );
                    if account == "account" {
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
    check_reanchor(bytes).unwrap();
}

#[cfg(test)]
#[test]
fn trust_record_corpus_and_dropped_publication_control() {
    let records: &[&[u8]] = &[
        include_bytes!("../../fuzz/corpus/trust_record/environment-rotation"),
        include_bytes!("../../fuzz/corpus/trust_record/environment-successor"),
        include_bytes!("../../fuzz/corpus/trust_record/environment-bootstrap"),
        include_bytes!("../../fuzz/corpus/trust_record/foreign-checkpoint"),
        include_bytes!("../../fuzz/corpus/trust_record/foreign-keyring"),
        include_bytes!("../../fuzz/corpus/trust_record/selected-epoch"),
        b"",
    ];
    for record in records {
        check(record);
    }
    // The same oracle must reject an input adapter dropping the publication.
    assert!(
        check_publication(records[0], |_, _| Ok(()))
            .unwrap_err()
            .contains("frontier differs")
    );
}
