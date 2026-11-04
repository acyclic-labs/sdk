//! One differential oracle for the original trust-record corpus and fuzzer.
use acyclic_auth::{
    crypto::BlockId,
    trust::{FenceCause, Follower, FollowerState, Issuer},
    verify::{Trust as _, VerifyError},
};
use ed25519_dalek::SigningKey;
use std::sync::Arc;

/// Re-anchoring a refused record must agree with fresh current-key admission.
pub fn check(bytes: &[u8]) -> Result<(), String> {
    check_publication(bytes, Follower::reanchor)
}

fn check_publication(
    bytes: &[u8],
    publish: impl FnOnce(&Follower, Arc<[BlockId]>) -> Result<(), VerifyError>,
) -> Result<(), String> {
    let keys = Issuer::new("test", SigningKey::from_bytes(&[1; 32])).anchor();
    let placeholder = Issuer::new("test", SigningKey::from_bytes(&[9; 32])).anchor();
    let recovering = Follower::new("test", placeholder);
    if let Err(cause) = recovering.apply_environment(bytes, 100) {
        if *recovering.state().borrow() != FollowerState::Fenced(FenceCause::Refused)
            || recovering.environment_refusal() != Some(cause)
            || recovering.environment_keys("test").err() != Some(VerifyError::Unavailable)
        {
            return Err(
                "refused environment must retain its cause behind an unavailable fence".into(),
            );
        }
        publish(&recovering, keys.clone().into()).map_err(|error| error.to_string())?;
        let fresh = Follower::new("test", keys);
        if recovering.apply_environment(bytes, 100) != fresh.apply_environment(bytes, 100) {
            return Err("reanchored frontier differs from a fresh current-anchor frontier".into());
        }
    } else {
        recovering.resume();
        let accepted = recovering.environment_keys("test").unwrap().to_vec();
        publish(&recovering, keys.into()).map_err(|error| error.to_string())?;
        if *recovering.environment_keys("test").unwrap() != accepted {
            return Err("anchor publication replaced an authenticated signed frontier".into());
        }
    }
    Ok(())
}

#[cfg(test)]
#[test]
fn reanchor_corpus_and_dropped_publication_control() {
    let records: &[&[u8]] = &[
        include_bytes!("../../fuzz/corpus/trust_record/environment-rotation"),
        include_bytes!("../../fuzz/corpus/trust_record/environment-successor"),
        include_bytes!("../../fuzz/corpus/trust_record/foreign-checkpoint"),
        include_bytes!("../../fuzz/corpus/trust_record/foreign-keyring"),
        include_bytes!("../../fuzz/corpus/trust_record/selected-epoch"),
        b"",
    ];
    for record in records {
        check(record).unwrap();
    }
    // The same oracle must reject an input adapter dropping the publication.
    assert!(
        check_publication(records[0], |_, _| Ok(()))
            .unwrap_err()
            .contains("frontier differs")
    );
}
