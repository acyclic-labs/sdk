//! Generated synthetic signing material only; no captured credentials.
use acyclic_auth::{contract::*, revocation::*, trust::Snapshot, verify::*};
use ed25519_dalek::{Signer, SigningKey};
#[path = "common/signed.rs"]
mod capability;

fn signed(publication: RevocationPublication) -> SignedRevocation {
    let signature = SigningKey::from_bytes(&[19; 32])
        .sign(&transcript(&publication))
        .to_bytes()
        .to_vec();
    SignedRevocation {
        publication,
        signature,
    }
}

fn checkpoint(sequence: u64, retired: u64, keys: Vec<KeyRevocation>) -> SignedRevocation {
    signed(RevocationPublication::Checkpoint(RevocationCheckpoint {
        account_id: "account".into(),
        sequence,
        retired_before_unix_seconds: retired,
        minimum_key_epoch: 0,
        keys,
        blocks: vec![],
    }))
}

fn key(id: &str, expiry: u64) -> KeyRevocation {
    KeyRevocation {
        key_id: id.into(),
        expires_at_unix_seconds: expiry,
    }
}

fn issuer() -> [u8; 32] {
    SigningKey::from_bytes(&[19; 32]).verifying_key().to_bytes()
}

fn owner() -> Snapshot {
    Snapshot {
        environment: "test".into(),
        environment_keys: vec![issuer()],
        account: "account".into(),
        issuer_keys: vec![issuer()],
        minimum_key_epoch: 0,
        revoked_key_ids: Default::default(),
        revoked_block_ids: Default::default(),
    }
}

fn delta(
    state: &RevocationState,
    keys: Vec<KeyRevocation>,
    epoch: Option<u64>,
) -> SignedRevocation {
    let (sequence, previous) = state.frontier();
    signed(RevocationPublication::Delta(RevocationDelta {
        account_id: "account".into(),
        sequence: sequence + 1,
        previous_digest: previous.to_vec(),
        issued_at_unix_seconds: 10,
        keys,
        blocks: vec![],
        revoke_all_before_epoch: epoch,
    }))
}

#[test]
fn targeted_growth_never_revokes_unrelated_keys_and_revoke_all_is_explicit() {
    let mut state =
        RevocationState::from_checkpoint(&checkpoint(1, 10, vec![]), "account", &issuer(), 10)
            .unwrap();
    // Cross the old downstream 4096-entry rollover threshold. Retained targets
    // grow while each incremental proof still carries just one changed identity.
    for n in 0..4100 {
        let proof = delta(&state, vec![key(&format!("target-{n}"), 100)], None);
        assert_eq!(
            state.apply(&proof, &issuer(), 10).unwrap(),
            RevocationEffect::Targeted
        );
    }
    let owner = owner();
    let trust = state.trust(&owner, 10).unwrap();
    for n in [0, 4096, 4099] {
        assert!(
            trust
                .revoked(
                    "account",
                    RevocationCheck::Key {
                        id: &format!("target-{n}"),
                        epoch: 0
                    }
                )
                .unwrap()
        );
    }
    assert!(
        !trust
            .revoked(
                "account",
                RevocationCheck::Key {
                    id: "unrelated",
                    epoch: 0
                }
            )
            .unwrap()
    );
    let proof = delta(&state, vec![], Some(1));
    assert_eq!(
        state.apply(&proof, &issuer(), 10).unwrap(),
        RevocationEffect::RevokeAll
    );
    let trust = state.trust(&owner, 10).unwrap();
    assert!(
        trust
            .revoked(
                "account",
                RevocationCheck::Key {
                    id: "unrelated",
                    epoch: 0
                }
            )
            .unwrap()
    );
    assert!(
        !trust
            .revoked(
                "account",
                RevocationCheck::Key {
                    id: "unrelated",
                    epoch: 1
                }
            )
            .unwrap()
    );
    let before = state.frontier();
    // Even after all targets expire, a checkpoint cannot remove revoke-all.
    assert!(
        state
            .apply(&checkpoint(before.0 + 1, 100, vec![]), &issuer(), 100)
            .is_err()
    );
    assert_eq!(state.frontier(), before);
}

#[test]
fn checkpoints_cannot_resurrect_revocations_or_roll_back_epoch_or_time() {
    let initial = checkpoint(1, 10, vec![]);
    let mut state = RevocationState::from_checkpoint(&initial, "account", &issuer(), 10).unwrap();
    let proof = delta(&state, vec![key("target", 100)], None);
    state.apply(&proof, &issuer(), 10).unwrap();
    let before = state.frontier();
    for bad in [
        initial,
        checkpoint(3, 20, vec![]),
        checkpoint(3, 20, vec![key("target", 99)]),
    ] {
        assert!(state.apply(&bad, &issuer(), 100).is_err());
        assert_eq!(state.frontier(), before);
    }
    state
        .apply(&checkpoint(3, 100, vec![]), &issuer(), 100)
        .unwrap();
    assert!(matches!(state.trust(&owner(), 99), Err(VerifyError::Time)));
    let before = state.frontier();
    assert!(state.apply(&proof, &issuer(), 100).is_err());
    assert_eq!(state.frontier(), before);

    // Exercise actual capability verification after retirement, including a
    // caller holding a fresh trust view but asking to verify at an old time.
    let owner = capability::Snapshot::default();
    let bytes = capability::root(|block| {
        block.certificates.as_mut().unwrap().keys[0]
            .key
            .validity
            .expires_at_unix_seconds = 100;
    });
    let verified = verify(&bytes, &owner, 99).unwrap();
    let mut state = RevocationState::from_checkpoint(
        &checkpoint(1, 10, vec![key("key", 100)]),
        "account",
        &issuer(),
        10,
    )
    .unwrap();
    assert!(matches!(
        verify(&bytes, &state.trust(&owner, 99).unwrap(), 99),
        Err(VerifyError::Revoked)
    ));
    state
        .apply(&checkpoint(2, 100, vec![]), &issuer(), 100)
        .unwrap();
    assert!(matches!(
        verify(&bytes, &state.trust(&owner, 100).unwrap(), 99),
        Err(VerifyError::Time)
    ));
    let attributes = RecordAttributes::default();
    let usage = capability::limits(None);
    let mut request = capability::request("fs:/public", &attributes, &usage);
    request.now_unix_seconds = 99;
    assert!(matches!(
        verified.authorize(
            &state.trust(&owner, 100).unwrap(),
            &request,
            acyclic_auth::authority::PolicyDecision::Allow,
            acyclic_auth::authority::Scope::Exact
        ),
        Err(VerifyError::Time)
    ));
}

#[test]
fn targeted_publication_does_not_retire_unrelated_historical_admissions() {
    let owner = capability::Snapshot::default();
    let bytes = capability::root(|_| {});
    let verified = verify(&bytes, &owner, 5).unwrap();
    let mut state =
        RevocationState::from_checkpoint(&checkpoint(1, 1, vec![]), "account", &issuer(), 1)
            .unwrap();
    state
        .apply(
            &delta(&state, vec![key("unrelated", 100)], None),
            &issuer(),
            10,
        )
        .unwrap();
    let trust = state.trust(&owner, 10).unwrap();
    assert!(verify(&bytes, &trust, 5).is_ok());
    let attributes = RecordAttributes::default();
    let usage = capability::limits(None);
    let mut request = capability::request("fs:/public", &attributes, &usage);
    request.now_unix_seconds = 5;
    assert!(
        verified
            .authorize(
                &trust,
                &request,
                acyclic_auth::authority::PolicyDecision::Allow,
                acyclic_auth::authority::Scope::Exact,
            )
            .is_ok()
    );

    // The same historical request still observes current targeted revocation.
    state
        .apply(&delta(&state, vec![key("key", 100)], None), &issuer(), 10)
        .unwrap();
    assert!(matches!(
        verify(&bytes, &state.trust(&owner, 10).unwrap(), 5),
        Err(VerifyError::Revoked)
    ));
}

#[test]
fn malformed_forged_gapped_and_wrong_account_updates_are_atomic() {
    let mut state =
        RevocationState::from_checkpoint(&checkpoint(1, 10, vec![]), "account", &issuer(), 10)
            .unwrap();
    let before = state.frontier();
    let base = match delta(&state, vec![key("target", 100)], None).publication {
        RevocationPublication::Delta(delta) => delta,
        _ => unreachable!(),
    };
    let mut cases = Vec::new();
    let mut gap = base.clone();
    gap.sequence += 1;
    cases.push(signed(RevocationPublication::Delta(gap)));
    let mut account = base.clone();
    account.account_id = "other".into();
    cases.push(signed(RevocationPublication::Delta(account)));
    let mut malformed = base.clone();
    malformed.blocks.push(Revocation {
        id: vec![],
        expires_at_unix_seconds: 100,
        reason: String::new(),
        label: None,
    });
    cases.push(signed(RevocationPublication::Delta(malformed)));
    let mut forged = signed(RevocationPublication::Delta(base));
    forged.signature.fill(0);
    cases.push(forged);
    for case in cases {
        assert!(state.apply(&case, &issuer(), 10).is_err());
        assert_eq!(state.frontier(), before);
        assert!(
            !state
                .trust(&owner(), 10)
                .unwrap()
                .revoked(
                    "account",
                    RevocationCheck::Key {
                        id: "target",
                        epoch: 0
                    }
                )
                .unwrap()
        );
    }
}
