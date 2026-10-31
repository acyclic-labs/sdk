//! Authenticated publication admission and atomic refusal, beyond wire conversion.
use acyclic_auth::{
    contract::*,
    revocation::{RevocationState, transcript},
    testing::{self, NOW, Snapshot},
    verify::{RevocationCheck, Trust, VerifyError},
};
use ed25519_dalek::Signer;
use prost::Message;
use std::sync::OnceLock;

const MESSAGE: &str = "acyclic.auth.v1.SignedRevocation";
const BLOCK: [u8; 32] = [17; 32];

fn signed(publication: RevocationPublication) -> SignedRevocation {
    let signature = testing::key(19)
        .sign(&transcript(&publication))
        .to_bytes()
        .to_vec();
    SignedRevocation {
        publication,
        signature,
    }
}

fn issuer() -> [u8; 32] {
    testing::key(19).verifying_key().to_bytes()
}

fn initial() -> &'static SignedRevocation {
    static INITIAL: OnceLock<SignedRevocation> = OnceLock::new();
    INITIAL.get_or_init(|| {
        signed(RevocationPublication::Checkpoint(RevocationCheckpoint {
            account_id: "account".into(),
            sequence: 1,
            retired_before_unix_seconds: 0,
            minimum_key_epoch: 1,
            keys: vec![KeyRevocation {
                key_id: "retained".into(),
                expires_at_unix_seconds: NOW + 1,
            }],
            blocks: vec![Revocation {
                id: BLOCK.to_vec(),
                expires_at_unix_seconds: NOW + 1,
            }],
            issued_at_unix_seconds: 10,
        }))
    })
}

fn base() -> RevocationState {
    RevocationState::from_checkpoint(initial(), "account", &issuer(), u64::MAX).unwrap()
}

fn entries(publication: &RevocationPublication) -> (&[KeyRevocation], &[Revocation]) {
    match publication {
        RevocationPublication::Delta(value) => (&value.keys, &value.blocks),
        RevocationPublication::Checkpoint(value) => (&value.keys, &value.blocks),
    }
}

#[derive(Debug, PartialEq)]
struct Observation {
    frontier: (u64, [u8; 32]),
    minimum_epoch: u64,
    not_before: u64,
    keys: Vec<bool>,
    blocks: Vec<bool>,
}

// Observe every candidate target plus retained and unrelated identities. A
// frontier-only assertion would miss a partially mutated revocation index.
fn observe(state: &RevocationState, publication: &RevocationPublication) -> Observation {
    let owner = Snapshot::default();
    let trust = state.trust(&owner, u64::MAX).unwrap();
    let account = trust.account("account").unwrap();
    let (keys, blocks) = entries(publication);
    Observation {
        frontier: state.frontier(),
        minimum_epoch: account.minimum_key_epoch,
        not_before: account.not_before_unix_seconds,
        keys: ["retained", "unrelated"]
            .into_iter()
            .chain(keys.iter().map(|entry| entry.key_id.as_str()))
            .map(|id| {
                trust
                    .revoked(
                        "account",
                        RevocationCheck::Key {
                            id,
                            epoch: u64::MAX,
                        },
                    )
                    .unwrap()
            })
            .collect(),
        blocks: [&BLOCK, &[18; 32]]
            .into_iter()
            .chain(
                blocks
                    .iter()
                    .filter_map(|entry| entry.id.as_slice().try_into().ok()),
            )
            .map(|id| {
                trust
                    .revoked("account", RevocationCheck::Block(id))
                    .unwrap()
            })
            .collect(),
    }
}

fn refuse(
    state: &mut RevocationState,
    candidate: &SignedRevocation,
    anchor: &[u8; 32],
    now: u64,
    expected: VerifyError,
) {
    let before = observe(state, &candidate.publication);
    assert_eq!(state.apply(candidate, anchor, now), Err(expected));
    assert_eq!(observe(state, &candidate.publication), before);
}

fn admission(candidate: &SignedRevocation) {
    let normalized = SignedRevocation::try_from(
        SignedRevocationProto::decode(
            SignedRevocationProto::from(candidate.clone())
                .encode_to_vec()
                .as_slice(),
        )
        .unwrap(),
    )
    .unwrap();
    let mut original = base();
    let mut canonical = base();
    let before = observe(&original, &candidate.publication);
    let result = original.apply(candidate, &issuer(), u64::MAX);
    assert_eq!(canonical.apply(&normalized, &issuer(), u64::MAX), result);
    assert_eq!(
        observe(&original, &candidate.publication),
        observe(&canonical, &candidate.publication)
    );
    if result.is_err() {
        assert_eq!(observe(&original, &candidate.publication), before);
    } else {
        refuse(
            &mut original,
            candidate,
            &issuer(),
            u64::MAX,
            VerifyError::Trust,
        );
    }

    if let Ok(mut checkpoint) =
        RevocationState::from_checkpoint(candidate, "account", &issuer(), u64::MAX)
    {
        let normalized =
            RevocationState::from_checkpoint(&normalized, "account", &issuer(), u64::MAX).unwrap();
        assert_eq!(
            observe(&checkpoint, &candidate.publication),
            observe(&normalized, &candidate.publication)
        );
        refuse(
            &mut checkpoint,
            candidate,
            &issuer(),
            u64::MAX,
            VerifyError::Trust,
        );
        assert!(matches!(
            RevocationState::from_checkpoint(candidate, "foreign", &issuer(), u64::MAX),
            Err(VerifyError::Trust)
        ));
        let RevocationPublication::Checkpoint(value) = &candidate.publication else {
            unreachable!()
        };
        if let Some(before) = value.issued_at_unix_seconds.checked_sub(1) {
            assert!(matches!(
                checkpoint.trust(&Snapshot::default(), before),
                Err(VerifyError::Time)
            ));
        }
    }
}

pub(super) fn check(name: &str, bytes: &[u8]) {
    if name != MESSAGE {
        return;
    }
    let Ok(wire) = SignedRevocationProto::decode(bytes) else {
        return;
    };
    let Ok(original) = SignedRevocation::try_from(wire) else {
        return;
    };
    admission(&original);

    // Test-owned signing reaches state admission after arbitrary byte mutations.
    // Preserve the original publication first, then bind only caller/frontier/
    // clock fields to the actual base; entry and retirement fields stay arbitrary.
    admission(&signed(original.publication.clone()));
    let mut publication = original.publication;
    let (sequence, previous) = base().frontier();
    match &mut publication {
        RevocationPublication::Delta(value) => {
            value.account_id = "account".into();
            value.sequence = sequence + 1;
            value.previous_digest = previous.to_vec();
            value.issued_at_unix_seconds = NOW;
        }
        RevocationPublication::Checkpoint(value) => {
            value.account_id = "account".into();
            value.sequence = sequence + 1;
            value.issued_at_unix_seconds = NOW;
        }
    }
    let candidate = signed(publication);
    admission(&candidate);
    let mut state = base();
    let mut forged = candidate.clone();
    forged.signature[0] ^= 1;
    refuse(
        &mut state,
        &forged,
        &issuer(),
        u64::MAX,
        VerifyError::Signature,
    );
    let foreign = testing::key(20).verifying_key().to_bytes();
    refuse(
        &mut state,
        &candidate,
        &foreign,
        u64::MAX,
        VerifyError::Signature,
    );
    refuse(&mut state, &candidate, &issuer(), 0, VerifyError::Trust);
    let mut foreign = candidate.publication.clone();
    match &mut foreign {
        RevocationPublication::Delta(value) => value.account_id = "foreign".into(),
        RevocationPublication::Checkpoint(value) => value.account_id = "foreign".into(),
    }
    refuse(
        &mut state,
        &signed(foreign),
        &issuer(),
        u64::MAX,
        VerifyError::Trust,
    );
    if let RevocationPublication::Delta(mut gap) = candidate.publication {
        gap.sequence += 1;
        refuse(
            &mut state,
            &signed(RevocationPublication::Delta(gap)),
            &issuer(),
            u64::MAX,
            VerifyError::Trust,
        );
    }
}

pub(super) fn seeds() -> Vec<(&'static str, &'static str, Vec<u8>)> {
    let mut outputs = vec![(
        MESSAGE,
        "revocation-checkpoint",
        SignedRevocationProto::from(initial().clone()).encode_to_vec(),
    )];
    let mut state = base();
    for (name, epoch) in [("revocation-targeted", None), ("revocation-floor", Some(2))] {
        let (sequence, previous) = state.frontier();
        let candidate = signed(RevocationPublication::Delta(RevocationDelta {
            account_id: "account".into(),
            sequence: sequence + 1,
            previous_digest: previous.to_vec(),
            issued_at_unix_seconds: NOW,
            keys: vec![KeyRevocation {
                key_id: "target".into(),
                expires_at_unix_seconds: NOW + 1,
            }],
            blocks: vec![Revocation {
                id: vec![19; 32],
                expires_at_unix_seconds: NOW + 1,
            }],
            revoke_all_before_epoch: epoch,
        }));
        state.apply(&candidate, &issuer(), u64::MAX).unwrap();
        outputs.push((
            MESSAGE,
            name,
            SignedRevocationProto::from(candidate).encode_to_vec(),
        ));
    }
    for (name, retired) in [("revocation-preserved", 0), ("revocation-retired", NOW + 1)] {
        let retain = retired == 0;
        let candidate = signed(RevocationPublication::Checkpoint(RevocationCheckpoint {
            account_id: "account".into(),
            sequence: state.frontier().0 + 1,
            retired_before_unix_seconds: retired,
            minimum_key_epoch: 2,
            keys: if retain {
                vec![
                    KeyRevocation {
                        key_id: "retained".into(),
                        expires_at_unix_seconds: NOW + 1,
                    },
                    KeyRevocation {
                        key_id: "target".into(),
                        expires_at_unix_seconds: NOW + 1,
                    },
                ]
            } else {
                vec![]
            },
            blocks: if retain {
                vec![
                    Revocation {
                        id: BLOCK.to_vec(),
                        expires_at_unix_seconds: NOW + 1,
                    },
                    Revocation {
                        id: vec![19; 32],
                        expires_at_unix_seconds: NOW + 1,
                    },
                ]
            } else {
                vec![]
            },
            issued_at_unix_seconds: NOW + 1,
        }));
        state.apply(&candidate, &issuer(), u64::MAX).unwrap();
        outputs.push((
            MESSAGE,
            name,
            SignedRevocationProto::from(candidate).encode_to_vec(),
        ));
    }
    for (message, _, bytes) in &outputs {
        check(message, bytes);
    }
    outputs
}
