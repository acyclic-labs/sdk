//! Cached authentication must retain exact bytes and recheck current authority.
use acyclic_auth::{
    contract::{CapabilityProto, Caveat, CaveatKind, TipProofProto},
    testing::{NOW, Snapshot},
    verify::{CertificateVerifier, VerifyError},
};
use prost::Message;

pub(super) fn check(bytes: &[u8]) {
    let trust = Snapshot::default();
    let mut verifier = CertificateVerifier::default();
    let Ok(original) = verifier.verify(bytes, &trust, NOW) else {
        return;
    };
    assert_eq!(original.encode().as_slice(), bytes);
    assert_eq!(original.encoded_len(), bytes.len());
    let repeated = verifier
        .verify(bytes, &trust, NOW)
        .expect("cached authentication preserves original bytes");
    assert_eq!(repeated.block_ids(), original.block_ids());
    assert_eq!(repeated.is_sealed(), original.is_sealed());

    let wire = CapabilityProto::decode(bytes).expect("authenticated wire envelope");
    let mut forged = wire.clone();
    let selected = bytes.len() % forged.blocks.len();
    let signature = &mut forged.blocks[selected].signature;
    let byte = bytes.len() % signature.len();
    signature[byte] ^= 1;
    assert!(matches!(
        verifier.verify(&forged.encode_canonical(), &trust, NOW),
        Err(VerifyError::Signature)
    ));
    let mut forged = wire.clone();
    let proof = match forged.proof.as_mut().expect("authenticated proof") {
        TipProofProto::Secret(secret) => secret,
        TipProofProto::Sealed(signature) => signature,
    };
    let byte = bytes.len() % proof.len();
    proof[byte] ^= 1;
    assert!(matches!(
        verifier.verify(&forged.encode_canonical(), &trust, NOW),
        Err(VerifyError::Signature)
    ));
    let mut missing = wire;
    missing.proof = None;
    assert!(matches!(
        verifier.verify(&missing.encode_canonical(), &trust, NOW),
        Err(VerifyError::Malformed)
    ));

    let sealed_bytes = original
        .encode_sealed()
        .expect("verified original exports sealed public evidence");
    let sealed = verifier
        .verify(&sealed_bytes, &trust, NOW)
        .expect("actual sealed writer retains original authentication");
    assert!(sealed.is_sealed());
    assert_eq!(sealed.block_ids(), original.block_ids());
    assert_eq!(sealed.account_id(), original.account_id());
    assert_eq!(
        sealed.encode_sealed().unwrap().as_slice(),
        sealed_bytes.as_slice()
    );
    assert!(matches!(
        sealed.attenuate(Vec::new(), &[7; 32]),
        Err(VerifyError::Sealed)
    ));

    for field in 0..4 {
        let mut foreign = Snapshot::default();
        match field {
            0 => foreign.available = false,
            1 => foreign.environments.clear(),
            2 => foreign.issuers.clear(),
            3 => foreign.revoked_blocks.push(original.block_ids()[selected]),
            _ => unreachable!(),
        }
        let expected = if field == 3 {
            VerifyError::Revoked
        } else {
            VerifyError::Trust
        };
        assert!(matches!(
            verifier.verify(bytes, &foreign, NOW), Err(found) if found == expected
        ));
    }
    assert!(matches!(
        verifier.verify(bytes, &trust, u64::MAX),
        Err(VerifyError::Time)
    ));
    assert_eq!(
        verifier
            .verify(bytes, &trust, NOW)
            .expect("refusal cannot poison original authentication")
            .encode()
            .as_slice(),
        bytes,
    );
}

/// Capture actual sealing/attenuation writer output from deterministic SDK keys.
pub fn seeds(root: &[u8]) -> Result<Vec<(&'static str, Vec<u8>)>, VerifyError> {
    let trust = Snapshot::default();
    let mut verifier = CertificateVerifier::default();
    let root = verifier.verify(root, &trust, NOW)?;
    let child = root.attenuate(
        vec![Caveat {
            kind: CaveatKind::ExpiresAtUnixSeconds(300),
        }],
        &[7; 32],
    )?;
    let seeds = vec![
        ("sealed-capability", root.encode_sealed()?.to_vec()),
        ("attenuated-capability", child.encode().to_vec()),
        (
            "sealed-attenuated-capability",
            child.encode_sealed()?.to_vec(),
        ),
    ];
    for (_, bytes) in &seeds {
        verifier.verify(bytes, &trust, NOW)?;
        check(bytes);
    }
    Ok(seeds)
}
