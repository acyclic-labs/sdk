use acyclic_auth::{contract::*, crypto::block_transcript, *};
use ed25519_dalek::{Signer, SigningKey};
use prost::Message;

#[path = "mod.rs"]
pub mod claims;

#[path = "trust.rs"]
mod trust;
pub use trust::*;

pub const TIP: [u8; 32] = [4; 32];

fn sign_claims(domain: &[u8], claims: &impl Message, signer: &SigningKey) -> Vec<u8> {
    let mut message = domain.to_vec();
    message.extend_from_slice(&claims.encode_to_vec());
    signer.sign(&message).to_bytes().to_vec()
}

// The fixture plays the runtime custodian. No root mint API exists in the crate.
pub fn root(mut change: impl FnMut(&mut Block)) -> Vec<u8> {
    let public = |byte| key(byte).verifying_key().to_bytes().to_vec();
    let mut block = claims::root();
    // The claims fixture's chain holds exactly one key, the root block signer.
    if let Some(certificates) = block.certificates.as_mut() {
        certificates.birth.birth.genesis_public_key = public(2);
        for certificate in &mut certificates.keys {
            certificate.key.public_key = public(3);
        }
    }
    block.next_public_key = public(4);
    change(&mut block);
    // A change may remove the chain; only a present chain has claims to sign.
    if let Some(certificates) = block.certificates.as_mut() {
        certificates.birth.signature = sign_claims(
            BIRTH_DOMAIN,
            &BirthProto::from(certificates.birth.birth.clone()),
            &key(1),
        );
        for certificate in &mut certificates.keys {
            certificate.signature = sign_claims(
                KEY_DOMAIN,
                &CertifiedKeyProto::from(certificate.key.clone()),
                &key(2),
            );
        }
    }
    let payload = BlockProto::from(block).encode_to_vec();
    let signature = key(3)
        .sign(&block_transcript(0, &[0; 32], &payload))
        .to_bytes()
        .to_vec();
    CapabilityProto {
        blocks: vec![SignedBlockProto { payload, signature }],
        proof: Some(TipProofProto::Secret(TIP.to_vec())),
        version: VERSION,
    }
    .encode_to_vec()
}
