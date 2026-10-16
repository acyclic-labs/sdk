use acyclic_auth::{contract::*, crypto::block_transcript, *};
use ed25519_dalek::{Signer, SigningKey};
use prost::Message;

#[path = "mod.rs"]
mod claims;

#[path = "trust.rs"]
mod trust;
pub use trust::*;

pub fn limits(value: Option<u64>) -> Limits {
    claims::limits(value)
}
pub const TIP: [u8; 32] = [4; 32];

fn sign_claims(domain: &[u8], claims: &impl Message, signer: &SigningKey) -> Vec<u8> {
    let mut message = domain.to_vec();
    claims.encode(&mut message).unwrap();
    signer.sign(&message).to_bytes().to_vec()
}

// The fixture plays the runtime custodian. No root mint API exists in the crate.
pub fn root(mut change: impl FnMut(&mut Block)) -> Vec<u8> {
    let mut block = claims::root();
    let certificates = block.certificates.as_mut().unwrap();
    certificates.birth.birth.genesis_public_key = key(2).verifying_key().to_bytes().to_vec();
    certificates.keys[0].key.public_key = key(3).verifying_key().to_bytes().to_vec();
    block.next_public_key = key(4).verifying_key().to_bytes().to_vec();
    change(&mut block);
    let certificates = block.certificates.as_mut().unwrap();
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

pub fn request<'a>(
    resource: &'a str,
    attributes: &'a RecordAttributes,
    usage: &'a Limits,
) -> authority::Request<'a> {
    authority::Request {
        target: authority::Target::Resource {
            action: "fs/read",
            resource,
        },
        audience: "workers:/test",
        holder: Some("account"),
        now_unix_seconds: NOW,
        keys: &[],
        attributes,
        usage,
        cpu_millis: 0,
        memory_bytes: 0,
    }
}
