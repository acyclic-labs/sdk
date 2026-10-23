use acyclic_auth::{contract::*, crypto::block_transcript, *};
use ed25519_dalek::{Signer, SigningKey};
use prost::Message;

#[path = "mod.rs"]
pub mod claims;

#[path = "trust.rs"]
mod trust;
pub use trust::*;

pub fn limits(value: Option<u64>) -> Limits {
    claims::limits(value)
}
pub const TIP: [u8; 32] = [4; 32];

fn sign_claims(domain: &[u8], claims: &impl Message, signer: &SigningKey) -> Vec<u8> {
    let mut message = domain.to_vec();
    message.extend_from_slice(&claims.encode_to_vec());
    signer.sign(&message).to_bytes().to_vec()
}

// The fixture supplies authenticated certificates for the runtime custodian.
pub fn root(mut change: impl FnMut(&mut Block)) -> Vec<u8> {
    let mut block = claims::root();
    if let Some(certificates) = block.certificates.as_mut() {
        certificates.birth.birth.genesis_public_key = key(2).verifying_key().to_bytes().to_vec();
        if let Some(certificate) = certificates.keys.first_mut() {
            certificate.key.public_key = key(3).verifying_key().to_bytes().to_vec();
        }
    }
    block.next_public_key = key(4).verifying_key().to_bytes().to_vec();
    change(&mut block);
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
