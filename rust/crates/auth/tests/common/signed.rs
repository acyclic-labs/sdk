use acyclic_auth::{contract::*, crypto::*, verify::*, *};
use ed25519_dalek::{Signer, SigningKey};
use prost::Message;

#[path = "mod.rs"]
mod claims;

pub const NOW: u64 = 100;
pub const TIP: [u8; 32] = [4; 32];

pub struct Snapshot {
    pub environments: Vec<BlockId>,
    pub issuers: Vec<BlockId>,
    pub minimum_epoch: u64,
    pub revoked_blocks: Vec<BlockId>,
    pub revoked_keys: Vec<String>,
    pub available: bool,
}
impl Default for Snapshot {
    fn default() -> Self {
        Self {
            environments: vec![key(1).verifying_key().to_bytes()],
            issuers: vec![key(2).verifying_key().to_bytes()],
            minimum_epoch: 1,
            revoked_blocks: vec![],
            revoked_keys: vec![],
            available: true,
        }
    }
}
impl Trust for Snapshot {
    fn environment_keys(&self, environment: &str) -> Result<&[BlockId], VerifyError> {
        if self.available && environment == "test" { Ok(&self.environments) } else { Err(VerifyError::Trust) }
    }
    fn account(&self, account: &str) -> Result<AccountState<'_>, VerifyError> {
        if self.available && account == "account" {
            Ok(AccountState { issuer_keys: &self.issuers, minimum_key_epoch: self.minimum_epoch })
        } else { Err(VerifyError::Trust) }
    }
    fn revoked(&self, account: &str, check: RevocationCheck<'_>) -> Result<bool, VerifyError> {
        if !self.available || account != "account" { return Err(VerifyError::Trust); }
        Ok(match check {
            RevocationCheck::Key { id, .. } => self.revoked_keys.iter().any(|key| key == id),
            RevocationCheck::Block(id) => self.revoked_blocks.contains(id),
        })
    }
}
pub fn key(byte: u8) -> SigningKey { SigningKey::from_bytes(&[byte; 32]) }
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
    certificates.birth.signature = sign_claims(BIRTH_DOMAIN, &BirthProto::from(certificates.birth.birth.clone()), &key(1));
    for certificate in &mut certificates.keys {
        certificate.signature = sign_claims(KEY_DOMAIN, &CertifiedKeyProto::from(certificate.key.clone()), &key(2));
    }
    let payload = BlockProto::from(block).encode_to_vec();
    let signature = key(3).sign(&block_transcript(0, &[0; 32], &payload)).to_bytes().to_vec();
    CapabilityProto { blocks: vec![SignedBlockProto { payload, signature }], proof: Some(TipProofProto::Secret(TIP.to_vec())), version: VERSION }.encode_to_vec()
}

pub fn request<'a>(resource: &'a str, attributes: &'a RecordAttributes, usage: &'a Limits) -> authority::Request<'a> {
    authority::Request {
        target: authority::Target::Resource { action: "fs/read", resource },
        audience: "workers:/test", holder: Some("account"), now_unix_seconds: NOW,
        keys: &[], attributes, usage, cpu_millis: 0, memory_bytes: 0,
    }
}
