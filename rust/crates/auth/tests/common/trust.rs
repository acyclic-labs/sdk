use acyclic_auth::{crypto::BlockId, verify::*};
use ed25519_dalek::SigningKey;

pub const NOW: u64 = 100;

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
        if self.available && environment == "test" {
            Ok(&self.environments)
        } else {
            Err(VerifyError::Trust)
        }
    }
    fn account(&self, account: &str) -> Result<AccountState<'_>, VerifyError> {
        if self.available && account == "account" {
            Ok(AccountState {
                issuer_keys: &self.issuers,
                minimum_key_epoch: self.minimum_epoch,
            })
        } else {
            Err(VerifyError::Trust)
        }
    }
    fn revoked(&self, account: &str, check: RevocationCheck<'_>) -> Result<bool, VerifyError> {
        if !self.available || account != "account" {
            return Err(VerifyError::Trust);
        }
        Ok(match check {
            RevocationCheck::Key { id, .. } => self.revoked_keys.iter().any(|key| key == id),
            RevocationCheck::Block(id) => self.revoked_blocks.contains(id),
        })
    }
}
pub fn key(byte: u8) -> SigningKey {
    SigningKey::from_bytes(&[byte; 32])
}
