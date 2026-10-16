use acyclic_auth::{crypto::BlockId, verify::*};
use std::collections::HashSet;

/// An authenticated owner-supplied trust snapshot, never derived from a token.
pub struct Snapshot {
    pub environment: String,
    pub environment_keys: Vec<BlockId>,
    pub account: String,
    pub issuer_keys: Vec<BlockId>,
    pub minimum_key_epoch: u64,
    pub revoked_key_ids: HashSet<String>,
    pub revoked_block_ids: HashSet<BlockId>,
}

/// Decode concatenated fixed-width public keys or revocation identities.
pub fn block_ids(bytes: &[u8]) -> Result<Vec<BlockId>, VerifyError> {
    bytes
        .chunks(32)
        .map(|chunk| chunk.try_into().map_err(|_| VerifyError::Malformed))
        .collect()
}

impl Trust for Snapshot {
    fn environment_keys(&self, environment: &str) -> Result<&[BlockId], VerifyError> {
        if environment == self.environment && !self.environment_keys.is_empty() {
            Ok(&self.environment_keys)
        } else {
            Err(VerifyError::Trust)
        }
    }

    fn account(&self, account: &str) -> Result<AccountState<'_>, VerifyError> {
        if account == self.account && !self.issuer_keys.is_empty() {
            Ok(AccountState {
                issuer_keys: &self.issuer_keys,
                minimum_key_epoch: self.minimum_key_epoch,
            })
        } else {
            Err(VerifyError::Trust)
        }
    }

    fn revoked(&self, account: &str, check: RevocationCheck<'_>) -> Result<bool, VerifyError> {
        self.account(account)?;
        Ok(match check {
            RevocationCheck::Key { id, .. } => self.revoked_key_ids.contains(id),
            RevocationCheck::Block(id) => self.revoked_block_ids.contains(id),
        })
    }
}
