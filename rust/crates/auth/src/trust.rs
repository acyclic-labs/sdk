//! Public trust snapshots shared by native and WebAssembly capability holders.
use crate::{crypto::BlockId, verify::*};
use std::collections::HashSet;

/// An authenticated owner-supplied trust snapshot, never derived from a token.
#[derive(Clone, PartialEq, Eq)]
pub struct Snapshot {
    /// Environment whose public issuer keys were authenticated by the owner.
    pub environment: String,
    /// Current and historical environment issuers.
    pub environment_keys: Vec<BlockId>,
    /// Account whose keyring supplied the remaining fields.
    pub account: String,
    /// Trusted account root issuers, never online leaves.
    pub issuer_keys: Vec<BlockId>,
    /// Monotone floor from the current keyring.
    pub minimum_key_epoch: u64,
    /// Cumulative revoked online key identities.
    pub revoked_key_ids: HashSet<String>,
    /// Cumulative unexpired ancestor block revocations.
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
