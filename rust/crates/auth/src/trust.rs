//! Public trust snapshots shared by native and WebAssembly capability holders.
use crate::{crypto::BlockId, verify::*};
use std::collections::HashSet;

/// An authenticated owner-supplied trust snapshot, never derived from a token.
#[derive(Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
                not_before_unix_seconds: 0,
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

/// Loss of the authenticated trust publication owner. Consumers must fence
/// active authority; source loss never means unchanged trust.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("authenticated trust change source closed")]
pub struct SourceClosed;

/// One registered publication event, with the same executor ownership as Auth.
#[cfg(not(target_arch = "wasm32"))]
pub type Changed = futures::future::BoxFuture<'static, Result<(), SourceClosed>>;
/// A browser publication event stays on its original event loop.
#[cfg(target_arch = "wasm32")]
pub type Changed = futures::future::LocalBoxFuture<'static, Result<(), SourceClosed>>;

/// Authenticated owner's trust publications, shared by native and hosted Auth.
/// Register synchronously when `changed` is called, before checking current
/// authority. Resolve after a publication is visible to the owner's validity
/// guard. Coalescing is allowed: consumers recheck exact authority after a wake.
/// Losing the producer must resolve `SourceClosed`, including while idle.
/// Never supply a detached local notifier or periodically probe current trust.
#[cfg(not(target_arch = "wasm32"))]
pub trait ChangeSource: Send + Sync {
    /// Register the next real owner publication or producer loss.
    fn changed(&self) -> Changed;
}
/// Same trust publication contract for a browser owner.
#[cfg(target_arch = "wasm32")]
pub trait ChangeSource {
    /// Register the next real owner publication or producer loss.
    fn changed(&self) -> Changed;
}
