//! Public trust snapshots shared by native and WebAssembly capability holders.
use crate::{crypto::BlockId, verify::*};
use std::collections::HashSet;
mod follower;
mod issuer;
pub use follower::{
    Applied, EnvironmentFrontier, FenceCause, Follower, FollowerState, account_path,
    environment_path,
};
pub use issuer::{AccountRoot, Certification, Issuer, KeyringFrontier, verify_possession};

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
    fn environment_keys(&self, environment: &str) -> Result<Keys<'_>, VerifyError> {
        if environment == self.environment && !self.environment_keys.is_empty() {
            Ok((&self.environment_keys).into())
        } else {
            Err(VerifyError::Trust)
        }
    }

    fn account(&self, account: &str) -> Result<AccountState<'_>, VerifyError> {
        if account == self.account && !self.issuer_keys.is_empty() {
            Ok(AccountState {
                issuer_keys: (&self.issuer_keys).into(),
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
    /// Synchronously observe producer loss without registering or consuming an
    /// event. Retaining historical public trust must not fabricate a live owner.
    fn check_open(&self) -> Result<(), SourceClosed>;
    /// Register the next real owner publication or producer loss.
    fn changed(&self) -> Changed;
    /// Register publications affecting this exact account, environment changes,
    /// protected time advancement, or producer loss. Owners without keyed
    /// publications retain the complete source event. Registration never admits
    /// or starts following an account; an unavailable account may refuse with
    /// `SourceClosed`. The caller must recheck authority after registering.
    fn changed_for(&self, _account: &str) -> Changed {
        self.changed()
    }
}
/// Same trust publication contract for a browser owner.
#[cfg(target_arch = "wasm32")]
pub trait ChangeSource {
    /// Same non-dispatching producer observation as the native owner.
    fn check_open(&self) -> Result<(), SourceClosed>;
    /// Register the next real owner publication or producer loss.
    fn changed(&self) -> Changed;
    /// Register publications affecting this exact account, environment changes,
    /// protected time advancement, or producer loss. Owners without keyed
    /// publications retain the complete source event. Registration never admits
    /// or starts following an account; an unavailable account may refuse with
    /// `SourceClosed`. The caller must recheck authority after registering.
    fn changed_for(&self, _account: &str) -> Changed {
        self.changed()
    }
}

/// Trust that announces its own publications: one owner answers both the
/// current authority and when it changed, so a consumer can never pair a
/// trust value with another owner's notifications. [`Follower`] is one.
pub trait Live: Trust + ChangeSource {}
impl<T: Trust + ChangeSource + ?Sized> Live for T {}
