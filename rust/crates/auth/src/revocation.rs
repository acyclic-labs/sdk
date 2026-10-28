//! Exact account-scoped revocation. Each changed identity costs one index
//! lookup; no delta scans account history. Checkpoints retire only expired authority. Storage and
//! event delivery belong to the owner; gaps fail closed until a signed checkpoint.
use crate::{
    contract::*,
    crypto::{BlockId, public_key, verify_signature},
    verify::{AccountState, RevocationCheck, Trust, VerifyError},
};
use prost::Message;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

/// Domain and version for the canonical SignedRevocation with signature cleared.
pub const REVOCATION_DOMAIN: &[u8] = b"acyclic.auth.v1/revocation\0";

/// Exact immutable publication transcript; also used by remote key custodians.
pub fn transcript(publication: &RevocationPublication) -> Vec<u8> {
    let wire: SignedRevocationProto = SignedRevocation {
        publication: publication.clone(),
        signature: Vec::new(),
    }
    .into();
    let mut bytes = REVOCATION_DOMAIN.to_vec();
    bytes.extend_from_slice(&wire.encode_to_vec());
    bytes
}

/// Accepted account revocation frontier. No implicit limit, eviction or epoch
/// rollover. Owners durably retain this frontier across process restarts.
pub struct RevocationState {
    account: String,
    sequence: u64,
    digest: BlockId,
    published_at: u64,
    retired_before: u64,
    minimum_key_epoch: u64,
    keys: BTreeMap<String, u64>,
    blocks: BTreeMap<BlockId, u64>,
}

/// A committed change for exact dependent-cache invalidation. Checkpoints may
/// add targets, so consumers replace their revocation observation on checkpoints.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RevocationEffect {
    /// Only keys/blocks listed in the accepted delta need invalidation.
    Targeted,
    /// Explicit epoch floor change invalidates certificates below that floor.
    RevokeAll,
    /// Replace the authenticated observation, preserving unexpired revocations.
    Checkpoint,
}

impl RevocationState {
    /// Bootstrap only from an independently authenticated current checkpoint.
    /// On restart use the durably retained checkpoint/frontier, never an older
    /// network snapshot. `now` must come from the trusted clock.
    pub fn from_checkpoint(
        signed: &SignedRevocation,
        account: &str,
        issuer: &BlockId,
        now: u64,
    ) -> Result<Self, VerifyError> {
        authenticate(signed, issuer)?;
        let RevocationPublication::Checkpoint(checkpoint) = &signed.publication else {
            return Err(VerifyError::Malformed);
        };
        let mut state = Self {
            account: account.to_owned(),
            sequence: 0,
            digest: [0; 32],
            published_at: 0,
            retired_before: 0,
            minimum_key_epoch: 0,
            keys: BTreeMap::new(),
            blocks: BTreeMap::new(),
        };
        state.checkpoint(checkpoint, now)?;
        state.digest = digest(signed);
        Ok(state)
    }

    /// Authenticate, validate completely, then publish one atomic in-memory
    /// change. A gap, stale checkpoint, time regression or forged proof changes
    /// nothing. Persist the new state before acknowledging its publication.
    pub fn apply(
        &mut self,
        signed: &SignedRevocation,
        issuer: &BlockId,
        now: u64,
    ) -> Result<RevocationEffect, VerifyError> {
        authenticate(signed, issuer)?;
        let effect = match &signed.publication {
            RevocationPublication::Delta(delta) => self.delta(delta, now)?,
            RevocationPublication::Checkpoint(checkpoint) => {
                self.checkpoint(checkpoint, now)?;
                RevocationEffect::Checkpoint
            }
        };
        self.digest = digest(signed);
        Ok(effect)
    }

    fn delta(
        &mut self,
        delta: &RevocationDelta,
        now: u64,
    ) -> Result<RevocationEffect, VerifyError> {
        if delta.account_id != self.account
            || self.sequence.checked_add(1) != Some(delta.sequence)
            || delta.previous_digest != self.digest
            || delta.issued_at_unix_seconds < self.published_at
            || delta.issued_at_unix_seconds > now
            || delta
                .revoke_all_before_epoch
                .is_some_and(|epoch| epoch <= self.minimum_key_epoch)
        {
            return Err(VerifyError::Trust);
        }
        // Validate the whole delta before touching live state, without cloning
        // the accumulated index. Repeated targets extend retention, never shorten it.
        let (keys, blocks) = entries(&delta.keys, &delta.blocks)?;
        for (key, expiry) in keys {
            self.keys
                .entry(key)
                .and_modify(|old| *old = (*old).max(expiry))
                .or_insert(expiry);
        }
        for (block, expiry) in blocks {
            self.blocks
                .entry(block)
                .and_modify(|old| *old = (*old).max(expiry))
                .or_insert(expiry);
        }
        self.sequence = delta.sequence;
        self.published_at = delta.issued_at_unix_seconds;
        if let Some(epoch) = delta.revoke_all_before_epoch {
            self.minimum_key_epoch = epoch;
            Ok(RevocationEffect::RevokeAll)
        } else {
            Ok(RevocationEffect::Targeted)
        }
    }

    fn checkpoint(
        &mut self,
        checkpoint: &RevocationCheckpoint,
        now: u64,
    ) -> Result<(), VerifyError> {
        let retired = checkpoint.retired_before_unix_seconds;
        let published = checkpoint.issued_at_unix_seconds;
        if checkpoint.account_id != self.account
            || checkpoint.sequence <= self.sequence
            || retired < self.retired_before
            || published < self.published_at
            || published < retired
            || published > now
            || checkpoint.minimum_key_epoch < self.minimum_key_epoch
        {
            return Err(VerifyError::Trust);
        }
        let (keys, blocks) = entries(&checkpoint.keys, &checkpoint.blocks)?;
        if !preserves(&self.keys, &keys, retired) || !preserves(&self.blocks, &blocks, retired) {
            return Err(VerifyError::Revoked);
        }
        self.keys = keys;
        self.blocks = blocks;
        self.minimum_key_epoch = checkpoint.minimum_key_epoch;
        self.sequence = checkpoint.sequence;
        self.published_at = published;
        self.retired_before = retired;
        Ok(())
    }

    /// Last accepted sequence and exact signed-publication digest. Persist with
    /// the checkpoint and replayed deltas; never separately advance this frontier.
    pub fn frontier(&self) -> (u64, BlockId) {
        (self.sequence, self.digest)
    }

    /// Compose with the account owner's existing trust policy. Retired state
    /// cannot be used to authenticate requests at an earlier trusted time.
    pub fn trust<'a, T: Trust>(
        &'a self,
        owner: &'a T,
        now: u64,
    ) -> Result<RevocationTrust<'a, T>, VerifyError> {
        if now < self.published_at {
            return Err(VerifyError::Time);
        }
        Ok(RevocationTrust { state: self, owner })
    }
}

type Keys = BTreeMap<String, u64>;
type Blocks = BTreeMap<BlockId, u64>;

fn entries(keys: &[KeyRevocation], blocks: &[Revocation]) -> Result<(Keys, Blocks), VerifyError> {
    let mut key_index = Keys::new();
    let mut block_index = Blocks::new();
    for key in keys {
        if key.key_id.is_empty()
            || key_index
                .insert(key.key_id.clone(), key.expires_at_unix_seconds)
                .is_some()
        {
            return Err(VerifyError::Malformed);
        }
    }
    for block in blocks {
        let id = block
            .id
            .as_slice()
            .try_into()
            .map_err(|_| VerifyError::Malformed)?;
        if block_index
            .insert(id, block.expires_at_unix_seconds)
            .is_some()
        {
            return Err(VerifyError::Malformed);
        }
    }
    Ok((key_index, block_index))
}

fn preserves<K: Ord>(old: &BTreeMap<K, u64>, new: &BTreeMap<K, u64>, retired: u64) -> bool {
    old.iter()
        .all(|(id, expiry)| *expiry <= retired || new.get(id).is_some_and(|new| new >= expiry))
}

fn authenticate(signed: &SignedRevocation, issuer: &BlockId) -> Result<(), VerifyError> {
    verify_signature(
        &public_key(issuer)?,
        &transcript(&signed.publication),
        &signed.signature,
    )
}

fn digest(signed: &SignedRevocation) -> BlockId {
    Sha256::new()
        .chain_update(transcript(&signed.publication))
        .chain_update(&signed.signature)
        .finalize()
        .into()
}

/// Revocation intersection with the existing owner trust implementation.
pub struct RevocationTrust<'a, T> {
    state: &'a RevocationState,
    owner: &'a T,
}

impl<T: Trust> Trust for RevocationTrust<'_, T> {
    fn environment_keys(&self, environment: &str) -> Result<&[BlockId], VerifyError> {
        self.owner.environment_keys(environment)
    }

    fn account(&self, account: &str) -> Result<AccountState<'_>, VerifyError> {
        if account != self.state.account {
            return Err(VerifyError::Trust);
        }
        let mut state = self.owner.account(account)?;
        state.minimum_key_epoch = state.minimum_key_epoch.max(self.state.minimum_key_epoch);
        state.not_before_unix_seconds =
            state.not_before_unix_seconds.max(self.state.retired_before);
        Ok(state)
    }

    fn revoked(&self, account: &str, check: RevocationCheck<'_>) -> Result<bool, VerifyError> {
        self.account(account)?;
        Ok(match check {
            RevocationCheck::Key { id, epoch } => {
                epoch < self.state.minimum_key_epoch || self.state.keys.contains_key(id)
            }
            RevocationCheck::Block(id) => self.state.blocks.contains_key(id),
        } || self.owner.revoked(account, check)?)
    }
}
