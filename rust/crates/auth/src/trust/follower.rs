//! The one trust implementation: a follower of signed trust Streams.
//!
//! Layout. One environment Stream, [`environment_path`], carries
//! [`SignedEnvironmentKeyring`] records. Each account has one child Stream,
//! [`account_path`], carrying that account's [`SignedKeyringSnapshot`] and
//! [`SignedRevocation`] records in one total order. Every record value is one
//! canonical [`TrustRecord`].
//!
//! Signers. A pinned anchor key signs environment keyring 1; each later
//! environment keyring is signed by a key of the previous one. Each account
//! keyring names a single `root_public_key`, the account's only issuer root:
//! an environment key may sign any keyring and is the only signer that sets a
//! new root (rotation ends the previous root); the current root may sign one
//! that keeps it, which can only narrow (epoch floor, key and block
//! revocations). The current account root signs that account's revocations.
//! "Everything before an epoch" is the keyring's `minimum_key_epoch` alone:
//! revocation records carrying an epoch floor are refused.
//!
//! Application. Each record is authenticated and gap-checked (sequence plus
//! previous digest), refused if it rolls back an epoch floor, publication time
//! or revocation, and applied under one write lock. Only after the lock is
//! released does [`ChangeSource::changed`] resolve, so every woken consumer
//! reads the new trust. A refused account record fences that account until a
//! record for it applies; a refused environment record or a lost follow fences
//! everything until the driver catches up again ([`Follower::resume`],
//! [`Follower::serve`]). Fenced reads refuse as
//! [`VerifyError::Unavailable`]. Cost: O(1) per account, no task per account,
//! one hash lookup per trust read.
use super::{ChangeSource, Changed, SourceClosed};
use crate::{
    ENVIRONMENT_DOMAIN, KEYRING_DOMAIN,
    contract::*,
    crypto::{BlockId, block_id, claims_message, public_key, verify_signature},
    revocation::RevocationState,
    verify::{AccountState, Keys, RevocationCheck, Trust, VerifyError},
};
use prost::Message;
use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

/// Stream path of an environment's anchor records.
#[must_use]
pub fn environment_path(environment: &str) -> String {
    format!(
        "trust/v1/{}",
        crate::encode_component(environment.as_bytes())
    )
}

/// Stream path of one account's keyring and revocation records, a child of
/// its environment's Stream.
#[must_use]
pub fn account_path(environment: &str, account: &str) -> String {
    format!(
        "{}/accounts/{}",
        environment_path(environment),
        crate::encode_component(account.as_bytes())
    )
}

/// What one accepted record did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Applied {
    /// The record changed published trust.
    Changed,
    /// The record was already applied; redelivery after a lost follow.
    Duplicate,
}

/// One environment's authenticated keyring frontier: its current keys and
/// the (sequence, digest) its next record must extend. Built only from the
/// pinned anchor and by replaying signed records, so it cannot be forged.
#[derive(Clone)]
pub struct EnvironmentFrontier {
    name: String,
    keys: Arc<[BlockId]>,
    sequence: u64,
    digest: BlockId,
    issued_at: u64,
}

impl EnvironmentFrontier {
    /// The frontier before environment keyring 1: only `anchor` signs.
    #[must_use]
    pub fn genesis(environment: impl Into<String>, anchor: Vec<BlockId>) -> Self {
        Self {
            name: environment.into(),
            keys: anchor.into(),
            sequence: 0,
            digest: [0; 32],
            issued_at: 0,
        }
    }

    /// Replays the environment Stream's records from genesis, through the
    /// same validation as [`Follower::apply`].
    ///
    /// # Errors
    /// A gap, a foreign signer, a rollback, or a non-environment record.
    pub fn resume(
        environment: impl Into<String>,
        anchor: Vec<BlockId>,
        records: &[&[u8]],
    ) -> Result<Self, VerifyError> {
        let mut frontier = Self::genesis(environment, anchor);
        for record in records {
            let TrustRecordKind::Environment(signed) = decode(record)?.record else {
                return Err(VerifyError::Trust);
            };
            frontier.apply(&signed, u64::MAX)?;
        }
        Ok(frontier)
    }

    /// The environment name.
    #[must_use]
    pub fn environment(&self) -> &str {
        &self.name
    }

    /// The current environment keys.
    #[must_use]
    pub fn keys(&self) -> &[BlockId] {
        &self.keys
    }

    /// Last applied sequence (0 before keyring 1) and its digest.
    #[must_use]
    pub const fn frontier(&self) -> (u64, BlockId) {
        (self.sequence, self.digest)
    }

    /// Last applied publication time.
    #[must_use]
    pub const fn issued_at(&self) -> u64 {
        self.issued_at
    }

    /// Authenticates and applies the next environment keyring at `now`. A
    /// refusal changes nothing.
    pub(super) fn apply(
        &mut self,
        signed: &SignedEnvironmentKeyring,
        now: u64,
    ) -> Result<(Applied, u64), VerifyError> {
        let next = &signed.keyring;
        if next.environment != self.name {
            return Err(VerifyError::Trust);
        }
        if next.sequence <= self.sequence {
            return Ok((Applied::Duplicate, self.issued_at));
        }
        if self.sequence.checked_add(1) != Some(next.sequence)
            || next.previous_digest != self.digest
            || next.issued_at_unix_seconds < self.issued_at
            || next.keys.is_empty()
        {
            return Err(VerifyError::Trust);
        }
        if next.issued_at_unix_seconds > now {
            return Err(VerifyError::Time);
        }
        let mut message = Vec::new();
        claims_message(
            &mut message,
            ENVIRONMENT_DOMAIN,
            &EnvironmentKeyringProto::from(next.clone()),
        )?;
        signed_by(&self.keys, &message, &signed.signature)?;
        let keys = next
            .keys
            .iter()
            .map(|key| {
                key.as_slice()
                    .try_into()
                    .map_err(|_| VerifyError::Malformed)
            })
            .collect::<Result<Vec<BlockId>, _>>()?;
        *self = Self {
            name: next.environment.clone(),
            keys: keys.into(),
            sequence: next.sequence,
            digest: block_id(&message, &signed.signature),
            issued_at: next.issued_at_unix_seconds,
        };
        Ok((Applied::Changed, next.issued_at_unix_seconds))
    }
}

#[derive(Clone)]
pub(super) struct Keyring {
    pub(super) sequence: u64,
    pub(super) digest: BlockId,
    pub(super) issued_at: u64,
    pub(super) root: Arc<[BlockId]>,
    pub(super) minimum_key_epoch: u64,
    pub(super) revoked_keys: HashSet<String>,
    pub(super) revoked_blocks: HashMap<BlockId, u64>,
}

/// One account's authenticated keyring and revocation frontiers: what a
/// follower serves and what an account root's custody extends.
#[derive(Default)]
pub(super) struct Account {
    pub(super) keyring: Option<Keyring>,
    pub(super) revocations: Option<RevocationState>,
    /// The driver applied this account's Stream through its observed tail.
    serving: bool,
    /// A refused record; cleared only by a record that applies.
    poisoned: bool,
}

impl Account {
    /// Authenticates and applies one record of account `id` at `now`: the one
    /// account validation path. An environment key signs every keyring that
    /// sets a root; the current root alone may sign one that keeps it.
    pub(super) fn apply(
        &mut self,
        id: &str,
        record: &TrustRecordKind,
        environment: &[BlockId],
        now: u64,
    ) -> Result<(Applied, u64), VerifyError> {
        match record {
            TrustRecordKind::Keyring(signed) if signed.snapshot.account_id == id => {
                keyring(self, signed, environment, now)
            }
            TrustRecordKind::Revocation(signed) if publication_account(signed) == id => {
                revocation(self, id, signed, now)
            }
            _ => Err(VerifyError::Trust),
        }
    }
}

pub(super) fn decode(record: &[u8]) -> Result<TrustRecord, VerifyError> {
    TrustRecordProto::decode(record)
        .map_err(|_| VerifyError::Malformed)
        .and_then(|proto| TrustRecord::try_from(proto).map_err(VerifyError::from))
}

pub(super) fn publication_account(signed: &SignedRevocation) -> &str {
    match &signed.publication {
        RevocationPublication::Delta(delta) => &delta.account_id,
        RevocationPublication::Checkpoint(checkpoint) => &checkpoint.account_id,
    }
}

/// Why a follower refuses trust.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FenceCause {
    /// A follow was lost; the driver reconnects and catches up.
    LostFollow,
    /// An environment record was refused; everything stays fenced.
    Refused,
    /// The driver stopped for good.
    Shutdown,
}

/// A follower's serving state. Every trust publication also notifies its
/// watch, so one channel carries both state and change events.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FollowerState {
    /// Catching up to the tails observed at (re)connect; refusing trust.
    Connecting,
    /// Serving the environment's trust.
    Serving,
    /// Refusing trust until the driver reconnects.
    Fenced(FenceCause),
    /// Refusing trust for good: every change wait resolves [`SourceClosed`].
    Closed(FenceCause),
}

/// Follows signed trust records for any number of accounts of one environment.
pub struct Follower {
    environment: RwLock<EnvironmentFrontier>,
    accounts: RwLock<HashMap<String, Account>>,
    open: AtomicBool,
    floor: AtomicU64,
    state: tokio::sync::watch::Sender<FollowerState>,
}

impl Follower {
    /// A fenced follower pinned to `environment`'s anchor keys. It serves trust
    /// once its driver has applied every Stream up to the tail it observed and
    /// called [`Self::resume`].
    #[must_use]
    pub fn new(environment: impl Into<String>, anchor: Vec<BlockId>) -> Self {
        Self {
            environment: RwLock::new(EnvironmentFrontier::genesis(environment, anchor)),
            accounts: RwLock::default(),
            open: AtomicBool::new(false),
            floor: AtomicU64::new(0),
            state: tokio::sync::watch::Sender::new(FollowerState::Connecting),
        }
    }

    /// The serving state, updated on every apply, fence, resume and serve.
    /// Await it rather than probing: a receiver wakes on every transition
    /// and every trust publication.
    #[must_use]
    pub fn state(&self) -> tokio::sync::watch::Receiver<FollowerState> {
        self.state.subscribe()
    }

    /// The followed environment.
    pub fn environment(&self) -> Result<String, VerifyError> {
        Ok(self
            .environment
            .read()
            .map_err(|_| VerifyError::Unavailable)?
            .name
            .clone())
    }

    /// Authenticate and apply one Stream record at trusted time `now`. Trust is
    /// swapped before any change wake. A refusal changes nothing and fences
    /// the record's account, or everything for an environment record.
    ///
    /// # Errors
    /// Malformed, forged, gapped, rolled-back or future-dated records.
    pub fn apply(&self, record: &[u8], now: u64) -> Result<Applied, VerifyError> {
        let record = decode(record)?.record;
        let result = match &record {
            TrustRecordKind::Environment(signed) => {
                let result = self
                    .environment
                    .write()
                    .map_err(|_| VerifyError::Unavailable)
                    .and_then(|mut environment| environment.apply(signed, now));
                if result.is_err() {
                    self.fence(FenceCause::Refused);
                    return result.map(|(applied, _)| applied);
                }
                result
            }
            TrustRecordKind::Keyring(signed) => {
                self.account_record(&signed.snapshot.account_id, &record, now)
            }
            TrustRecordKind::Revocation(signed) => {
                self.account_record(publication_account(signed), &record, now)
            }
        };
        match result {
            Ok((Applied::Duplicate, _)) => {}
            Ok((Applied::Changed, issued_at)) => {
                self.floor.fetch_max(issued_at, Ordering::SeqCst);
                // Wake only after the write lock is released: woken
                // consumers read the swapped trust.
                self.notify(None);
            }
            // The account is fenced; its consumers recheck and refuse.
            Err(_) => self.notify(None),
        }
        result.map(|(applied, _)| applied)
    }

    fn account_record(
        &self,
        id: &str,
        record: &TrustRecordKind,
        now: u64,
    ) -> Result<(Applied, u64), VerifyError> {
        let environment = self
            .environment
            .read()
            .map_err(|_| VerifyError::Unavailable)?
            .keys
            .clone();
        let mut accounts = self
            .accounts
            .write()
            .map_err(|_| VerifyError::Unavailable)?;
        let account = accounts.entry(id.to_owned()).or_default();
        let result = account.apply(id, record, &environment, now);
        match &result {
            Ok((Applied::Changed, _)) => account.poisoned = false,
            Ok((Applied::Duplicate, _)) => {}
            Err(_) => account.poisoned = true,
        }
        result
    }

    /// Refuse all trust and wake every change wait, so consumers recheck and
    /// fence live authority. Every account stays fenced until its Stream is
    /// caught up again ([`Self::serve`]). Waits registered while fenced wake
    /// on the next transition.
    pub fn fence(&self, cause: FenceCause) {
        self.withdraw(FollowerState::Fenced(cause));
    }

    /// The driver is (re)connecting: refuse trust until it catches up.
    pub fn connect(&self) {
        self.withdraw(FollowerState::Connecting);
    }

    /// Refuse trust for good; every change wait, current or later, resolves
    /// [`SourceClosed`].
    pub fn close(&self, cause: FenceCause) {
        self.withdraw(FollowerState::Closed(cause));
    }

    fn withdraw(&self, state: FollowerState) {
        self.open.store(false, Ordering::SeqCst);
        if let Ok(mut accounts) = self.accounts.write() {
            for account in accounts.values_mut() {
                account.serving = false;
            }
        }
        self.notify(Some(state));
    }

    /// The driver applied the environment Stream through the tail it observed
    /// when it (re)connected. Redelivered records are duplicates; a gap is
    /// refused, so nothing is skipped. A closed follower stays closed.
    pub fn resume(&self) {
        if self.closed() {
            return;
        }
        self.open.store(true, Ordering::SeqCst);
        self.notify(Some(FollowerState::Serving));
    }

    /// The driver applied `account`'s Stream through the tail it observed
    /// when it (re)connected; its trust may now be served.
    pub fn serve(&self, account: &str) {
        if let Ok(mut accounts) = self.accounts.write() {
            accounts.entry(account.to_owned()).or_default().serving = true;
        }
        self.notify(None);
    }

    /// Monotone protected time: never earlier than any time this follower has
    /// returned or any accepted publication. A rolled-back host clock cannot
    /// revive expired authority.
    pub fn protected_now(&self, observed_unix_seconds: u64) -> u64 {
        self.floor
            .fetch_max(observed_unix_seconds, Ordering::SeqCst)
            .max(observed_unix_seconds)
    }

    // One notifier: a transition or publication bumps the watch, waking
    // every receiver whether or not the state value changed. Closed is final.
    fn notify(&self, next: Option<FollowerState>) {
        self.state.send_modify(|state| {
            if let Some(next) = next
                && !matches!(state, FollowerState::Closed(_))
            {
                *state = next;
            }
        });
    }

    fn closed(&self) -> bool {
        matches!(*self.state.borrow(), FollowerState::Closed(_))
    }

    fn check(&self) -> Result<(), VerifyError> {
        if self.open.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(VerifyError::Unavailable)
        }
    }

    /// `account`'s current trust as a self-contained [`Snapshot`], for an owner
    /// that hands trust to an isolated guest along with its own publications.
    ///
    /// # Errors
    /// The same refusals as serving the account's trust.
    pub fn snapshot(&self, account: &str) -> Result<super::Snapshot, VerifyError> {
        let (environment, environment_keys) = {
            let current = self
                .environment
                .read()
                .map_err(|_| VerifyError::Unavailable)?;
            (current.name.clone(), current.keys.to_vec())
        };
        self.with_account(account, |keyring, revocations| {
            let (keys, blocks, floor) = revocations.revoked();
            super::Snapshot {
                environment,
                environment_keys,
                account: account.into(),
                issuer_keys: keyring.root.to_vec(),
                minimum_key_epoch: keyring.minimum_key_epoch.max(floor),
                revoked_key_ids: keyring.revoked_keys.iter().chain(keys).cloned().collect(),
                revoked_block_ids: keyring
                    .revoked_blocks
                    .keys()
                    .chain(blocks)
                    .copied()
                    .collect(),
            }
        })
    }

    fn with_account<T>(
        &self,
        id: &str,
        read: impl FnOnce(&Keyring, &RevocationState) -> T,
    ) -> Result<T, VerifyError> {
        self.check()?;
        let accounts = self.accounts.read().map_err(|_| VerifyError::Unavailable)?;
        match accounts.get(id) {
            Some(Account {
                keyring: Some(keyring),
                revocations: Some(revocations),
                serving: true,
                poisoned: false,
            }) => Ok(read(keyring, revocations)),
            _ => Err(VerifyError::Trust),
        }
    }
}

fn signed_by(keys: &[BlockId], message: &[u8], signature: &[u8]) -> Result<(), VerifyError> {
    if keys.iter().any(|key| {
        public_key(key).is_ok_and(|key| verify_signature(&key, message, signature).is_ok())
    }) {
        Ok(())
    } else {
        Err(VerifyError::Signature)
    }
}

fn keyring(
    account: &mut Account,
    signed: &SignedKeyringSnapshot,
    environment: &[BlockId],
    now: u64,
) -> Result<(Applied, u64), VerifyError> {
    let next = &signed.snapshot;
    if let Some(current) = &account.keyring {
        if next.sequence <= current.sequence {
            return Ok((Applied::Duplicate, current.issued_at));
        }
        let rolled_back = next.minimum_key_epoch < current.minimum_key_epoch
            || next.issued_at_unix_seconds < current.issued_at
            || !current
                .revoked_keys
                .iter()
                .all(|key| next.revoked_key_ids.contains(key))
            || !current.revoked_blocks.iter().all(|(id, expiry)| {
                *expiry <= next.issued_at_unix_seconds
                    || next.revocations.iter().any(|revocation| {
                        revocation.id == id && revocation.expires_at_unix_seconds >= *expiry
                    })
            });
        if current.sequence.checked_add(1) != Some(next.sequence)
            || next.previous_digest != current.digest
            || rolled_back
        {
            return Err(VerifyError::Trust);
        }
    } else if next.previous_digest != [0; 32] {
        return Err(VerifyError::Trust);
    }
    if next.issued_at_unix_seconds > now {
        return Err(VerifyError::Time);
    }
    let mut message = Vec::new();
    claims_message(
        &mut message,
        KEYRING_DOMAIN,
        &KeyringSnapshotProto::from(next.clone()),
    )?;
    // The environment attests every root it sets; the current root alone may
    // only re-sign itself, so it can narrow (raise its epoch, revoke keys or
    // blocks) but never rotate.
    let kept = account
        .keyring
        .as_ref()
        .filter(|current| {
            current
                .root
                .first()
                .is_some_and(|root| root.as_slice() == next.root_public_key.as_slice())
        })
        .map(|current| current.root.clone());
    signed_by(environment, &message, &signed.signature).or_else(|refused| match &kept {
        Some(root) => signed_by(root, &message, &signed.signature),
        None => Err(refused),
    })?;
    let root: BlockId = next
        .root_public_key
        .as_slice()
        .try_into()
        .map_err(|_| VerifyError::Malformed)?;
    let mut revoked_blocks = HashMap::with_capacity(next.revocations.len());
    for revocation in &next.revocations {
        let id = revocation
            .id
            .as_slice()
            .try_into()
            .map_err(|_| VerifyError::Malformed)?;
        revoked_blocks.insert(id, revocation.expires_at_unix_seconds);
    }
    account.keyring = Some(Keyring {
        sequence: next.sequence,
        digest: block_id(&message, &signed.signature),
        issued_at: next.issued_at_unix_seconds,
        root: Arc::from([root]),
        minimum_key_epoch: next.minimum_key_epoch,
        revoked_keys: next.revoked_key_ids.iter().cloned().collect(),
        revoked_blocks,
    });
    Ok((Applied::Changed, next.issued_at_unix_seconds))
}

fn revocation(
    account: &mut Account,
    id: &str,
    signed: &SignedRevocation,
    now: u64,
) -> Result<(Applied, u64), VerifyError> {
    let root = *account
        .keyring
        .as_ref()
        .and_then(|keyring| keyring.root.first())
        .ok_or(VerifyError::Trust)?;
    // "Everything before an epoch" is the keyring's minimum_key_epoch alone.
    let (sequence, issued_at) = match &signed.publication {
        RevocationPublication::Delta(delta) if delta.revoke_all_before_epoch.is_none() => {
            (delta.sequence, delta.issued_at_unix_seconds)
        }
        RevocationPublication::Checkpoint(checkpoint) if checkpoint.minimum_key_epoch == 0 => {
            (checkpoint.sequence, checkpoint.issued_at_unix_seconds)
        }
        _ => return Err(VerifyError::Trust),
    };
    if let Some(state) = &mut account.revocations {
        if sequence <= state.frontier().0 {
            return Ok((Applied::Duplicate, state.published_at()));
        }
        // Gap-checked and rollback-refusing; a refusal changes nothing.
        state.apply(signed, &root, now)?;
    } else {
        account.revocations = Some(RevocationState::from_checkpoint(signed, id, &root, now)?);
    }
    Ok((Applied::Changed, issued_at))
}

impl Trust for Follower {
    fn environment_keys(&self, environment: &str) -> Result<Keys<'_>, VerifyError> {
        self.check()?;
        let current = self
            .environment
            .read()
            .map_err(|_| VerifyError::Unavailable)?;
        if current.name == environment {
            Ok(Keys::Shared(current.keys.clone()))
        } else {
            Err(VerifyError::Trust)
        }
    }

    fn account(&self, account: &str) -> Result<AccountState<'_>, VerifyError> {
        self.with_account(account, |keyring, revocations| {
            let published = revocations.published_at();
            AccountState {
                issuer_keys: Keys::Shared(keyring.root.clone()),
                minimum_key_epoch: keyring.minimum_key_epoch,
                not_before_unix_seconds: keyring.issued_at.max(published),
            }
        })
    }

    fn revoked(&self, account: &str, check: RevocationCheck<'_>) -> Result<bool, VerifyError> {
        self.with_account(account, |keyring, revocations| {
            revocations.revokes(check)
                || match check {
                    RevocationCheck::Key { id, epoch } => {
                        epoch < keyring.minimum_key_epoch || keyring.revoked_keys.contains(id)
                    }
                    RevocationCheck::Block(id) => keyring.revoked_blocks.contains_key(id),
                }
        })
    }
}

impl ChangeSource for Follower {
    fn check_open(&self) -> Result<(), SourceClosed> {
        if self.open.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(SourceClosed)
        }
    }

    fn changed(&self) -> Changed {
        // Subscribed synchronously, before the caller checks authority: the
        // receiver has seen the current version, so the next transition or
        // publication wakes it, in any state, even before the first poll
        // (`borrow` leaves the version unseen). Only a close is an error.
        let mut state = self.state.subscribe();
        Box::pin(async move {
            if !matches!(*state.borrow(), FollowerState::Closed(_))
                && state.changed().await.is_ok()
                && !matches!(*state.borrow(), FollowerState::Closed(_))
            {
                Ok(())
            } else {
                Err(SourceClosed)
            }
        })
    }
}

#[cfg(test)]
#[path = "follower_tests.rs"]
mod tests;
