//! The one trust implementation: a follower of signed trust Streams.
//!
//! Layout. One environment Stream, [`environment_path`], carries
//! [`SignedEnvironmentKeyring`] records. Each account has one child Stream,
//! [`account_path`], carrying that account's [`SignedKeyringSnapshot`] and
//! [`SignedRevocation`] records in one total order. Every record value is one
//! canonical [`TrustRecord`].
//!
//! Signers. Each environment keyring is signed by a key of the previous one.
//! The configured anchor ([`AnchorKeys`], read live from its owner's watch,
//! one snapshot per record) authenticates a keyring with no applied
//! predecessor: one it signed, or one naming only anchor keys. Cold history
//! may predate the current anchor, so a follower with no applied keyring
//! accepts the first such keyring at any sequence; earlier records are
//! superseded, never applied. Until then the anchor itself is the
//! environment's key set. A retired anchor key authenticates nothing after
//! its overlap window. Each account
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
//! [`Follower::serve`]). A global withdrawal advances one generation without
//! locking or visiting accounts; each account's caught-up acknowledgement is
//! valid only in that generation. Reads check the generation before and after
//! accessing account state. Fenced reads refuse as [`VerifyError::Unavailable`];
//! a refused account's reads refuse as [`VerifyError::Refused`] until a record
//! for it applies.
//! No task per account; one hash lookup per trust read. Publication wakes still
//! reach only the selected account and explicitly unscoped subscribers; global
//! changes reach every dependency. Record verification holds only its account lock;
//! keyring publication also checks the environment revision it authenticated.
//! Environment signatures hold no publication lock and must extend the same
//! retained environment revision when they publish.
use super::{AnchorKeys, ChangeSource, Changed, SourceClosed};
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
        atomic::{AtomicU64, Ordering},
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
    anchor: AnchorKeys,
    revision: EnvironmentRevision,
}

#[derive(Clone, Default)]
struct EnvironmentRevision {
    keys: Arc<[BlockId]>,
    sequence: u64,
    digest: BlockId,
    issued_at: u64,
}

impl EnvironmentFrontier {
    /// The frontier before any environment keyring: only `anchor`
    /// authenticates.
    #[must_use]
    pub fn genesis(environment: impl Into<String>, anchor: AnchorKeys) -> Self {
        Self {
            name: environment.into(),
            anchor,
            revision: EnvironmentRevision::default(),
        }
    }

    /// Replays the environment Stream's records from its start, through the
    /// same validation as [`Follower::apply`]: records before the first one
    /// `anchor` authenticates are superseded history.
    ///
    /// # Errors
    /// A gap, a foreign signer, a rollback, a non-environment record, or
    /// records none of which `anchor` authenticates.
    pub fn resume(
        environment: impl Into<String>,
        anchor: AnchorKeys,
        records: &[&[u8]],
    ) -> Result<Self, VerifyError> {
        let mut frontier = Self::genesis(environment, anchor);
        for record in records {
            let TrustRecordKind::Environment(signed) = decode(record)?.record else {
                return Err(VerifyError::Trust);
            };
            match frontier.apply(&signed, u64::MAX) {
                Err(_) if frontier.revision.cold() => {}
                result => {
                    result?;
                }
            }
        }
        if !records.is_empty() && frontier.revision.cold() {
            return Err(VerifyError::Trust);
        }
        Ok(frontier)
    }

    /// The environment name.
    #[must_use]
    pub fn environment(&self) -> &str {
        &self.name
    }

    /// The current environment keys: before any keyring, the anchor's.
    #[must_use]
    pub fn keys(&self) -> &[BlockId] {
        if self.revision.cold() {
            self.anchor.current()
        } else {
            &self.revision.keys
        }
    }

    /// Last applied sequence (0 before keyring 1) and its digest.
    #[must_use]
    pub const fn frontier(&self) -> (u64, BlockId) {
        (self.revision.sequence, self.revision.digest)
    }

    /// Last applied publication time.
    #[must_use]
    pub const fn issued_at(&self) -> u64 {
        self.revision.issued_at
    }

    /// Authenticates and applies the next environment keyring at `now`. A
    /// refusal changes nothing.
    pub(super) fn apply(
        &mut self,
        signed: &SignedEnvironmentKeyring,
        now: u64,
    ) -> Result<(Applied, u64), VerifyError> {
        if signed.keyring.environment != self.name {
            return Err(VerifyError::Trust);
        }
        self.revision.apply(signed, &self.anchor, now)
    }
}

impl EnvironmentRevision {
    /// No keyring applied: the anchor is the environment's key set.
    const fn cold(&self) -> bool {
        self.sequence == 0
    }

    /// The environment keys at `now` under the anchor snapshot `anchor`.
    fn keys(&self, anchor: &AnchorKeys, now: u64) -> Arc<[BlockId]> {
        if self.cold() {
            anchor.at(now)
        } else {
            Arc::clone(&self.keys)
        }
    }

    /// Authenticates and applies the next keyring under one anchor snapshot.
    fn apply(
        &mut self,
        signed: &SignedEnvironmentKeyring,
        anchor: &AnchorKeys,
        now: u64,
    ) -> Result<(Applied, u64), VerifyError> {
        let next = &signed.keyring;
        if next.sequence <= self.sequence {
            return Ok((Applied::Duplicate, self.issued_at));
        }
        let linked = self.sequence.checked_add(1) == Some(next.sequence)
            && next.previous_digest == self.digest
            && next.issued_at_unix_seconds >= self.issued_at;
        // Only a keyring with no applied predecessor may start past keyring 1.
        if !(linked || (self.cold() && next.sequence > 1)) || next.keys.is_empty() {
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
        let keys = next
            .keys
            .iter()
            .map(|key| {
                key.as_slice()
                    .try_into()
                    .map_err(|_| VerifyError::Malformed)
            })
            .collect::<Result<Vec<BlockId>, _>>()?;
        #[cfg(test)]
        tests::environment_verifying();
        // The previous keyring signs its successor. With none applied, the
        // anchor snapshot authenticates a keyring it signed, or one naming
        // only its keys; it never overrides an applied chain.
        if self.cold() {
            let anchor = anchor.at(now);
            if !keys.iter().all(|key| anchor.contains(key)) {
                signed_by(&anchor, &message, &signed.signature)?;
            }
        } else {
            signed_by(&self.keys, &message, &signed.signature)?;
        }
        *self = Self {
            keys: keys.into(),
            sequence: next.sequence,
            digest: block_id(&message, &signed.signature),
            issued_at: next.issued_at_unix_seconds,
        };
        Ok((Applied::Changed, next.issued_at_unix_seconds))
    }
}

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
    /// Generation in which the driver caught up to this account's observed tail.
    serving_generation: u64,
    /// A refused record; cleared only by a record that applies.
    poisoned: bool,
}

impl Account {
    /// Check a proposed keyring without cloning or advancing retained trust.
    /// Issuance and application use the same canonical preparation below.
    pub(super) fn check_keyring(
        &self,
        id: &str,
        record: &TrustRecordKind,
        environment: &[BlockId],
        now: u64,
    ) -> Result<(), VerifyError> {
        match record {
            TrustRecordKind::Keyring(signed) if signed.snapshot.account_id == id => {
                prepare_keyring(self, signed, environment, now).map(|_| ())
            }
            _ => Err(VerifyError::Trust),
        }
    }

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
    /// The authenticated bootstrap key-set publication owner was lost.
    AnchorLost,
    /// An environment record was refused; await an owner anchor publication.
    Refused,
    /// The driver stopped for good.
    Shutdown,
    /// Local generations cannot advance without reusing an old acknowledgement.
    GenerationExhausted,
}

/// A follower's serving state. Every trust publication also notifies its
/// watch, so one channel carries both state and change events.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FollowerState {
    /// Catching up to the tails observed at (re)connect; refusing trust.
    Connecting,
    /// Serving the environment's trust.
    Serving,
    /// Refusing trust until its producer recovers or its anchor changes.
    Fenced(FenceCause),
    /// Refusing trust for good: every change wait resolves [`SourceClosed`].
    Closed(FenceCause),
}

/// Follows signed trust records for any number of accounts of one environment.
pub struct Follower {
    name: String,
    environment: RwLock<EnvironmentRevision>,
    anchor: tokio::sync::watch::Receiver<AnchorKeys>,
    environment_refusal: RwLock<Option<VerifyError>>,
    accounts: RwLock<HashMap<String, Arc<AccountEntry>>>,
    floor: AtomicU64,
    // The original state watch serializes generation updates and their wakes.
    generation: AtomicU64,
    state: tokio::sync::watch::Sender<FollowerState>,
    global: tokio::sync::watch::Sender<()>,
}

struct AccountEntry {
    state: RwLock<Account>,
    changes: tokio::sync::watch::Sender<()>,
}

impl Default for AccountEntry {
    fn default() -> Self {
        Self {
            state: RwLock::default(),
            changes: tokio::sync::watch::Sender::new(()),
        }
    }
}

enum Target<'a> {
    Environment,
    Account(&'a str),
}

impl Follower {
    /// A fenced follower of `environment`, authenticated by the anchor its
    /// owner publishes on `anchor` ([`AnchorKeys::fixed`] for one that never
    /// rotates). Every authentication reads the latest published snapshot, so
    /// no follower is pinned to the keys it was built with. It serves trust
    /// once its driver has applied every Stream up to the tail it observed and
    /// called [`Self::resume`].
    #[must_use]
    pub fn new(
        environment: impl Into<String>,
        anchor: tokio::sync::watch::Receiver<AnchorKeys>,
    ) -> Self {
        Self {
            name: environment.into(),
            environment: RwLock::default(),
            anchor,
            environment_refusal: RwLock::new(None),
            accounts: RwLock::default(),
            floor: AtomicU64::new(0),
            generation: AtomicU64::new(1),
            state: tokio::sync::watch::Sender::new(FollowerState::Connecting),
            global: tokio::sync::watch::Sender::new(()),
        }
    }

    /// Replace the owner's bootstrap keys before the first environment record
    /// authenticates. An established signed chain keeps its original frontier;
    /// an anchor update cannot replace its keys or skip a signed rotation.
    /// The feed calls this only on an actual owner key-set publication.
    ///
    /// # Errors
    /// Inaccessible frontier storage. This does not serve trust:
    /// the refused record must still authenticate and catch up first.
    pub fn reanchor(&self, anchor: Arc<[BlockId]>) -> Result<(), VerifyError> {
        let retired = {
            let mut environment = self
                .environment
                .write()
                .map_err(|_| VerifyError::Unavailable)?;
            if environment.revision.sequence != 0
                || Arc::ptr_eq(&environment.revision.keys, &anchor)
            {
                return Ok(());
            }
            std::mem::replace(&mut environment.revision.keys, anchor)
        };
        drop(retired);
        self.notify(None);
        self.global.send_replace(());
        Ok(())
    }

    /// Original environment-record failure behind the retryable refusal fence.
    /// This is a diagnostic, never an AccessDenied verdict on a caller.
    pub fn environment_refusal(&self) -> Option<VerifyError> {
        self.environment_refusal
            .read()
            .ok()
            .and_then(|cause| *cause)
    }

    fn refuse_environment(&self, cause: VerifyError) {
        if let Ok(mut refusal) = self.environment_refusal.write() {
            *refusal = Some(cause);
        }
        // Original follows and catch-up acknowledgements remain live: no
        // account generation withdrawal or population scan is needed.
        self.notify(Some(FollowerState::Fenced(FenceCause::Refused)));
        self.global.send_replace(());
    }

    /// The serving state, updated on every apply, fence, resume and serve.
    /// Await it rather than probing: a receiver wakes on every transition
    /// and every trust publication.
    #[must_use]
    pub fn state(&self) -> tokio::sync::watch::Receiver<FollowerState> {
        self.state.subscribe()
    }

    /// The followed environment.
    #[must_use]
    pub fn environment(&self) -> &str {
        &self.name
    }

    /// The anchor's publications: a driver awaits `changed` on it to retry a
    /// refused environment record, never probing.
    #[must_use]
    pub fn anchor(&self) -> tokio::sync::watch::Receiver<AnchorKeys> {
        self.anchor.clone()
    }

    /// The anchor published a new snapshot. Before any environment keyring the
    /// anchor is the environment's key set, so every dependency rechecks.
    pub fn anchor_changed(&self) {
        self.notify(None);
        self.global.send_replace(());
    }

    /// Whether an environment keyring has been authenticated. Before one, a
    /// refused environment record is history the anchor may yet authenticate.
    pub fn environment_initialized(&self) -> Result<bool, VerifyError> {
        Ok(!self
            .environment
            .read()
            .map_err(|_| VerifyError::Unavailable)?
            .cold())
    }

    /// Authenticate and apply one Stream record at trusted time `now`. Trust is
    /// swapped before any change wake. A refusal changes nothing and fences
    /// the record's account, or everything for an environment record.
    ///
    /// # Errors
    /// Malformed, forged, gapped, rolled-back or future-dated records.
    pub fn apply(&self, record: &[u8], now: u64) -> Result<Applied, VerifyError> {
        self.apply_record(record, now, true, None)
    }

    /// Apply only a record carried by the environment's own follow.
    pub fn apply_environment(&self, record: &[u8], now: u64) -> Result<Applied, VerifyError> {
        self.apply_record(record, now, true, Some(Target::Environment))
    }

    /// Apply only a record carried by this account's own follow.
    pub fn apply_account(
        &self,
        account: &str,
        record: &[u8],
        now: u64,
    ) -> Result<Applied, VerifyError> {
        self.apply_record(record, now, true, Some(Target::Account(account)))
    }

    /// Apply a retained record without releasing its account's refusal fence.
    /// The driver calls [`Self::release`] only after the retained queue drains.
    pub fn apply_queued(
        &self,
        account: &str,
        record: &[u8],
        now: u64,
    ) -> Result<Applied, VerifyError> {
        self.apply_record(record, now, false, Some(Target::Account(account)))
    }

    /// Release a successfully drained account queue, then wake its consumers.
    pub fn release(&self, account: &str) {
        if let Ok(Some(account)) = self.find_account(account) {
            if let Ok(mut state) = account.state.write() {
                state.poisoned = false;
            }
            account.changes.send_replace(());
        }
        self.notify(None);
    }

    fn apply_record(
        &self,
        record: &[u8],
        now: u64,
        release: bool,
        target: Option<Target<'_>>,
    ) -> Result<Applied, VerifyError> {
        let record = decode(record)
            .inspect_err(|cause| {
                if matches!(target, Some(Target::Environment)) {
                    self.refuse_environment(*cause);
                }
            })?
            .record;
        let matches = match (&target, &record) {
            (None, _) | (Some(Target::Environment), TrustRecordKind::Environment(_)) => true,
            (Some(Target::Account(id)), TrustRecordKind::Keyring(signed)) => {
                *id == signed.snapshot.account_id
            }
            (Some(Target::Account(id)), TrustRecordKind::Revocation(signed)) => {
                *id == publication_account(signed)
            }
            _ => false,
        };
        if !matches {
            if matches!(target, Some(Target::Environment)) {
                self.refuse_environment(VerifyError::Trust);
            }
            return Err(VerifyError::Trust);
        }
        let account_id = match &record {
            TrustRecordKind::Environment(_) => None,
            TrustRecordKind::Keyring(signed) => Some(signed.snapshot.account_id.as_str()),
            TrustRecordKind::Revocation(signed) => Some(publication_account(signed)),
        };
        let account = account_id
            .map(|id| self.account_entry(id).map(|entry| (id, entry)))
            .transpose()
            .inspect_err(|_| {
                // Storage loss cannot leave existing account waits idle.
                self.notify(None);
                self.global.send_replace(());
            })?;
        let result = match (&record, &account) {
            (TrustRecordKind::Environment(signed), _) => {
                let result = self.environment_record(signed, now);
                if let Err(cause) = result {
                    self.refuse_environment(cause);
                    return result.map(|(applied, _)| applied);
                }
                if let Ok(mut refusal) = self.environment_refusal.write() {
                    *refusal = None;
                }
                result
            }
            (_, Some((id, entry))) => self.account_record(entry, id, &record, now, release),
            _ => return Err(VerifyError::Trust),
        };
        let time_advanced = if let Ok((Applied::Changed, issued_at)) = result {
            self.floor.fetch_max(issued_at, Ordering::SeqCst) < issued_at
        } else {
            false
        };
        if !matches!(result, Ok((Applied::Duplicate, _))) {
            // The new trust, refusal and protected time precede every wake.
            self.notify(None);
            if let Some((_, entry)) = &account {
                entry.changes.send_replace(());
            }
            // The protected time floor is a dependency of every account.
            if account.is_none() || time_advanced {
                self.global.send_replace(());
            }
        }
        result.map(|(applied, _)| applied)
    }

    fn environment_record(
        &self,
        signed: &SignedEnvironmentKeyring,
        now: u64,
    ) -> Result<(Applied, u64), VerifyError> {
        if signed.keyring.environment != self.name {
            return Err(VerifyError::Trust);
        }
        let mut prepared = self
            .environment
            .read()
            .map_err(|_| VerifyError::Unavailable)?
            .clone();
        let sequence = prepared.sequence;
        let keys = prepared.keys.clone();
        // Exactly one published anchor snapshot authenticates this record.
        let anchor = self.anchor.borrow().clone();
        let result = prepared.apply(signed, &anchor, now)?;
        if result.0 == Applied::Duplicate {
            return Ok(result);
        }
        let retired = {
            let mut environment = self
                .environment
                .write()
                .map_err(|_| VerifyError::Unavailable)?;
            if environment.sequence != sequence || !Arc::ptr_eq(&environment.keys, &keys) {
                return Err(VerifyError::Unavailable);
            }
            std::mem::replace(&mut *environment, prepared)
        };
        // The same signature proof can publish only against its captured
        // frontier. Retiring the replaced key owner holds no publication lock.
        drop(retired);
        Ok(result)
    }

    fn account_record(
        &self,
        entry: &AccountEntry,
        id: &str,
        record: &TrustRecordKind,
        now: u64,
        release: bool,
    ) -> Result<(Applied, u64), VerifyError> {
        let mut account = entry.state.write().map_err(|_| VerifyError::Unavailable)?;
        let result = match record {
            TrustRecordKind::Keyring(signed) => {
                let (keys, revision) = {
                    let environment = self
                        .environment
                        .read()
                        .map_err(|_| VerifyError::Unavailable)?;
                    let anchor = self.anchor.borrow().clone();
                    (environment.keys(&anchor, now), environment.sequence)
                };
                prepare_keyring(&account, signed, &keys, now).and_then(|prepared| {
                    self.publish_keyring(&mut account, revision, &keys, prepared)
                })
            }
            _ => account.apply(id, record, &[], now),
        };
        match &result {
            Ok((Applied::Changed, _)) if release => account.poisoned = false,
            Ok((Applied::Changed, _)) => {}
            Ok((Applied::Duplicate, _)) => {}
            Err(_) => account.poisoned = true,
        }
        result
    }

    fn publish_keyring(
        &self,
        account: &mut Account,
        revision: u64,
        keys: &Arc<[BlockId]>,
        prepared: PreparedKeyring,
    ) -> Result<(Applied, u64), VerifyError> {
        let environment = self
            .environment
            .read()
            .map_err(|_| VerifyError::Unavailable)?;
        if environment.sequence != revision || !Arc::ptr_eq(&environment.keys, keys) {
            return Err(VerifyError::Unavailable);
        }
        let retired = prepared.next.and_then(|next| account.keyring.replace(next));
        // Retiring an old checkpoint can free accumulated indexes. It must not
        // retain the environment publication lock during that destruction.
        drop(environment);
        drop(retired);
        Ok(prepared.result)
    }

    fn find_account(&self, id: &str) -> Result<Option<Arc<AccountEntry>>, VerifyError> {
        Ok(self
            .accounts
            .read()
            .map_err(|_| VerifyError::Unavailable)?
            .get(id)
            .cloned())
    }

    fn account_entry(&self, id: &str) -> Result<Arc<AccountEntry>, VerifyError> {
        if let Some(account) = self.find_account(id)? {
            return Ok(account);
        }
        let mut accounts = self
            .accounts
            .write()
            .map_err(|_| VerifyError::Unavailable)?;
        Ok(accounts.entry(id.to_owned()).or_default().clone())
    }

    /// Refuse all trust and wake every change wait, so consumers recheck and
    /// fence live authority. Every account stays fenced until its Stream is
    /// caught up again ([`Self::serve`]). Waits registered while fenced wake
    /// on the next transition. Invalidating account acknowledgements does not
    /// acquire account storage or traverse the followed population; notifying
    /// the original watch still costs work proportional to actual waiters.
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
        self.state.send_modify(|current| {
            if !matches!(current, FollowerState::Closed(_)) {
                *current = if matches!(state, FollowerState::Closed(_)) {
                    state
                } else if let Some(next) = self.generation.load(Ordering::Relaxed).checked_add(1) {
                    self.generation.store(next, Ordering::Relaxed);
                    state
                } else {
                    FollowerState::Closed(FenceCause::GenerationExhausted)
                };
            }
        });
        self.global.send_replace(());
    }

    /// The driver applied the environment Stream through the tail it observed
    /// when it (re)connected. Redelivered records are duplicates; a gap is
    /// refused, so nothing is skipped. A closed follower stays closed.
    pub fn resume(&self) {
        self.notify(Some(FollowerState::Serving));
        self.global.send_replace(());
    }

    /// The driver applied `account`'s Stream through the tail it observed
    /// when it (re)connected; its trust may now be served.
    pub fn serve(&self, account: &str) {
        let generation = {
            let state = self.state.borrow();
            if matches!(*state, FollowerState::Closed(_)) {
                return;
            }
            self.generation.load(Ordering::Relaxed)
        };
        self.acknowledge(account, generation);
    }

    fn acknowledge(&self, account: &str, generation: u64) {
        if let Ok(account) = self.account_entry(account) {
            if let Ok(mut state) = account.state.write() {
                // A delayed older call must not undo a newer acknowledgement.
                state.serving_generation = state.serving_generation.max(generation);
            }
            account.changes.send_replace(());
        }
        self.notify(None);
    }

    /// Whether authenticated keyring and revocation frontiers are retained.
    /// This is replay progress only: it does not assert serving, current trust
    /// or permission. Readers must still use the `Trust` implementation.
    pub fn initialized(&self, account: &str) -> Result<bool, VerifyError> {
        let Some(account) = self.find_account(account)? else {
            return Ok(false);
        };
        let account = account.state.read().map_err(|_| VerifyError::Unavailable)?;
        Ok(account.keyring.is_some() && account.revocations.is_some())
    }

    /// A record on `account`'s Stream was refused, whatever the cause,
    /// an undecodable record included: fence that account until a record
    /// for it applies.
    pub fn refuse(&self, account: &str) {
        if let Ok(account) = self.account_entry(account) {
            if let Ok(mut state) = account.state.write() {
                state.poisoned = true;
            }
            account.changes.send_replace(());
        }
        self.notify(None);
    }

    /// Monotone protected time: never earlier than any time this follower has
    /// returned or any accepted publication. A rolled-back host clock cannot
    /// revive expired authority. Advancing it notifies the original global
    /// dependency event after the new floor is visible; unchanged reads do not.
    pub fn protected_now(&self, observed_unix_seconds: u64) -> u64 {
        let previous = self
            .floor
            .fetch_max(observed_unix_seconds, Ordering::SeqCst);
        if previous < observed_unix_seconds {
            self.notify(None);
            self.global.send_replace(());
        }
        previous.max(observed_unix_seconds)
    }

    // Preserve the public unscoped publication watch. Account dependencies
    // register only their own event and the global event. Closed is final.
    fn notify(&self, next: Option<FollowerState>) {
        self.state.send_modify(|state| {
            if let Some(next) = next
                && !matches!(state, FollowerState::Closed(_))
            {
                *state = next;
            }
        });
    }

    /// Current environment keys at the protected time floor.
    fn environment_keys_now(&self) -> Result<Arc<[BlockId]>, VerifyError> {
        let environment = self
            .environment
            .read()
            .map_err(|_| VerifyError::Unavailable)?;
        let anchor = self.anchor.borrow().clone();
        Ok(environment.keys(&anchor, self.floor.load(Ordering::SeqCst)))
    }

    fn check(&self) -> Result<(), VerifyError> {
        if matches!(*self.state.borrow(), FollowerState::Serving) {
            Ok(())
        } else {
            Err(VerifyError::Unavailable)
        }
    }

    fn serving_generation(&self) -> Result<u64, VerifyError> {
        let state = self.state.borrow();
        if matches!(*state, FollowerState::Serving) {
            Ok(self.generation.load(Ordering::Relaxed))
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
        let environment = self.name.clone();
        let environment_keys = self.environment_keys_now()?.to_vec();
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
        let generation = self.serving_generation()?;
        let result = match self.find_account(id)? {
            Some(account) => {
                let account = account.state.read().map_err(|_| VerifyError::Unavailable)?;
                match &*account {
                    Account { poisoned: true, .. } => Err(VerifyError::Refused),
                    Account {
                        keyring: Some(keyring),
                        revocations: Some(revocations),
                        serving_generation: served,
                        poisoned: false,
                    } if *served == generation => Ok(read(keyring, revocations)),
                    _ => Err(VerifyError::Trust),
                }
            }
            None => Err(VerifyError::Trust),
        };
        // Withdrawal never waits for account locks or record verification.
        // Discard a read spanning any disconnect/reconnect, even if the driver
        // has already resumed by the time this read finishes.
        if self.serving_generation()? != generation {
            return Err(VerifyError::Unavailable);
        }
        result
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
    let prepared = prepare_keyring(account, signed, environment, now)?;
    if let Some(next) = prepared.next {
        account.keyring = Some(next);
    }
    Ok(prepared.result)
}

struct PreparedKeyring {
    result: (Applied, u64),
    next: Option<Keyring>,
}

fn prepare_keyring(
    account: &Account,
    signed: &SignedKeyringSnapshot,
    environment: &[BlockId],
    now: u64,
) -> Result<PreparedKeyring, VerifyError> {
    let next = &signed.snapshot;
    let mut revoked_blocks = HashMap::with_capacity(next.revocations.len());
    for revocation in &next.revocations {
        let id = revocation
            .id
            .as_slice()
            .try_into()
            .map_err(|_| VerifyError::Malformed)?;
        if revoked_blocks
            .insert(id, revocation.expires_at_unix_seconds)
            .is_some()
        {
            return Err(VerifyError::Malformed);
        }
    }

    if let Some(current) = &account.keyring {
        if next.sequence <= current.sequence {
            return Ok(PreparedKeyring {
                result: (Applied::Duplicate, current.issued_at),
                next: None,
            });
        }
        if current.sequence.checked_add(1) != Some(next.sequence)
            || next.previous_digest != current.digest
            || next.minimum_key_epoch < current.minimum_key_epoch
            || next.issued_at_unix_seconds < current.issued_at
        {
            return Err(VerifyError::Trust);
        }
    } else if next.sequence == 0 || (next.sequence == 1 && next.previous_digest != [0; 32]) {
        return Err(VerifyError::Trust);
    }
    let revoked_keys: HashSet<_> = next.revoked_key_ids.iter().cloned().collect();
    if let Some(current) = &account.keyring {
        let rolled_back = !current.revoked_keys.is_subset(&revoked_keys)
            || !current.revoked_blocks.iter().all(|(id, expiry)| {
                *expiry <= next.issued_at_unix_seconds
                    || revoked_blocks.get(id).is_some_and(|next| next >= expiry)
            });
        if rolled_back {
            return Err(VerifyError::Trust);
        }
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
    // A current environment signature authenticates the complete checkpoint,
    // including when replay starts after its historical signer was retired.
    // A retained frontier must still extend exactly; an account root cannot
    // bootstrap itself or rotate. Revocation trust remains unavailable until
    // its independently authenticated checkpoint is replayed as well.
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
    #[cfg(test)]
    tests::keyring_verifying();
    signed_by(environment, &message, &signed.signature).or_else(|refused| match &kept {
        Some(root) => signed_by(root, &message, &signed.signature),
        None => Err(refused),
    })?;
    let root: BlockId = next
        .root_public_key
        .as_slice()
        .try_into()
        .map_err(|_| VerifyError::Malformed)?;
    let next_keyring = Keyring {
        sequence: next.sequence,
        digest: block_id(&message, &signed.signature),
        issued_at: next.issued_at_unix_seconds,
        root: Arc::from([root]),
        minimum_key_epoch: next.minimum_key_epoch,
        revoked_keys,
        revoked_blocks,
    };
    Ok(PreparedKeyring {
        result: (Applied::Changed, next.issued_at_unix_seconds),
        next: Some(next_keyring),
    })
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
        if self.name == environment {
            Ok(Keys::Shared(self.environment_keys_now()?))
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
        if matches!(*self.state.borrow(), FollowerState::Serving) {
            Ok(())
        } else {
            Err(SourceClosed)
        }
    }

    fn changed_for(&self, account: &str) -> Changed {
        // Subscribe before the caller's validity check. Receivers retain no
        // sender, so neither an idle wait nor an account operation can keep the
        // global producer alive. Unknown names never create retained accounts.
        let mut global = self.global.subscribe();
        let state = self.state.subscribe();
        let account = self
            .find_account(account)
            .ok()
            .flatten()
            .map(|entry| entry.changes.subscribe());
        Box::pin(async move {
            let mut account = account.ok_or(SourceClosed)?;
            if matches!(*state.borrow(), FollowerState::Closed(_)) {
                return Err(SourceClosed);
            }
            let global_event = global.changed();
            let account_event = account.changed();
            futures::pin_mut!(global_event, account_event);
            match futures::future::select(global_event, account_event).await {
                futures::future::Either::Left((result, _))
                | futures::future::Either::Right((result, _)) => {
                    result.map_err(|_| SourceClosed)?;
                }
            }
            if matches!(*state.borrow(), FollowerState::Closed(_)) {
                Err(SourceClosed)
            } else {
                Ok(())
            }
        })
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
