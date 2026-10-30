//! The one trust implementation: a follower of signed trust Streams.
//!
//! Layout. One environment Stream, [`environment_path`], carries
//! [`SignedEnvironmentKeyring`] records. Each account has one child Stream,
//! [`account_path`], carrying that account's [`SignedKeyringSnapshot`] and
//! [`SignedRevocation`] records in one total order. Every record value is one
//! canonical [`TrustRecord`].
//!
//! Signers. A pinned anchor key signs environment keyring 1; each later
//! environment keyring is signed by a key of the previous one. An environment
//! key signs each account keyring, whose single `root_public_key` is the
//! account's only issuer root: rotation ends the previous root. The current
//! account root signs that account's revocations.
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
use futures::channel::oneshot;
use prost::Message;
use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex, RwLock,
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

struct Environment {
    name: String,
    keys: Arc<[BlockId]>,
    sequence: u64,
    digest: BlockId,
    issued_at: u64,
}

struct Keyring {
    sequence: u64,
    digest: BlockId,
    issued_at: u64,
    root: Arc<[BlockId]>,
    minimum_key_epoch: u64,
    revoked_keys: HashSet<String>,
    revoked_blocks: HashMap<BlockId, u64>,
}

#[derive(Default)]
struct Account {
    keyring: Option<Keyring>,
    revocations: Option<RevocationState>,
    /// The driver applied this account's Stream through its observed tail.
    serving: bool,
    /// A refused record; cleared only by a record that applies.
    poisoned: bool,
}

/// Follows signed trust records for any number of accounts of one environment.
pub struct Follower {
    environment: RwLock<Environment>,
    accounts: RwLock<HashMap<String, Account>>,
    open: AtomicBool,
    floor: AtomicU64,
    waiters: Mutex<Vec<oneshot::Sender<Result<(), SourceClosed>>>>,
}

impl Follower {
    /// A fenced follower pinned to `environment`'s anchor keys. It serves trust
    /// once its driver has applied every Stream up to the tail it observed and
    /// called [`Self::resume`].
    #[must_use]
    pub fn new(environment: impl Into<String>, anchor: Vec<BlockId>) -> Self {
        Self {
            environment: RwLock::new(Environment {
                name: environment.into(),
                keys: anchor.into(),
                sequence: 0,
                digest: [0; 32],
                issued_at: 0,
            }),
            accounts: RwLock::default(),
            open: AtomicBool::new(false),
            floor: AtomicU64::new(0),
            waiters: Mutex::default(),
        }
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
        let record = TrustRecordProto::decode(record)
            .map_err(|_| VerifyError::Malformed)
            .and_then(|proto| TrustRecord::try_from(proto).map_err(VerifyError::from))?;
        let result = match &record.record {
            TrustRecordKind::Environment(signed) => {
                let result = self.environment_record(signed, now);
                if result.is_err() {
                    self.fence();
                    return result;
                }
                result
            }
            TrustRecordKind::Keyring(signed) => self
                .account_record(&signed.snapshot.account_id, |account, environment| {
                    keyring(account, signed, environment, now)
                }),
            TrustRecordKind::Revocation(signed) => {
                let id = match &signed.publication {
                    RevocationPublication::Delta(delta) => &delta.account_id,
                    RevocationPublication::Checkpoint(checkpoint) => &checkpoint.account_id,
                };
                self.account_record(id, |account, _| revocation(account, id, signed, now))
            }
        };
        match result {
            Ok((Applied::Duplicate, _)) => {}
            Ok((Applied::Changed, issued_at)) => {
                self.floor.fetch_max(issued_at, Ordering::SeqCst);
                // Wake only after the write lock is released: woken
                // consumers read the swapped trust.
                self.notify(Ok(()));
            }
            // The account is fenced; its consumers recheck and refuse.
            Err(_) => self.notify(Ok(())),
        }
        result.map(|(applied, _)| applied)
    }

    fn environment_record(
        &self,
        signed: &SignedEnvironmentKeyring,
        now: u64,
    ) -> Result<(Applied, u64), VerifyError> {
        let mut environment = self
            .environment
            .write()
            .map_err(|_| VerifyError::Unavailable)?;
        let next = &signed.keyring;
        if next.environment != environment.name {
            return Err(VerifyError::Trust);
        }
        if next.sequence <= environment.sequence {
            return Ok((Applied::Duplicate, environment.issued_at));
        }
        if environment.sequence.checked_add(1) != Some(next.sequence)
            || next.previous_digest != environment.digest
            || next.issued_at_unix_seconds < environment.issued_at
            || next.issued_at_unix_seconds > now
        {
            return Err(VerifyError::Trust);
        }
        let mut message = Vec::new();
        claims_message(
            &mut message,
            ENVIRONMENT_DOMAIN,
            &EnvironmentKeyringProto::from(next.clone()),
        )?;
        signed_by(&environment.keys, &message, &signed.signature)?;
        let keys = next
            .keys
            .iter()
            .map(|key| {
                key.as_slice()
                    .try_into()
                    .map_err(|_| VerifyError::Malformed)
            })
            .collect::<Result<Vec<BlockId>, _>>()?;
        *environment = Environment {
            name: next.environment.clone(),
            keys: keys.into(),
            sequence: next.sequence,
            digest: block_id(&message, &signed.signature),
            issued_at: next.issued_at_unix_seconds,
        };
        Ok((Applied::Changed, next.issued_at_unix_seconds))
    }

    fn account_record(
        &self,
        id: &str,
        apply: impl FnOnce(&mut Account, &[BlockId]) -> Result<(Applied, u64), VerifyError>,
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
        let result = apply(account, &environment);
        match &result {
            Ok((Applied::Changed, _)) => account.poisoned = false,
            Ok((Applied::Duplicate, _)) => {}
            Err(_) => account.poisoned = true,
        }
        result
    }

    /// A lost follow: refuse all trust and resolve every change wait with
    /// [`SourceClosed`], so consumers fence live authority. Every account
    /// stays fenced until its Stream is caught up again ([`Self::serve`]).
    pub fn fence(&self) {
        self.open.store(false, Ordering::SeqCst);
        if let Ok(mut accounts) = self.accounts.write() {
            for account in accounts.values_mut() {
                account.serving = false;
            }
        }
        self.notify(Err(SourceClosed));
    }

    /// The driver applied the environment Stream through the tail it observed
    /// when it (re)connected. Redelivered records are duplicates; a gap is
    /// refused, so nothing is skipped.
    pub fn resume(&self) {
        self.open.store(true, Ordering::SeqCst);
        self.notify(Ok(()));
    }

    /// The driver applied `account`'s Stream through the tail it observed
    /// when it (re)connected; its trust may now be served.
    pub fn serve(&self, account: &str) {
        if let Ok(mut accounts) = self.accounts.write() {
            accounts.entry(account.to_owned()).or_default().serving = true;
        }
        self.notify(Ok(()));
    }

    /// Monotone protected time: never earlier than any time this follower has
    /// returned or any accepted publication. A rolled-back host clock cannot
    /// revive expired authority.
    pub fn protected_now(&self, observed_unix_seconds: u64) -> u64 {
        self.floor
            .fetch_max(observed_unix_seconds, Ordering::SeqCst)
            .max(observed_unix_seconds)
    }

    fn notify(&self, event: Result<(), SourceClosed>) {
        let waiters = match self.waiters.lock() {
            Ok(mut waiters) => std::mem::take(&mut *waiters),
            Err(_) => return,
        };
        for waiter in waiters {
            let _ = waiter.send(event);
        }
    }

    fn check(&self) -> Result<(), VerifyError> {
        if self.open.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(VerifyError::Unavailable)
        }
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
    signed_by(environment, &message, &signed.signature)?;
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
    let (sequence, issued_at) = match &signed.publication {
        RevocationPublication::Delta(delta) => (delta.sequence, delta.issued_at_unix_seconds),
        RevocationPublication::Checkpoint(checkpoint) => {
            (checkpoint.sequence, checkpoint.issued_at_unix_seconds)
        }
    };
    if let Some(state) = &mut account.revocations {
        if sequence <= state.frontier().0 {
            return Ok((Applied::Duplicate, state.floor().1));
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
            let (epoch, published) = revocations.floor();
            AccountState {
                issuer_keys: Keys::Shared(keyring.root.clone()),
                minimum_key_epoch: keyring.minimum_key_epoch.max(epoch),
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
        let (sender, receiver) = oneshot::channel();
        // Registered before the caller checks authority; `fence` stores the
        // flag before draining, so a registration either sees it or is drained.
        let registered = self.waiters.lock().is_ok_and(|mut waiters| {
            if !self.open.load(Ordering::SeqCst) {
                return false;
            }
            if waiters.len() == waiters.capacity() {
                waiters.retain(|waiter| !waiter.is_canceled());
            }
            waiters.push(sender);
            true
        });
        Box::pin(async move {
            if !registered {
                return Err(SourceClosed);
            }
            receiver.await.unwrap_or(Err(SourceClosed))
        })
    }
}

#[cfg(test)]
#[path = "follower_tests.rs"]
mod tests;
