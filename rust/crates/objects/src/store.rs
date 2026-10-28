//! The one blob engine, over any [`Medium`].
//!
//! Per account, under `blake3(account)`:
//! - `p/<aa>/<digest>.<version>` holds one put's payload, written first;
//! - `e/<aa>/<digest>` holds the envelope, whose creation publishes the blob;
//! - `r/<blake3(root)>/<blake3(batch)>.<nonce>` holds one retain's batch;
//! - `i/<child>/<holder>` marks one inbound reference: `e<parent>.<version>`
//!   for an envelope, `r<blake3(root)>.<blake3(batch)>.<nonce>` for a root batch;
//! - `g/<revision>` is a collection revision, created by its one collector;
//! - `a/<revision>/<digest>` admits a put at that revision.
//!
//! Delete never breaks a live reference, across processes, by two flags
//! over create-if-absent, conditional replace and listing. A holder creates
//! its `i/` key on each child, then refuses a child whose envelope is absent
//! or carries a deleter's mark; a deleter marks the envelope as its own with a
//! conditional replace, then refuses when the blob has an `i/` key. One of the
//! two always sees the other's flag.
//!
//! Destructive work is fenced by the exact bytes it observed, never by a
//! clock or by re-reading shared state. Each put writes a fresh version into
//! its envelope, and that version names its payload `p/<aa>/<digest>.<v>`
//! and its outbound index keys `e<digest>.<v>`. A deleter removes only the
//! envelope still holding its own mark, then that version's payload and keys,
//! so a deleter delayed across any number of collections and re-puts matches
//! nothing of a later blob. A kept blob whose mark outlived its deleter is
//! revived by collection, which also takes over the removal of an unkept one.
//!
//! Publish, then commit the reference. Collection is fenced by revision, never
//! by wall clock: the collection that creates revision `n` keeps every blob
//! admitted at `n - 1` or later, so a put protects its blob for one full
//! collection after it returns, within which a retain or a referencing put
//! must pin it. A put admits, creates its envelope pending, writes its
//! payload, then publishes by conditionally replacing its exact pending
//! bytes. Collection revokes a pending envelope only once its admission is
//! fenced, by conditionally removing those same bytes, and only then removes
//! the payload. One of the two conditional writes wins, so a put stalled
//! across any number of collections either publishes a payload no collector
//! removed or reports its outcome unknown; it never reports a lost payload
//! created. A put re-admits when collection advanced while it ran. An index
//! key of a holder that crashed before committing pins its child; it never
//! refuses wrongly in the unsafe direction.
use crate::blob::{Envelope, corrupt, decode_batch, encode_batch, invalid};
use crate::{
    ByteRange, Collection, Delete, Deletion, Digest, Error, Head, Kind, Manifest, Medium,
    MediumError, MemoryMedium, Object, Page, Put, Released, Retained, RootId, Usage, Verified,
    wire::ErrorCode,
};
use acyclic_stream::{AppliedRevisions, OwnedBytes, Revision};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

const PAGE: usize = 1000;

/// A medium shared by many accounts. Each [`Account`] is isolated: equal bytes
/// in two accounts are two blobs, and no account observes another's.
pub struct Store<M> {
    medium: Arc<M>,
    accounts: Arc<Mutex<HashMap<Digest, Arc<State>>>>,
}

impl<M> Clone for Store<M> {
    fn clone(&self) -> Self {
        Self {
            medium: Arc::clone(&self.medium),
            accounts: Arc::clone(&self.accounts),
        }
    }
}

#[derive(Default)]
struct State {
    gate: tokio::sync::RwLock<()>,
    /// This account's revisions, opened from the medium on first use.
    revisions: tokio::sync::OnceCell<Issued>,
}

/// One run's revisions of an account: every mutation commits the next index
/// once its effect is visible, so `current` covers it. Each open of a store
/// claims the next run of the account's stored incarnation, and each run is
/// its own authority: the account's scope bytes followed by the run, so
/// several stores over one medium never share an index sequence.
struct Issued {
    applied: AppliedRevisions,
    issued: AtomicU64,
    account: acyclic_stream::RevisionScope,
    run: u64,
}

impl Issued {
    fn new(account: acyclic_stream::RevisionScope, incarnation: u128, run: u64) -> Self {
        let scope = [account.as_bytes(), &run.to_be_bytes()].concat();
        Self {
            applied: AppliedRevisions::new(
                acyclic_stream::RevisionScope::new(scope),
                incarnation,
                0,
            ),
            issued: AtomicU64::new(0),
            account,
            run,
        }
    }

    /// The run of this account that `scope` names, if any.
    fn run_of(&self, scope: &acyclic_stream::RevisionScope) -> Option<u64> {
        let run = scope.as_bytes().strip_prefix(self.account.as_bytes())?;
        Some(u64::from_be_bytes(run.try_into().ok()?))
    }

    /// Commits one visible mutation.
    fn commit(&self) -> Revision {
        let index = self.issued.fetch_add(1, Ordering::AcqRel) + 1;
        self.applied.advance(index);
        self.applied.at(index)
    }

    /// Stamps a durable put with the revision committing it.
    fn committed(&self, put: Put) -> Put {
        match put {
            Put::Created { mut verified } => {
                verified.revision = Some(self.commit());
                Put::Created { verified }
            }
            Put::AlreadyExists { mut verified } => {
                verified.revision = Some(self.applied.current());
                Put::AlreadyExists { verified }
            }
            Put::Unknown => Put::Unknown,
        }
    }
}

/// An account's stored incarnation: 16 incarnation bytes, then the last
/// claimed run, both big-endian.
fn incarnation_record(incarnation: u128, run: u64) -> OwnedBytes {
    [incarnation.to_be_bytes().as_slice(), &run.to_be_bytes()]
        .concat()
        .into()
}

fn decode_incarnation(record: &[u8]) -> Result<(u128, u64), Error> {
    let (incarnation, run) = record.split_first_chunk::<16>().ok_or_else(corrupt)?;
    let run: [u8; 8] = run.try_into().map_err(|_| corrupt())?;
    Ok((u128::from_be_bytes(*incarnation), u64::from_be_bytes(run)))
}

impl<M: Medium> Store<M> {
    /// Shares `medium` among accounts.
    pub fn new(medium: M) -> Self {
        Self {
            medium: Arc::new(medium),
            accounts: Arc::default(),
        }
    }

    /// The blob namespace of one verified account.
    pub fn account(&self, account: &[u8]) -> Result<Account<M>, Error> {
        if account.is_empty() {
            return Err(invalid());
        }
        let id = Digest::of(account);
        let state = Arc::clone(
            self.accounts
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .entry(id)
                .or_default(),
        );
        Ok(Account {
            medium: Arc::clone(&self.medium),
            prefix: format!("{}/", id.to_hex()),
            state,
        })
    }

    /// The shared medium.
    pub fn medium(&self) -> &Arc<M> {
        &self.medium
    }
}

/// One account's blobs over a shared medium.
pub struct Account<M> {
    medium: Arc<M>,
    prefix: String,
    state: Arc<State>,
}

impl<M> Clone for Account<M> {
    fn clone(&self) -> Self {
        Self {
            medium: Arc::clone(&self.medium),
            prefix: self.prefix.clone(),
            state: Arc::clone(&self.state),
        }
    }
}

/// The deterministic in-process provider.
pub type MemoryObjects = Account<MemoryMedium>;

impl MemoryObjects {
    /// A fresh in-memory account.
    #[must_use]
    pub fn memory() -> Self {
        Self::over_memory(MemoryMedium::default())
    }

    /// A fresh in-memory account whose stored bytes never exceed `budget`.
    #[must_use]
    pub fn bounded(budget: u64) -> Self {
        Self::over_memory(MemoryMedium::bounded(budget))
    }

    fn over_memory(medium: MemoryMedium) -> Self {
        Self {
            medium: Arc::new(medium),
            prefix: format!("{}/", Digest::of(b"memory").to_hex()),
            state: Arc::default(),
        }
    }
}

impl Default for MemoryObjects {
    fn default() -> Self {
        Self::memory()
    }
}

fn medium(error: MediumError) -> Error {
    match error {
        MediumError::Ambiguous(_) | MediumError::Failed(_) => ErrorCode::Unavailable.into(),
    }
}

/// A fresh owner nonce.
fn nonce() -> Result<String, Error> {
    Ok(hex::encode(version()?))
}

/// Fresh bytes unique to one write.
fn version() -> Result<[u8; 16], Error> {
    let mut version = [0; 16];
    getrandom::fill(&mut version).map_err(|_| Error::from(ErrorCode::Unavailable))?;
    Ok(version)
}

/// A conditional write's unknown outcome means this actor's work may no
/// longer be its own.
fn moved(error: MediumError) -> Error {
    match error {
        MediumError::Ambiguous(_) => ErrorCode::OutcomeUnknown.into(),
        error => medium(error),
    }
}

fn canonical(mut references: Vec<Digest>) -> Vec<Digest> {
    references.sort_unstable();
    references.dedup();
    references
}

impl<M> Account<M> {
    /// This account's medium.
    pub(crate) fn shared_medium(&self) -> Arc<M> {
        Arc::clone(&self.medium)
    }

    /// The same account, sharing its state, over `medium`.
    pub(crate) fn over<N>(&self, medium: Arc<N>) -> Account<N> {
        Account {
            medium,
            prefix: self.prefix.clone(),
            state: Arc::clone(&self.state),
        }
    }
}

impl<M: Medium> Account<M> {
    fn payload_key(&self, digest: &Digest, version: &[u8; 16]) -> String {
        let hex = digest.to_hex();
        format!(
            "{}p/{}/{hex}.{}",
            self.prefix,
            &hex[..2],
            hex::encode(version)
        )
    }

    /// This account's revisions, opening them on first use.
    async fn issued(&self) -> Result<&Issued, Error> {
        self.state
            .revisions
            .get_or_try_init(|| self.open_revisions())
            .await
    }

    fn incarnation_key(&self) -> String {
        format!("{}incarnation", self.prefix)
    }

    /// Reads back the incarnation this account was created with, creating it
    /// for a new or wiped account, and claims the next run.
    async fn open_revisions(&self) -> Result<Issued, Error> {
        let key = self.incarnation_key();
        let account = AppliedRevisions::storage_scope("objects", self.prefix.as_bytes());
        loop {
            let Some(record) = self.medium.read(&key, None).await.map_err(medium)? else {
                let incarnation = u128::from_be_bytes(version()?);
                let record = incarnation_record(incarnation, 1);
                if self.medium.create(&key, record).await.map_err(medium)? {
                    return Ok(Issued::new(account, incarnation, 1));
                }
                continue;
            };
            let (incarnation, run) = decode_incarnation(&record)?;
            let run = run.checked_add(1).ok_or_else(corrupt)?;
            let claimed = incarnation_record(incarnation, run);
            if self
                .medium
                .replace(&key, &record, claimed)
                .await
                .map_err(medium)?
            {
                return Ok(Issued::new(account, incarnation, run));
            }
        }
    }

    fn envelope_key(&self, digest: &Digest) -> String {
        let hex = digest.to_hex();
        format!("{}e/{}/{hex}", self.prefix, &hex[..2])
    }

    fn root_prefix(&self, root: &RootId) -> String {
        format!("{}r/{}/", self.prefix, Digest::of(root.as_bytes()).to_hex())
    }

    fn index_prefix(&self, child: &Digest) -> String {
        format!("{}i/{}/", self.prefix, child.to_hex())
    }

    /// Whether any key exists under `prefix`.
    async fn any(&self, prefix: &str) -> Result<bool, Error> {
        Ok(!self
            .medium
            .list(prefix, None, 1)
            .await
            .map_err(medium)?
            .is_empty())
    }

    /// The `i/` holder tag of an envelope.
    fn envelope_tag(parent: &Digest, version: &[u8; 16]) -> String {
        format!("e{}.{}", parent.to_hex(), hex::encode(version))
    }

    /// The `i/` holder tag of the root batch named `batch`.
    fn batch_tag(root: &RootId, batch: &str) -> String {
        format!("r{}.{batch}", Digest::of(root.as_bytes()).to_hex())
    }

    /// Marks `tag` as holding each of `children`, then admits them: refused
    /// when one is absent or marked by a deleter. `None` means an index write's outcome
    /// is unknown, so the holder must not commit.
    async fn reference(&self, children: &[Digest], tag: &str) -> Result<Option<()>, Error> {
        for child in children {
            match self
                .medium
                .create(
                    &format!("{}{tag}", self.index_prefix(child)),
                    OwnedBytes::new(),
                )
                .await
            {
                Ok(_) => {}
                Err(MediumError::Ambiguous(_)) => return Ok(None),
                Err(error) => return Err(medium(error)),
            }
        }
        for child in children {
            match self.envelope(child).await? {
                Some(envelope) if envelope.dying.is_none() => {}
                _ => return Err(ErrorCode::MissingReference.into()),
            }
        }
        Ok(Some(()))
    }

    /// Removes `tag`'s index keys, once its holder is gone.
    async fn unreference(&self, children: &[Digest], tag: &str) -> Result<(), Error> {
        for child in children {
            self.medium
                .remove(&format!("{}{tag}", self.index_prefix(child)))
                .await
                .map_err(medium)?;
        }
        Ok(())
    }

    fn digest_from_key(&self, key: &str) -> Result<Digest, Error> {
        key.rsplit('/')
            .next()
            .and_then(|name| name.split('.').next())
            .ok_or_else(corrupt)
            .and_then(|hex| Digest::from_hex(hex).map_err(|_| corrupt()))
    }

    /// The digest and version a payload key names.
    fn payload_of(&self, key: &str) -> Result<(Digest, [u8; 16]), Error> {
        let version = key
            .rsplit_once('.')
            .and_then(|(_, version)| hex::decode(version).ok())
            .and_then(|version| version.try_into().ok())
            .ok_or_else(corrupt)?;
        Ok((self.digest_from_key(key)?, version))
    }

    fn revision_key(&self, revision: u64) -> String {
        format!("{}g/{revision:020}", self.prefix)
    }

    fn admission_prefix(&self, revision: u64) -> String {
        format!("{}a/{revision:020}/", self.prefix)
    }

    /// The revision a `g/` or `a/` key names.
    fn revision_of(key: &str, depth: usize) -> Result<u64, Error> {
        key.rsplit('/')
            .nth(depth)
            .and_then(|revision| revision.parse().ok())
            .ok_or_else(corrupt)
    }

    /// The current collection revision: the greatest `g/` key, zero before any.
    async fn revision(&self) -> Result<u64, Error> {
        let mut current = 0;
        for (key, _) in self.list_all(&format!("{}g/", self.prefix)).await? {
            current = current.max(Self::revision_of(&key, 0)?);
        }
        Ok(current)
    }

    /// Admits `digest` at the current revision, confirmed by reading the
    /// revision again after the write, and returns that revision. `None`
    /// means a write's outcome is unknown.
    async fn admit(&self, digest: &Digest) -> Result<Option<u64>, Error> {
        let mut revision = self.revision().await?;
        loop {
            let key = format!("{}{}", self.admission_prefix(revision), digest.to_hex());
            match self.medium.create(&key, OwnedBytes::new()).await {
                Ok(_) => {}
                Err(MediumError::Ambiguous(_)) => return Ok(None),
                Err(error) => return Err(medium(error)),
            }
            let current = self.revision().await?;
            if current == revision {
                return Ok(Some(revision));
            }
            revision = current;
        }
    }

    /// Whether `digest` was admitted at revision `from` or later.
    async fn admitted(&self, digest: &Digest, from: u64) -> Result<bool, Error> {
        for revision in from..=self.revision().await? {
            let key = format!("{}{}", self.admission_prefix(revision), digest.to_hex());
            if self
                .medium
                .read(&key, Some(0..0))
                .await
                .map_err(medium)?
                .is_some()
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Whether `digest` is referenced or, for a collection fenced at `fence`,
    /// admitted since.
    async fn held(&self, digest: &Digest, fence: Option<u64>) -> Result<bool, Error> {
        if let Some(from) = fence {
            if self.admitted(digest, from).await? {
                return Ok(true);
            }
        }
        self.any(&self.index_prefix(digest)).await
    }

    /// Removes `digest` unless it is held. The deleter marks the envelope as
    /// its own, checks for holders, then removes exactly the envelope bearing
    /// its mark: a later blob, a revival or another deleter's takeover makes
    /// that removal match nothing, however long the deleter was delayed.
    /// `OutcomeUnknown` means another actor moved the envelope first.
    async fn remove_blob(&self, digest: Digest, fence: Option<u64>) -> Result<Delete, Error> {
        let key = self.envelope_key(&digest);
        let Some((observed, envelope)) = self.stored_envelope(&digest).await? else {
            return Ok(Delete::Absent);
        };
        if envelope.pending {
            // Its put owns it until collection fences the put's admission.
            return match fence {
                Some(_) => self.revoke(digest, &key, &observed, &envelope, fence).await,
                None => Ok(Delete::Absent),
            };
        }
        if self.held(&digest, fence).await? {
            return Ok(Delete::Referenced);
        }
        let marked = Envelope {
            dying: Some(version()?),
            ..envelope.clone()
        }
        .encode();
        if !self
            .medium
            .replace(&key, &observed, marked.clone().into())
            .await
            .map_err(moved)?
        {
            return Err(ErrorCode::OutcomeUnknown.into());
        }
        if self.held(&digest, fence).await? {
            self.revive(&key, &marked, envelope).await?;
            return Ok(Delete::Referenced);
        }
        if !self.medium.remove_if(&key, &marked).await.map_err(moved)? {
            return Err(ErrorCode::OutcomeUnknown.into());
        }
        // This blob's own keys: no later blob of the digest shares them.
        self.medium
            .remove(&self.payload_key(&digest, &envelope.version))
            .await
            .map_err(medium)?;
        self.unreference(
            &envelope.references,
            &Self::envelope_tag(&digest, &envelope.version),
        )
        .await?;
        Ok(Delete::Deleted)
    }

    /// Clears the mark `marked` from a kept blob, unless another actor
    /// already moved it; an unknown outcome is left for collection.
    async fn revive(&self, key: &str, marked: &[u8], envelope: Envelope) -> Result<(), Error> {
        let live = Envelope {
            dying: None,
            ..envelope
        };
        match self.medium.replace(key, marked, live.encode().into()).await {
            Ok(_) | Err(MediumError::Ambiguous(_)) => Ok(()),
            Err(error) => Err(medium(error)),
        }
    }

    /// Collection's removal: an actor that moved first keeps the blob.
    async fn sweep(&self, digest: Digest, fence: u64) -> Result<Delete, Error> {
        match self.remove_blob(digest, Some(fence)).await {
            Err(error) if error.code == ErrorCode::OutcomeUnknown => Ok(Delete::Referenced),
            outcome => outcome,
        }
    }

    /// The published envelope of `digest`; a pending one is not yet a blob.
    async fn envelope(&self, digest: &Digest) -> Result<Option<Envelope>, Error> {
        Ok(self
            .stored_envelope(digest)
            .await?
            .map(|(_, envelope)| envelope)
            .filter(|envelope| !envelope.pending))
    }

    /// Revokes a pending envelope whose put's admission is fenced, then
    /// removes that put's payload and keys. Its put's publication replaces
    /// the same exact bytes, so exactly one of the two succeeds.
    async fn revoke(
        &self,
        digest: Digest,
        key: &str,
        observed: &[u8],
        envelope: &Envelope,
        fence: Option<u64>,
    ) -> Result<Delete, Error> {
        if self.held(&digest, fence).await? {
            return Ok(Delete::Referenced);
        }
        if !self.medium.remove_if(key, observed).await.map_err(moved)? {
            return Err(ErrorCode::OutcomeUnknown.into());
        }
        self.clear(&digest, envelope).await?;
        Ok(Delete::Absent)
    }

    /// Removes the payload and index keys of a pending version no longer on
    /// its envelope key; no publication of that version can follow.
    async fn clear(&self, digest: &Digest, envelope: &Envelope) -> Result<(), Error> {
        self.medium
            .remove(&self.payload_key(digest, &envelope.version))
            .await
            .map_err(medium)?;
        self.unreference(
            &envelope.references,
            &Self::envelope_tag(digest, &envelope.version),
        )
        .await
    }

    /// The envelope with the exact bytes a conditional write must match.
    async fn stored_envelope(
        &self,
        digest: &Digest,
    ) -> Result<Option<(OwnedBytes, Envelope)>, Error> {
        let Some(bytes) = self
            .medium
            .read(&self.envelope_key(digest), None)
            .await
            .map_err(medium)?
        else {
            return Ok(None);
        };
        let envelope = Envelope::decode(&bytes)?;
        Ok(Some((bytes, envelope)))
    }

    async fn list_all(&self, prefix: &str) -> Result<Vec<(String, u64)>, Error> {
        let mut keys = Vec::new();
        loop {
            let page = self
                .medium
                .list(
                    prefix,
                    keys.last().map(|(key, _): &(String, u64)| key.as_str()),
                    PAGE,
                )
                .await
                .map_err(medium)?;
            let done = page.len() < PAGE;
            keys.extend(page);
            if done {
                return Ok(keys);
            }
        }
    }

    /// Publishes payload then envelope under a fresh version; the envelope is
    /// the commit point.
    async fn publish(
        &self,
        digest: Digest,
        stored: OwnedBytes,
        mut envelope: Envelope,
    ) -> Result<Put, Error> {
        if let Some(existing) = self.envelope(&digest).await? {
            return self.existing(digest, &existing, &envelope).await;
        }
        envelope.version = version()?;
        let tag = Self::envelope_tag(&digest, &envelope.version);
        match self.reference(&envelope.references, &tag).await {
            Ok(Some(())) => {}
            Ok(None) => return Ok(Put::Unknown),
            Err(error) => {
                self.unreference(&envelope.references, &tag).await?;
                return Err(error);
            }
        }
        // Admit before anything exists: collection keeps what was admitted
        // since its fence, and revokes only what it fenced.
        let Some(admitted) = self.admit(&digest).await? else {
            return Ok(Put::Unknown);
        };
        let key = self.envelope_key(&digest);
        let pending = Envelope {
            pending: true,
            ..envelope.clone()
        }
        .encode();
        match self.medium.create(&key, pending.clone().into()).await {
            Ok(true) => {}
            Ok(false) => match self.stored_envelope(&digest).await? {
                // An abandoned or concurrent put's pending envelope: take it
                // over by its exact bytes. That put's publication then never
                // matches, so its payload and keys are cleared here.
                Some((observed, abandoned)) if abandoned.pending => {
                    match self
                        .medium
                        .replace(&key, &observed, pending.clone().into())
                        .await
                    {
                        Ok(true) => {
                            self.clear(&digest, &abandoned).await?;
                        }
                        Ok(false) | Err(MediumError::Ambiguous(_)) => {
                            self.unreference(&envelope.references, &tag).await?;
                            return Ok(Put::Unknown);
                        }
                        Err(error) => return Err(medium(error)),
                    }
                }
                // An equal put won; these keys are this one's.
                _ => {
                    self.unreference(&envelope.references, &tag).await?;
                    return match self.envelope(&digest).await? {
                        Some(existing) => self.existing(digest, &existing, &envelope).await,
                        None => Ok(Put::Unknown),
                    };
                }
            },
            Err(MediumError::Ambiguous(_)) => return Ok(Put::Unknown),
            Err(error) => return Err(medium(error)),
        }
        let payload = self.payload_key(&digest, &envelope.version);
        match self.medium.create(&payload, stored).await {
            Ok(_) => {}
            Err(MediumError::Ambiguous(_)) => return Ok(Put::Unknown),
            Err(error) => return Err(medium(error)),
        }
        // Publishes only while no collector revoked the pending bytes.
        match self
            .medium
            .replace(&key, &pending, envelope.encode().into())
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                // Collection fenced this put and owns its removal.
                self.medium.remove(&payload).await.map_err(medium)?;
                self.unreference(&envelope.references, &tag).await?;
                return Ok(Put::Unknown);
            }
            Err(MediumError::Ambiguous(_)) => return Ok(Put::Unknown),
            Err(error) => return Err(medium(error)),
        }
        // The blob stays protected for a full collection after this returns.
        // Collection advanced while this put ran: re-admit, then confirm the
        // published version survived, as an existing blob's put does.
        if self.revision().await? != admitted {
            if self.admit(&digest).await?.is_none() {
                return Ok(Put::Unknown);
            }
            match self.envelope(&digest).await? {
                Some(current) if current.version == envelope.version && current.dying.is_none() => {
                }
                _ => return Ok(Put::Unknown),
            }
        }
        Ok(Put::Created {
            verified: Verified {
                digest,
                length: envelope.length,
                revision: None,
            },
        })
    }

    /// An envelope observed present is durable unless a deleter holds it.
    async fn existing(
        &self,
        digest: Digest,
        existing: &Envelope,
        wanted: &Envelope,
    ) -> Result<Put, Error> {
        if existing.kind != wanted.kind
            || existing.length != wanted.length
            || existing.stored != wanted.stored
        {
            return Err(corrupt());
        }
        if existing.references != wanted.references {
            return Err(ErrorCode::Conflict.into());
        }
        if existing.dying.is_some() || self.admit(&digest).await?.is_none() {
            return Ok(Put::Unknown);
        }
        // Admitted, then still unmarked: a deleter marking it later checks
        // admissions after, and a mark already made is seen here.
        match self.envelope(&digest).await? {
            Some(current) if current.version == existing.version && current.dying.is_none() => {}
            _ => return Ok(Put::Unknown),
        }
        Ok(Put::AlreadyExists {
            verified: Verified {
                digest,
                length: existing.length,
                revision: None,
            },
        })
    }

    async fn read_payload(
        &self,
        digest: &Digest,
        envelope: &Envelope,
        range: Option<std::ops::Range<u64>>,
    ) -> Result<OwnedBytes, Error> {
        let full = range.is_none();
        let bytes = self
            .medium
            .read(&self.payload_key(digest, &envelope.version), range)
            .await
            .map_err(medium)?
            .ok_or_else(corrupt)?;
        if full && (bytes.len() as u64 != envelope.stored || Digest::of(&bytes) != *digest) {
            return Err(corrupt());
        }
        Ok(bytes)
    }

    async fn manifest(&self, digest: &Digest, envelope: &Envelope) -> Result<Manifest, Error> {
        Manifest::decode(&self.read_payload(digest, envelope, None).await?)
    }

    /// Appends the selected logical bytes of `digest` to `out`.
    fn select<'a>(
        &'a self,
        digest: Digest,
        envelope: Envelope,
        range: std::ops::Range<u64>,
        out: &'a mut Vec<OwnedBytes>,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>> {
        Box::pin(async move {
            if range.is_empty() {
                return Ok(());
            }
            match envelope.kind {
                Kind::Blob => {
                    let whole = range.start == 0 && range.end == envelope.length;
                    out.push(
                        self.read_payload(&digest, &envelope, (!whole).then_some(range))
                            .await?,
                    );
                }
                Kind::Manifest => {
                    let manifest = self.manifest(&digest, &envelope).await?;
                    let mut offset = 0u64;
                    for part in manifest.parts {
                        let end = offset.checked_add(part.length).ok_or_else(corrupt)?;
                        let start = range.start.max(offset);
                        let stop = range.end.min(end);
                        if start < stop {
                            let child = self.envelope(&part.digest).await?.ok_or_else(corrupt)?;
                            if child.length != part.length {
                                return Err(corrupt());
                            }
                            self.select(part.digest, child, start - offset..stop - offset, out)
                                .await?;
                        }
                        if end >= range.end {
                            break;
                        }
                        offset = end;
                    }
                }
            }
            Ok(())
        })
    }

    /// Every reference held by a retained root. Any failure is `Suspended`:
    /// an incomplete scan never means an empty root set.
    async fn rooted(&self) -> Result<Vec<Digest>, Error> {
        let suspended = |_| Error::from(ErrorCode::Suspended);
        let mut references = Vec::new();
        for (key, _) in self
            .list_all(&format!("{}r/", self.prefix))
            .await
            .map_err(suspended)?
        {
            let batch = self
                .medium
                .read(&key, None)
                .await
                .map_err(medium)
                .and_then(|bytes| bytes.ok_or_else(corrupt))
                .map_err(suspended)?;
            references.extend(decode_batch(&batch).map_err(suspended)?);
        }
        Ok(references)
    }

    fn head_of(digest: Digest, envelope: Envelope) -> Head {
        Head {
            digest,
            kind: envelope.kind,
            length: envelope.length,
            references: envelope.references,
            revision: None,
        }
    }

    pub(crate) async fn get_inner(
        &self,
        digest: Digest,
        range: ByteRange,
        maximum_bytes: u64,
    ) -> Result<Object, Error> {
        // Everything committed by `served` is visible to the read after it.
        let served = self.issued().await?.applied.current();
        let envelope = self
            .envelope(&digest)
            .await?
            .ok_or(Error::from(ErrorCode::NotFound))?;
        let selected = range.resolve(envelope.length)?;
        if selected.end - selected.start > maximum_bytes {
            return Err(ErrorCode::QuotaExceeded.into());
        }
        let mut parts = Vec::new();
        self.select(digest, envelope.clone(), selected.clone(), &mut parts)
            .await?;
        let body = match parts.len() {
            0 => OwnedBytes::new(),
            1 => parts.pop().ok_or_else(corrupt)?,
            _ => {
                self.medium
                    .allocate(selected.end - selected.start)
                    .map_err(medium)?;
                let mut bytes = Vec::new();
                bytes
                    .try_reserve_exact(
                        usize::try_from(selected.end - selected.start)
                            .map_err(|_| Error::from(ErrorCode::QuotaExceeded))?,
                    )
                    .map_err(|_| Error::from(ErrorCode::QuotaExceeded))?;
                for part in &parts {
                    bytes.extend_from_slice(part);
                }
                bytes.into()
            }
        };
        if body.len() as u64 != selected.end - selected.start {
            return Err(corrupt());
        }
        Ok(
            Object::new(Self::head_of(digest, envelope), selected.start, body)
                .served_at(Some(served)),
        )
    }

    pub(crate) async fn put_inner(
        &self,
        body: OwnedBytes,
        references: Vec<Digest>,
    ) -> Result<Put, Error> {
        let references = canonical(references);
        let issued = self.issued().await?;
        let _gate = self.state.gate.read().await;
        let digest = Digest::of(&body);
        let length = body.len() as u64;
        self.publish(
            digest,
            body,
            Envelope {
                kind: Kind::Blob,
                length,
                stored: length,
                references,
                // Assigned by `publish`.
                version: [0; 16],
                dying: None,
                pending: false,
            },
        )
        .await
        .map(|put| issued.committed(put))
    }
}

#[async_trait::async_trait]
impl<M: Medium> crate::Blobs for Account<M> {
    async fn put(&self, body: OwnedBytes, references: Vec<Digest>) -> Result<Put, Error> {
        self.put_inner(body, references).await
    }

    async fn put_manifest(&self, manifest: Manifest) -> Result<Put, Error> {
        if manifest.parts.is_empty() {
            return Err(invalid());
        }
        let length = manifest.length()?;
        let issued = self.issued().await?;
        let _gate = self.state.gate.read().await;
        for part in &manifest.parts {
            let envelope = self
                .envelope(&part.digest)
                .await?
                .ok_or(Error::from(ErrorCode::MissingReference))?;
            if envelope.length != part.length {
                return Err(invalid());
            }
        }
        let encoded = manifest.encode();
        let digest = Digest::of(&encoded);
        let stored = encoded.len() as u64;
        self.publish(
            digest,
            encoded.into(),
            Envelope {
                kind: Kind::Manifest,
                length,
                stored,
                references: canonical(manifest.references()),
                // Assigned by `publish`.
                version: [0; 16],
                dying: None,
                pending: false,
            },
        )
        .await
        .map(|put| issued.committed(put))
    }

    async fn get(
        &self,
        digest: Digest,
        range: ByteRange,
        maximum_bytes: u64,
    ) -> Result<Object, Error> {
        self.get_inner(digest, range, maximum_bytes).await
    }

    async fn head(&self, digest: Digest) -> Result<Head, Error> {
        let served = self.issued().await?.applied.current();
        let envelope = self
            .envelope(&digest)
            .await?
            .ok_or(Error::from(ErrorCode::NotFound))?;
        let mut head = Self::head_of(digest, envelope);
        head.revision = Some(served);
        Ok(head)
    }

    async fn delete(&self, digest: Digest) -> Result<Deletion, Error> {
        let issued = self.issued().await?;
        let _gate = self.state.gate.read().await;
        let outcome = self.remove_blob(digest, None).await?;
        let revision = if outcome == Delete::Deleted {
            issued.commit()
        } else {
            issued.applied.current()
        };
        Ok(Deletion {
            outcome,
            revision: Some(revision),
        })
    }

    async fn list(&self, after: Option<Digest>, limit: u32) -> Result<Page, Error> {
        if limit == 0 {
            return Err(invalid());
        }
        let served = self.issued().await?.applied.current();
        let after = after.map(|digest| self.envelope_key(&digest));
        let limit = usize::try_from(limit).map_err(|_| invalid())?;
        // Pending envelopes are not blobs: page past them to a full page.
        let mut digests = Vec::new();
        let mut cursor = after;
        loop {
            let wanted = limit - digests.len();
            let keys = self
                .medium
                .list(&format!("{}e/", self.prefix), cursor.as_deref(), wanted)
                .await
                .map_err(medium)?;
            for (key, size) in &keys {
                if Envelope::published_size(*size) {
                    digests.push(self.digest_from_key(key)?);
                }
            }
            if keys.len() < wanted || digests.len() == limit {
                break;
            }
            cursor = keys.last().map(|(key, _)| key.clone());
        }
        let next = (digests.len() == limit)
            .then(|| digests.last().copied())
            .flatten();
        Ok(Page {
            digests,
            next,
            revision: Some(served),
        })
    }

    async fn retain(&self, root: &RootId, references: Vec<Digest>) -> Result<Retained, Error> {
        let references = canonical(references);
        if references.is_empty() {
            return Err(invalid());
        }
        let issued = self.issued().await?;
        let _gate = self.state.gate.read().await;
        let batch = encode_batch(&references);
        // Each retain owns its batch and keys, so a concurrent release of an
        // equal batch never removes them.
        let name = format!("{}.{}", Digest::of(&batch).to_hex(), nonce()?);
        let tag = Self::batch_tag(root, &name);
        match self.reference(&references, &tag).await {
            Ok(Some(())) => {}
            Ok(None) => return Err(ErrorCode::OutcomeUnknown.into()),
            Err(error) => {
                self.unreference(&references, &tag).await?;
                return Err(error);
            }
        }
        let key = format!("{}{name}", self.root_prefix(root));
        match self.medium.create(&key, batch.into()).await {
            Ok(_) => {}
            Err(MediumError::Ambiguous(_)) => return Err(ErrorCode::OutcomeUnknown.into()),
            Err(error) => return Err(medium(error)),
        }
        Ok(Retained {
            references: references.len() as u64,
            revision: Some(issued.commit()),
        })
    }

    async fn release(&self, root: &RootId) -> Result<Released, Error> {
        let issued = self.issued().await?;
        let _gate = self.state.gate.read().await;
        let batches = self.list_all(&self.root_prefix(root)).await?;
        for (key, _) in &batches {
            let Some(batch) = self.medium.read(key, None).await.map_err(medium)? else {
                continue;
            };
            let references = decode_batch(&batch)?;
            match self.medium.remove(key).await {
                Ok(_) => {}
                Err(MediumError::Ambiguous(_)) => return Err(ErrorCode::OutcomeUnknown.into()),
                Err(error) => return Err(medium(error)),
            }
            let tag = Self::batch_tag(root, key.rsplit('/').next().ok_or_else(corrupt)?);
            self.unreference(&references, &tag).await?;
        }
        let revision = if batches.is_empty() {
            issued.applied.current()
        } else {
            issued.commit()
        };
        Ok(Released {
            existed: !batches.is_empty(),
            revision: Some(revision),
        })
    }

    async fn collect(&self) -> Result<Collection, Error> {
        let issued = self.issued().await?;
        let _gate = self.state.gate.write().await;
        let fence = self.revision().await?;
        let epoch = fence + 1;
        // One collector wins each revision, across processes.
        match self
            .medium
            .create(&self.revision_key(epoch), OwnedBytes::new())
            .await
        {
            Ok(true) => {}
            Ok(false) => return Err(ErrorCode::Conflict.into()),
            Err(MediumError::Ambiguous(_)) => return Err(ErrorCode::OutcomeUnknown.into()),
            Err(error) => return Err(medium(error)),
        }
        for (key, _) in self.list_all(&format!("{}g/", self.prefix)).await? {
            if Self::revision_of(&key, 0)? < fence {
                self.medium.remove(&key).await.map_err(medium)?;
            }
        }
        let mut marked = HashSet::new();
        let mut marks = Vec::new();
        let mut pending = self.rooted().await?;
        for revision in [fence, epoch] {
            for (key, _) in self.list_all(&self.admission_prefix(revision)).await? {
                pending.push(self.digest_from_key(&key)?);
            }
        }
        while let Some(digest) = pending.pop() {
            if !marked.insert(digest) {
                continue;
            }
            match self.stored_envelope(&digest).await {
                Ok(Some((observed, envelope))) => {
                    pending.extend(envelope.references.iter().copied());
                    if envelope.dying.is_some() {
                        marks.push((digest, observed, envelope));
                    }
                }
                // A retained reference to nothing protects nothing.
                Ok(None) => {}
                Err(_) => return Err(ErrorCode::Suspended.into()),
            }
        }
        // A kept blob outlives any deleter's mark.
        for (digest, observed, envelope) in marks {
            self.revive(&self.envelope_key(&digest), &observed, envelope)
                .await?;
        }
        let mut collection = Collection {
            epoch,
            ..Collection::default()
        };
        for (key, _) in self.list_all(&format!("{}e/", self.prefix)).await? {
            let digest = self.digest_from_key(&key)?;
            if marked.contains(&digest) {
                collection.marked += 1;
                continue;
            }
            let stored = self
                .envelope(&digest)
                .await?
                .map_or(0, |envelope| envelope.stored);
            if self.sweep(digest, fence).await? == Delete::Deleted {
                collection.swept += 1;
                collection.swept_bytes += stored;
            }
        }
        // Payloads of no envelope, or of one a parent's sweep above unpinned.
        for (key, size) in self.list_all(&format!("{}p/", self.prefix)).await? {
            let (digest, version) = self.payload_of(&key)?;
            if marked.contains(&digest) || self.admitted(&digest, fence).await? {
                continue;
            }
            match self.stored_envelope(&digest).await? {
                // A pending one is revoked before its payload goes.
                Some((_, envelope)) if envelope.version == version => {
                    if self.sweep(digest, fence).await? == Delete::Deleted {
                        collection.swept += 1;
                        collection.swept_bytes += size;
                    }
                }
                // Its put was revoked, lost to an equal put, or its deleter
                // stopped: no publication of this version can follow.
                _ => {
                    self.medium.remove(&key).await.map_err(medium)?;
                    collection.swept_bytes += size;
                }
            }
        }
        // Admissions before the fence no longer protect anything.
        for (key, _) in self.list_all(&format!("{}a/", self.prefix)).await? {
            if Self::revision_of(&key, 1)? < fence {
                self.medium.remove(&key).await.map_err(medium)?;
            }
        }
        collection.revision = Some(if collection.swept > 0 {
            issued.commit()
        } else {
            issued.applied.current()
        });
        Ok(collection)
    }

    async fn revisions(&self) -> Result<Option<&AppliedRevisions>, Error> {
        Ok(Some(&self.issued().await?.applied))
    }

    /// Every run commits only after its write is visible on the medium, so a
    /// minimum in another claimed run of this incarnation is already applied
    /// here. This run's own minimum still waits on its exact applied index.
    async fn admitted(
        &self,
        mut minimum: acyclic_stream::RevisionCut,
    ) -> Result<acyclic_stream::RevisionCut, Error> {
        let issued = self.issued().await?;
        let incarnation = issued.applied.current().incarnation;
        let runs: Vec<_> = minimum
            .iter()
            .filter_map(|wanted| {
                let run = issued.run_of(&wanted.scope)?;
                Some((wanted.scope.clone(), wanted.incarnation, run))
            })
            .collect();
        let mut claimed = None;
        for (scope, wanted, run) in runs {
            if wanted != incarnation {
                return Err(ErrorCode::RevisionForeign.into());
            }
            if run == issued.run {
                continue;
            }
            if run > issued.run {
                let claimed = match claimed {
                    Some(claimed) => claimed,
                    None => {
                        let record = self
                            .medium
                            .read(&self.incarnation_key(), None)
                            .await
                            .map_err(medium)?
                            .ok_or(Error::from(ErrorCode::RevisionForeign))?;
                        let (stored, run) = decode_incarnation(&record)?;
                        if stored != incarnation {
                            return Err(ErrorCode::RevisionForeign.into());
                        }
                        *claimed.insert(run)
                    }
                };
                // A run nobody claimed issued nothing; the wait refuses it.
                if run > claimed {
                    continue;
                }
            }
            minimum.forget(&scope);
        }
        Ok(minimum)
    }

    async fn usage(&self) -> Result<Usage, Error> {
        let mut usage = Usage::default();
        let mut after: Option<String> = None;
        loop {
            let page = self
                .medium
                .list(&format!("{}e/", self.prefix), after.as_deref(), PAGE)
                .await
                .map_err(medium)?;
            for (key, _) in &page {
                let digest = self.digest_from_key(key)?;
                if let Some(envelope) = self.envelope(&digest).await? {
                    usage.blobs += 1;
                    usage.unique_bytes += envelope.stored;
                }
            }
            if page.len() < PAGE {
                return Ok(usage);
            }
            after = page.last().map(|(key, _)| key.clone());
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn get_native(
        &self,
        request: crate::ObjectsNativeRequest<(Digest, ByteRange, u64)>,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<crate::ObjectsNativeFuture<'_, Object>, crate::NativeObjectsError> {
        crate::native::get(self, request, meter)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn get_batch_native(
        &self,
        requests: crate::ObjectsNativeRequest<Vec<(Digest, ByteRange, u64)>>,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<
        crate::ObjectsNativeFuture<'_, crate::ObjectsBatch<Object>>,
        crate::NativeObjectsError,
    > {
        crate::native::get_batch(self, requests, meter)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn put_native(
        &self,
        request: crate::ObjectsNativeRequest<(OwnedBytes, Vec<Digest>)>,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<crate::ObjectsNativeFuture<'_, crate::ObjectsPutReceipt>, crate::NativeObjectsError>
    {
        crate::native::put(self, request, meter)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn put_batch_native(
        &self,
        requests: crate::ObjectsNativeRequest<Vec<(OwnedBytes, Vec<Digest>)>>,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<
        crate::ObjectsNativeFuture<'_, crate::ObjectsBatch<crate::ObjectsPutReceipt>>,
        crate::NativeObjectsError,
    > {
        crate::native::put_batch(self, requests, meter)
    }
}
