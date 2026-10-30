//! The one blob engine, over any [`Medium`].
//!
//! Per account, under `blake3(account)`:
//! - `e/<aa>/<digest>` holds the envelope, which a put creates first;
//! - `p/<aa>/<digest>.<version>` holds the payload of that envelope's
//!   version, written next; the blob exists once both do;
//! - `r/<blake3(root)>/<blake3(batch)>.<nonce>` holds one retain's batch;
//! - `i/<child>/<holder>` marks one inbound reference: `e<parent>.<version>`
//!   for an envelope, `r<blake3(root)>.<blake3(batch)>.<nonce>` for a root batch;
//! - `epoch` holds the collection epoch, advanced by its one collector.
//!
//! Delete never breaks a live reference, across processes, by two flags
//! over create-if-absent, conditional replace and listing. A holder creates
//! its `i/` key on each child, then refuses a child whose blob is absent or
//! whose envelope carries a deleter's mark; a deleter marks the envelope as
//! its own with a conditional replace, then refuses when the blob has an `i/`
//! key. One of the two always sees the other's flag.
//!
//! Destructive work is fenced by the exact bytes it observed, never by a
//! clock or by re-reading shared state. Each put writes a fresh version into
//! its envelope, and that version names its payload `p/<aa>/<digest>.<v>`
//! and its outbound index keys `e<digest>.<v>`. A deleter removes only the
//! envelope still holding its own mark, then that version's payload and keys,
//! so a deleter delayed across any number of collections and re-puts matches
//! nothing of a later blob. A removed version never returns, so a payload
//! whose envelope names another version, or none, is an orphan anyone may
//! remove.
//!
//! A put makes nothing durable. Content addressing makes a blob no commit
//! references free to lose, so durability is paid once, by the commit: a
//! retain, a run claim, or a caller's own commit after [`crate::Blobs::flush`]
//! first flushes, in one batch, the envelope, payload and outbound index keys
//! of every blob in its closure that this process wrote and has not flushed.
//! A blob outside that set is durable with everything it references. A put
//! adds its blob to the set before any of its keys becomes visible, so an
//! equal put adopting it, and that put's commit, flush it too. A crash may
//! tear an unflushed key; no commit references it, so a put replaces it and
//! a collection removes it.
//!
//! A put of a new digest creates its envelope under a fresh version, then
//! the payload that version names, and reads nothing. A put finding an equal
//! envelope adopts its version, clearing a deleter's mark or a collection's
//! stamp. Either writes the payload only after indexing and admitting the
//! blob's references, so a complete blob's children are held.
//!
//! Collection is fenced by epoch, never by wall clock or by a put's reads.
//! It stamps each unreferenced envelope it finds unstamped with its epoch,
//! and removes one stamped two epochs earlier: a blob survives the whole
//! collection after the one that stamped it, within which a commit must
//! reference it. A commit re-checks each blob as it references it, so a put
//! stalled past that grace fails its commit; no commit references a lost
//! blob. An index key of a holder that crashed before committing pins its
//! child; it never refuses wrongly in the unsafe direction.
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

/// Why a root batch did not commit.
enum Commit {
    /// It may have: its keys stay.
    Unknown,
    /// It did not: its keys are removed.
    Failed(Error),
    /// Its batch exists but may not be durable: its keys stay, and a crash
    /// before they are leaves a batch repair discards.
    Unflushed(Error),
}

/// A medium shared by many accounts. Each [`Account`] is isolated: equal bytes
/// in two accounts are two blobs, and no account observes another's.
pub struct Store<M> {
    medium: Arc<M>,
    accounts: Arc<Mutex<HashMap<Digest, Arc<State>>>>,
    scrub: Scrub,
}

/// The repair of a medium that recovered from a crash, done once before any
/// account uses it.
type Scrub = Arc<tokio::sync::OnceCell<()>>;

impl<M> Clone for Store<M> {
    fn clone(&self) -> Self {
        Self {
            medium: Arc::clone(&self.medium),
            accounts: Arc::clone(&self.accounts),
            scrub: Arc::clone(&self.scrub),
        }
    }
}

#[derive(Default)]
struct State {
    gate: tokio::sync::RwLock<()>,
    availability: crate::AvailabilitySource,
    /// This account's revisions, opened from the medium on first use.
    revisions: tokio::sync::OnceCell<Issued>,
    /// Over a volatile medium, the blob versions this process wrote and has
    /// not flushed.
    written: Mutex<HashMap<Digest, Vec<Written>>>,
}

/// One unflushed blob version, with what a flush of it covers.
struct Written {
    version: [u8; 16],
    references: Vec<Digest>,
    /// Whether its payload was complete, so a flush leaves it durable.
    complete: bool,
    /// The puts through it still in flight.
    puts: usize,
    /// Whether any put wrote through it.
    wrote: bool,
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
            scrub: Arc::default(),
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
            scrub: Arc::clone(&self.scrub),
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
    scrub: Scrub,
}

/// A prefix's listing, one page per call.
struct Pages<'a, M> {
    medium: &'a M,
    prefix: String,
    after: Option<String>,
    done: bool,
}

impl<M: Medium> Pages<'_, M> {
    async fn next(&mut self) -> Result<Option<Vec<(String, u64)>>, Error> {
        if self.done {
            return Ok(None);
        }
        let page = self
            .medium
            .list(&self.prefix, self.after.as_deref(), PAGE)
            .await
            .map_err(medium)?;
        self.done = page.len() < PAGE;
        self.after = page.last().map(|(key, _)| key.clone());
        Ok(Some(page))
    }
}

impl<M> Clone for Account<M> {
    fn clone(&self) -> Self {
        Self {
            medium: Arc::clone(&self.medium),
            prefix: self.prefix.clone(),
            state: Arc::clone(&self.state),
            scrub: Arc::clone(&self.scrub),
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
            scrub: Arc::default(),
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
    /// The account's original physical availability producer. A repair service
    /// that writes bytes outside this engine must publish completion here,
    /// including same-reference repairs, and fail it on lost producer custody.
    /// This capability belongs to trusted storage code, never a remote caller.
    pub fn availability_source(&self) -> &crate::AvailabilitySource {
        &self.state.availability
    }

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
            scrub: Arc::clone(&self.scrub),
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
        self.scrubbed().await?;
        self.state
            .revisions
            .get_or_try_init(|| self.open_revisions())
            .await
    }

    /// Reads observe an already opened writer authority; they never claim a
    /// run on the selected medium. A fresh reader has no local revision proof.
    async fn read_revision(&self) -> Result<Option<Revision>, Error> {
        self.scrubbed().await?;
        Ok(self
            .state
            .revisions
            .get()
            .map(|issued| issued.applied.current()))
    }

    /// Waits until the medium is as a clean close leaves it.
    async fn scrubbed(&self) -> Result<(), Error> {
        if self.medium.recovered() {
            self.scrub.get_or_try_init(|| self.scrub_medium()).await?;
        }
        Ok(())
    }

    /// Repairs a medium that recovered from a crash, every account of it, as
    /// a clean close leaves it. Every key that survived is durable, but an
    /// unflushed one can be torn, damaged or alone: torn envelopes and every
    /// payload not whole are removed, then, up the reference graph, the
    /// payload of each parent of no blob, so an envelope is a blob only with
    /// its whole closure; each blob's lost index keys are restored. The
    /// medium settles once every repair is flushed; a crash before then
    /// repairs again. A clean medium never pays this: it costs one read of
    /// every key, once per crash.
    async fn scrub_medium(&self) -> Result<(), Error> {
        let mut after: Option<String> = None;
        while let Some((key, _)) = self
            .medium
            .list("", after.as_deref(), 1)
            .await
            .map_err(medium)?
            .pop()
        {
            let Some((account, _)) = key.split_once('/') else {
                after = Some(key);
                continue;
            };
            let account = Account {
                medium: Arc::clone(&self.medium),
                prefix: format!("{account}/"),
                state: Arc::default(),
                scrub: Arc::clone(&self.scrub),
            };
            let repaired = account.repair().await?;
            self.medium.flush(&repaired).await.map_err(medium)?;
            // Past every key of this account.
            after = Some(format!("{}{}", account.prefix, char::MAX));
        }
        self.medium.settle();
        Ok(())
    }

    /// Repairs this account after a crash; the keys it changed.
    async fn repair(&self) -> Result<Vec<String>, Error> {
        let mut repaired = Vec::new();
        let mut blobs = HashMap::new();
        let mut parents: HashMap<Digest, Vec<Digest>> = HashMap::new();
        let mut pages = self.pages(format!("{}e/", self.prefix));
        while let Some(page) = pages.next().await? {
            for (key, _) in page {
                let digest = self.digest_from_key(&key)?;
                let Some((_, envelope)) = self.stored_envelope(&digest).await? else {
                    self.clear_torn(&key).await?;
                    repaired.push(key);
                    continue;
                };
                let payload = self.payload_key(&digest, &envelope.version);
                match self.medium.read(&payload, None).await.map_err(medium)? {
                    Some(bytes)
                        if bytes.len() as u64 == envelope.stored
                            && Digest::of(&bytes) == digest =>
                    {
                        for child in &envelope.references {
                            parents.entry(*child).or_default().push(digest);
                        }
                        blobs.insert(digest, envelope);
                    }
                    Some(_) => {
                        self.medium.remove(&payload).await.map_err(medium)?;
                        repaired.push(payload);
                    }
                    None => {}
                }
            }
        }
        let mut broken: Vec<Digest> = parents
            .keys()
            .filter(|child| !blobs.contains_key(child))
            .copied()
            .collect();
        while let Some(child) = broken.pop() {
            for parent in parents.remove(&child).into_iter().flatten() {
                if let Some(envelope) = blobs.remove(&parent) {
                    let payload = self.payload_key(&parent, &envelope.version);
                    self.medium.remove(&payload).await.map_err(medium)?;
                    repaired.push(payload);
                    broken.push(parent);
                }
            }
        }
        for (digest, envelope) in &blobs {
            let tag = Self::envelope_tag(digest, &envelope.version);
            for child in &envelope.references {
                let key = self.index_key(child, &tag);
                if self
                    .medium
                    .create(&key, OwnedBytes::new())
                    .await
                    .map_err(medium)?
                {
                    repaired.push(key);
                }
            }
        }
        // A root batch is acknowledged only once its closure and index keys
        // are durable, so one missing any of them was never acknowledged.
        let mut pages = self.pages(format!("{}r/", self.prefix));
        while let Some(page) = pages.next().await? {
            for (key, _) in page {
                if self.batch_survived(&key, &blobs).await? {
                    continue;
                }
                self.medium.remove(&key).await.map_err(medium)?;
                repaired.push(key);
            }
        }
        Ok(repaired)
    }

    /// Whether the root batch at `key` is whole, with every member a whole
    /// blob held by its index key.
    async fn batch_survived(
        &self,
        key: &str,
        blobs: &HashMap<Digest, Envelope>,
    ) -> Result<bool, Error> {
        let Some(bytes) = self.medium.read(key, None).await.map_err(medium)? else {
            return Ok(true);
        };
        let Ok(members) = decode_batch(&bytes) else {
            return Ok(false);
        };
        let (root, name) = key
            .strip_prefix(&format!("{}r/", self.prefix))
            .and_then(|rest| rest.split_once('/'))
            .ok_or_else(corrupt)?;
        let tag = format!("r{root}.{name}");
        for member in &members {
            if !blobs.contains_key(member)
                || self
                    .medium
                    .read(&self.index_key(member, &tag), None)
                    .await
                    .map_err(medium)?
                    .is_none()
            {
                return Ok(false);
            }
        }
        Ok(true)
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
                    self.medium.flush(&[key]).await.map_err(medium)?;
                    return Ok(Issued::new(account, incarnation, 1));
                }
                continue;
            };
            let (incarnation, run) = match decode_incarnation(&record) {
                Ok(claimed) => claimed,
                // Torn before its flush: neither its run nor any earlier one
                // can be told apart, so the account starts a new incarnation.
                Err(_) if self.medium.volatile() => {
                    self.medium.remove_if(&key, &record).await.map_err(medium)?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let run = run.checked_add(1).ok_or_else(corrupt)?;
            let claimed = incarnation_record(incarnation, run);
            if self
                .medium
                .replace(&key, &record, claimed)
                .await
                .map_err(medium)?
            {
                // A run claimed twice would reuse its revisions.
                self.medium.flush(&[key]).await.map_err(medium)?;
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
    /// when one is absent or marked by a deleter. Whether it created any key;
    /// `None` means an index write's outcome is unknown, so the holder must
    /// not commit.
    async fn reference(&self, children: &[Digest], tag: &str) -> Result<Option<bool>, Error> {
        let mut created = false;
        for child in children {
            match self
                .medium
                .create(&self.index_key(child, tag), OwnedBytes::new())
                .await
            {
                Ok(new) => created |= new,
                Err(MediumError::Ambiguous(_)) => return Ok(None),
                Err(error) => return Err(medium(error)),
            }
        }
        for child in children {
            match self.blob(child).await? {
                Some(envelope) if envelope.dying.is_none() => {}
                _ => return Err(ErrorCode::MissingReference.into()),
            }
        }
        Ok(Some(created))
    }

    fn index_key(&self, child: &Digest, tag: &str) -> String {
        format!("{}{tag}", self.index_prefix(child))
    }

    /// Indexes `references` under `tag`, creates the root batch `key` that
    /// commits them, then flushes the closure, its index keys and the batch
    /// in one round, acknowledged only once all are durable. A crash inside
    /// the round may keep the batch without its closure; it was never
    /// acknowledged, and [`Self::repair`] discards it. `Failed` leaves the
    /// keys for the caller to remove; `Unknown` may have committed.
    async fn commit_batch(
        &self,
        references: &[Digest],
        tag: &str,
        key: &str,
        batch: Vec<u8>,
    ) -> Result<(), Commit> {
        match self.reference(references, tag).await {
            Ok(Some(_)) => {}
            Ok(None) => return Err(Commit::Unknown),
            Err(error) => return Err(Commit::Failed(error)),
        }
        match self.medium.create(key, batch.into()).await {
            Ok(_) => {}
            Err(MediumError::Ambiguous(_)) => return Err(Commit::Unknown),
            Err(error) => return Err(Commit::Failed(medium(error))),
        }
        let mut keys: Vec<String> = references
            .iter()
            .map(|child| self.index_key(child, tag))
            .collect();
        keys.push(key.to_owned());
        self.flush_blobs(Some(references), keys)
            .await
            .map_err(Commit::Unflushed)
    }

    /// Removes `tag`'s index keys, once its holder is gone.
    async fn unreference(&self, children: &[Digest], tag: &str) -> Result<(), Error> {
        for child in children {
            self.medium
                .remove(&self.index_key(child, tag))
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

    fn epoch_key(&self) -> String {
        format!("{}epoch", self.prefix)
    }

    /// Holds `envelope`'s version of `digest` in the unflushed set for one
    /// put, before any of its keys becomes visible.
    fn track(&self, digest: &Digest, envelope: &Envelope) {
        if !self.medium.volatile() {
            return;
        }
        let mut written = self
            .state
            .written
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let versions = written.entry(*digest).or_default();
        match versions
            .iter_mut()
            .find(|blob| blob.version == envelope.version)
        {
            Some(blob) => blob.puts += 1,
            None => versions.push(Written {
                version: envelope.version,
                references: envelope.references.clone(),
                complete: false,
                puts: 1,
                wrote: false,
            }),
        }
    }

    /// Drops `version` of `digest` from the unflushed set: removed, or
    /// flushed complete.
    fn untrack(&self, digest: &Digest, version: &[u8; 16]) {
        let mut written = self
            .state
            .written
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(versions) = written.get_mut(digest) {
            versions.retain(|blob| blob.version != *version);
            if versions.is_empty() {
                written.remove(digest);
            }
        }
    }

    /// Ends one put's hold on `version` of `digest`, its payload complete.
    /// A version no put wrote through is as durable as it was, and leaves
    /// the set with its last hold; one any put wrote stays until flushed.
    fn completed(&self, digest: &Digest, version: &[u8; 16], wrote: bool) {
        let mut written = self
            .state
            .written
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let Some(versions) = written.get_mut(digest) else {
            return;
        };
        for blob in versions.iter_mut() {
            if blob.version == *version {
                blob.complete = true;
                blob.wrote |= wrote;
                blob.puts = blob.puts.saturating_sub(1);
            }
        }
        versions.retain(|blob| blob.puts > 0 || blob.wrote);
        if versions.is_empty() {
            written.remove(digest);
        }
    }

    /// Makes durable, in one batch with `keys`, every unflushed blob version
    /// in the closure of `only`, or every one. The walk stops at a blob
    /// outside the set: it is durable with everything it references, since a
    /// crash's damage is scrubbed before any account runs.
    async fn flush_blobs(
        &self,
        only: Option<&[Digest]>,
        mut keys: Vec<String>,
    ) -> Result<(), Error> {
        if !self.medium.volatile() {
            return Ok(());
        }
        let mut covered = Vec::new();
        let mut seen = HashSet::new();
        let mut pending: Vec<Digest> = match only {
            Some(only) => only.to_vec(),
            None => {
                let written = self
                    .state
                    .written
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                written.keys().copied().collect()
            }
        };
        {
            let written = self
                .state
                .written
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            while let Some(digest) = pending.pop() {
                if !seen.insert(digest) {
                    continue;
                }
                let Some(versions) = written.get(&digest) else {
                    continue;
                };
                keys.push(self.envelope_key(&digest));
                for blob in versions {
                    keys.push(self.payload_key(&digest, &blob.version));
                    let tag = Self::envelope_tag(&digest, &blob.version);
                    keys.extend(
                        blob.references
                            .iter()
                            .map(|child| self.index_key(child, &tag)),
                    );
                    pending.extend(blob.references.iter().copied());
                    if blob.complete {
                        covered.push((digest, blob.version));
                    }
                }
            }
        }
        if !keys.is_empty() {
            self.medium.flush(&keys).await.map_err(medium)?;
        }
        for (digest, version) in covered {
            self.untrack(&digest, &version);
        }
        Ok(())
    }

    /// Removes `digest` unless it is held. The deleter marks the envelope as
    /// its own, checks for holders, then removes exactly the envelope bearing
    /// its mark: a later blob, a revival or another deleter's takeover makes
    /// that removal match nothing, however long the deleter was delayed.
    /// `OutcomeUnknown` means another actor moved the envelope first.
    async fn remove_blob(&self, digest: Digest, fence: Option<u64>) -> Result<Delete, Error> {
        let key = self.envelope_key(&digest);
        let Some((observed, envelope)) = self.stored_envelope(&digest).await? else {
            if fence.is_some() {
                self.clear_torn(&key).await?;
            }
            return Ok(Delete::Absent);
        };
        match fence {
            // An envelope whose payload is still to come belongs to its put;
            // only collection, once its grace ends, removes it.
            None if !self.complete(&digest, &envelope).await? => return Ok(Delete::Absent),
            // A deleter's mark is taken over below, whatever the grace.
            None => {}
            Some(_) if envelope.dying.is_some() => {}
            // This collection starts its grace.
            Some(fence) if envelope.condemned == 0 => {
                let stamped = Envelope {
                    condemned: fence,
                    ..envelope
                };
                return match self
                    .medium
                    .replace(&key, &observed, stamped.encode().into())
                    .await
                {
                    Ok(_) | Err(MediumError::Ambiguous(_)) => Ok(Delete::Referenced),
                    Err(error) => Err(medium(error)),
                };
            }
            Some(fence) if envelope.condemned.saturating_add(1) >= fence => {
                return Ok(Delete::Referenced);
            }
            Some(_) => {}
        }
        if self.any(&self.index_prefix(&digest)).await? {
            // A kept blob outlives a mark its deleter left behind.
            if envelope.dying.is_some() && fence.is_some() {
                self.revive(&key, &observed, envelope).await?;
            }
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
        if self.any(&self.index_prefix(&digest)).await? {
            self.revive(&key, &marked, envelope).await?;
            return Ok(Delete::Referenced);
        }
        if !self.medium.remove_if(&key, &marked).await.map_err(moved)? {
            return Err(ErrorCode::OutcomeUnknown.into());
        }
        self.untrack(&digest, &envelope.version);
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
            condemned: 0,
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

    /// The envelope of `digest` once its payload is stored: a put creates
    /// the envelope first, so until its payload follows it is not a blob.
    async fn blob(&self, digest: &Digest) -> Result<Option<Envelope>, Error> {
        let Some((_, envelope)) = self.stored_envelope(digest).await? else {
            return Ok(None);
        };
        Ok(self.complete(digest, &envelope).await?.then_some(envelope))
    }

    /// Whether a read failed on a key a crash tore: only an unflushed write,
    /// which no commit references, can be, and it reads short.
    fn torn(&self, error: &MediumError) -> bool {
        matches!(error, MediumError::Failed(error) if error.kind() == std::io::ErrorKind::InvalidInput)
            && self.medium.volatile()
    }

    /// Whether the payload of `envelope`'s version is complete. A medium
    /// publishes a key only with all its bytes, and a crash's damage is
    /// scrubbed before any account runs, so presence at full length is
    /// completeness.
    async fn complete(&self, digest: &Digest, envelope: &Envelope) -> Result<bool, Error> {
        let key = self.payload_key(digest, &envelope.version);
        let range = envelope.stored..envelope.stored;
        match self.medium.read(&key, Some(range)).await {
            Ok(Some(_)) => Ok(true),
            Ok(None) => Ok(false),
            Err(error) if self.torn(&error) => Ok(false),
            Err(error) => Err(medium(error)),
        }
    }

    /// The envelope with the exact bytes a conditional write must match;
    /// none for one a crash tore.
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
        match Envelope::decode(&bytes) {
            Ok(envelope) => Ok(Some((bytes, envelope))),
            Err(_) if self.medium.volatile() => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Removes the envelope at `key` if a crash tore it.
    async fn clear_torn(&self, key: &str) -> Result<(), Error> {
        let Some(bytes) = self.medium.read(key, None).await.map_err(medium)? else {
            return Ok(());
        };
        if self.medium.volatile() && Envelope::decode(&bytes).is_err() {
            self.medium.remove_if(key, &bytes).await.map_err(medium)?;
        }
        Ok(())
    }

    /// One listing page at a time, so a pass over a prefix holds one page.
    fn pages(&self, prefix: String) -> Pages<'_, M> {
        Pages {
            medium: &*self.medium,
            prefix,
            after: None,
            done: false,
        }
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

    /// Publishes the blob `wanted` describes: creates its envelope under a
    /// fresh version, or adopts an equal one, then indexes its references
    /// and writes its payload unless complete. A new blob costs one envelope
    /// and one payload write and no read; a stored one, two reads.
    async fn publish(
        &self,
        digest: Digest,
        payload: OwnedBytes,
        wanted: Envelope,
    ) -> Result<Put, Error> {
        let key = self.envelope_key(&digest);
        let (envelope, created) = loop {
            let fresh = Envelope {
                version: version()?,
                ..wanted.clone()
            };
            self.track(&digest, &fresh);
            match self.medium.create(&key, fresh.encode().into()).await {
                Ok(true) => break (fresh, true),
                Ok(false) => self.untrack(&digest, &fresh.version),
                Err(MediumError::Ambiguous(_)) => return Ok(Put::Unknown),
                Err(error) => return Err(medium(error)),
            }
            let Some((observed, existing)) = self.stored_envelope(&digest).await? else {
                // Removed since, or torn by a crash.
                self.clear_torn(&key).await?;
                continue;
            };
            let existing = Self::equal(existing, &wanted)?;
            if existing.dying.is_some() || existing.condemned != 0 {
                let live = Envelope {
                    dying: None,
                    condemned: 0,
                    ..existing
                };
                match self
                    .medium
                    .replace(&key, &observed, live.encode().into())
                    .await
                {
                    Ok(true) => {}
                    // Moved since: the next pass decides.
                    Ok(false) | Err(MediumError::Ambiguous(_)) => continue,
                    Err(error) => return Err(medium(error)),
                }
                self.track(&digest, &live);
                break (live, false);
            }
            // A version outside the unflushed set is durable, unless this put
            // writes to it; tracked first, so its writes are never invisible
            // to a commit.
            self.track(&digest, &existing);
            break (existing, false);
        };
        let verified = Verified {
            digest,
            length: envelope.length,
            revision: None,
        };
        // A complete version was indexed before its payload was written, so
        // a put adopting one writes nothing.
        if !created && self.complete(&digest, &envelope).await? {
            self.completed(&digest, &envelope.version, false);
            return Ok(Put::AlreadyExists { verified });
        }
        // Any writer of the payload first holds every child; an index key
        // outlives a refusal, pinning its child until this version's removal.
        let tag = Self::envelope_tag(&digest, &envelope.version);
        let Some(indexed) = self.reference(&envelope.references, &tag).await? else {
            return Ok(Put::Unknown);
        };
        let Some(wrote) = self.write_payload(&digest, &envelope, payload).await? else {
            return Ok(Put::Unknown);
        };
        self.completed(&digest, &envelope.version, created || indexed || wrote);
        Ok(if created {
            Put::Created { verified }
        } else {
            Put::AlreadyExists { verified }
        })
    }

    /// Writes `payload` as `envelope`'s version, replacing one a crash tore;
    /// whether this call wrote it, or `None` when that is unknown.
    async fn write_payload(
        &self,
        digest: &Digest,
        envelope: &Envelope,
        payload: OwnedBytes,
    ) -> Result<Option<bool>, Error> {
        let key = self.payload_key(digest, &envelope.version);
        loop {
            match crate::availability::Restoration::write(
                &self.state.availability,
                *digest,
                self.medium.create(&key, payload.clone()),
            )
            .await
            {
                Ok(true) => return Ok(Some(true)),
                Ok(false) => {}
                Err(MediumError::Ambiguous(_)) => return Ok(None),
                Err(error) => return Err(medium(error)),
            }
            if self.complete(digest, envelope).await? {
                return Ok(Some(false));
            }
            if let Some(torn) = self.medium.read(&key, None).await.map_err(medium)? {
                self.medium.remove_if(&key, &torn).await.map_err(medium)?;
            }
        }
    }

    /// An existing envelope a put adopts: the same bytes, the same references.
    fn equal(existing: Envelope, wanted: &Envelope) -> Result<Envelope, Error> {
        if existing.kind != wanted.kind
            || existing.length != wanted.length
            || existing.stored != wanted.stored
        {
            return Err(corrupt());
        }
        if existing.references != wanted.references {
            return Err(ErrorCode::Conflict.into());
        }
        Ok(existing)
    }

    async fn read_payload(
        &self,
        digest: &Digest,
        envelope: &Envelope,
        range: Option<std::ops::Range<u64>>,
    ) -> Result<OwnedBytes, Error> {
        let full = range.is_none();
        let bytes = match self
            .medium
            .read(&self.payload_key(digest, &envelope.version), range)
            .await
        {
            Ok(Some(bytes)) => bytes,
            // Its put has not written it yet, a collector removed it, or a
            // crash tore it before any commit.
            Ok(None) => return Err(ErrorCode::NotFound.into()),
            Err(error) if self.torn(&error) => return Err(ErrorCode::NotFound.into()),
            Err(error) => return Err(medium(error)),
        };
        if full && (bytes.len() as u64) < envelope.stored && self.medium.volatile() {
            // Torn by a crash before any commit: not yet written.
            return Err(ErrorCode::NotFound.into());
        }
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
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            if range.is_empty() {
                return Ok(());
            }
            match envelope.kind {
                Kind::Blob => {
                    let whole = range.start == 0 && range.end == envelope.length;
                    // A whole read checks its bytes; a partial one first
                    // proves a payload a crash could have torn complete.
                    if !whole
                        && self.medium.volatile()
                        && !self.complete(&digest, &envelope).await?
                    {
                        return Err(ErrorCode::NotFound.into());
                    }
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
                            let (_, child) = self
                                .stored_envelope(&part.digest)
                                .await?
                                .ok_or_else(corrupt)?;
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
                .batch(&key)
                .await
                .and_then(|batch| batch.ok_or_else(corrupt))
                .map_err(suspended)?;
            references.extend(batch);
        }
        Ok(references)
    }

    /// The root batch at `key`. A crash may tear one only before its retain
    /// acknowledged it, so a torn batch is removed as never written; its
    /// index keys only pin.
    async fn batch(&self, key: &str) -> Result<Option<Vec<Digest>>, Error> {
        let Some(bytes) = self.medium.read(key, None).await.map_err(medium)? else {
            return Ok(None);
        };
        match decode_batch(&bytes) {
            Ok(batch) => Ok(Some(batch)),
            Err(_) if self.medium.volatile() => {
                self.medium.remove_if(key, &bytes).await.map_err(medium)?;
                Ok(Some(Vec::new()))
            }
            Err(error) => Err(error),
        }
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
        acyclic_observability::async_operation!(
            "acyclic.objects.get",
            |error: &Error| error.code.as_str_name(),
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                // Everything committed by `served` is visible to the read after it.
                let served = self.read_revision().await?;
                // Reading the payload proves the blob complete; an empty selection
                // reads none, so it checks the payload instead.
                let (_, envelope) = self
                    .stored_envelope(&digest)
                    .await?
                    .ok_or(Error::from(ErrorCode::NotFound))?;
                let selected = range.resolve(envelope.length)?;
                if selected.is_empty() && !self.complete(&digest, &envelope).await? {
                    return Err(ErrorCode::NotFound.into());
                }
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
                        .served_at(served),
                )
            }
        )
    }

    pub(crate) async fn put_inner(
        &self,
        body: crate::Sealed,
        references: Vec<Digest>,
    ) -> Result<Put, Error> {
        acyclic_observability::async_operation!(
            "acyclic.objects.put",
            |error: &Error| error.code.as_str_name(),
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                let references = canonical(references);
                let issued = self.issued().await?;
                let _gate = self.state.gate.read().await;
                let digest = Digest::sealed(&body);
                let body = body.into_bytes();
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
                        condemned: 0,
                    },
                )
                .await
                .map(|put| issued.committed(put))
            }
        )
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<M: Medium> crate::Blobs for Account<M> {
    async fn availability(&self, digest: Digest) -> Result<crate::Availability, Error> {
        acyclic_observability::async_operation!(
            "acyclic.objects.availability",
            |error: &Error| error.code.as_str_name(),
            acyclic_native_runtime::obs::OperationSpan::new,
            async { self.state.availability.subscribe(digest) }
        )
    }

    async fn put(&self, body: OwnedBytes, references: Vec<Digest>) -> Result<Put, Error> {
        self.put_inner(crate::Sealed::new(body), references).await
    }

    async fn put_sealed(&self, body: crate::Sealed, references: Vec<Digest>) -> Result<Put, Error> {
        self.put_inner(body, references).await
    }

    async fn put_manifest(&self, manifest: Manifest) -> Result<Put, Error> {
        acyclic_observability::async_operation!(
            "acyclic.objects.put_manifest",
            |error: &Error| error.code.as_str_name(),
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                if manifest.parts.is_empty() {
                    return Err(invalid());
                }
                let length = manifest.length()?;
                let issued = self.issued().await?;
                let _gate = self.state.gate.read().await;
                for part in &manifest.parts {
                    let envelope = self
                        .blob(&part.digest)
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
                        condemned: 0,
                    },
                )
                .await
                .map(|put| issued.committed(put))
            }
        )
    }

    async fn flush(&self, only: Option<&[Digest]>) -> Result<(), Error> {
        acyclic_observability::async_operation!(
            "acyclic.objects.flush",
            |error: &Error| error.code.as_str_name(),
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                self.issued().await?;
                self.flush_blobs(only, Vec::new()).await?;
                // A blob collected since its put is not durable however flushed.
                for digest in only.into_iter().flatten() {
                    match self.blob(digest).await? {
                        Some(envelope) if envelope.dying.is_none() => {}
                        _ => return Err(ErrorCode::MissingReference.into()),
                    }
                }
                Ok(())
            }
        )
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
        acyclic_observability::async_operation!(
            "acyclic.objects.head",
            |error: &Error| error.code.as_str_name(),
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                let served = self.read_revision().await?;
                let envelope = self
                    .blob(&digest)
                    .await?
                    .ok_or(Error::from(ErrorCode::NotFound))?;
                let mut head = Self::head_of(digest, envelope);
                head.revision = served;
                Ok(head)
            }
        )
    }

    async fn delete(&self, digest: Digest) -> Result<Deletion, Error> {
        acyclic_observability::async_operation!(
            "acyclic.objects.delete",
            |error: &Error| error.code.as_str_name(),
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
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
        )
    }

    async fn list(&self, after: Option<Digest>, limit: u32) -> Result<Page, Error> {
        acyclic_observability::async_operation!(
            "acyclic.objects.list",
            |error: &Error| error.code.as_str_name(),
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                if limit == 0 {
                    return Err(invalid());
                }
                let served = self.read_revision().await?;
                let after = after.map(|digest| self.envelope_key(&digest));
                let limit = usize::try_from(limit).map_err(|_| invalid())?;
                // Envelopes still awaiting their payload are not blobs: page past
                // them to a full page.
                let mut digests = Vec::new();
                let mut cursor = after;
                loop {
                    let wanted = limit - digests.len();
                    let keys = self
                        .medium
                        .list(&format!("{}e/", self.prefix), cursor.as_deref(), wanted)
                        .await
                        .map_err(medium)?;
                    for (key, _) in &keys {
                        let digest = self.digest_from_key(key)?;
                        if self.blob(&digest).await?.is_some() {
                            digests.push(digest);
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
                    revision: served,
                })
            }
        )
    }

    async fn retain(&self, root: &RootId, references: Vec<Digest>) -> Result<Retained, Error> {
        acyclic_observability::async_operation!(
            "acyclic.objects.retain",
            |error: &Error| error.code.as_str_name(),
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                let references = canonical(references);
                if references.is_empty() {
                    return Err(invalid());
                }
                let issued = self.issued().await?;
                let _gate = self.state.gate.read().await;
                self.medium
                    .allocate(references.len() as u64 * 32)
                    .map_err(medium)?;
                let batch = encode_batch(&references);
                // Each retain owns its batch and keys, so a concurrent release of an
                // equal batch never removes them.
                let name = format!("{}.{}", Digest::of(&batch).to_hex(), nonce()?);
                let tag = Self::batch_tag(root, &name);
                // The index-key vector is charged before any key is written, so a
                // refused charge leaves nothing to clean up. Every key has one
                // length: a fixed-width prefix and this tag.
                let key = self.index_key(&references[0], &tag).len();
                let held = (std::mem::size_of::<String>() + key) as u64 * references.len() as u64;
                self.medium.allocate(held).map_err(medium)?;
                // Every write is withheld before the first key: n keys, then either
                // their n removals or the batch and the one flush. A
                // refused budget leaves nothing behind, and removing the keys of a
                // retain that failed is never refused.
                let n = references.len() as u64;
                self.medium
                    .reserve_writes(2 * n + 2, batch.len() as u64)
                    .map_err(medium)?;
                let key = format!("{}{name}", self.root_prefix(root));
                match self.commit_batch(&references, &tag, &key, batch).await {
                    Ok(()) => {}
                    Err(Commit::Unknown) => return Err(ErrorCode::OutcomeUnknown.into()),
                    Err(Commit::Failed(error)) => {
                        self.unreference(&references, &tag).await?;
                        return Err(error);
                    }
                    Err(Commit::Unflushed(error)) => return Err(error),
                }
                Ok(Retained {
                    references: references.len() as u64,
                    revision: Some(issued.commit()),
                })
            }
        )
    }

    async fn release(&self, root: &RootId) -> Result<Released, Error> {
        acyclic_observability::async_operation!(
            "acyclic.objects.release",
            |error: &Error| error.code.as_str_name(),
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                let issued = self.issued().await?;
                let _gate = self.state.gate.read().await;
                let batches = self.list_all(&self.root_prefix(root)).await?;
                let mut removed = Vec::new();
                for (key, _) in &batches {
                    let Some(references) = self.batch(key).await? else {
                        continue;
                    };
                    match self.medium.remove(key).await {
                        Ok(_) => {}
                        Err(MediumError::Ambiguous(_)) => {
                            return Err(ErrorCode::OutcomeUnknown.into());
                        }
                        Err(error) => return Err(medium(error)),
                    }
                    removed.push((key, references));
                }
                // A batch back from a crash must find its index keys: its removal is
                // durable before theirs.
                let batch_keys: Vec<_> = removed.iter().map(|(key, _)| (*key).clone()).collect();
                self.medium.flush(&batch_keys).await.map_err(medium)?;
                let mut index_keys = Vec::new();
                for (key, references) in &removed {
                    let tag = Self::batch_tag(root, key.rsplit('/').next().ok_or_else(corrupt)?);
                    self.unreference(references, &tag).await?;
                    index_keys.extend(references.iter().map(|child| self.index_key(child, &tag)));
                }
                // An index key back from a crash would pin its child forever.
                self.medium.flush(&index_keys).await.map_err(medium)?;
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
        )
    }

    async fn collect(&self) -> Result<Collection, Error> {
        acyclic_observability::async_operation!(
            "acyclic.objects.collect",
            |error: &Error| error.code.as_str_name(),
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                let issued = self.issued().await?;
                let _gate = self.state.gate.write().await;
                let mut marked = HashSet::new();
                let mut marks = Vec::new();
                let mut pending = self.rooted().await?;
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
                // Only a pass that can sweep advances the epoch: marking first never
                // keeps less, since every sweep still consults the inbound index.
                // Losing the claim to a crash only repeats the epoch, which keeps more.
                let observed = self
                    .medium
                    .read(&self.epoch_key(), None)
                    .await
                    .map_err(medium)?;
                let fence = match observed.as_deref().map(<[u8; 8]>::try_from) {
                    Some(Ok(bytes)) => u64::from_be_bytes(bytes),
                    Some(Err(_)) if !self.medium.volatile() => return Err(corrupt()),
                    // Torn by a crash, or never claimed.
                    Some(Err(_)) | None => 0,
                };
                let epoch = fence.checked_add(1).ok_or_else(corrupt)?;
                let advanced = OwnedBytes::from(epoch.to_be_bytes().to_vec());
                // One collector wins each epoch, across processes.
                let claimed = match &observed {
                    Some(observed) => {
                        self.medium
                            .replace(&self.epoch_key(), observed, advanced)
                            .await
                    }
                    None => self.medium.create(&self.epoch_key(), advanced).await,
                };
                match claimed {
                    Ok(true) => {}
                    Ok(false) => return Err(ErrorCode::Conflict.into()),
                    Err(MediumError::Ambiguous(_)) => return Err(ErrorCode::OutcomeUnknown.into()),
                    Err(error) => return Err(medium(error)),
                }
                // Revisions and admissions of the earlier put protocol fence nothing.
                for prefix in ["g/", "a/"] {
                    let mut pages = self.pages(format!("{}{prefix}", self.prefix));
                    while let Some(page) = pages.next().await? {
                        for (key, _) in page {
                            self.medium.remove(&key).await.map_err(medium)?;
                        }
                    }
                }
                let mut collection = Collection {
                    epoch,
                    ..Collection::default()
                };
                let mut pages = self.pages(format!("{}e/", self.prefix));
                while let Some(page) = pages.next().await? {
                    for (key, _) in page {
                        let digest = self.digest_from_key(&key)?;
                        if marked.contains(&digest) {
                            collection.marked += 1;
                            continue;
                        }
                        let stored = self
                            .blob(&digest)
                            .await?
                            .map_or(0, |envelope| envelope.stored);
                        if self.sweep(digest, epoch).await? == Delete::Deleted {
                            collection.swept += 1;
                            collection.swept_bytes += stored;
                        }
                    }
                }
                // Payloads of no envelope or of a removed version: none can return.
                let mut pages = self.pages(format!("{}p/", self.prefix));
                while let Some(page) = pages.next().await? {
                    for (key, size) in page {
                        let (digest, version) = self.payload_of(&key)?;
                        match self.stored_envelope(&digest).await? {
                            // Its envelope's pass above decided, or a put just wrote it.
                            Some((_, envelope)) if envelope.version == version => {}
                            _ => {
                                self.medium.remove(&key).await.map_err(medium)?;
                                collection.swept_bytes += size;
                            }
                        }
                    }
                }
                collection.revision = Some(if collection.swept > 0 {
                    issued.commit()
                } else {
                    issued.applied.current()
                });
                Ok(collection)
            }
        )
    }

    async fn revisions(&self) -> Result<Option<&AppliedRevisions>, Error> {
        acyclic_observability::async_operation!(
            "acyclic.objects.revisions",
            |error: &Error| error.code.as_str_name(),
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                self.scrubbed().await?;
                Ok(self.state.revisions.get().map(|issued| &issued.applied))
            }
        )
    }

    /// Every run commits only after its write is visible on the medium, so a
    /// minimum in another claimed run of this incarnation is already applied
    /// here. This run's own minimum still waits on its exact applied index.
    async fn admitted(
        &self,
        mut minimum: acyclic_stream::RevisionCut,
    ) -> Result<acyclic_stream::RevisionCut, Error> {
        acyclic_observability::async_operation!(
            "acyclic.objects.admitted",
            |error: &Error| error.code.as_str_name(),
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                let Some(issued) = self.state.revisions.get() else {
                    // A stored run claim alone does not prove that this selected read
                    // has applied any index of it. Retain the caller's minimum.
                    return Ok(minimum);
                };
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
        )
    }

    async fn usage(&self) -> Result<Usage, Error> {
        acyclic_observability::async_operation!(
            "acyclic.objects.usage",
            |error: &Error| error.code.as_str_name(),
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                self.scrubbed().await?;
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
                        if let Some(envelope) = self.blob(&digest).await? {
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
        )
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
        request: crate::ObjectsNativeRequest<(crate::Sealed, Vec<Digest>)>,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<crate::ObjectsNativeFuture<'_, crate::ObjectsPutReceipt>, crate::NativeObjectsError>
    {
        crate::native::put(self, request, meter)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn put_batch_native(
        &self,
        requests: crate::ObjectsNativeRequest<Vec<(crate::Sealed, Vec<Digest>)>>,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<
        crate::ObjectsNativeFuture<'_, crate::ObjectsBatch<crate::ObjectsPutReceipt>>,
        crate::NativeObjectsError,
    > {
        crate::native::put_batch(self, requests, meter)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn retain_native(
        &self,
        request: crate::ObjectsNativeRequest<(RootId, Vec<Digest>)>,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<crate::ObjectsNativeFuture<'_, Retained>, crate::NativeObjectsError> {
        crate::native::retain(self, request, meter)
    }
}

#[cfg(test)]
mod written_tests {
    use super::*;

    #[test]
    fn a_version_another_put_wrote_through_stays_unflushed() {
        let store = Store::new(crate::tests::Counting::default());
        let account = store.account(b"account").unwrap();
        let digest = Digest::of(b"blob");
        let envelope = Envelope {
            kind: Kind::Blob,
            length: 4,
            stored: 4,
            references: Vec::new(),
            version: [1; 16],
            dying: None,
            condemned: 0,
        };
        let tracked = || {
            account
                .state
                .written
                .lock()
                .unwrap()
                .get(&digest)
                .is_some_and(|versions| versions.iter().any(|blob| blob.complete))
        };
        // Two equal puts hold the version; the one that finishes last wrote
        // nothing, but the other wrote its payload.
        account.track(&digest, &envelope);
        account.track(&digest, &envelope);
        account.completed(&digest, &envelope.version, true);
        account.completed(&digest, &envelope.version, false);
        assert!(tracked());
        account.untrack(&digest, &envelope.version);
        // Control: a version no put wrote through leaves with its last hold.
        account.track(&digest, &envelope);
        account.track(&digest, &envelope);
        account.completed(&digest, &envelope.version, false);
        account.completed(&digest, &envelope.version, false);
        assert!(!tracked());
    }
}
