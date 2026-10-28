//! The one blob engine, over any [`Medium`].
//!
//! Per account, under `blake3(account)`:
//! - `p/<aa>/<digest>` holds the payload, written first;
//! - `e/<aa>/<digest>` holds the envelope, whose creation publishes the blob;
//! - `r/<blake3(root)>/<blake3(batch)>.<nonce>` holds one retain's batch;
//! - `i/<child>/<holder>` marks one inbound reference: `e<parent>` for an
//!   envelope, `r<blake3(root)>.<blake3(batch)>.<nonce>` for a root batch;
//! - `w/<digest>/<revision>.<nonce>` is one in-flight put's intent;
//! - `d/<digest>/<revision>.<nonce>` is one deleter's tombstone;
//! - `g/<revision>` is a collection revision, created by its one collector;
//! - `a/<revision>/<digest>` admits a put at that revision.
//!
//! Delete never breaks a live reference, across processes, by two flags over
//! create-if-absent and listing. A writer creates its flag (an `i/` key on
//! each child, or a `w/` intent on the blob it puts), then refuses when the
//! target has a tombstone; a deleter creates its tombstone, then refuses when
//! the target has an `i/` or `w/` key. One of the two always sees the other's
//! flag. A put creates its payload and child keys only after that check, so
//! bytes being deleted are never reported durable.
//!
//! Every actor removes only keys it owns: a deleter its own tombstone and its
//! blob's outbound `i/` keys, a release its own batches' keys, a put its own
//! intent. No actor clears another's flag, so no interleaving lets a live
//! flag vanish under the actor still relying on it. Index keys are written
//! before their holder and removed after it; a stale key only ever refuses,
//! leaking its child rather than breaking a reference.
//!
//! Publish, then commit the reference. Collection is fenced by revision, never
//! by wall clock: the collection that creates revision `n` keeps every blob
//! admitted at `n - 1` or later, so a put protects its blob for one full
//! collection after it returns, within which a retain or a referencing put
//! must pin it. A put admits before its envelope; a collection sweeps through
//! the delete protocol and refuses any blob admitted since its fence. Each
//! collection also clears tombstones and intents from before its fence: those
//! of actors that crashed, or that stalled across a whole collection pass.
//! Closing that stall needs a medium commit conditioned on revision. An index
//! key of a writer that crashed before committing pins its child until that
//! holder commits; it never refuses wrongly in the unsafe direction.
use crate::blob::{Envelope, corrupt, decode_batch, encode_batch, invalid};
use crate::{
    ByteRange, Collection, Delete, Digest, Error, Head, Kind, Manifest, Medium, MediumError,
    MemoryMedium, Object, Page, Put, Retained, RootId, Usage, Verified, wire::ErrorCode,
};
use acyclic_stream::OwnedBytes;
use std::collections::{HashMap, HashSet};
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
        Self {
            medium: Arc::new(MemoryMedium::default()),
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
    let mut nonce = [0; 16];
    getrandom::fill(&mut nonce).map_err(|_| Error::from(ErrorCode::Unavailable))?;
    Ok(hex::encode(nonce))
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
    fn payload_key(&self, digest: &Digest) -> String {
        let hex = digest.to_hex();
        format!("{}p/{}/{hex}", self.prefix, &hex[..2])
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

    fn tombstone_prefix(&self, digest: &Digest) -> String {
        format!("{}d/{}/", self.prefix, digest.to_hex())
    }

    fn intent_prefix(&self, digest: &Digest) -> String {
        format!("{}w/{}/", self.prefix, digest.to_hex())
    }

    /// Creates a fresh `<revision>.<nonce>` flag under `prefix`, owned by its
    /// caller alone. `None` means the create's outcome is unknown.
    async fn flag(&self, prefix: &str) -> Result<Option<String>, Error> {
        let key = format!("{prefix}{:020}.{}", self.revision().await?, nonce()?);
        match self.medium.create(&key, OwnedBytes::new()).await {
            Ok(_) => Ok(Some(key)),
            Err(MediumError::Ambiguous(_)) => Ok(None),
            Err(error) => Err(medium(error)),
        }
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
    fn envelope_tag(parent: &Digest) -> String {
        format!("e{}", parent.to_hex())
    }

    /// The `i/` holder tag of the root batch named `batch`.
    fn batch_tag(root: &RootId, batch: &str) -> String {
        format!("r{}.{batch}", Digest::of(root.as_bytes()).to_hex())
    }

    /// Marks `tag` as holding each of `children`, then admits them: refused
    /// when one is absent or tombstoned. `None` means an index write's outcome
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
            if self.doomed(child).await? {
                return Err(ErrorCode::MissingReference.into());
            }
        }
        self.require(children).await?;
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

    /// Whether some deleter holds a tombstone on `digest`.
    async fn doomed(&self, digest: &Digest) -> Result<bool, Error> {
        self.any(&self.tombstone_prefix(digest)).await
    }

    fn digest_from_key(&self, key: &str) -> Result<Digest, Error> {
        key.rsplit('/')
            .next()
            .ok_or_else(corrupt)
            .and_then(|hex| Digest::from_hex(hex).map_err(|_| corrupt()))
    }

    fn revision_key(&self, revision: u64) -> String {
        format!("{}g/{revision:020}", self.prefix)
    }

    fn admission_prefix(&self, revision: u64) -> String {
        format!("{}a/{revision:020}/", self.prefix)
    }

    /// The revision a `d/` or `w/` flag was created at.
    fn flag_revision(key: &str) -> Result<u64, Error> {
        key.rsplit('/')
            .next()
            .and_then(|name| name.split('.').next())
            .and_then(|revision| revision.parse().ok())
            .ok_or_else(corrupt)
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
    /// revision again after the write. `None` means a write's outcome is unknown.
    async fn admit(&self, digest: &Digest) -> Result<Option<()>, Error> {
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
                return Ok(Some(()));
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

    /// Removes `digest` under a tombstone unless it is referenced, being put,
    /// or, for a collection fenced at `fence`, admitted since. An absent
    /// envelope still clears a collection's orphan payload.
    async fn remove_blob(&self, digest: Digest, fence: Option<u64>) -> Result<Delete, Error> {
        let Some(tombstone) = self.flag(&self.tombstone_prefix(&digest)).await? else {
            return Err(ErrorCode::OutcomeUnknown.into());
        };
        // A failed step leaves the tombstone, so no put trusts a half-removed
        // blob; collection clears it once its revision is fenced.
        let outcome = self.unless_held(digest, fence).await?;
        self.medium.remove(&tombstone).await.map_err(medium)?;
        Ok(outcome)
    }

    async fn unless_held(&self, digest: Digest, fence: Option<u64>) -> Result<Delete, Error> {
        if let Some(from) = fence {
            if self.admitted(&digest, from).await? {
                return Ok(Delete::Referenced);
            }
        }
        let held = self.any(&self.index_prefix(&digest)).await?
            || self.any(&self.intent_prefix(&digest)).await?;
        let Some(envelope) = self.envelope(&digest).await? else {
            if fence.is_some() && !held {
                self.medium
                    .remove(&self.payload_key(&digest))
                    .await
                    .map_err(medium)?;
            }
            return Ok(Delete::Absent);
        };
        if held {
            return Ok(Delete::Referenced);
        }
        self.medium
            .remove(&self.envelope_key(&digest))
            .await
            .map_err(medium)?;
        self.medium
            .remove(&self.payload_key(&digest))
            .await
            .map_err(medium)?;
        self.unreference(&envelope.references, &Self::envelope_tag(&digest))
            .await?;
        Ok(Delete::Deleted)
    }

    async fn envelope(&self, digest: &Digest) -> Result<Option<Envelope>, Error> {
        self.medium
            .read(&self.envelope_key(digest), None)
            .await
            .map_err(medium)?
            .map(|bytes| Envelope::decode(&bytes))
            .transpose()
    }

    async fn require(&self, references: &[Digest]) -> Result<(), Error> {
        for reference in references {
            if self.envelope(reference).await?.is_none() {
                return Err(ErrorCode::MissingReference.into());
            }
        }
        Ok(())
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

    /// Publishes payload then envelope; the envelope is the commit point.
    async fn publish(
        &self,
        digest: Digest,
        stored: OwnedBytes,
        envelope: Envelope,
    ) -> Result<Put, Error> {
        if let Some(existing) = self.envelope(&digest).await? {
            return self.existing(digest, &existing, &envelope).await;
        }
        let Some(intent) = self.flag(&self.intent_prefix(&digest)).await? else {
            return Ok(Put::Unknown);
        };
        let put = self.commit(digest, stored, envelope).await;
        // A failed removal leaves the intent pinning the blob until
        // collection clears it.
        let _ = self.medium.remove(&intent).await;
        put
    }

    /// Writes payload, child keys and envelope under this put's intent: past
    /// its tombstone check no deleter removes the blob or its children.
    async fn commit(
        &self,
        digest: Digest,
        stored: OwnedBytes,
        envelope: Envelope,
    ) -> Result<Put, Error> {
        if self.doomed(&digest).await? {
            return Ok(Put::Unknown);
        }
        if self
            .reference(&envelope.references, &Self::envelope_tag(&digest))
            .await?
            .is_none()
        {
            return Ok(Put::Unknown);
        }
        match self.medium.create(&self.payload_key(&digest), stored).await {
            Ok(_) => {}
            Err(MediumError::Ambiguous(_)) => return Ok(Put::Unknown),
            Err(error) => return Err(medium(error)),
        }
        // Admit at the revision now current: the next collection keeps it.
        if self.admit(&digest).await?.is_none() {
            return Ok(Put::Unknown);
        }
        let verified = Verified {
            digest,
            length: envelope.length,
        };
        match self
            .medium
            .create(&self.envelope_key(&digest), envelope.encode().into())
            .await
        {
            Ok(true) => Ok(Put::Created { verified }),
            Ok(false) => {
                let existing = self.envelope(&digest).await?.ok_or_else(corrupt)?;
                self.existing(digest, &existing, &envelope).await
            }
            Err(MediumError::Ambiguous(_)) => Ok(Put::Unknown),
            Err(error) => Err(medium(error)),
        }
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
        if self.admit(&digest).await?.is_none() || self.doomed(&digest).await? {
            return Ok(Put::Unknown);
        }
        Ok(Put::AlreadyExists {
            verified: Verified {
                digest,
                length: existing.length,
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
            .read(&self.payload_key(digest), range)
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
        }
    }

    pub(crate) async fn get_inner(
        &self,
        digest: Digest,
        range: ByteRange,
        maximum_bytes: u64,
    ) -> Result<Object, Error> {
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
        Ok(Object::new(
            Self::head_of(digest, envelope),
            selected.start,
            body,
        ))
    }

    pub(crate) async fn put_inner(
        &self,
        body: OwnedBytes,
        references: Vec<Digest>,
    ) -> Result<Put, Error> {
        let references = canonical(references);
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
            },
        )
        .await
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
            },
        )
        .await
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
        let envelope = self
            .envelope(&digest)
            .await?
            .ok_or(Error::from(ErrorCode::NotFound))?;
        Ok(Self::head_of(digest, envelope))
    }

    async fn delete(&self, digest: Digest) -> Result<Delete, Error> {
        let _gate = self.state.gate.read().await;
        self.remove_blob(digest, None).await
    }

    async fn list(&self, after: Option<Digest>, limit: u32) -> Result<Page, Error> {
        if limit == 0 {
            return Err(invalid());
        }
        let after = after.map(|digest| self.envelope_key(&digest));
        let limit = usize::try_from(limit).map_err(|_| invalid())?;
        let keys = self
            .medium
            .list(&format!("{}e/", self.prefix), after.as_deref(), limit)
            .await
            .map_err(medium)?;
        let digests = keys
            .iter()
            .map(|(key, _)| self.digest_from_key(key))
            .collect::<Result<Vec<_>, _>>()?;
        let next = (digests.len() == limit)
            .then(|| digests.last().copied())
            .flatten();
        Ok(Page { digests, next })
    }

    async fn retain(&self, root: &RootId, references: Vec<Digest>) -> Result<Retained, Error> {
        let references = canonical(references);
        if references.is_empty() {
            return Err(invalid());
        }
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
        })
    }

    async fn release(&self, root: &RootId) -> Result<bool, Error> {
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
        Ok(!batches.is_empty())
    }

    async fn collect(&self) -> Result<Collection, Error> {
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
            match self.envelope(&digest).await {
                Ok(Some(envelope)) => pending.extend(envelope.references),
                // A retained reference to nothing protects nothing.
                Ok(None) => {}
                Err(_) => return Err(ErrorCode::Suspended.into()),
            }
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
            if self.remove_blob(digest, Some(fence)).await? == Delete::Deleted {
                collection.swept += 1;
                collection.swept_bytes += stored;
            }
        }
        // Payloads without an envelope, or whose envelope a parent's sweep
        // above unpinned.
        for (key, size) in self.list_all(&format!("{}p/", self.prefix)).await? {
            let digest = self.digest_from_key(&key)?;
            if marked.contains(&digest) {
                continue;
            }
            match self.remove_blob(digest, Some(fence)).await? {
                Delete::Deleted => {
                    collection.swept += 1;
                    collection.swept_bytes += size;
                }
                Delete::Absent => collection.swept_bytes += size,
                Delete::Referenced => {}
            }
        }
        // Flags from before the fence belong to actors that crashed or
        // stalled across a whole pass.
        for prefix in ["d/", "w/"] {
            for (key, _) in self.list_all(&format!("{}{prefix}", self.prefix)).await? {
                if Self::flag_revision(&key)? < fence {
                    self.medium.remove(&key).await.map_err(medium)?;
                }
            }
        }
        // Admissions before the fence no longer protect anything.
        for (key, _) in self.list_all(&format!("{}a/", self.prefix)).await? {
            if Self::revision_of(&key, 1)? < fence {
                self.medium.remove(&key).await.map_err(medium)?;
            }
        }
        Ok(collection)
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
