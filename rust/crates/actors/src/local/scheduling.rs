//! Stable scheduling identity, independent of placement and process hash seeds.
//!
//! Each shard is one stream (its activation log) whose direct children are
//! deadline windows `<shard>/<020 minute>`: a commit that arms an alarm
//! appends the actor to the window holding its deadline. The stream count
//! grows with distinct minutes, never with distinct milliseconds, and a host
//! finds its earliest pending window with one bounded children page after its
//! consumed watermark, never by scanning actors. A due window wakes its
//! actors; each delivery then installs its exact deadline as a resident wake.
//!
//! `interests-v1/<shard>` holds one marker child per actor that has held
//! guest-added interests. Their producers do not know their observers, so a
//! dormant holder has no activation entry; a placing host restores holders
//! from these markers at startup, one paged credit at a time.
use super::{ActorReference, Error, Store, StreamPath, StreamProvider};
use acyclic_stream::{
    ChildrenPageRequest, CommitEffect, CommitMutation, CommitRequest, IdempotencyKey, ReadRequest,
    StreamError, StreamProviderExt as _,
};
use futures::TryStreamExt as _;
use sha2::{Digest, Sha256};

/// Records read per deadline-window page.
const WINDOW_PAGE: u32 = 256;
/// Interest holders restored per page; the next page waits for this one's
/// wakes to settle.
const INTEREST_PAGE: u32 = 256;
/// The versioned root child holding each shard's interest markers.
const INTERESTS: &str = "interests-v1";
/// Width of one deadline window.
const WINDOW_MS: u64 = 60_000;
/// Scheduling partitions of a store unless its deployment sets its own.
pub const DEFAULT_SCHEDULING_SHARDS: std::num::NonZeroU16 =
    std::num::NonZeroU16::MIN.saturating_add(15);

/// The deadline window holding `unix_ms`.
pub(super) const fn window(unix_ms: u64) -> u64 {
    unix_ms / WINDOW_MS
}

/// The first instant of `window`.
pub(super) const fn window_start(window: u64) -> u64 {
    window.saturating_mul(WINDOW_MS)
}

/// Learn a deadline window of `shard` that alarms now occupy. `windows` holds
/// each shard's earliest unopened known window; an opened window's
/// consumption finds the next one, so only an earlier window is new.
pub(super) fn learn(
    windows: &mut std::collections::BTreeSet<(u64, u16)>,
    consumed: u64,
    shard: u16,
    window: u64,
) {
    if window >= consumed
        && !windows
            .iter()
            .any(|&(known, owner)| owner == shard && known <= window)
    {
        windows.insert((window, shard));
    }
}

/// Start reading every known window that has started by `now`. A window
/// already being read, or one its shard has consumed, is not read again: a
/// window is known, being read or consumed, never two at once.
pub(super) fn open(
    windows: &mut std::collections::BTreeSet<(u64, u16)>,
    reading: &mut std::collections::BTreeSet<(u16, u64)>,
    consumed: &std::collections::BTreeMap<u16, u64>,
    now: u64,
) -> Vec<(u16, u64)> {
    let mut opened = Vec::new();
    while let Some(&(window, shard)) = windows.first()
        && window_start(window) <= now
    {
        windows.pop_first();
        if consumed
            .get(&shard)
            .is_some_and(|&consumed| window >= consumed)
            && reading.insert((shard, window))
        {
            opened.push((shard, window));
        }
    }
    opened
}

/// The last page of `shard`'s `window` was read, and `next` is the shard's
/// next filed window past it. `consumed` passes the window only when no
/// earlier window of the shard is still in `reading`: an unread window's
/// actors hold no alarm token yet, so this alone keeps the checkpoint behind
/// it. That earlier window's own completion then finds what follows.
pub(super) fn read(
    reading: &mut std::collections::BTreeSet<(u16, u64)>,
    consumed: &mut std::collections::BTreeMap<u16, u64>,
    windows: &mut std::collections::BTreeSet<(u64, u16)>,
    (shard, window): (u16, u64),
    next: Option<u64>,
) -> Result<(), Error> {
    let from = window
        .checked_add(1)
        .ok_or(Error::Invalid("alarm window overflow"))?;
    reading.remove(&(shard, window));
    if reading.range((shard, 0)..(shard, window)).next().is_none()
        && consumed
            .get(&shard)
            .is_some_and(|&consumed| consumed < from)
    {
        consumed.insert(shard, from);
        windows.extend(next.map(|next| (next, shard)));
    }
    Ok(())
}

/// The activation checkpoint of a shard that has observed entries before
/// `seen`, and whether it carries every failed actor forward. `holds` has one
/// item per failed actor: its latest activation entry at or past the durable
/// checkpoint, or None. Without a carry, the checkpoint stops at the earliest
/// such entry, so a restart replays every failed actor. Once another actor's
/// entry follows them all, or a failed actor has none, it passes `seen` and
/// commits a fresh entry per failed actor with it. A carried entry alone never
/// carries again, so a poisoned actor holds back only its own entry.
pub(super) fn checkpoint(seen: u64, holds: &[Option<u64>]) -> (u64, bool) {
    let mut earliest = seen;
    let mut latest = None;
    for &hold in holds {
        let Some(hold) = hold else {
            return (seen, true);
        };
        earliest = earliest.min(hold);
        latest = latest.max(Some(hold));
    }
    match latest {
        Some(latest) if seen > latest.saturating_add(1) => (seen, true),
        Some(_) => (earliest, false),
        None => (seen, false),
    }
}

/// The root child holding every shard's activation log and deadline windows.
const SCHEDULE: &str = "_schedule";

/// Whether `name`, relative to a state root, is a shard's activation log.
pub(super) fn wake_log(name: &str) -> bool {
    name.strip_prefix(SCHEDULE)
        .and_then(|name| name.strip_prefix('/'))
        .is_some_and(|shard| shard.len() == 4 && shard.bytes().all(|byte| byte.is_ascii_hexdigit()))
}

/// The scheduling partition of `actor` among `count`.
pub(super) fn shard(count: std::num::NonZeroU16, actor: &ActorReference) -> u16 {
    let mut hash = Sha256::new();
    hash.update(b"acyclic.actors.scheduling.v1\0");
    hash.update(actor.encode());
    let hash = hash.finalize();
    u16::from_be_bytes([hash[0], hash[1]]) % count.get()
}

/// A deadline window's child name: exactly the 20 ASCII decimal digits
/// `window_path` writes. Any other form (a sign, whitespace, another
/// length, or a value past `u64::MAX`) is a typed refusal, never parsed
/// leniently.
fn window_name(name: &str) -> Result<u64, Error> {
    let invalid = || Error::Invalid("invalid deadline window");
    if name.len() != 20 || !name.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid());
    }
    name.parse().map_err(|_| invalid())
}

/// A malformed runtime record is a typed refusal of that record alone,
/// observed by its error kind; the dispatcher continues past it.
fn refused<T>(decoded: Result<T, Error>) -> Option<T> {
    acyclic_observability::operation!("acyclic.actors.runtime_record", Error::kind, || decoded).ok()
}

impl<P: StreamProvider + ?Sized> Store<P> {
    /// Use `count` scheduling partitions. The count is a deployment chunking
    /// parameter: every producer and host sharing this state root must use
    /// the same one. Partitions split scheduling ownership, not actor capacity.
    #[must_use]
    pub const fn with_scheduling_shards(mut self, count: std::num::NonZeroU16) -> Self {
        self.scheduling_shards = count;
        self
    }

    /// This store's partition count.
    pub const fn scheduling_shards(&self) -> u16 {
        self.scheduling_shards.get()
    }

    /// The domain-separated SHA-256 of the canonical actor address, reduced to
    /// this store's partitions. Moving a partition between nodes never moves
    /// or renames its durable streams.
    pub fn scheduling_shard(&self, actor: &ActorReference) -> u16 {
        shard(self.scheduling_shards, actor)
    }

    /// The wake log of one stable scheduling partition. Producers append here
    /// in the same publication as input; only its assigned host follows it.
    pub fn scheduling_path(&self, shard: u16) -> Result<StreamPath, Error> {
        Ok(StreamPath::new(format!(
            "{}/{SCHEDULE}/{shard:04x}",
            self.root()
        ))?)
    }

    fn window_path(&self, shard: u16, window: u64) -> Result<StreamPath, Error> {
        Ok(StreamPath::new(format!(
            "{}/{window:020}",
            self.scheduling_path(shard)?
        ))?)
    }

    /// The unconditioned window append riding in the commit that arms an alarm.
    pub(super) fn deadline(
        &self,
        actor: &ActorReference,
        deadline_unix_ms: u64,
    ) -> Result<CommitEffect, Error> {
        Ok(CommitEffect::hard(
            Vec::new(),
            vec![CommitMutation::Append {
                path: self.window_path(self.scheduling_shard(actor), window(deadline_unix_ms))?,
                records: vec![actor.encode().into()],
            }],
        ))
    }

    /// The earliest deadline window of `shard` at or after `from`: one
    /// children page of limit one, independent of how many actors exist.
    /// A child that is not a window is refused and paged past, never fatal.
    pub(super) async fn next_window(&self, shard: u16, from: u64) -> Result<Option<u64>, Error> {
        let parent = self.scheduling_path(shard)?;
        let mut after = match from.checked_sub(1) {
            Some(before) => Some(self.window_path(shard, before)?),
            None => None,
        };
        loop {
            let request = ChildrenPageRequest {
                tail_stream: None,
                parent: Some(parent.clone()),
                after,
                hierarchy_version: None,
                limit: 1,
            };
            let page = match self.provider().children_page(request).await {
                Ok(page) => page,
                Err(error) if error.category() == StreamError::NotFound => return Ok(None),
                Err(error) => return Err(error.into()),
            };
            let Some(child) = page.children.into_iter().next() else {
                return Ok(None);
            };
            let window = refused(
                child
                    .path
                    .as_str()
                    .strip_prefix(parent.as_str())
                    .and_then(|name| name.strip_prefix('/'))
                    .ok_or(Error::Invalid("invalid deadline window"))
                    .and_then(window_name),
            );
            if window.is_some() {
                return Ok(window);
            }
            after = Some(child.path);
        }
    }

    /// One bounded page of the actors armed in a deadline window, from entry
    /// `from`, with the entry after it when the window has more.
    pub(super) async fn window_actors(
        &self,
        shard: u16,
        window: u64,
        from: u64,
    ) -> Result<(Vec<ActorReference>, Option<u64>), Error> {
        let page = self
            .provider()
            .read(ReadRequest {
                key: None,
                path: self.window_path(shard, window)?,
                from,
                limit: WINDOW_PAGE,
                at_least: Default::default(),
            })
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        let next = (page.len() == WINDOW_PAGE as usize)
            .then(|| from.saturating_add(u64::from(WINDOW_PAGE)));
        // A malformed record refuses only itself; the window's actors wake.
        let actors = page
            .into_iter()
            .filter_map(|record| {
                refused(
                    ActorReference::decode(record.value.as_ref())
                        .map_err(|_| Error::Invalid("invalid deadline window record")),
                )
            })
            .collect();
        Ok((actors, next))
    }

    /// The interest markers of `shard`: one child per actor that has held
    /// guest-added interests, named by its hex-encoded address.
    fn interests_path(&self, shard: u16) -> Result<StreamPath, Error> {
        Ok(StreamPath::new(format!(
            "{}/{SCHEDULE}/{INTERESTS}/{shard:04x}",
            self.root()
        ))?)
    }

    /// Mark `publication`'s actor as an interest holder when its new state
    /// subscribes beyond `manifest`. Producers into a guest-added interest do
    /// not know its observers, and nothing enforces their activation pairing
    /// (see [`Store::activation_record`]), so a dormant interest holder has no
    /// other wake after a restart: a placing host restores it from this
    /// marker. The marker rides soft in the same commit and is skipped once it
    /// exists, so it costs one condition and is written once per actor.
    pub(super) fn mark_interests(
        &self,
        publication: &mut super::PreparedPublish,
        actor: &ActorReference,
        manifest: &crate::guest::Manifest,
    ) -> Result<(), Error> {
        let Some(state) = &publication.state else {
            return Ok(());
        };
        match self.interest_marker(actor, state, manifest)? {
            Some(marker) => publication.join_host(marker),
            None => Ok(()),
        }
    }

    /// Persist the marker of an actor that holds guest-added interests but
    /// was created or resumed without a publication of its own (no input
    /// yet). Its activation is checkpointed once it goes idle, so without the
    /// marker a later host could never restore its interests. One soft commit,
    /// skipped once the marker exists.
    pub(super) async fn persist_interests(
        &self,
        actor: &ActorReference,
        state: &super::State,
        manifest: &crate::guest::Manifest,
    ) -> Result<(), Error> {
        let Some(marker) = self.interest_marker(actor, state, manifest)? else {
            return Ok(());
        };
        // Bounded and rooted: the marker's own path names exactly one write.
        let mut key = b"interests/".to_vec();
        key.extend(Sha256::digest(self.interest_marker_path(actor)?.as_str()));
        self.provider()
            .commit(CommitRequest {
                deadline_unix_millis: None,
                effects: vec![marker],
                idempotency_key: IdempotencyKey::new(key)?,
            })
            .await?;
        Ok(())
    }

    /// The soft marker effect for `actor` when `state` subscribes beyond
    /// `manifest`.
    fn interest_marker(
        &self,
        actor: &ActorReference,
        state: &super::State,
        manifest: &crate::guest::Manifest,
    ) -> Result<Option<CommitEffect>, Error> {
        if state
            .subscriptions()
            .keys()
            .all(|id| manifest.subscriptions.contains(id))
        {
            return Ok(None);
        }
        let path = self.interest_marker_path(actor)?;
        Ok(Some(CommitEffect {
            conditions: vec![acyclic_stream::CommitCondition::Absent { path: path.clone() }],
            mutations: vec![CommitMutation::Append {
                path,
                records: vec![actor.encode().into()],
            }],
            soft: true,
        }))
    }

    /// `actor`'s marker under its shard, named by its hex-encoded address.
    fn interest_marker_path(&self, actor: &ActorReference) -> Result<StreamPath, Error> {
        let name: String = actor
            .encode()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        Ok(StreamPath::new(format!(
            "{}/{name}",
            self.interests_path(self.scheduling_shard(actor))?
        ))?)
    }

    /// One page of `shard`'s interest holders after `after`, and where the
    /// next page starts. A malformed marker refuses only itself.
    pub(super) async fn interest_holders(
        &self,
        shard: u16,
        after: Option<StreamPath>,
    ) -> Result<(Vec<ActorReference>, Option<StreamPath>), Error> {
        let request = ChildrenPageRequest {
            tail_stream: None,
            parent: Some(self.interests_path(shard)?),
            after,
            hierarchy_version: None,
            limit: INTEREST_PAGE,
        };
        let page = match self.provider().children_page(request.clone()).await {
            Ok(page) => page,
            Err(error) if error.category() == StreamError::NotFound => {
                return Ok((Vec::new(), None));
            }
            Err(error) => return Err(error.into()),
        };
        page.validate_for(&request)?;
        let actors = page
            .children
            .iter()
            .filter_map(|child| {
                refused(
                    child
                        .path
                        .as_str()
                        .rsplit_once('/')
                        .map(|(_, name)| name)
                        .filter(|name| name.len() % 2 == 0)
                        .and_then(|name| {
                            (0..name.len())
                                .step_by(2)
                                .map(|at| u8::from_str_radix(name.get(at..at + 2)?, 16).ok())
                                .collect::<Option<Vec<u8>>>()
                        })
                        .and_then(|bytes| ActorReference::decode(&bytes).ok())
                        .ok_or(Error::Invalid("invalid interest marker")),
                )
            })
            .collect();
        Ok((actors, page.next_after))
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
