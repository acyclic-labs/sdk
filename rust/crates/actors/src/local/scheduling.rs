//! Stable scheduling identity, independent of placement and process hash seeds.
//!
//! Each shard is one stream (its activation log) whose direct children are
//! deadline windows `<shard>/<020 minute>`: a commit that arms an alarm
//! appends the actor to the window holding its deadline. The stream count
//! grows with distinct minutes, never with distinct milliseconds, and a host
//! finds its earliest pending window with one bounded children page after its
//! consumed watermark, never by scanning actors. A due window wakes its
//! actors; each delivery then installs its exact deadline as a resident wake.
use super::{ActorReference, Error, Store, StreamPath, StreamProvider};
use acyclic_stream::{
    ChildrenPageRequest, CommitEffect, CommitMutation, ReadRequest, StreamError,
    StreamProviderExt as _,
};
use futures::TryStreamExt as _;
use sha2::{Digest, Sha256};

/// Records read per deadline-window page.
const WINDOW_PAGE: u32 = 256;
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
                    .filter(|name| name.len() == 20)
                    .and_then(|name| name.parse::<u64>().ok())
                    .ok_or(Error::Invalid("invalid deadline window")),
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
}
