// Ephemeral subtrees. A lease owns its root subtree and every subtree it
// adopts; all of them live only in this incarnation's memory: never
// journalled, snapshotted, listed or counted in the canonical decision order.
// Releasing the lease, or losing the process, removes exactly the subtrees it
// owns; each is recorded as it is created, so nothing is ever scanned to reap.

/// Where one write publishes. Every write is classified once, under the state
/// lock that plans it.
#[derive(Clone)]
enum Scope {
    /// The canonical journalled decision order.
    Durable,
    /// The subtrees of one live lease, by its root.
    Ephemeral(StreamPath),
}

#[derive(Default)]
struct Ephemeral {
    roots: std::collections::BTreeMap<StreamPath, EphemeralRoot>,
    /// The top of every owned subtree, rooted or adopted, to its lease root.
    owned: std::collections::BTreeMap<StreamPath, StreamPath>,
    /// Lease and ephemeral commit identities issued by this incarnation.
    issued: u64,
    receipts: EphemeralReceipts,
}

struct EphemeralRoot {
    lease: u64,
    live: Arc<std::sync::atomic::AtomicBool>,
    /// Subtrees this lease created outside its root, released with it.
    adopted: Vec<StreamPath>,
    /// Retry identities of this subtree's publications, released with it.
    first_commit: Option<CommitId>,
    records: usize,
    payload_bytes: usize,
}

/// A retained retry result: its digest, outcome, revision and original scope.
type Replayed<'a> = (&'a [u8; 32], &'a IdempotencyOutcome, u64, bool);

impl Ephemeral {
    /// The root of the lease owning `path`: through the path itself or its
    /// nearest owned ancestor.
    fn root_of(&self, path: &str) -> Option<&StreamPath> {
        if self.owned.is_empty() {
            return None;
        }
        path.match_indices('/')
            .map(|(end, _)| end)
            .chain(std::iter::once(path.len()))
            .find_map(|end| self.owned.get(path.get(..end)?))
    }

    /// Owned subtrees strictly beneath `path`, relative to it.
    fn nested(&self, path: &StreamPath) -> Result<Vec<crate::RelativePath>, StreamError> {
        if self.owned.is_empty() {
            return Ok(Vec::new());
        }
        let lower = format!("{}/", path.as_str());
        let upper = format!("{}0", path.as_str());
        self.owned
            .range::<str, _>((
                std::ops::Bound::Included(lower.as_str()),
                std::ops::Bound::Excluded(upper.as_str()),
            ))
            .map(|(top, _)| {
                StreamPath::new(
                    top.as_str()
                        .strip_prefix(lower.as_str())
                        .ok_or(StreamError::InvalidPath)?,
                )
            })
            .collect()
    }

    /// A write publishing into exactly one lease is ephemeral, provided every
    /// other path it publishes is created by it: those are adopted by that
    /// lease, so ownership always extends from a path the lease already
    /// holds. A fresh path needs a present parent, or one this write creates,
    /// so adoption never takes a shared ancestor. A write publishing into no
    /// lease is durable and its selected effects may name no ephemeral path, since replay
    /// could never re-evaluate it. Anything else is refused.
    fn scope<'a>(
        &self,
        paths: &Namespace,
        published: impl IntoIterator<Item = &'a StreamPath> + Clone,
        observed: impl IntoIterator<Item = &'a StreamPath>,
    ) -> Result<Scope, StreamError> {
        if self.owned.is_empty() {
            return Ok(Scope::Durable);
        }
        let mut lease = None;
        for root in published
            .clone()
            .into_iter()
            .filter_map(|path| self.root_of(path.as_str()))
        {
            if lease.is_some_and(|lease| lease != root) {
                return Err(StreamError::InvalidArgument);
            }
            lease = Some(root);
        }
        let Some(root) = lease else {
            return if observed
                .into_iter()
                .any(|path| self.root_of(path.as_str()).is_some())
            {
                Err(StreamError::InvalidArgument)
            } else {
                Ok(Scope::Durable)
            };
        };
        let all = published.clone();
        for path in published {
            if self.root_of(path.as_str()).is_some() {
                continue;
            }
            if paths.contains_key(path) {
                return Err(StreamError::InvalidArgument);
            }
            if path.parent().is_some_and(|parent| {
                !paths.contains_key(&parent)
                    && !all.clone().into_iter().any(|other| *other == parent)
            }) {
                return Err(StreamError::NotFound);
            }
        }
        Ok(Scope::Ephemeral(root.clone()))
    }

    fn append_scope(
        &self,
        paths: &Namespace,
        request: &AppendRequest,
    ) -> Result<Scope, StreamError> {
        self.scope(paths, [&request.path], [])
    }

    fn fork_scope(&self, paths: &Namespace, request: &ForkRequest) -> Result<Scope, StreamError> {
        self.scope(paths, [&request.destination], [&request.source])
    }

    /// Records the topmost path `published` created, absent from `previous`,
    /// as owned by `scope`'s lease unless that lease already owns it.
    fn adopt(&mut self, scope: &Scope, previous: &Namespace, published: &StreamPath) {
        let Scope::Ephemeral(root) = scope else {
            return;
        };
        if self.root_of(published.as_str()).is_some() {
            return;
        }
        let Some(lease) = self.roots.get_mut(root) else {
            return;
        };
        let mut top = published.clone();
        while let Some(parent) = top.parent().filter(|parent| !previous.contains_key(parent)) {
            top = parent;
        }
        lease.adopted.push(top.clone());
        self.owned.insert(top, root.clone());
    }
}

impl State {
    /// Advances the canonical decision order past one durable publication.
    /// An ephemeral one leaves it, and the commit clock, exactly as replay
    /// will rebuild them.
    fn advance(&mut self, scope: &Scope, time: u64) {
        if matches!(scope, Scope::Durable) {
            self.decision += 1;
            self.last_committed_at_micros = time;
        }
    }

    /// The revision index and identity of one publication of `digest`. A
    /// durable one takes the next canonical decision; an ephemeral one takes
    /// none, so replay assigns every durable identity exactly as issued.
    fn identity(
        &mut self,
        scope: &Scope,
        digest: [u8; 32],
    ) -> Result<(u64, CommitId), StreamError> {
        match scope {
            Scope::Durable => {
                let decision = self.decision.checked_add(1).ok_or(StreamError::Capacity)?;
                Ok((decision, commit_identity(decision, digest)))
            }
            Scope::Ephemeral(_) => {
                self.ephemeral.issued = self
                    .ephemeral
                    .issued
                    .checked_add(1)
                    .ok_or(StreamError::Capacity)?;
                Ok((
                    self.decision,
                    crate::preparation::ephemeral_identity(
                        self.decision,
                        self.ephemeral.issued,
                        digest,
                    ),
                ))
            }
        }
    }
}

impl MemoryStream {
    /// The retained result of one retry identity in `scope`.
    fn replay_in<'a>(
        &self,
        state: &'a State,
        key: &[u8],
    ) -> Result<Option<Replayed<'a>>, StreamError> {
        state.lookup_replay(key, |items| {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(work) = &self.work {
                work.record(work.meter.examine(items, 0))?;
            }
            let _ = items;
            Ok(())
        })
    }

    /// Whether each path lies in a lease's subtree.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn ephemeral_mask(&self, paths: &[StreamPath]) -> Vec<bool> {
        let state = self.state.read().await;
        paths
            .iter()
            .map(|path| state.ephemeral.root_of(path.as_str()).is_some())
            .collect()
    }

    /// Reserves `root`, which must be absent beneath a present parent, as an
    /// ephemeral subtree held by the returned lease.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn lease_ephemeral_inner(
        &self,
        root: StreamPath,
    ) -> Result<crate::EphemeralLease, StreamError> {
        let _publication = self.publication.lock().await;
        let mut state = self.state.write().await;
        if state.paths.contains_key(&root)
            || state.ephemeral.root_of(root.as_str()).is_some()
            || !state.ephemeral.nested(&root)?.is_empty()
        {
            return Err(StreamError::AlreadyExists);
        }
        if root
            .parent()
            .is_some_and(|parent| !state.paths.contains_key(&parent))
        {
            return Err(StreamError::NotFound);
        }
        let lease = state
            .ephemeral
            .issued
            .checked_add(1)
            .ok_or(StreamError::Capacity)?;
        state.ephemeral.issued = lease;
        state.ephemeral.roots.insert(
            root.clone(),
            EphemeralRoot {
                lease,
                live: Arc::new(std::sync::atomic::AtomicBool::new(true)),
                adopted: Vec::new(),
                first_commit: None,
                records: 0,
                payload_bytes: 0,
            },
        );
        state.ephemeral.owned.insert(root.clone(), root.clone());
        let stream = Self {
            work: None,
            ..self.clone()
        };
        let released = root.clone();
        Ok(crate::EphemeralLease::new(
            root,
            Box::pin(async move { stream.release_ephemeral(&released, lease).await }),
        ))
    }

    /// Removes every subtree one lease owns and wakes every follow in them.
    /// A later lease of the same root is never touched.
    async fn release_ephemeral(&self, root: &StreamPath, lease: u64) -> Result<(), StreamError> {
        let _publication = self.publication.lock().await;
        let mut state = self.state.write().await;
        if state
            .ephemeral
            .roots
            .get(root)
            .is_none_or(|held| held.lease != lease)
        {
            return Ok(());
        }
        let held = state
            .ephemeral
            .roots
            .get(root)
            .ok_or(StreamError::Unavailable)?;
        let mut next = state.paths.clone();
        let mut gone = Vec::new();
        for top in std::iter::once(root).chain(held.adopted.iter()) {
            if !next.contains_key(top) {
                continue;
            }
            next.remove(top, &mut NamespaceBuilder::new(&|_| Ok(None)))?;
            let lower = format!("{}/", top.as_str());
            let upper = format!("{}0", top.as_str());
            gone.extend(state.signals.keys_in::<str, _>((
                std::ops::Bound::Included(lower.as_str()),
                std::ops::Bound::Excluded(upper.as_str()),
            ))?);
            gone.push(top.clone());
        }
        let (mut notify, _notification_claim) = self.vector(gone.len())?;
        for path in gone {
            if let Some(signals) = state.signals.get(&path)? {
                notify.push(signals);
            }
        }
        // All fallible namespace preparation precedes the visibility change.
        let held = state
            .ephemeral
            .roots
            .remove(root)
            .ok_or(StreamError::Unavailable)?;
        held.live.store(false, std::sync::atomic::Ordering::Release);
        for top in std::iter::once(root).chain(held.adopted.iter()) {
            state.ephemeral.owned.remove(top);
        }
        if next.len() == next.canonical().len() {
            // Only private paths can disappear; no scan or reconciliation is needed.
            next = next.canonical();
        }
        state.record_count = state.record_count.saturating_sub(held.records);
        state.payload_bytes = state.payload_bytes.saturating_sub(held.payload_bytes);
        state.path_bytes = next.bytes();
        let previous = std::mem::replace(&mut state.paths, next);
        for signals in notify {
            signals.changed.send_replace(0);
            crate::follow_index::FollowIndex::created(&signals.follows, 0);
        }
        drop(state);
        drop(_publication);
        drop(previous);
        // Facts are already invisible. Retire one bounded pair of index
        // entries per lock hold, dropping payloads outside every store lock.
        let mut first = held.first_commit;
        while let Some(id) = first {
            let retired = self.state.write().await.ephemeral.receipts.release_one(id);
            first = retired.value().next;
            drop(retired);
        }
        Ok(())
    }
}
