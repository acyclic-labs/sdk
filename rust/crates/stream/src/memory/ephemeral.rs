// Ephemeral subtrees. A lease root and everything published beneath it live
// only in this incarnation's memory: never journalled, snapshotted, listed or
// counted in the canonical decision order. Releasing the lease, or losing the
// process, removes the whole subtree; nothing is ever scanned to reap it.

/// Where one write publishes. Every write is classified once, under the state
/// lock that plans it.
#[derive(Clone)]
enum Scope {
    /// The canonical journalled decision order.
    Durable,
    /// The subtree of one live lease, by its root.
    Ephemeral(StreamPath),
}

#[derive(Default)]
struct Ephemeral {
    roots: std::collections::BTreeMap<StreamPath, EphemeralRoot>,
    /// Lease and ephemeral commit identities issued by this incarnation.
    issued: u64,
    /// Releases so far: a guard cut moves whenever a subtree may have vanished.
    released: u64,
}

struct EphemeralRoot {
    lease: u64,
    /// Retry identities of this subtree's publications, released with it.
    replays: std::collections::HashMap<Vec<u8>, EphemeralReplay>,
    records: usize,
    payload_bytes: usize,
}

/// A retained retry result: its request digest, outcome and revision index.
type Replayed<'a> = (&'a [u8; 32], &'a IdempotencyOutcome, u64);

struct EphemeralReplay {
    digest: [u8; 32],
    result: IdempotencyOutcome,
    index: u64,
    _allocation: Allocation,
}

impl Ephemeral {
    /// The lease root holding `path`: the path itself or its nearest ancestor.
    fn root_of(&self, path: &str) -> Option<&StreamPath> {
        if self.roots.is_empty() {
            return None;
        }
        path.match_indices('/')
            .map(|(end, _)| end)
            .chain(std::iter::once(path.len()))
            .find_map(|end| self.roots.get_key_value(path.get(..end)?))
            .map(|(root, _)| root)
    }

    /// Lease roots strictly beneath `path`, relative to it.
    fn nested(&self, path: &StreamPath) -> Result<Vec<crate::RelativePath>, StreamError> {
        if self.roots.is_empty() {
            return Ok(Vec::new());
        }
        let lower = format!("{}/", path.as_str());
        let upper = format!("{}0", path.as_str());
        self.roots
            .range::<str, _>((
                std::ops::Bound::Included(lower.as_str()),
                std::ops::Bound::Excluded(upper.as_str()),
            ))
            .map(|(root, _)| {
                StreamPath::new(
                    root.as_str()
                        .strip_prefix(lower.as_str())
                        .ok_or(StreamError::InvalidPath)?,
                )
            })
            .collect()
    }

    /// A write whose every published path lies in one lease's subtree is
    /// ephemeral; one publishing none is durable and may name no ephemeral
    /// path at all, since replay could never re-evaluate it. Anything else is
    /// refused.
    fn scope<'a>(
        &self,
        published: impl IntoIterator<Item = &'a StreamPath>,
        observed: impl IntoIterator<Item = &'a StreamPath>,
    ) -> Result<Scope, StreamError> {
        if self.roots.is_empty() {
            return Ok(Scope::Durable);
        }
        let mut published = published
            .into_iter()
            .map(|path| self.root_of(path.as_str()));
        let first = published.next().flatten();
        if published.any(|root| root != first) {
            return Err(StreamError::InvalidArgument);
        }
        match first {
            Some(root) => Ok(Scope::Ephemeral(root.clone())),
            None if observed
                .into_iter()
                .any(|path| self.root_of(path.as_str()).is_some()) =>
            {
                Err(StreamError::InvalidArgument)
            }
            None => Ok(Scope::Durable),
        }
    }

    fn append_scope(&self, request: &AppendRequest) -> Result<Scope, StreamError> {
        self.scope([&request.path], [])
    }

    fn fork_scope(&self, request: &ForkRequest) -> Result<Scope, StreamError> {
        self.scope([&request.destination], [&request.source])
    }

    fn commit_scope(&self, request: &CommitRequest) -> Result<Scope, StreamError> {
        self.scope(
            request.mutations().map(CommitMutation::path),
            request
                .conditions()
                .map(CommitCondition::path)
                .chain(request.mutations().filter_map(|mutation| match mutation {
                    CommitMutation::Fork { source, .. } => Some(source),
                    CommitMutation::Append { .. } => None,
                })),
        )
    }

    fn write_scope(&self, command: &crate::WriteRequest) -> Result<Scope, StreamError> {
        match command {
            crate::WriteRequest::Append(request) => self.append_scope(request),
            crate::WriteRequest::Fork(request) => self.fork_scope(request),
            crate::WriteRequest::Commit(request) | crate::WriteRequest::CommitAccepted(request) => {
                self.commit_scope(request)
            }
        }
    }

    fn replay(&self, scope: &Scope, key: &[u8]) -> Option<&EphemeralReplay> {
        let Scope::Ephemeral(root) = scope else {
            return None;
        };
        self.roots.get(root)?.replays.get(key)
    }

    fn retain(
        &mut self,
        scope: &Scope,
        key: &IdempotencyKey,
        digest: [u8; 32],
        result: IdempotencyOutcome,
        index: u64,
        allocation: Allocation,
    ) {
        if let Scope::Ephemeral(root) = scope
            && let Some(lease) = self.roots.get_mut(root)
        {
            lease.replays.insert(
                key.as_bytes().to_vec(),
                EphemeralReplay {
                    digest,
                    result,
                    index,
                    _allocation: allocation,
                },
            );
        }
    }

    /// `paths` without any lease's subtree: exactly the durable namespace.
    fn without(&self, paths: &Namespace) -> Namespace {
        let mut durable = paths.clone();
        for root in self.roots.keys().filter(|root| paths.contains_key(*root)) {
            durable
                .remove(root, &mut NamespaceBuilder::new(&|_| Ok(None)))
                .unwrap_or_else(|_| unreachable!("a present path's ancestors are present"));
        }
        durable
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
        scope: &Scope,
        key: &[u8],
    ) -> Result<Option<Replayed<'a>>, StreamError> {
        Ok(match scope {
            Scope::Durable => self
                .replay(state, key)?
                .map(|replay| (&replay.digest, &replay.result, replay.index)),
            Scope::Ephemeral(_) => state
                .ephemeral
                .replay(scope, key)
                .map(|replay| (&replay.digest, &replay.result, replay.index)),
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
                replays: Default::default(),
                records: 0,
                payload_bytes: 0,
            },
        );
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

    /// Removes one lease's whole subtree and wakes every follow beneath it.
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
            .remove(root)
            .ok_or(StreamError::Unavailable)?;
        state.ephemeral.released = state.ephemeral.released.wrapping_add(1);
        state.record_count = state.record_count.saturating_sub(held.records);
        state.payload_bytes = state.payload_bytes.saturating_sub(held.payload_bytes);
        if !state.paths.contains_key(root) {
            return Ok(());
        }
        let mut next = state.paths.clone();
        next.remove(root, &mut NamespaceBuilder::new(&|_| Ok(None)))?;
        state.path_bytes = next.bytes();
        let previous = std::mem::replace(&mut state.paths, next);
        let lower = format!("{}/", root.as_str());
        let upper = format!("{}0", root.as_str());
        let mut gone = state.signals.keys_in::<str, _>((
            std::ops::Bound::Included(lower.as_str()),
            std::ops::Bound::Excluded(upper.as_str()),
        ))?;
        gone.push(root.clone());
        for path in gone {
            if let Some(signals) = state.signals.get(&path)? {
                signals.changed.send_replace(0);
                crate::follow_index::FollowIndex::created(&signals.follows, 0);
            }
        }
        drop(state);
        drop(previous);
        Ok(())
    }
}
