// Live recovery facts share the collections radix mechanism. The lease owns
// an intrusive list of commit IDs; release visits only that lease's facts.
// Neither index participates in the durable snapshot inventory.
const EPHEMERAL_KEY_BYTES: usize = crate::MAX_IDEMPOTENCY_KEY_BYTES + 8;
type EphemeralKey = [u8; EPHEMERAL_KEY_BYTES];
type EphemeralKeys = acyclic_collections::FixedIndex<CommitId, Allocation, EPHEMERAL_KEY_BYTES>;
type EphemeralCommits = acyclic_collections::FixedIndex<EphemeralReplay, Allocation, 32>;

#[derive(Default)]
struct EphemeralReceipts {
    keys: EphemeralKeys,
    commits: EphemeralCommits,
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct ReplayPin {
    fact: RetainedFactRef,
    pub(crate) ephemeral: bool,
}

#[cfg(not(target_arch = "wasm32"))]
impl MemoryStream {
    fn project_pinned_append(
        &self,
        request: &AppendRequest,
        pin: &ReplayPin,
    ) -> Result<AppendOutcome, StreamError> {
        if self.append_request_digest(request)? != pin.fact.digest {
            return Err(StreamError::IdempotencyMismatch);
        }
        let IdempotencyOutcome::Append(value) = &pin.fact.result else {
            return Err(StreamError::IdempotencyMismatch);
        };
        Ok(self.replayed_append(value))
    }

    pub(crate) fn project_pinned_replay(
        &self,
        request: &mut CommitRequest,
        pin: &ReplayPin,
        accepted: bool,
    ) -> Result<(crate::WriteOutcome, Allocation), StreamError> {
        let digest = self.digest(request)?;
        self.project_replayed_commit(
            request,
            digest,
            (&pin.fact.digest, &pin.fact.result, pin.fact.index),
            accepted,
        )
    }

    fn project_replayed_commit(
        &self,
        request: &CommitRequest,
        digest: [u8; 32],
        (replayed, result, index): Replayed<'_>,
        accepted: bool,
    ) -> Result<(crate::WriteOutcome, Allocation), StreamError> {
        if *replayed != digest {
            return Err(StreamError::IdempotencyMismatch);
        }
        let IdempotencyOutcome::Commit(value) = result else {
            return Err(StreamError::IdempotencyMismatch);
        };
        let (receipt, mut claim) = self.replayed(request, value, self.revisions.at(index))?;
        let envelope = self.selected_envelope(value, accepted, &mut claim)?;
        commit_projection(CommitOutcome::Committed(receipt), envelope)
            .map(|outcome| (outcome, claim))
    }
}

struct EphemeralReplay {
    live: Arc<std::sync::atomic::AtomicBool>,
    next: Option<CommitId>,
    fact: RetainedFactRef,
}

impl std::ops::Deref for EphemeralReplay {
    type Target = RetainedFact;
    fn deref(&self) -> &Self::Target {
        &self.fact
    }
}

impl EphemeralReplay {
    fn is_live(&self) -> bool {
        self.live.load(std::sync::atomic::Ordering::Acquire)
    }
    fn envelope(&self) -> &CommittedEnvelope {
        self.fact.envelope().unwrap_or_else(|| {
            unreachable!("successful append, fork and commit retain their original envelope")
        })
    }
}

struct ReceiptProbe<F>(F);

impl<E, F: FnMut(u64) -> Result<(), E>> acyclic_collections::IndexReadWork for ReceiptProbe<F> {
    type Error = E;
    fn validate(&mut self) -> Result<(), E> {
        (self.0)(0)
    }
    fn examine(&mut self, items: u64) -> Result<(), E> {
        (self.0)(items)
    }
}

fn ephemeral_key<E>(
    key: &[u8],
    work: &mut impl acyclic_collections::IndexReadWork<Error = E>,
) -> Result<EphemeralKey, E> {
    match acyclic_collections::binary_key(key, work) {
        Ok(key) => Ok(key),
        Err(acyclic_collections::BinaryKeyError::Work(error)) => Err(error),
        Err(_) => unreachable!("a validated Stream retry key fits its complete index width"),
    }
}

impl State {
    fn lookup_fact<E>(
        &self,
        key: &[u8],
        mut probe: impl FnMut(u64) -> Result<(), E>,
    ) -> Result<Option<(&RetainedFactRef, bool)>, E> {
        if let Some(replay) = self.replay(key, &mut probe)? {
            return Ok(Some((&replay.fact, false)));
        }
        if self.ephemeral.roots.is_empty() {
            return Ok(None);
        }
        Ok(self
            .ephemeral
            .receipts
            .lookup(key, probe)?
            .map(|replay| (&replay.fact, true)))
    }

    fn lookup_replay<E>(
        &self,
        key: &[u8],
        probe: impl FnMut(u64) -> Result<(), E>,
    ) -> Result<Option<Replayed<'_>>, E> {
        Ok(self
            .lookup_fact(key, probe)?
            .map(|(fact, _)| (&fact.digest, &fact.result, fact.index)))
    }

    fn ephemeral_key_is_spent<E>(
        &self,
        key: &[u8],
        probe: impl FnMut(u64) -> Result<(), E>,
    ) -> Result<bool, E> {
        if self.ephemeral.roots.is_empty() {
            return Ok(false);
        }
        Ok(self.ephemeral.receipts.lookup(key, probe)?.is_some())
    }
}

struct EphemeralRetention {
    key: EphemeralKey,
    allocation: Allocation,
    probes: ReplayProbes,
    fact: RetainedFactRef,
}

impl MemoryStream {
    fn reserve_ephemeral(
        &self,
        scope: &Scope,
        key: &IdempotencyKey,
    ) -> Result<Option<EphemeralRetention>, StreamError> {
        if matches!(scope, Scope::Durable) {
            return Ok(None);
        }
        let key = ephemeral_key(key.as_bytes(), &mut HistoryIndexWork(self))?;
        let keys = EphemeralKeys::insertion_bound(1).ok_or(StreamError::Capacity)?;
        let commits = EphemeralCommits::insertion_bound(1).ok_or(StreamError::Capacity)?;
        let bytes = keys
            .allocation_bytes
            .checked_add(commits.allocation_bytes)
            .and_then(|bytes| usize::try_from(bytes).ok())
            .ok_or(StreamError::Capacity)?;
        let items = keys
            .examined_items
            .checked_add(commits.examined_items)
            .ok_or(StreamError::Capacity)?;
        Ok(Some(EphemeralRetention {
            key,
            allocation: self.allocate(bytes)?,
            probes: self.reserve_replay_probes(items)?,
            fact: RetainedFact::reserved(self.allocate(
                std::mem::size_of::<RetainedFact>() + 2 * std::mem::size_of::<usize>(),
            )?),
        }))
    }
}

struct ReceiptInsertWork<'a> {
    stream: &'a MemoryStream,
    allocation: &'a mut Allocation,
    probes: &'a mut ReplayProbes,
}

impl acyclic_collections::IndexWork for ReceiptInsertWork<'_> {
    type Error = std::convert::Infallible;
    type Allocation = Allocation;

    fn check(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
    fn charge_items(&mut self, items: u64) -> Result<(), Self::Error> {
        // Acceptance consumes the original reservation even after cancellation;
        // a meter's recording error cannot interrupt namespace/receipt install.
        self.stream.record_replay_probe(self.probes, items);
        Ok(())
    }
    fn claim(&mut self, bytes: u64) -> Result<Allocation, Self::Error> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(allocation) = self.allocation.as_mut() {
            let bytes = usize::try_from(bytes).unwrap_or_else(|_| {
                unreachable!("a physical index node has a representable layout")
            });
            return Ok(Some(allocation.split(bytes).unwrap_or_else(|_| {
                unreachable!("two fixed-index insertions fit their admitted backing bound")
            })));
        }
        let _ = bytes;
        Ok(None)
    }
}

impl Ephemeral {
    fn retain(
        &mut self,
        stream: &MemoryStream,
        scope: &Scope,
        retention: EphemeralRetention,
        mut fact: RetainedFact,
    ) {
        let Scope::Ephemeral(root) = scope else {
            unreachable!("only leased writes reserve ephemeral retention")
        };
        let lease = self
            .roots
            .get_mut(root)
            .unwrap_or_else(|| unreachable!("the publication lock retains the write's lease"));
        let EphemeralRetention {
            key: indexed_key,
            mut allocation,
            mut probes,
            fact: mut owner,
        } = retention;
        let reserved = owner
            .get_mut()
            .unwrap_or_else(|| unreachable!("unpublished fact has one owner"));
        fact._header = reserved._header.take();
        *reserved = fact;
        let id = owner
            .envelope()
            .unwrap_or_else(|| unreachable!("successful write envelope"))
            .commit_id;
        let fact = EphemeralReplay {
            live: Arc::clone(&lease.live),
            next: lease.first_commit,
            fact: owner,
        };
        let mut work = ReceiptInsertWork {
            stream,
            allocation: &mut allocation,
            probes: &mut probes,
        };
        *self
            .receipts
            .keys
            .get_or_try_insert_with(indexed_key, &mut work, |_| Ok(id))
            .unwrap_or_else(|never| match never {}) = id;
        self.receipts
            .commits
            .get_or_try_insert_with(*id.as_bytes(), &mut work, |_| Ok(fact))
            .unwrap_or_else(|never| match never {});
        lease.first_commit = Some(id);
    }
}

impl EphemeralReceipts {
    fn lookup<E>(
        &self,
        key: &[u8],
        probe: impl FnMut(u64) -> Result<(), E>,
    ) -> Result<Option<&EphemeralReplay>, E> {
        let mut work = ReceiptProbe(probe);
        let key = ephemeral_key(key, &mut work)?;
        let Some(id) = self.keys.get(key, &mut work)? else {
            return Ok(None);
        };
        Ok(self
            .commits
            .get(*id.as_bytes(), &mut work)?
            .filter(|fact| fact.is_live()))
    }

    fn release_one(
        &mut self,
        id: CommitId,
    ) -> acyclic_collections::Entry<EphemeralReplay, Allocation, 32> {
        let fact = self
            .commits
            .remove_if(*id.as_bytes(), |_| true)
            .unwrap_or_else(|| unreachable!("a lease owns every fact in its retirement list"));
        let key = ephemeral_key(
            fact.value()
                .replay
                .as_deref()
                .unwrap_or_else(|| unreachable!("leased retry key")),
            &mut ReceiptProbe(|_| Ok::<(), std::convert::Infallible>(())),
        )
        .unwrap_or_else(|never| match never {});
        self.keys.retire_if(key, |owner| *owner == id);
        fact
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn ephemeral_lookup_bound() -> Result<u64, StreamError> {
    // The shared variable-width bound also bounds a fixed-width lookup.
    // Add the exact encoding work; no population-dependent traversal exists.
    BinaryKey(&[0; EPHEMERAL_KEY_BYTES])
        .lookup_examinations()
        .and_then(|keys| keys.checked_add(BinaryKey(&[0; 32]).lookup_examinations()?))
        .and_then(|items| items.checked_add(EPHEMERAL_KEY_BYTES as u64))
        .ok_or(StreamError::Capacity)
}
