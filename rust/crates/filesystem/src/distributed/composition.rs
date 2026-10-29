//! Filesystem contributions into one caller-built Streams commit.
//!
//! A caller (for example an agent spawn) builds one `CommitRequest` holding its
//! own conditions, mutations and idempotency key. Volume creations and forks
//! add their authority conditions and mutations to that same request, so the
//! whole composition commits all-or-none under the caller's single key. The
//! caller then submits it once, through [`StreamAuthorityStore::submit`], or
//! through its own provider and [`StreamAuthorityStore::settle`].
use super::*;

/// One prepared actor transition, sealed only by the composed commit receipt.
struct Contribution {
    actor: crate::kernel::allocation::retained::RetainedArc<crate::kernel::AuthorityActor>,
    revision: super::actor::Revision,
    projection: super::actor::PreparedProjection,
}

/// A caller-built Streams commit that filesystem operations contribute to.
///
/// Dropping it without a committed receipt is safe: every contributed actor
/// reloads its durable state on next use.
#[must_use = "a composed filesystem commit does nothing until it is submitted"]
pub struct FilesystemCommit {
    request: CommitRequest,
    authorities: Vec<AuthorityId>,
    contributions: Vec<Contribution>,
    holds: Vec<crate::PublicationHold>,
    /// Set once the request was fenced: retries resend these exact
    /// bytes and key, never a request re-fenced under a newer policy cut.
    sealed: bool,
    /// Every contribution and retry belongs to the same provider scope.
    owner: Option<[u8; 16]>,
    // Last: request vectors and held publication guards retire before funding.
    allocation: AuthorityAllocation,
}

/// Terminal and nonterminal results of one composed submission.
#[must_use]
pub enum FilesystemCommitOutcome {
    /// Every caller and filesystem mutation committed under one receipt.
    Committed(acyclic_stream::CommitReceipt),
    /// Nothing changed; a caller or filesystem condition did not hold.
    Rejected(Vec<acyclic_stream::CommitConflict>),
    /// The result is unknown. Retry exactly this commit: its idempotency key
    /// and bytes are unchanged, so a lost acknowledgement replays the original
    /// receipt. Custody of every contribution stays with the commit.
    Unknown {
        /// The unchanged commit to resubmit.
        commit: FilesystemCommit,
        /// Why the result is unknown.
        error: AuthorityStoreError,
    },
}

impl FilesystemCommit {
    pub(super) fn seal_request(&mut self, request: CommitRequest) {
        self.request = request;
        self.seal_current_request();
    }

    pub(super) fn seal_current_request(&mut self) {
        self.sealed = true;
    }
    /// Starts from the caller's own conditions, mutations and idempotency key.
    pub fn new(request: CommitRequest) -> Self {
        Self {
            request,
            authorities: Vec::new(),
            contributions: Vec::new(),
            holds: Vec::new(),
            sealed: false,
            owner: None,
            allocation: None,
        }
    }

    /// The composed request. Call [`StreamAuthorityStore::seal`] before
    /// submitting it through an external provider.
    #[must_use]
    pub fn request(&self) -> &CommitRequest {
        &self.request
    }

    /// Frozen exclusive expiry carried by the canonical request itself.
    /// External submitters send that same request, including its deadline.
    #[must_use]
    pub fn deadline(&self) -> Option<u64> {
        self.request.deadline_unix_millis
    }

    /// Adds caller conditions or mutations. Filesystem contributions own their
    /// own entries; changing those makes the commit fail its conditions.
    /// Returns `None` once the commit was sealed: its bytes and key
    /// are fixed so an exact retry replays the original receipt.
    pub fn request_mut(&mut self) -> Option<&mut CommitRequest> {
        (!self.sealed).then_some(&mut self.request)
    }

    /// Volume authorities this commit creates.
    #[must_use]
    pub fn authorities(&self) -> &[AuthorityId] {
        &self.authorities
    }

    pub(crate) fn hold(&mut self, hold: crate::PublicationHold) {
        self.holds.push(hold);
    }

    pub(super) fn reserve_hold(
        &mut self,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), AuthorityStoreError> {
        grow(&mut self.holds, 1, work)
    }

    pub(super) fn check_owner(&self, owner: [u8; 16]) -> Result<(), AuthorityStoreError> {
        if self.owner.is_some_and(|bound| bound != owner) {
            return Err(AuthorityStoreError::RejectedStatic(
                "filesystem commit belongs to another Streams owner",
            ));
        }
        Ok(())
    }

    pub(super) fn bind_owner(&mut self, owner: [u8; 16]) {
        self.owner = Some(owner);
    }

    pub(super) fn retain_request_custody(&mut self, work: &mut AuthorityWork<'_>) {
        let _ = work;
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.allocation = work.request_allocation.take();
        }
    }

    pub(super) fn record_holds(
        &self,
        prepared: &Self,
        work: &mut AuthorityWork<'_>,
    ) -> Result<Vec<crate::PublicationHold>, AuthorityStoreError> {
        work.vec(
            self.holds
                .len()
                .checked_add(prepared.holds.len())
                .ok_or(crate::WorkError::Overflow)?,
        )
    }

    pub(super) fn install_records(
        &mut self,
        prepared: Self,
        mut combined: Vec<crate::PublicationHold>,
    ) {
        let Self {
            request,
            mut holds,
            sealed,
            owner,
            allocation,
            ..
        } = prepared;
        combined.append(&mut self.holds);
        combined.append(&mut holds);
        self.request = request;
        self.holds = combined;
        self.sealed = sealed;
        self.owner = owner;
        self.allocation = allocation;
    }

    fn ensure_unclaimed(&self, authority: AuthorityId) -> Result<(), AuthorityStoreError> {
        if self.sealed {
            return Err(AuthorityStoreError::RejectedStatic(
                "a sealed filesystem commit cannot accept contributions",
            ));
        }
        if self.authorities.contains(&authority) {
            return Err(AuthorityStoreError::RejectedStatic(
                "a composed commit names one authority twice",
            ));
        }
        Ok(())
    }

    pub(crate) fn ensure_sealed(&self) -> Result<(), AuthorityStoreError> {
        if self.sealed {
            Ok(())
        } else {
            Err(AuthorityStoreError::RejectedStatic(
                "filesystem commit was not sealed before submission",
            ))
        }
    }

    /// Guards a multi-part preparation: unless disarmed after every part
    /// succeeded, dropping the guard (on an error, or when the preparing
    /// future itself is dropped) removes everything added since.
    pub(crate) fn guard(&mut self) -> CommitGuard<'_> {
        CommitGuard {
            mark: Some(self.mark()),
            commit: self,
        }
    }

    fn mark(&self) -> CommitMark {
        CommitMark {
            conditions: self.request.conditions.len(),
            mutations: self.request.mutations.len(),
            authorities: self.authorities.len(),
            contributions: self.contributions.len(),
            holds: self.holds.len(),
            owner: self.owner,
        }
    }

    /// Drops everything added after `mark`. Dropped contributions never
    /// complete, so their actors keep their heads.
    fn rollback(&mut self, mark: CommitMark) {
        self.request.conditions.truncate(mark.conditions);
        self.request.mutations.truncate(mark.mutations);
        self.authorities.truncate(mark.authorities);
        self.contributions.truncate(mark.contributions);
        self.holds.truncate(mark.holds);
        self.owner = mark.owner;
    }
}

/// A position in a [`FilesystemCommit`] that a failed preparation rolls back to.
#[derive(Clone, Copy)]
struct CommitMark {
    owner: Option<[u8; 16]>,
    conditions: usize,
    mutations: usize,
    authorities: usize,
    contributions: usize,
    holds: usize,
}

/// Rolls a [`FilesystemCommit`] back to its mark on drop unless disarmed.
pub(crate) struct CommitGuard<'c> {
    commit: &'c mut FilesystemCommit,
    mark: Option<CommitMark>,
}

impl CommitGuard<'_> {
    pub(crate) fn commit(&mut self) -> &mut FilesystemCommit {
        self.commit
    }

    /// Keeps everything added since the mark.
    pub(crate) fn disarm(mut self) {
        self.mark = None;
    }
}

impl Drop for CommitGuard<'_> {
    fn drop(&mut self) {
        if let Some(mark) = self.mark.take() {
            self.commit.rollback(mark);
        }
    }
}

/// Upper bound on the conditions, and separately the mutations, that one
/// creation adds; matches the standalone creation request.
const CREATION_ENTRIES: usize = 9;

/// Makes room for `additional` entries through funded authority work. The
/// caller built its request with only its own capacity.
pub(super) fn grow<T>(
    entries: &mut Vec<T>,
    additional: usize,
    work: &mut AuthorityWork<'_>,
) -> Result<(), AuthorityStoreError> {
    if entries.capacity() - entries.len() >= additional {
        return Ok(());
    }
    let capacity = entries
        .len()
        .checked_add(additional)
        .ok_or(crate::WorkError::Overflow)?;
    let mut grown = work.vec(capacity)?;
    grown.append(entries);
    *entries = grown;
    Ok(())
}

impl<P: crate::distributed::AuthorityProvider + ?Sized> StreamAuthorityStore<P> {
    async fn contribute_creation(
        &self,
        commit: &mut FilesystemCommit,
        authority: AuthorityId,
        proposed: &ProposedCommit,
        source: Option<(StreamPath, u64, GenerationId)>,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), AuthorityStoreError> {
        let owner = self.operation_claims().owner_identity();
        commit.check_owner(owner)?;
        commit.ensure_unclaimed(authority)?;
        // The creation is built in its own request and joins the caller's only
        // once every entry is prepared: a failed preparation leaves the
        // commit exactly as it was, with no publishable fragment. One creation
        // adds at most this many conditions and mutations: four absent
        // streams, records, epoch, gate, lineage and namespace changes.
        let mut part = CommitRequest {
            deadline_unix_millis: None,
            conditions: work.vec(CREATION_ENTRIES)?,
            mutations: work.vec(CREATION_ENTRIES)?,
            idempotency_key: commit.request.idempotency_key.clone(),
        };
        let actor = self.actor(authority, work)?;
        let journal = actor.lease(work).await?;
        let revision = actor.begin(journal)?;
        let projection = self.creation_request(
            &mut part,
            authority,
            Epoch::GENESIS,
            Some(proposed),
            source,
            work,
        )?;
        grow(&mut commit.request.conditions, part.conditions.len(), work)?;
        grow(&mut commit.request.mutations, part.mutations.len(), work)?;
        commit.request.conditions.append(&mut part.conditions);
        commit.request.mutations.append(&mut part.mutations);
        commit.authorities.push(authority);
        commit.contributions.push(Contribution {
            actor,
            revision,
            projection,
        });
        commit.bind_owner(owner);
        Ok(())
    }

    /// Adds one volume's first record to `commit`.
    pub(crate) async fn contribute_first_record(
        &self,
        commit: &mut FilesystemCommit,
        authority: AuthorityId,
        proposed: &ProposedCommit,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), AuthorityStoreError> {
        self.contribute_creation(commit, authority, proposed, None, work)
            .await
    }

    /// Adds one workspace fork (its retention and destination authorities) to
    /// `commit`. A published-prefix fork shares the source lineage by a Streams
    /// fork mutation in the same commit.
    pub(crate) async fn contribute_fork(
        &self,
        commit: &mut FilesystemCommit,
        fork: &WorkspaceForkCommit,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), AuthorityStoreError> {
        if fork.destination == fork.retention {
            return Err(AuthorityStoreError::RejectedStatic(
                "a fork retains into its own destination",
            ));
        }
        let lineage = match fork.lineage {
            Some(source) if source.lineage == crate::GenerationFork::PublishedPrefix => {
                let (lineage, tail) = self
                    .resolve_source_fork_point(source.authority, source.generation, work)
                    .await?;
                Some((lineage, tail, source.generation))
            }
            _ => None,
        };
        self.contribute_creation(commit, fork.retention, &fork.retained, None, work)
            .await?;
        self.contribute_creation(commit, fork.destination, &fork.creation, lineage, work)
            .await
    }

    /// Conditions `commit` on `authority`'s head still being `generation`
    /// when it applies: its lineage holds exactly the records up to and
    /// including `generation`. A source that published past `generation`
    /// between the caller's read and the commit makes the whole commit fail,
    /// with nothing published. Requiring the same head twice adds one
    /// condition; requiring two different heads of one source is refused.
    pub(crate) async fn contribute_head(
        &self,
        commit: &mut FilesystemCommit,
        authority: AuthorityId,
        generation: GenerationId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), AuthorityStoreError> {
        let owner = self.operation_claims().owner_identity();
        commit.check_owner(owner)?;
        if commit.sealed {
            return Err(AuthorityStoreError::RejectedStatic(
                "a sealed filesystem commit cannot accept contributions",
            ));
        }
        let (path, expected) = self
            .resolve_source_fork_point(authority, generation, work)
            .await?;
        grow(&mut commit.request.conditions, 1, work)?;
        super::policy::merge_condition(
            &mut commit.request.conditions,
            CommitCondition::Tail { path, expected },
            work,
        )?;
        commit.bind_owner(owner);
        Ok(())
    }

    /// Fences the active access-policy cut once and freezes the request.
    ///
    /// External submitters must call this before sending [`FilesystemCommit::request`].
    /// Repeated calls preserve the captured conditions and idempotency key,
    /// even if the current policy cut changes before an exact retry.
    pub fn seal(
        &self,
        commit: &mut FilesystemCommit,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), AuthorityStoreError> {
        let owner = self.operation_claims().owner_identity();
        commit.check_owner(owner)?;
        if !commit.sealed {
            let copied = work.copy_commit(&commit.request)?;
            let mut request = self.with_policy_cuts(copied, &[], work)?;
            acyclic_stream::request::validate_commit(&mut request).map_err(map_stream_error)?;
            commit.request = request;
            commit.sealed = true;
            commit.bind_owner(owner);
            commit.retain_request_custody(work);
        }
        Ok(())
    }

    /// Seals and submits a composed commit once, through this store's provider.
    /// Local refusal also returns Unknown with the original owner: this may be
    /// a retry of an earlier attempt whose publication is still uncertain.
    pub async fn submit(
        &self,
        mut commit: FilesystemCommit,
        work: &mut AuthorityWork<'_>,
    ) -> Result<FilesystemCommitOutcome, AuthorityStoreError> {
        let prepared = self
            .seal(&mut commit, work)
            .and_then(|()| work.copy_commit(&commit.request));
        let request = match prepared {
            Ok(request) => request,
            Err(error) => return Ok(FilesystemCommitOutcome::Unknown { commit, error }),
        };
        let result = self
            .commit(request, commit.deadline(), work, None::<&mut ()>)
            .await;
        if !work.is_native()
            && let Err(error) = work.release_request()
        {
            return Ok(FilesystemCommitOutcome::Unknown { commit, error });
        }
        match result {
            Ok((outcome, _allocation)) => self.settle(commit, outcome, work).await,
            Err(error) => Ok(FilesystemCommitOutcome::Unknown {
                commit,
                error: error.into_error(std::convert::identity),
            }),
        }
    }

    /// Settles a composed commit that the caller submitted itself, from the
    /// exact outcome its provider returned for [`FilesystemCommit::request`].
    pub async fn settle(
        &self,
        commit: FilesystemCommit,
        outcome: CommitOutcome,
        work: &mut AuthorityWork<'_>,
    ) -> Result<FilesystemCommitOutcome, AuthorityStoreError> {
        if let Err(error) = commit
            .check_owner(self.operation_claims().owner_identity())
            .and_then(|()| commit.ensure_sealed())
        {
            return Ok(FilesystemCommitOutcome::Unknown { commit, error });
        }
        Ok(match outcome {
            CommitOutcome::Committed(envelope) => {
                self.refresh_committed(commit, envelope.commit_id, work)
                    .await;
                FilesystemCommitOutcome::Committed(envelope)
            }
            CommitOutcome::Conflict(conflicts) => FilesystemCommitOutcome::Rejected(conflicts),
        })
    }

    /// Refreshes cached heads after `commit` committed as `commit_id`. Every
    /// mutation is already durable, so this never decides the result: a
    /// failure leaves the journal Vacant from `begin`, and the next lease
    /// re-reads the durable head instead of trusting a stale one.
    pub(crate) async fn refresh_committed(
        &self,
        commit: FilesystemCommit,
        commit_id: acyclic_stream::CommitId,
        work: &mut AuthorityWork<'_>,
    ) {
        let FilesystemCommit {
            contributions,
            holds,
            allocation,
            ..
        } = commit;
        for Contribution {
            actor,
            revision,
            projection,
        } in contributions
        {
            let completed = actor
                .complete(revision, projection.seal(commit_id), work)
                .await;
            #[cfg(not(target_arch = "wasm32"))]
            if let Err(error) = &completed {
                tracing::warn!(?error, "composed commit cache refresh failed");
            }
            drop(completed);
        }
        drop(holds);
        drop(allocation);
    }
}
