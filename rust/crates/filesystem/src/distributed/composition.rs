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
    actor: Arc<crate::kernel::AuthorityActor>,
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
    /// Set once the request was fenced and sent: retries resend these exact
    /// bytes and key, never a request re-fenced under a newer policy cut.
    submitted: bool,
}

/// Terminal and nonterminal results of one composed submission.
#[must_use]
pub enum FilesystemCommitOutcome {
    /// Every caller and filesystem mutation committed under one receipt.
    Committed(acyclic_stream::CommittedEnvelope),
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
    /// Starts from the caller's own conditions, mutations and idempotency key.
    pub fn new(request: CommitRequest) -> Self {
        Self {
            request,
            authorities: Vec::new(),
            contributions: Vec::new(),
            holds: Vec::new(),
            submitted: false,
        }
    }

    /// The composed request as it will be submitted.
    #[must_use]
    pub fn request(&self) -> &CommitRequest {
        &self.request
    }

    /// Adds caller conditions or mutations. Filesystem contributions own their
    /// own entries; changing those makes the commit fail its conditions.
    /// Returns `None` once the commit was submitted: its sent bytes and key
    /// are fixed so an exact retry replays the original receipt.
    pub fn request_mut(&mut self) -> Option<&mut CommitRequest> {
        (!self.submitted).then_some(&mut self.request)
    }

    /// Volume authorities this commit creates.
    #[must_use]
    pub fn authorities(&self) -> &[AuthorityId] {
        &self.authorities
    }

    pub(crate) fn hold(&mut self, hold: crate::PublicationHold) {
        self.holds.push(hold);
    }

    fn ensure_unclaimed(&self, authority: AuthorityId) -> Result<(), AuthorityStoreError> {
        if self.authorities.contains(&authority) {
            return Err(AuthorityStoreError::RejectedStatic(
                "a composed commit names one authority twice",
            ));
        }
        Ok(())
    }

    /// Marks the commit's current contents, so a multi-part preparation can
    /// undo every part it added when a later part fails.
    pub(crate) fn mark(&self) -> CommitMark {
        CommitMark {
            conditions: self.request.conditions.len(),
            mutations: self.request.mutations.len(),
            authorities: self.authorities.len(),
            contributions: self.contributions.len(),
            holds: self.holds.len(),
        }
    }

    /// Drops everything added after `mark`. Dropped contributions never
    /// complete, so their actors keep their heads.
    pub(crate) fn rollback(&mut self, mark: CommitMark) {
        self.request.conditions.truncate(mark.conditions);
        self.request.mutations.truncate(mark.mutations);
        self.authorities.truncate(mark.authorities);
        self.contributions.truncate(mark.contributions);
        self.holds.truncate(mark.holds);
    }
}

/// A position in a [`FilesystemCommit`] that a failed preparation rolls back to.
#[derive(Clone, Copy)]
pub(crate) struct CommitMark {
    conditions: usize,
    mutations: usize,
    authorities: usize,
    contributions: usize,
    holds: usize,
}

/// Upper bound on the conditions, and separately the mutations, that one
/// creation adds; matches the standalone creation request.
const CREATION_ENTRIES: usize = 9;

/// Makes room for `additional` entries through funded authority work. The
/// caller built its request with only its own capacity.
fn grow<T>(
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
        commit.ensure_unclaimed(authority)?;
        // The creation is built in its own request and joins the caller's only
        // once every entry is prepared: a failed preparation leaves the
        // commit exactly as it was, with no publishable fragment. One creation
        // adds at most this many conditions and mutations: four absent
        // streams, records, epoch, gate, lineage and namespace changes.
        let mut part = CommitRequest {
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

    /// Submits a composed commit once, through this store's provider.
    pub async fn submit(
        &self,
        mut commit: FilesystemCommit,
        work: &mut AuthorityWork<'_>,
    ) -> Result<FilesystemCommitOutcome, AuthorityStoreError> {
        // The first submission fences the active access-policy cut, as every
        // filesystem publication does, and keeps the fenced request. Retries
        // after `Unknown` resend exactly those bytes and that key.
        if !commit.submitted {
            commit.request = self.with_policy_cuts(commit.request.clone(), &[], work)?;
            commit.submitted = true;
        }
        let result = self
            .commit(commit.request.clone(), None, work, None::<&mut ()>)
            .await;
        if !work.is_native() {
            work.release_request()?;
        }
        match result {
            Ok((outcome, _allocation)) => self.settle(commit, outcome, work).await,
            Err(error) => match error.into_error(std::convert::identity) {
                // The key already holds different bytes: a retry cannot succeed.
                error @ AuthorityStoreError::NativeStream(
                    acyclic_stream::StreamError::IdempotencyMismatch,
                ) => Err(error),
                error => Ok(FilesystemCommitOutcome::Unknown { commit, error }),
            },
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
        match outcome {
            CommitOutcome::Committed(envelope) => {
                let FilesystemCommit {
                    contributions,
                    holds,
                    ..
                } = commit;
                for Contribution {
                    actor,
                    revision,
                    projection,
                } in contributions
                {
                    // Every mutation is already durable: the receipt is the
                    // result. Refreshing the cached head is best-effort. A
                    // failure leaves the journal Vacant from `begin`, so the
                    // next lease re-reads the durable head instead of trusting
                    // a stale one.
                    let completed = actor
                        .complete(revision, projection.seal(envelope.commit_id), work)
                        .await;
                    #[cfg(not(target_arch = "wasm32"))]
                    if let Err(error) = &completed {
                        tracing::warn!(?error, "composed commit cache refresh failed");
                    }
                    drop(completed);
                }
                drop(holds);
                Ok(FilesystemCommitOutcome::Committed(envelope))
            }
            CommitOutcome::Conflict(conflicts) => Ok(FilesystemCommitOutcome::Rejected(conflicts)),
        }
    }
}
