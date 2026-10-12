use super::*;

/// Authority lineage semantics for one workspace fork.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GenerationFork {
    /// Reuse the source authority's durable lineage through the selected generation.
    PublishedPrefix,
    /// Start independent authority history from an authenticated unpublished generation.
    Independent,
}

/// Exact source and lineage semantics for one authority fork.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GenerationForkSource {
    /// Source authority containing the selected generation.
    pub authority: AuthorityId,
    /// Exact authenticated source generation.
    pub generation: GenerationId,
    /// Whether durable source lineage is reused.
    pub lineage: GenerationFork,
}

/// A new workspace's complete authority state: its own authority, holding
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkspaceForkCommit {
    /// Source lineage the destination authority starts from, on backends
    pub lineage: Option<GenerationForkSource>,
    /// The new workspace's authority.
    pub destination: AuthorityId,
    /// Its creation record, the destination's first commit.
    pub creation: ProposedCommit,
    /// The authority retaining the source generation for the new workspace.
    pub retention: AuthorityId,
    /// The retention record, the retention authority's first commit.
    pub retained: ProposedCommit,
}

/// How one workspace fork's authority state resolved.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkspaceForkOutcome {
    /// Both authorities hold exactly the requested first records, now or
    Committed,
    /// The retention authority already holds a different first record.
    RetentionConflict,
    /// The destination authority already holds a different first record.
    CreationRejected,
}

/// Largest first record a fork compares when resolving a retry.
const MAXIMUM_FIRST_RECORD_BYTES: u64 = 4 * 1024;

pub(crate) async fn seal<G: AuthorityGuard>(guard: Option<&mut G>, work: &mut AuthorityWork<'_>)
    -> Result<(), AuthorityError<G::Error>> {
    if let Some(guard) = guard { guard.seal(work).await?; }
    Ok(())
}

pub(crate) async fn workspace_fork<S: AsyncAuthorityStore + ?Sized, G: AuthorityGuard>(
    store: &S, fork: WorkspaceForkCommit, permit: PublicationPermit, work: &mut AuthorityWork<'_>, mut guard: Option<&mut G>,
) -> Result<WorkspaceForkOutcome, AuthorityError<G::Error>> {
    #[cfg(not(target_arch = "wasm32"))]
    if work.original.is_some() { work.check()?; }
    if permit != PublicationPermit::Unrestricted {
        return Err(crate::AuthorityStoreError::RejectedStatic("authority backend cannot atomically evaluate fork publication leases").into());
    }
    let WorkspaceForkCommit { lineage, destination, creation, retention, retained } = fork;
    #[cfg(not(target_arch = "wasm32"))]
    let native = work.original.is_some();
    #[cfg(target_arch = "wasm32")]
    let native = false;
    let retained = if native {
        store.create_first_record(retention, retained, work, None::<&mut ()>).await
    } else {
        first_record(store, retention, retained, work, None::<&mut ()>).await
    }.map_err(|error| AuthorityError::Authority(error.into_error(std::convert::identity)))?;
    if !retained { return Ok(WorkspaceForkOutcome::RetentionConflict); }
    match lineage {
        Some(source) => store.fork_authority(source, destination, creation.operation_id, work, guard.as_deref_mut()).await?,
        None => store.create_authority(destination, Epoch::GENESIS, work, guard.as_deref_mut()).await?,
    };
    let created = if native || guard.is_some() {
        store.create_first_record(destination, creation, work, guard).await?
    } else {
        first_record(store, destination, creation, work, None::<&mut ()>).await
            .map_err(|error| AuthorityError::Authority(error.into_error(std::convert::identity)))?
    };
    Ok(if created { WorkspaceForkOutcome::Committed } else { WorkspaceForkOutcome::CreationRejected })
}

pub(crate) async fn first_record<S: AsyncAuthorityStore + ?Sized, G: AuthorityGuard>(
    store: &S, authority: AuthorityId, commit: ProposedCommit, work: &mut AuthorityWork<'_>, mut guard: Option<&mut G>,
) -> Result<bool, AuthorityError<G::Error>> {
    let created =store.create_authority(authority, Epoch::GENESIS, work, guard.as_deref_mut()).await?;
    let head = match created { CreateAuthorityOutcome::Created(head) | CreateAuthorityOutcome::Existing(head) => head };
    let identity = (commit.operation_id, commit.fingerprint);
    seal(guard.as_deref_mut(), work).await?;
    let (appended, allocation) = store.compare_and_append(GuardedAppend {
        authority_id: authority, epoch: head.epoch, expected: Head::genesis(head.epoch), commit,
        permit: PublicationPermit::Unrestricted,
    }, work, None::<&mut ()>).await.map_err(|error| AuthorityError::Authority(error.into_error(std::convert::identity)))?;
    let retry = matches!(&appended, AppendOutcome::AlreadyCommitted(_) | AppendOutcome::Conflict { .. });
    let matches = match appended {
        AppendOutcome::Committed(_) | AppendOutcome::AlreadyCommitted(_) => true,
        AppendOutcome::Conflict { .. } => {
            let (first, _allocation) = store.replay(authority, Sequence::GENESIS,
                ReplayLimit { records: 1, payload_bytes: MAXIMUM_FIRST_RECORD_BYTES }, work).await?;
            first.first().is_some_and(|record| record.sequence == Sequence::new(1)
                && (record.operation_id, record.fingerprint) == identity)
        }
        AppendOutcome::Fenced { .. } | AppendOutcome::IdempotencyConflict { .. } => false,
    };
    drop(allocation);
    if matches && retry { seal(guard.as_deref_mut(), work).await?; }
    Ok(matches)
}

