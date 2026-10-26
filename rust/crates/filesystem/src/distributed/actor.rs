//! Journal facts owned by the same provider/authority entry as proposal batches.
//! No provider or caller callback is retained by an idle projection.
use super::*;
mod generations;
use generations::{GenerationCatalog, GenerationIdentity};
pub(super) use generations::{GenerationKnowledge, GenerationSeeds};

#[derive(Clone)]
pub(crate) struct Projection {
    pub(super) snapshot: AuthoritySnapshot,
    pub(super) origin: Option<OperationId>,
    pub(super) gate_tail: u64,
    pub(super) active: Option<OperationId>,
    pub(super) changes: Option<(crate::VolumeId, u64)>,
    pub(super) lineage_tail: u64,
    generations: Option<crate::kernel::allocation::retained::RetainedArc<GenerationCatalog>>,
    publication: Option<(crate::VolumeId, ObjectId)>,
}

/// Only Ready grants a submission cursor. Unsettled retains the same durable
/// facts solely for sizing; it never predicts the publication's receipt.
pub(crate) enum Journal {
    Vacant,
    Ready(Projection),
    Unsettled(Projection),
}

impl Journal {
    pub(crate) fn writable(&self) -> Option<&Projection> {
        match self {
            Self::Ready(projection) => Some(projection),
            Self::Vacant | Self::Unsettled(_) => None,
        }
    }

    fn writable_mut(&mut self) -> Option<&mut Projection> {
        match self {
            Self::Ready(projection) => Some(projection),
            Self::Vacant | Self::Unsettled(_) => None,
        }
    }
}

pub(super) struct Revision(u64);

impl Revision {
    pub(super) fn get(&self) -> u64 {
        self.0
    }
}

impl crate::kernel::AuthorityActor {
    pub(super) fn begin(
        &self,
        journal: tokio::sync::MutexGuard<'_, Journal>,
    ) -> Result<Revision, AuthorityStoreError> {
        self.begin_transition(journal, false)
    }

    pub(super) fn begin_publication(
        &self,
        journal: tokio::sync::MutexGuard<'_, Journal>,
    ) -> Result<Revision, AuthorityStoreError> {
        self.begin_transition(journal, true)
    }

    fn begin_transition(
        &self,
        mut journal: tokio::sync::MutexGuard<'_, Journal>,
        retain_observation: bool,
    ) -> Result<Revision, AuthorityStoreError> {
        let revision = self
            .revision
            .fetch_update(
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
                |revision| revision.checked_add(1),
            )
            .map_err(|_| crate::WorkError::Overflow)?
            + 1;
        let previous = std::mem::replace(&mut *journal, Journal::Vacant);
        if retain_observation && let Journal::Ready(durable) = previous {
            *journal = Journal::Unsettled(durable);
        }
        Ok(Revision(revision))
    }

    pub(super) async fn complete(
        &self,
        revision: Revision,
        projection: Projection,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), AuthorityStoreError> {
        let mut journal = self.lease(work).await?;
        if self.revision.load(std::sync::atomic::Ordering::Relaxed) == revision.0 {
            *journal = Journal::Ready(projection);
        }
        Ok(())
    }

    pub(crate) async fn lease(
        &self,
        work: &mut AuthorityWork<'_>,
    ) -> Result<tokio::sync::MutexGuard<'_, Journal>, AuthorityStoreError> {
        work.check()?;
        if let Ok(lease) = self.journal.try_lock() {
            return Ok(lease);
        }
        let cancellation = work.cancellation;
        let cancelled = work
            .accounting()
            .cancellation_wait(cancellation)
            .map_err(authority_error)?;
        let locked = self.journal.lock();
        tokio::pin!(locked, cancelled);
        let lease = match futures::future::select(locked, cancelled).await {
            futures::future::Either::Left((lease, _)) => lease,
            futures::future::Either::Right(_) => return Err(AuthorityStoreError::Cancelled),
        };
        work.check()?;
        Ok(lease)
    }
}

impl<P: crate::distributed::AuthorityProvider + ?Sized> StreamAuthorityStore<P> {
    pub(super) fn actor(
        &self,
        authority: AuthorityId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<Arc<crate::kernel::AuthorityActor>, AuthorityStoreError> {
        crate::kernel::authority_actor(
            self.operation_claims(),
            self.placement.actor_scope(),
            authority,
            &mut work.accounting(),
        )
        .map_err(authority_error)
    }

    async fn initialize_projection(
        &self,
        authority: AuthorityId,
        snapshot: AuthoritySnapshot,
        work: &mut AuthorityWork<'_>,
    ) -> Result<Projection, AuthorityStoreError> {
        let (_, gate_tail, active) = self.reservation_gate(authority, work).await?;
        let lineage_tail = match self.tail_of(authority, "/lineage", work).await {
            Ok(tail) => tail,
            Err(error) if authority_missing(&error) => 0,
            Err(error) => return Err(error),
        };
        let origin = if snapshot.head.sequence == Sequence::GENESIS {
            None
        } else {
            Some(
                self.read_at(authority, "/records", 1, work, |record, work| {
                    let durable = authority_decode_borrowed(authority, &record.value, work)?;
                    if durable.sequence != Sequence::new(1) || record.sequence != 1 {
                        return Err(authority_corrupt(
                            "actor origin has an invalid sequence",
                            work,
                        ));
                    }
                    Ok(durable.operation_id)
                })
                .await?,
            )
        };
        Ok(Projection {
            snapshot,
            origin,
            gate_tail,
            active,
            changes: None,
            lineage_tail,
            generations: None,
            publication: None,
        })
    }

    pub(super) async fn revalidate_projection(
        &self,
        authority: AuthorityId,
        projection: &mut Journal,
        work: &mut AuthorityWork<'_>,
    ) -> Result<Projection, AuthorityStoreError> {
        let snapshot = self.fork_snapshot(authority, work).await?;
        if projection
            .writable()
            .is_none_or(|known| known.snapshot != snapshot)
        {
            *projection = Journal::Vacant;
            *projection = Journal::Ready(
                self.initialize_projection(authority, snapshot, work)
                    .await?,
            );
        }
        work.check()?;
        projection
            .writable()
            .cloned()
            .ok_or(AuthorityStoreError::Missing)
    }

    pub(super) async fn activate(
        &self,
        authority: AuthorityId,
        volume: Option<crate::VolumeId>,
        projection: &mut Journal,
        work: &mut AuthorityWork<'_>,
    ) -> Result<Projection, AuthorityStoreError> {
        if projection.writable().is_none() {
            // An unsettled predecessor supplies no right to submit against
            // its preceding immutable record identity.
            *projection = Journal::Vacant;
            let snapshot = self.fork_snapshot(authority, work).await?;
            *projection = Journal::Ready(
                self.initialize_projection(authority, snapshot, work)
                    .await?,
            );
        }
        let activated = projection
            .writable_mut()
            .ok_or(AuthorityStoreError::RejectedStatic(
                "authority activation did not install its projection",
            ))?;
        self.bind_projection(authority, volume, activated, work)
            .await
    }

    pub(super) async fn quotation_projection(
        &self,
        authority: AuthorityId,
        volume: Option<crate::VolumeId>,
        journal: &mut Journal,
        work: &mut AuthorityWork<'_>,
    ) -> Result<Projection, AuthorityStoreError> {
        if let Journal::Unsettled(durable) = journal {
            return self.bind_projection(authority, volume, durable, work).await;
        }
        self.activate(authority, volume, journal, work).await
    }

    async fn bind_projection(
        &self,
        authority: AuthorityId,
        volume: Option<crate::VolumeId>,
        activated: &mut Projection,
        work: &mut AuthorityWork<'_>,
    ) -> Result<Projection, AuthorityStoreError> {
        if let Some(volume) = volume {
            if volume_authority_id(volume) != authority {
                return Err(AuthorityStoreError::RejectedStatic(
                    "projection volume differs from authority",
                ));
            }
            match activated.changes {
                Some((owned, _)) if owned != volume => {
                    return Err(AuthorityStoreError::RejectedStatic(
                        "projection changed volume identity",
                    ));
                }
                Some(_) => {}
                None => {
                    let path = super::changes::changes_path(&self.placement, volume, work)?;
                    let tail = match self.tail(path, work).await {
                        Ok(tail) => tail,
                        Err(error) if authority_missing(&error) => 0,
                        Err(error) => return Err(error),
                    };
                    activated.changes = Some((volume, tail));
                }
            }
        };
        work.check()?;
        Ok(activated.clone())
    }
}

/// A pending cursor cannot supply a Streams identity until durability settles.
pub(super) struct PreparedProjection {
    head: Head,
    epoch_tail: u64,
    record_identity: PreparedRecordIdentity,
    origin: Option<OperationId>,
    gate_tail: u64,
    active: Option<OperationId>,
    changes: Option<(crate::VolumeId, u64)>,
    lineage_tail: u64,
    generations: Option<crate::kernel::allocation::retained::RetainedArc<GenerationCatalog>>,
    generation_identity: Option<GenerationIdentity>,
    publication: Option<(crate::VolumeId, ObjectId)>,
}

enum PreparedRecordIdentity {
    Appended,
    Unchanged(acyclic_stream::CommitId),
}

impl Projection {
    pub(super) fn prepare_epoch_fence(
        self,
        epoch: Epoch,
    ) -> Result<PreparedProjection, AuthorityStoreError> {
        if self.snapshot.head.epoch.get().checked_add(1) != Some(epoch.get()) {
            return Err(AuthorityStoreError::EpochExhausted);
        }
        Ok(PreparedProjection {
            head: Head {
                epoch,
                ..self.snapshot.head
            },
            epoch_tail: self
                .snapshot
                .epoch_tail
                .checked_add(1)
                .ok_or(crate::WorkError::Overflow)?,
            // A fence appends epochs, never an authority record. Its receipt
            // cannot replace the preceding record's immutable identity.
            record_identity: PreparedRecordIdentity::Unchanged(self.snapshot.record_commit_id),
            origin: self.origin,
            gate_tail: self.gate_tail,
            active: self.active,
            changes: self.changes,
            lineage_tail: self.lineage_tail,
            generations: self.generations,
            generation_identity: None,
            publication: self.publication,
        })
    }

    /// An observation for a proposed CAS, never a linearizable head read.
    /// A takeover is discovered by the same durable Head condition as append.
    pub(crate) fn publication_root(
        &self,
        request: crate::kernel::PublishGenerationRequest,
    ) -> Option<(Head, ObjectId)> {
        let (volume, root) = self.publication?;
        (volume == request.volume_id
            && self.snapshot.head.epoch == request.expected.epoch
            && self.snapshot.head.sequence >= request.expected.sequence)
            .then_some((self.snapshot.head, root))
    }

    pub(super) fn prepare(
        mut self,
        head: Head,
        members: (OperationId, Option<&crate::PublicationChanges>),
        records: &StreamPath,
        transaction: &CommitRequest,
        (placement, authority): (&AuthorityPlacement, AuthorityId),
        work: &mut AuthorityWork<'_>,
    ) -> Result<PreparedProjection, AuthorityStoreError> {
        if head.epoch != self.snapshot.head.epoch || head.sequence <= self.snapshot.head.sequence {
            return Err(AuthorityStoreError::RejectedStatic(
                "prepared actor cursor did not advance",
            ));
        }
        let view = super::append::AppendView {
            transaction: Some(transaction),
            ..Default::default()
        };
        let Some((start, rows)) = view.staged(records, work)? else {
            return Err(AuthorityStoreError::RejectedStatic(
                "prepared actor cursor has no records",
            ));
        };
        if start != self.snapshot.head.sequence.get() + 1
            || start.checked_add(crate::foundation::usize_to_u64(rows.len()))
                != head.sequence.get().checked_add(1)
        {
            return Err(AuthorityStoreError::RejectedStatic(
                "prepared actor cursor disagrees with its prefix",
            ));
        }
        let lineage = work.path(placement, authority, "/lineage", None)?;
        let mut generation_identity = None;
        if let Some((start, rows)) = view.staged(&lineage, work)? {
            if let Some(catalog) = &self.generations {
                generation_identity = Some(catalog.prepare(start, rows, work)?);
            }
            self.lineage_tail = start
                .checked_add(crate::foundation::usize_to_u64(rows.len()))
                .ok_or(crate::WorkError::Overflow)?;
        }
        if let Some((volume, tail)) = &mut self.changes {
            let changes = super::changes::changes_path(placement, *volume, work)?;
            if let Some((start, rows)) = view.staged(&changes, work)? {
                *tail = start
                    .checked_add(crate::foundation::usize_to_u64(rows.len()))
                    .ok_or(crate::WorkError::Overflow)?;
            }
        }
        Ok(PreparedProjection {
            head,
            epoch_tail: self.snapshot.epoch_tail,
            record_identity: PreparedRecordIdentity::Appended,
            origin: self
                .origin
                .or((self.snapshot.head.sequence == Sequence::GENESIS).then_some(members.0)),
            gate_tail: self.gate_tail,
            active: self.active,
            changes: self.changes,
            lineage_tail: self.lineage_tail,
            generations: self.generations,
            generation_identity,
            // Opaque records cannot inherit a preceding publication's root.
            publication: members.1.map(|seal| (seal.volume(), seal.root())),
        })
    }
}

impl PreparedProjection {
    pub(super) fn creation(
        head: Head,
        origin: Option<OperationId>,
        changes: Option<(crate::VolumeId, u64)>,
        lineage_tail: u64,
        publication: Option<&crate::PublicationChanges>,
        seeds: GenerationSeeds,
        work: &mut AuthorityWork<'_>,
    ) -> Result<Self, AuthorityStoreError> {
        let (generations, identity) = GenerationCatalog::creation(
            seeds.inherited,
            seeds.published.map(|generation| (generation, lineage_tail)),
            work,
        )?;
        Ok(Self {
            head,
            epoch_tail: 1,
            record_identity: PreparedRecordIdentity::Appended,
            origin,
            gate_tail: 1,
            active: None,
            changes,
            lineage_tail,
            generations: Some(generations),
            generation_identity: Some(identity),
            publication: publication.map(|seal| (seal.volume(), seal.root())),
        })
    }

    pub(super) fn seal(self, commit_id: acyclic_stream::CommitId) -> Projection {
        if let Some(identity) = self.generation_identity {
            identity.seal(commit_id);
        }
        Projection {
            snapshot: AuthoritySnapshot {
                head: self.head,
                epoch_tail: self.epoch_tail,
                record_commit_id: match self.record_identity {
                    PreparedRecordIdentity::Appended => commit_id,
                    PreparedRecordIdentity::Unchanged(identity) => identity,
                },
            },
            origin: self.origin,
            gate_tail: self.gate_tail,
            active: self.active,
            changes: self.changes,
            lineage_tail: self.lineage_tail,
            generations: self.generations,
            publication: self.publication,
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn unsettled_journal_facts_never_grant_submission_rights()
    -> Result<(), Box<dyn std::error::Error>> {
        let (store, authority) = super::super::append::group_tests::fixture().await?;
        let cancellation = CancellationToken::new();
        let mut work = AuthorityWork::new(crate::performance::BOUNDARY_BUDGET, &cancellation);
        let actor = store.actor(authority, &mut work)?;
        let mut journal = actor.lease(&mut work).await?;
        let durable = store
            .activate(authority, None, &mut journal, &mut work)
            .await?;
        let publication = actor.begin_publication(journal)?;
        let mut journal = actor.lease(&mut work).await?;
        assert!(
            journal.writable().is_none(),
            "unsettled facts granted an old submission cursor"
        );
        let quote = store
            .quotation_projection(authority, None, &mut journal, &mut work)
            .await?;
        assert!(quote.snapshot == durable.snapshot);
        assert!(
            matches!(&*journal, Journal::Unsettled(_)),
            "quotation promoted an unsealed journal"
        );
        assert!(
            journal.writable().is_none(),
            "quotation granted submission rights"
        );
        let mutation = actor.begin(journal)?;
        actor
            .complete(publication, durable.clone(), &mut work)
            .await?;
        assert!(
            matches!(&*actor.lease(&mut work).await?, Journal::Vacant),
            "obsolete publication restored facts after another mutation"
        );
        actor.complete(mutation, durable, &mut work).await?;
        assert!(actor.lease(&mut work).await?.writable().is_some());
        Ok(())
    }

    #[tokio::test]
    async fn actor_completion_cannot_replace_a_newer_journal_revision()
    -> Result<(), Box<dyn std::error::Error>> {
        let (store, authority) = super::super::append::group_tests::fixture().await?;
        let cancellation = CancellationToken::new();
        let mut work = AuthorityWork::new(crate::performance::BOUNDARY_BUDGET, &cancellation);
        let actor = store.actor(authority, &mut work)?;
        let mut journal = actor.lease(&mut work).await?;
        let original = store
            .activate(authority, None, &mut journal, &mut work)
            .await?;
        let first = actor.begin(journal)?;
        let second = actor.begin(actor.lease(&mut work).await?)?;
        let newer = Projection {
            gate_tail: original.gate_tail + 1,
            ..original.clone()
        };
        actor.complete(second, newer.clone(), &mut work).await?;
        actor.complete(first, original, &mut work).await?;
        assert_eq!(
            actor
                .lease(&mut work)
                .await?
                .writable()
                .ok_or("missing completed projection")?
                .gate_tail,
            newer.gate_tail,
            "late actor completion overwrote a newer journal revision"
        );
        Ok(())
    }

    #[tokio::test]
    async fn actor_lease_cancellation_releases_a_waiter_without_taking_the_journal()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::future::Future;
        use std::task::{Context, Poll};
        crate::initialize_test_native_io();
        let mut failures = Vec::new();
        for native in [false, true] {
            let (store, authority) = super::super::append::group_tests::fixture().await?;
            let cancellation = CancellationToken::new();
            let original = crate::NativeMaterializationWork::new(
                crate::performance::BOUNDARY_BUDGET,
                &cancellation,
            )
            .map_err(|(error, _)| error)?;
            let mut work = if native {
                AuthorityWork::native(&original, &cancellation)
            } else {
                AuthorityWork::new(crate::performance::BOUNDARY_BUDGET, &cancellation)
            };
            let actor = store.actor(authority, &mut work)?;
            let mut held = actor.journal.lock().await;
            store
                .activate(authority, None, &mut held, &mut work)
                .await?;
            let waiter = actor.lease(&mut work);
            let mut waiter = std::pin::pin!(waiter);
            let mut context = Context::from_waker(futures::task::noop_waker_ref());
            assert!(matches!(waiter.as_mut().poll(&mut context), Poll::Pending));
            cancellation.cancel();
            if !matches!(
                waiter.as_mut().poll(&mut context),
                Poll::Ready(Err(AuthorityStoreError::Cancelled))
            ) {
                failures.push(native);
            }
            assert!(
                held.writable().is_some(),
                "a cancelled waiter invalidated the journal"
            );
            drop(held);
        }
        assert!(
            failures.is_empty(),
            "cancelled actor waiter remained queued behind the journal owner: {failures:?}"
        );
        Ok(())
    }

    async fn publication_entry(
        store: &StreamAuthorityStore<acyclic_stream::MemoryStream>,
        authority: AuthorityId,
        requests: &[crate::GuardedAppend],
        entry: u8,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), AuthorityError<AuthorityStoreError>> {
        let request = &requests[0];
        match entry {
            0 => {
                store
                    .compare_and_append(request.clone(), work, None::<&mut ()>)
                    .await?;
            }
            1 => {
                store
                    .compare_and_append_group(requests, &[], work, None::<&mut ()>)
                    .await?;
            }
            2 => {
                store
                    .create_authority(authority, Epoch::GENESIS, work, None::<&mut ()>)
                    .await?;
            }
            3 => {
                store
                    .create_first_record(authority, request.commit.clone(), work, None::<&mut ()>)
                    .await?;
            }
            4 => {
                store
                    .reserve_publication(
                        authority,
                        request.expected,
                        request.commit.operation_id,
                        work,
                    )
                    .await?;
            }
            5 => {
                store.fence(authority, request.expected, work).await?;
            }
            6 => {
                store.retire_authority(authority, work).await?;
            }
            7 => {
                store.quote_group_append(request, 0, work).await?;
            }
            8 => {
                store
                    .release_publication(
                        crate::PublicationReservation {
                            authority_id: authority,
                            operation_id: request.commit.operation_id,
                            expected: request.expected,
                            gate_tail: 1,
                        },
                        work,
                    )
                    .await?;
            }
            _ => {
                fork_entry(store, authority, request, entry, work).await?;
            }
        }
        Ok(())
    }

    async fn fork_entry(
        store: &StreamAuthorityStore<acyclic_stream::MemoryStream>,
        authority: AuthorityId,
        request: &crate::GuardedAppend,
        entry: u8,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), AuthorityError<AuthorityStoreError>> {
        if entry == 9 {
            store
                .fork_authority(
                    crate::GenerationForkSource {
                        authority: AuthorityId::from_bytes([31; 16]),
                        generation: GenerationId::new(Digest::ZERO),
                        lineage: crate::GenerationFork::PublishedPrefix,
                    },
                    authority,
                    request.commit.operation_id,
                    work,
                    None::<&mut ()>,
                )
                .await?;
        } else {
            // Exercise cancellation while acquiring either of the two ordered
            // identities, including release of the already acquired first one.
            let retention = AuthorityId::from_bytes([if entry == 10 { 34 } else { 32 }; 16]);
            store
                .fork_workspace(
                    WorkspaceForkCommit {
                        lineage: None,
                        destination: authority,
                        creation: request.commit.clone(),
                        retention,
                        retained: request.commit.clone(),
                    },
                    PublicationPermit::Unrestricted,
                    work,
                    None::<&mut ()>,
                )
                .await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_publication_entry_points_leave_the_actor_owner_undisturbed()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::future::Future;
        use std::task::{Context, Poll};
        crate::initialize_test_native_io();
        let mut failures = Vec::new();
        for native in [false, true] {
            for entry in 0..12 {
                let (store, authority) = super::super::append::group_tests::fixture().await?;
                let requests = super::super::append::group_tests::requests(authority, 1)?;
                let cancellation = CancellationToken::new();
                let original = crate::NativeMaterializationWork::new(
                    crate::performance::BOUNDARY_BUDGET,
                    &cancellation,
                )
                .map_err(|(error, _)| error)?;
                let mut work = if native {
                    AuthorityWork::native(&original, &cancellation)
                } else {
                    AuthorityWork::new(crate::performance::BOUNDARY_BUDGET, &cancellation)
                };
                let actor = store.actor(authority, &mut work)?;
                let mut held = actor.journal.lock().await;
                store
                    .activate(authority, None, &mut held, &mut work)
                    .await?;
                let future = publication_entry(&store, authority, &requests, entry, &mut work);
                let mut future = std::pin::pin!(future);
                let mut context = Context::from_waker(futures::task::noop_waker_ref());
                assert!(
                    matches!(future.as_mut().poll(&mut context), Poll::Pending),
                    "entry {entry}, native {native}"
                );
                cancellation.cancel();
                if !matches!(
                    future.as_mut().poll(&mut context),
                    Poll::Ready(Err(AuthorityError::Authority(
                        AuthorityStoreError::Cancelled
                    )))
                ) {
                    failures.push((entry, native));
                }
                assert!(held.writable().is_some());
                drop(held);
            }
        }
        assert!(
            failures.is_empty(),
            "publication entry remained queued after cancellation: {failures:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn owned_journal_projection_reuses_the_durable_cursor_across_batches()
    -> Result<(), Box<dyn std::error::Error>> {
        crate::initialize_test_native_io();
        for native in [false, true] {
            let (store, authority) = super::super::append::group_tests::fixture().await?;
            let requests = super::super::append::group_tests::requests(authority, 2)?;
            let cancellation = CancellationToken::new();
            let original = crate::NativeMaterializationWork::new(
                crate::performance::BOUNDARY_BUDGET,
                &cancellation,
            )
            .map_err(|(error, _)| error)?;
            for request in &requests {
                let mut work = if native {
                    AuthorityWork::native(&original, &cancellation)
                } else {
                    AuthorityWork::new(crate::performance::BOUNDARY_BUDGET, &cancellation)
                };
                let outcome = store
                    .compare_and_append_group(
                        std::slice::from_ref(request),
                        &[],
                        &mut work,
                        None::<&mut ()>,
                    )
                    .await?;
                let AppendGroupOutcome::Published {
                    receipts,
                    allocation,
                } = outcome
                else {
                    return Err("actor rejected an exact successor".into());
                };
                assert!(matches!(
                    receipts.as_slice(),
                    [FsAppendOutcome::Committed(_)]
                ));
                {
                    // A fresh exact-head operation is admitted by the existing
                    // atomic locator condition, with no preflight authority I/O.
                    assert_eq!(
                        work.spent().authority_records_read,
                        0,
                        "owned journal projection resampled its head or gate"
                    );
                }
                drop(receipts);
                drop(allocation);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn optimistic_operation_admission_recovers_an_existing_non_origin_receipt()
    -> Result<(), Box<dyn std::error::Error>> {
        let (store, authority) = super::super::append::group_tests::fixture().await?;
        let requests = super::super::append::group_tests::requests(authority, 3)?;
        let cancellation = CancellationToken::new();
        let mut work = AuthorityWork::new(crate::performance::BOUNDARY_BUDGET, &cancellation);
        for request in &requests[..2] {
            assert!(matches!(
                store
                    .compare_and_append(request.clone(), &mut work, None::<&mut ()>)
                    .await?,
                FsAppendOutcome::Committed(_)
            ));
        }
        let mut retry = requests[1].clone();
        // The retry deliberately observes today's exact head, so only the
        // operation's atomic locator can discover the earlier durable result.
        retry.expected = requests[2].expected;
        assert!(
            matches!(store.compare_and_append(retry.clone(), &mut work, None::<&mut ()>).await?, FsAppendOutcome::AlreadyCommitted(ref commit) if commit.operation_id == retry.commit.operation_id)
        );
        retry.commit.fingerprint = Digest::from_bytes([0xec; 32]);
        assert!(matches!(
            store
                .compare_and_append(retry, &mut work, None::<&mut ()>)
                .await?,
            FsAppendOutcome::IdempotencyConflict { .. }
        ));
        Ok(())
    }

    #[tokio::test]
    async fn creation_operation_identity_cannot_be_reused_at_an_exact_new_head()
    -> Result<(), Box<dyn std::error::Error>> {
        let store = StreamAuthorityStore::new(Arc::new(acyclic_stream::MemoryStream::default()));
        let authority = AuthorityId::from_bytes([34; 16]);
        let requests = super::super::append::group_tests::requests(authority, 3)?;
        let cancellation = CancellationToken::new();
        let mut work = AuthorityWork::new(crate::performance::BOUNDARY_BUDGET, &cancellation);
        assert!(
            store
                .create_first_record(
                    authority,
                    requests[0].commit.clone(),
                    &mut work,
                    None::<&mut ()>
                )
                .await?
        );
        assert!(matches!(
            store
                .compare_and_append(requests[1].clone(), &mut work, None::<&mut ()>)
                .await?,
            FsAppendOutcome::Committed(_)
        ));
        let mut retry = requests[0].clone();
        retry.expected = requests[2].expected;
        assert!(
            matches!(store.compare_and_append(retry.clone(), &mut work, None::<&mut ()>).await?, FsAppendOutcome::AlreadyCommitted(ref commit) if commit.operation_id == retry.commit.operation_id),
            "creation identity was admitted again at a newer head"
        );
        retry.commit.fingerprint = Digest::from_bytes([0xeb; 32]);
        assert!(matches!(
            store
                .compare_and_append(retry, &mut work, None::<&mut ()>)
                .await?,
            FsAppendOutcome::IdempotencyConflict { .. }
        ));
        Ok(())
    }

    #[tokio::test]
    async fn journal_mutation_invalidates_the_same_projection_before_publication()
    -> Result<(), Box<dyn std::error::Error>> {
        let (store, authority) = super::super::append::group_tests::fixture().await?;
        let requests = super::super::append::group_tests::requests(authority, 2)?;
        let cancellation = CancellationToken::new();
        let mut work = AuthorityWork::new(crate::performance::BOUNDARY_BUDGET, &cancellation);
        let first = store
            .compare_and_append(requests[0].clone(), &mut work, None::<&mut ()>)
            .await?;
        assert!(matches!(first, FsAppendOutcome::Committed(_)));
        let expected = requests[1].expected;
        let reservation = store
            .reserve_publication(
                authority,
                expected,
                OperationId::from_bytes([98; 16]),
                &mut work,
            )
            .await?;
        let ReservationOutcome::Reserved(reservation) = reservation else {
            return Err("reservation rejected".into());
        };
        let writes = work.spent().backend_write_operations;
        assert!(matches!(
            store
                .compare_and_append(requests[1].clone(), &mut work, None::<&mut ()>)
                .await?,
            FsAppendOutcome::Fenced { .. }
        ));
        assert_eq!(
            work.spent().backend_write_operations,
            writes,
            "reservation was discovered only by a rejected publication append"
        );
        store.release_publication(reservation, &mut work).await?;
        let FsAppendOutcome::Committed(second) = store
            .compare_and_append(requests[1].clone(), &mut work, None::<&mut ()>)
            .await?
        else {
            return Err("successor was not committed".into());
        };
        let advanced = store
            .fence(
                authority,
                Head {
                    epoch: second.epoch,
                    sequence: second.sequence,
                    digest: second.digest,
                },
                &mut work,
            )
            .await?;
        assert!(matches!(advanced, FenceOutcome::Advanced(_)));
        assert!(matches!(
            store
                .compare_and_append(requests[1].clone(), &mut work, None::<&mut ()>)
                .await?,
            FsAppendOutcome::AlreadyCommitted(_)
        ));
        store.retire_authority(authority, &mut work).await?;
        assert!(matches!(
            store
                .compare_and_append(requests[1].clone(), &mut work, None::<&mut ()>)
                .await,
            Err(AuthorityError::Authority(AuthorityStoreError::Retired))
        ));
        Ok(())
    }
}
