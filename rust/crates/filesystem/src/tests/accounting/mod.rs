use crate::accounting::{
    AccountingCursor, AccountingError, AccountingOperationClass, AccountingTransition,
    accounting_page,
};
use crate::distributed::StreamAuthorityStore;
use crate::foundation::{
    AuthorityId, Digest, DurableCommit, Epoch, Head, OperationId, ProposedCommit, Sequence,
};
use crate::kernel::{RetentionKind, retention_authority_id, volume_authority_id};
use crate::memory::MemoryAuthorityStore;
use crate::storage::{
    AppendOutcome, AuthorityResult, AuthorityStoreError, CreateAuthorityOutcome, FenceOutcome,
    ObjectId, ObjectKind, ReplayLimit,
};
use crate::{
    AsyncAuthorityStore, CancellationToken, Fs, GenerationForkSource, IdempotencyKey, MemoryFs,
    MemoryObjectBackend, WorkBudget,
};
use std::error::Error;
use std::sync::Arc;

const BUDGET: WorkBudget = crate::performance::work_budget(4096, 16 * 1024 * 1024);

fn fixture() -> (
    MemoryFs,
    StreamAuthorityStore<acyclic_stream::MemoryStream>,
    MemoryObjectBackend,
) {
    let stream = Arc::new(acyclic_stream::MemoryStream::default());
    let (objects, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
    let objects = Arc::new(objects);
    let fs = Fs::from_memory_providers(Arc::clone(&stream), Arc::clone(&objects), bucket.clone());
    (
        fs,
        StreamAuthorityStore::new(stream),
        crate::LogicalObjectStore::new(objects, bucket),
    )
}

fn root(id: crate::GenerationId) -> ObjectId {
    ObjectId {
        kind: ObjectKind::GenerationRoot,
        digest: id.digest(),
    }
}

async fn records(
    authority: &StreamAuthorityStore<acyclic_stream::MemoryStream>,
    authority_id: AuthorityId,
) -> Result<Vec<DurableCommit>, Box<dyn Error>> {
    Ok(({
        let authority_input_2 = ReplayLimit {
            records: 256,
            payload_bytes: 1024 * 1024,
        };
        let authority_cancellation = &CancellationToken::new();

        let mut __authority_work = crate::AuthorityWork::new(BUDGET, authority_cancellation);
        let __authority_result = authority
            .replay(
                authority_id,
                Sequence::GENESIS,
                authority_input_2,
                &mut __authority_work,
            )
            .await
            .map(|(value, _allocation)| value);
        __authority_work.receipt(__authority_result)
    })?
    .value)
}

#[derive(Clone, Copy)]
enum ReplayDamage {
    Digest,
    Operation,
    MissingTime,
    ZeroTime,
    RegressingTime(u64),
}

/// Fault injection only: every record comes from the actual Stream authority.
/// This wrapper never manufactures authority acceptance or replacement proofs.
struct DamagedReplay<'a> {
    inner: &'a StreamAuthorityStore<acyclic_stream::MemoryStream>,
    damage: ReplayDamage,
}

impl AsyncAuthorityStore for DamagedReplay<'_> {
    async fn create_authority<G: crate::AuthorityGuard>(
        &self,
        authority: crate::AuthorityId,
        epoch: crate::Epoch,
        work: &mut crate::AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<crate::CreateAuthorityOutcome, crate::AuthorityError<G::Error>> {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into());
        }
        crate::async_storage::seal(guard.as_deref_mut(), work).await?;
        let value = self
            .inner
            .create_authority(authority, epoch, work, None::<&mut ()>)
            .await
            .map_err(|error| {
                crate::AuthorityError::Authority(error.into_error(std::convert::identity))
            })?;
        if matches!(value, crate::CreateAuthorityOutcome::Existing(_)) {
            crate::async_storage::seal(guard.as_deref_mut(), work).await?;
        }
        Ok(value)
    }
    async fn fork_authority<G: crate::AuthorityGuard>(
        &self,
        source: crate::GenerationForkSource,
        destination: crate::AuthorityId,
        operation: crate::OperationId,
        work: &mut crate::AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<crate::CreateAuthorityOutcome, crate::AuthorityError<G::Error>> {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into());
        }
        crate::async_storage::seal(guard.as_deref_mut(), work).await?;
        let value = self
            .inner
            .fork_authority(source, destination, operation, work, None::<&mut ()>)
            .await
            .map_err(|error| {
                crate::AuthorityError::Authority(error.into_error(std::convert::identity))
            })?;
        if matches!(value, crate::CreateAuthorityOutcome::Existing(_)) {
            crate::async_storage::seal(guard.as_deref_mut(), work).await?;
        }
        Ok(value)
    }
    async fn compare_and_append<G: crate::AuthorityGuard>(
        &self,
        request: crate::GuardedAppend,
        work: &mut crate::AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<(crate::AppendOutcome, crate::AuthorityAllocation), crate::AuthorityError<G::Error>>
    {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into());
        }
        if request.permit != crate::PublicationPermit::Unrestricted {
            return Err(crate::AuthorityStoreError::Rejected(
                "authority backend cannot atomically evaluate operation leases".to_owned(),
            )
            .into());
        }
        crate::async_storage::seal(guard.as_deref_mut(), work).await?;
        self.inner
            .compare_and_append(request, work, None::<&mut ()>)
            .await
            .map_err(|error| {
                crate::AuthorityError::Authority(error.into_error(std::convert::identity))
            })
    }
    async fn head(
        &self,
        authority: crate::AuthorityId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::Head, crate::AuthorityStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into());
        }

        self.inner.head(authority, work).await
    }
    async fn replay(
        &self,
        authority: crate::AuthorityId,
        after: crate::Sequence,
        limit: crate::ReplayLimit,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<crate::DurableCommit>, crate::AuthorityAllocation), crate::AuthorityStoreError>
    {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into());
        }

        let (mut records, allocation) = self.inner.replay(authority, after, limit, work).await?;
        for record in &mut records {
            match self.damage {
                ReplayDamage::Digest => record.digest = Digest::ZERO,
                ReplayDamage::Operation => record.operation_id = OperationId::from_bytes([121; 16]),
                ReplayDamage::MissingTime => record.settled_at_micros = None,
                ReplayDamage::ZeroTime => record.settled_at_micros = Some(0),
                ReplayDamage::RegressingTime(later) if record.sequence == Sequence::new(1) => {
                    record.settled_at_micros = Some(later);
                }
                ReplayDamage::RegressingTime(_) => {}
            }
        }
        Ok((records, allocation))
    }
    async fn fence(
        &self,
        authority: crate::AuthorityId,
        expected: crate::Head,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::FenceOutcome, crate::AuthorityStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into());
        }

        self.inner.fence(authority, expected, work).await
    }
    async fn find_operation(
        &self,
        authority: crate::AuthorityId,
        operation: crate::OperationId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<
        (Option<crate::DurableCommit>, crate::AuthorityAllocation),
        crate::AuthorityStoreError,
    > {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into());
        }

        self.inner.find_operation(authority, operation, work).await
    }
}

async fn prove_original(
    objects: &MemoryObjectBackend,
    root: ObjectId,
    config: crate::model::VolumeConfig,
) -> Result<crate::kernel::GenerationProof, Box<dyn Error>> {
    Ok(crate::kernel::prove_generation_closure(
        objects,
        root,
        crate::kernel::ClosureLimits {
            decode: crate::kernel::DecodeLimits::default(),
            maximum_objects: 4096,
            maximum_files: 4096,
            maximum_object_bytes: 16 * 1024 * 1024,
            profile: config.profile,
            symbolic_links: config.symbolic_links,
            hard_links: config.hard_links,
            sparse_files: config.sparse_files,
        },
        BUDGET,
        &CancellationToken::new(),
    )
    .await?)
}

fn original_append(authority_id: AuthorityId, record: &DurableCommit) -> crate::GuardedAppend {
    crate::GuardedAppend {
        authority_id,
        epoch: record.epoch,
        expected: Head {
            epoch: record.epoch,
            sequence: Sequence::new(record.sequence.get() - 1),
            digest: record.previous_digest,
        },
        commit: ProposedCommit {
            operation_id: record.operation_id,
            fingerprint: record.fingerprint,
            payload: record.payload.clone(),
        },
        permit: crate::PublicationPermit::Unrestricted,
    }
}

/// One immutable test-owned original, not a runtime journal or global registry.
/// The actual Objects provider remains empty so no fresh closure can substitute.
struct OriginalArchive {
    inner: MemoryObjectBackend,
    authority_id: AuthorityId,
    original_digest: Digest,
    evidence: crate::kernel::HistoricalGenerationEvidence,
}

impl crate::AsyncObjectStore for OriginalArchive {
    async fn put(
        &self,
        object_id: ObjectId,
        bytes: acyclic_stream::OwnedBytes,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(crate::ObjectStoreError::NativeUnavailable);
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
            {
                let mut __object_work = crate::AuthorityWork::new(budget, cancellation);
                let __object_result =
                    crate::AsyncObjectStore::put(&self.inner, object_id, bytes, &mut __object_work)
                        .await;
                __object_work.receipt(__object_result)
            }
        }
        .await;
        context.accept(result)
    }

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::ObjectRead, crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(crate::ObjectStoreError::NativeUnavailable);
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
            {
                let mut __object_work = crate::AuthorityWork::new(budget, cancellation);
                let __object_result = crate::AsyncObjectStore::read(
                    &self.inner,
                    object_id,
                    maximum_bytes,
                    &mut __object_work,
                )
                .await;
                __object_work.receipt(__object_result)
            }
        }
        .await;
        context.accept(result)
    }

    async fn read_many(
        &self,
        requests: &[crate::storage::ObjectReadRequest],
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<crate::ObjectRead>, crate::AuthorityAllocation), crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(crate::ObjectStoreError::NativeUnavailable);
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
            {
                let mut __object_work = crate::AuthorityWork::new(budget, cancellation);
                let __object_result =
                    crate::AsyncObjectStore::read_many(&self.inner, requests, &mut __object_work)
                        .await
                        .map(|(values, _allocation)| values);
                __object_work.receipt(__object_result)
            }
        }
        .await;
        context.accept(result).map(|values| (values, None))
    }

    async fn contains(
        &self,
        object_id: ObjectId,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<bool, crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(crate::ObjectStoreError::NativeUnavailable);
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
            {
                let mut __object_work = crate::AuthorityWork::new(budget, cancellation);
                let __object_result =
                    crate::AsyncObjectStore::contains(&self.inner, object_id, &mut __object_work)
                        .await;
                __object_work.receipt(__object_result)
            }
        }
        .await;
        context.accept(result)
    }

    async fn historical_generation_evidence(
        &self,
        authority_id: AuthorityId,
        record: &DurableCommit,
        maximum_bytes: u64,
        _maximum_objects: u64,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<Option<crate::kernel::HistoricalGenerationEvidence>, crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(crate::ObjectStoreError::NativeUnavailable);
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
            use crate::storage::{ObjectFailure, ObjectStoreError};
            cancellation
                .check()
                .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
            if self.evidence.bytes.len() as u64 > maximum_bytes {
                return Err(ObjectFailure::before_work(ObjectStoreError::Rejected(
                    "original evidence exceeds requested byte bound".to_owned(),
                )));
            }
            let value = (authority_id == self.authority_id
                && record.digest == self.original_digest)
                .then(|| self.evidence.clone());
            let work = crate::WorkCounters {
                items_examined: 1,
                items_returned: u64::from(value.is_some()),
                ..crate::WorkCounters::default()
            };
            work.verify(budget)
                .map_err(|error| ObjectFailure::new(error.into(), work))?;
            Ok(crate::OperationReceipt { value, work })
        }
        .await;
        context.accept(result)
    }
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    #[test]
    fn accounting_dense_cursor_property_uses_actual_stream_and_objects(
        lengths in proptest::collection::vec(1_usize..65, 1..13),
        page_size in 1_u32..9,
    ) {
        let result: Result<(), Box<dyn Error>> = futures::executor::block_on(async {
            let (fs, authority, objects) = fixture();
            let workspace = fs.create_workspace("property-accounting", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
            let authority_id = volume_authority_id(workspace.id().volume_id());
            let mut expected = vec![(root(workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id()), 0_u128)];
            for (index, length) in lengths.iter().copied().enumerate() {
                workspace.write("/file", bytes::Bytes::from(vec![index as u8 + 1; length])).await?;
                expected.push((root(workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id()), length as u128));
            }
            let original_records = records(&authority, authority_id).await?;
            assert_eq!(original_records.len(), expected.len());
            let cancellation = CancellationToken::new();
            let mut cursor = AccountingCursor::genesis();
            let mut pages = Vec::new();
            let mut all_facts = Vec::new();
            loop {
                let page = authority.accounting_page(
                    &objects, authority_id, cursor, page_size, BUDGET, &cancellation,
                ).await?;
                assert!(!page.facts.is_empty());
                assert!(page.facts.len() <= page_size as usize);
                assert_eq!(page.watermark_sequence, original_records.last().ok_or("no actual records")?.sequence);
                assert_eq!(page.facts[0].sequence.get(), cursor.sequence.get() + 1);
                page.work.verify(BUDGET)?;
                pages.push((cursor, page.facts.clone()));
                all_facts.extend(page.facts);
                cursor = page.next;
                if page.complete {
                    break;
                }
            }
            assert_eq!(all_facts.len(), original_records.len());
            for ((fact, record), (generation_root, logical_regular_file_bytes)) in
                all_facts.iter().zip(&original_records).zip(expected)
            {
                assert_eq!(fact.sequence, record.sequence);
                assert_eq!(fact.operation_id, record.operation_id);
                assert_eq!(fact.commit_digest, record.digest);
                assert_eq!(Some(fact.settled_at_micros), record.settled_at_micros);
                assert_eq!(fact.operation_class, AccountingOperationClass::CustomerMutation);
                assert_eq!(fact.transition, AccountingTransition::Published {
                    volume_id: workspace.id().volume_id(), generation_root,
                    logical_regular_file_bytes,
                });
            }
            workspace.write("/file", bytes::Bytes::from(vec![99; 73])).await?;
            let later_root = root(workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id());
            let later_records = records(&authority, authority_id).await?;
            let later_record = later_records.last().ok_or("later actual record missing")?;
            let deletion_key = IdempotencyKey::new();
            workspace.delete(deletion_key).await?;
            // Every already-consumed cursor still denotes exactly the original
            // immutable facts after a later publication and terminal retirement.
            for (anchor, facts) in pages {
                let replay = authority.accounting_page(
                    &objects, authority_id, anchor, facts.len() as u32, BUDGET, &cancellation,
                ).await?;
                assert_eq!(replay.facts, facts);
            }
            let terminal = authority.accounting_page(
                &objects, authority_id, cursor, 2, BUDGET, &cancellation,
            ).await?;
            assert!(terminal.complete);
            assert_eq!(terminal.facts.len(), 2);
            assert_eq!(terminal.facts[0].transition, AccountingTransition::Published {
                volume_id: workspace.id().volume_id(), generation_root: later_root,
                logical_regular_file_bytes: 73,
            });
            assert_eq!(terminal.facts[0].operation_id, later_record.operation_id);
            assert_eq!(terminal.facts[0].commit_digest, later_record.digest);
            assert_eq!(Some(terminal.facts[0].settled_at_micros), later_record.settled_at_micros);
            assert_eq!(terminal.facts[1].transition, AccountingTransition::WorkspaceDeleted {
                volume_id: workspace.id().volume_id(),
            });
            assert_eq!(terminal.facts[1].operation_id, deletion_key.operation_id());
            assert_eq!(terminal.next.digest, terminal.facts[1].commit_digest);
            let failure =({ let authority_input_1 = Head {
                    epoch: later_record.epoch,
                    sequence: terminal.next.sequence,
                    digest: terminal.next.digest,
                };
             let mut __authority_work = crate::AuthorityWork::new(BUDGET, &cancellation); let __authority_result = authority.fence(authority_id, authority_input_1, &mut __authority_work).await; __authority_work.receipt(__authority_result) }).err().ok_or("retired fence unexpectedly succeeded")?;
            assert!(matches!(failure.error, AuthorityStoreError::Retired));
            Ok(())
        });
        proptest::prop_assert!(result.is_ok(), "{:?}", result.err());
    }
}

mod archive;
mod pages;
mod validation;
