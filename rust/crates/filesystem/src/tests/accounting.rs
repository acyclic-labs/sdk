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

#[tokio::test]
async fn dense_accounting_pages_preserve_old_generations_and_cursor_after_later_append()
-> Result<(), Box<dyn Error>> {
    let (fs, authority, objects) = fixture();
    let workspace = fs
        .create_workspace(
            "dense-accounting",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    workspace.write_text("/file", "old").await?;
    let old = workspace
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?
        .id();
    workspace.write_text("/file", "newer").await?;
    let newer = workspace
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?
        .id();
    let authority_id = volume_authority_id(workspace.id().volume_id());
    let cancellation = CancellationToken::new();
    let first = accounting_page(
        &authority,
        &objects,
        authority_id,
        AccountingCursor::genesis(),
        1,
        BUDGET,
        &cancellation,
    )
    .await?;
    assert_eq!(first.facts.len(), 1);
    assert_eq!(first.next.sequence, Sequence::new(1));
    assert_eq!(first.watermark_sequence, Sequence::new(3));
    assert!(!first.complete);
    workspace.write_text("/file", "latest!").await?;
    let latest = workspace
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?
        .id();
    let second = accounting_page(
        &authority,
        &objects,
        authority_id,
        first.next,
        2,
        BUDGET,
        &cancellation,
    )
    .await?;
    assert_eq!(second.facts.len(), 2);
    assert_eq!(second.next.sequence, Sequence::new(3));
    assert_eq!(second.watermark_sequence, Sequence::new(4));
    assert!(!second.complete);
    assert_eq!(
        second.facts[0].transition,
        AccountingTransition::Published {
            volume_id: workspace.id().volume_id(),
            generation_root: root(old),
            logical_regular_file_bytes: 3,
        }
    );
    assert_eq!(
        second.facts[1].transition,
        AccountingTransition::Published {
            volume_id: workspace.id().volume_id(),
            generation_root: root(newer),
            logical_regular_file_bytes: 5,
        }
    );
    let last = authority
        .accounting_page(
            &objects,
            authority_id,
            second.next,
            1,
            BUDGET,
            &cancellation,
        )
        .await?;
    assert!(last.complete);
    assert_eq!(last.facts.len(), 1);
    assert_eq!(
        last.facts[0].transition,
        AccountingTransition::Published {
            volume_id: workspace.id().volume_id(),
            generation_root: root(latest),
            logical_regular_file_bytes: 7,
        }
    );
    let durable = records(&authority, authority_id).await?;
    assert_eq!(durable.len(), 4);
    for (fact, record) in first
        .facts
        .iter()
        .chain(&second.facts)
        .chain(&last.facts)
        .zip(&durable)
    {
        assert_eq!(fact.sequence, record.sequence);
        assert_eq!(fact.operation_id, record.operation_id);
        assert_eq!(fact.commit_digest, record.digest);
        assert_eq!(Some(fact.settled_at_micros), record.settled_at_micros);
        assert!(fact.settled_at_micros > 0);
        assert_eq!(
            fact.operation_class,
            AccountingOperationClass::CustomerMutation
        );
    }
    assert_eq!(last.next.digest, durable[3].digest);
    assert_eq!(
        Some(last.watermark_settled_at_micros),
        durable[3].settled_at_micros
    );
    assert!(first.work.authority_records_read > 0);
    assert!(second.work.object_bytes_read > 0);
    first.work.verify(BUDGET)?;
    second.work.verify(BUDGET)?;
    last.work.verify(BUDGET)?;
    let empty = authority
        .accounting_page(&objects, authority_id, last.next, 2, BUDGET, &cancellation)
        .await?;
    assert!(empty.complete);
    assert!(empty.facts.is_empty());
    assert_eq!(empty.next, last.next);
    let replayed = accounting_page(
        &authority,
        &objects,
        authority_id,
        first.next,
        2,
        BUDGET,
        &cancellation,
    )
    .await?;
    assert_eq!(replayed.facts, second.facts);
    assert_eq!(replayed.next, second.next);
    Ok(())
}

#[tokio::test]
async fn accounting_projects_retention_and_deleted_workspace_without_reopening_retired_authority()
-> Result<(), Box<dyn Error>> {
    let (fs, authority, objects) = fixture();
    let workspace = fs
        .create_workspace(
            "retained-accounting",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    workspace.write_text("/file", "retained").await?;
    let generation = workspace
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?
        .id();
    workspace.checkpoint("checkpoint").await?;
    workspace.pin("pin").await?;
    let cancellation = CancellationToken::new();
    for (kind, label) in [
        (RetentionKind::Checkpoint, "checkpoint"),
        (RetentionKind::Pin, "pin"),
    ] {
        let authority_id = retention_authority_id(workspace.id().volume_id(), kind, label);
        let page = authority
            .accounting_page(
                &objects,
                authority_id,
                AccountingCursor::genesis(),
                1,
                BUDGET,
                &cancellation,
            )
            .await?;
        assert!(page.complete);
        assert_eq!(page.facts.len(), 1);
        assert_eq!(
            page.facts[0].transition,
            AccountingTransition::Retained {
                volume_id: workspace.id().volume_id(),
                owner_volume_id: workspace.id().volume_id(),
                kind,
                generation_root: root(generation),
                logical_regular_file_bytes: 8,
            }
        );
        assert_eq!(
            page.facts[0].operation_class,
            AccountingOperationClass::CustomerMutation
        );
        assert_eq!(
            Some(page.facts[0].settled_at_micros),
            records(&authority, authority_id).await?[0].settled_at_micros
        );
    }
    let fork = workspace
        .fork(
            "accounting-fork",
            crate::ForkOptions::from_generation(
                workspace
                    .head(
                        crate::WorkBudget::UNBOUNDED,
                        &crate::CancellationToken::new(),
                    )
                    .await
                    .map(|receipt| receipt.value)
                    .map_err(|failure| failure.error)?,
                IdempotencyKey::new(),
            ),
            crate::PublicationPermit::Unrestricted,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
            |_| {
                std::future::ready(Ok(crate::OperationReceipt {
                    value: (),
                    work: crate::WorkCounters::UNCHARGED,
                }))
            },
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    let fork_retention = retention_authority_id(
        workspace.id().volume_id(),
        RetentionKind::ForkBase,
        &hex::encode(fork.id().volume_id().into_bytes()),
    );
    let retained = authority
        .accounting_page(
            &objects,
            fork_retention,
            AccountingCursor::genesis(),
            1,
            BUDGET,
            &cancellation,
        )
        .await?;
    assert!(retained.complete);
    assert_eq!(retained.facts.len(), 1);
    assert_eq!(
        retained.facts[0].transition,
        AccountingTransition::Retained {
            volume_id: workspace.id().volume_id(),
            owner_volume_id: fork.id().volume_id(),
            kind: RetentionKind::ForkBase,
            generation_root: root(generation),
            logical_regular_file_bytes: 8,
        }
    );
    assert_eq!(
        retained.facts[0].operation_class,
        AccountingOperationClass::InternalForkRetention
    );
    let authority_id = volume_authority_id(workspace.id().volume_id());
    let before = authority
        .accounting_page(
            &objects,
            authority_id,
            AccountingCursor::genesis(),
            2,
            BUDGET,
            &cancellation,
        )
        .await?;
    assert!(before.complete);
    workspace.delete(IdempotencyKey::new()).await?;
    let deleted = authority
        .accounting_page(
            &objects,
            authority_id,
            before.next,
            1,
            BUDGET,
            &cancellation,
        )
        .await?;
    assert!(deleted.complete);
    assert_eq!(deleted.facts.len(), 1);
    assert_eq!(
        deleted.facts[0].transition,
        AccountingTransition::WorkspaceDeleted {
            volume_id: workspace.id().volume_id(),
        }
    );
    assert_eq!(
        deleted.facts[0].operation_class,
        AccountingOperationClass::CustomerMutation
    );
    for failure in [
        ({
            let mut __authority_work = crate::AuthorityWork::new(BUDGET, &cancellation);
            let __authority_result = authority.head(authority_id, &mut __authority_work).await;
            __authority_work.receipt(__authority_result)
        })
        .err(),
        ({
            let authority_input_1 = Head {
                epoch: Epoch::GENESIS,
                sequence: deleted.next.sequence,
                digest: deleted.next.digest,
            };
            let mut __authority_work = crate::AuthorityWork::new(BUDGET, &cancellation);
            let __authority_result = authority
                .fence(authority_id, authority_input_1, &mut __authority_work)
                .await;
            __authority_work.receipt(__authority_result)
        })
        .err(),
    ] {
        assert!(matches!(
            failure
                .ok_or("retired ordinary authority operation succeeded")?
                .error,
            AuthorityStoreError::Retired
        ));
    }
    let repeated = authority
        .accounting_page(
            &objects,
            authority_id,
            before.next,
            1,
            BUDGET,
            &cancellation,
        )
        .await?;
    assert_eq!(repeated.facts, deleted.facts);
    assert_eq!(repeated.next, deleted.next);
    Ok(())
}

#[tokio::test]
async fn accounting_requires_original_objects_and_authoritative_time_not_offline_defaults()
-> Result<(), Box<dyn Error>> {
    let (fs, authority, objects) = fixture();
    let workspace = fs
        .create_workspace(
            "fail-closed-accounting",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    workspace.write_text("/file", "original").await?;
    workspace
        .write_text("/file", "different current head")
        .await?;
    let authority_id = volume_authority_id(workspace.id().volume_id());
    let cancellation = CancellationToken::new();
    // A different actual Objects provider has none of the original closure.
    let (absent, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
    let absent = crate::LogicalObjectStore::new(Arc::new(absent), bucket);
    assert!(matches!(
        accounting_page(
            &authority,
            &absent,
            authority_id,
            AccountingCursor::genesis(),
            2,
            BUDGET,
            &cancellation,
        )
        .await,
        Err(AccountingError::Proof(_))
    ));
    // Simulate collection of old closures while preserving the complete current
    // closure in an actual independent Objects provider.
    let latest = workspace
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?
        .id();
    let current = crate::kernel::prove_generation_closure(
        &objects,
        root(latest),
        crate::kernel::ClosureLimits {
            decode: crate::kernel::DecodeLimits::default(),
            maximum_objects: 4096,
            maximum_files: 4096,
            maximum_object_bytes: 16 * 1024 * 1024,
            profile: crate::model::FilesystemProfile::Portable,
            symbolic_links: true,
            hard_links: true,
            sparse_files: true,
        },
        BUDGET,
        &cancellation,
    )
    .await?;
    for object in current.objects {
        let bytes = crate::AsyncObjectStore::read(
            &objects,
            object,
            16 * 1024 * 1024,
            BUDGET,
            &cancellation,
        )
        .await?
        .value
        .bytes;
        crate::AsyncObjectStore::put(&absent, object, bytes, BUDGET, &cancellation).await?;
    }
    let durable = records(&authority, authority_id).await?;
    let cursor = |record: &DurableCommit| -> Result<AccountingCursor, Box<dyn Error>> {
        Ok(AccountingCursor {
            sequence: record.sequence,
            digest: record.digest,
            settled_at_micros: record.settled_at_micros.ok_or("Stream timestamp missing")?,
        })
    };
    let present = accounting_page(
        &authority,
        &absent,
        authority_id,
        cursor(&durable[1])?,
        1,
        BUDGET,
        &cancellation,
    )
    .await?;
    assert!(present.complete);
    assert_eq!(
        present.facts[0].transition,
        AccountingTransition::Published {
            volume_id: workspace.id().volume_id(),
            generation_root: root(latest),
            logical_regular_file_bytes: 22,
        }
    );
    assert!(matches!(
        accounting_page(
            &authority,
            &absent,
            authority_id,
            cursor(&durable[0])?,
            1,
            BUDGET,
            &cancellation,
        )
        .await,
        Err(AccountingError::Proof(_))
    ));
    let initial = records(&authority, authority_id).await?.remove(0);
    let offline = MemoryAuthorityStore::default();
    let CreateAuthorityOutcome::Created(head) = ({
        let authority_input_1 = initial.epoch;
        let authority_cancellation = &crate::CancellationToken::new();
        let mut __authority_work = crate::AuthorityWork::new(BUDGET, authority_cancellation);
        let __authority_result = AsyncAuthorityStore::create_authority(
            &offline,
            authority_id,
            authority_input_1,
            &mut __authority_work,
            None::<&mut ()>,
        )
        .await
        .map_err(|error| error.into_error(std::convert::identity));
        __authority_work.receipt(__authority_result)
    })?
    .value
    else {
        return Err("offline fixture authority already existed".into());
    };
    let AppendOutcome::Committed(committed) = ({
        let authority_input_1 = initial.epoch;
        let authority_input_3 = ProposedCommit {
            operation_id: initial.operation_id,
            fingerprint: initial.fingerprint,
            payload: initial.payload,
        };
        let authority_cancellation = &crate::CancellationToken::new();
        let mut __authority_work = crate::AuthorityWork::new(BUDGET, authority_cancellation);
        let __authority_result = AsyncAuthorityStore::compare_and_append(
            &offline,
            crate::GuardedAppend {
                authority_id: authority_id,
                epoch: authority_input_1,
                expected: head,
                commit: authority_input_3,
                permit: crate::PublicationPermit::Unrestricted,
            },
            &mut __authority_work,
            None::<&mut ()>,
        )
        .await
        .map_err(|error| error.into_error(std::convert::identity))
        .map(|(value, _allocation)| value);
        __authority_work.receipt(__authority_result)
    })?
    .value
    else {
        return Err("offline actual Memory authority did not commit".into());
    };
    assert_eq!(committed.settled_at_micros, None);
    assert!(matches!(
        accounting_page(
            &offline,
            &objects,
            authority_id,
            AccountingCursor::genesis(),
            1,
            BUDGET,
            &cancellation,
        )
        .await,
        Err(AccountingError::MissingTime)
    ));
    Ok(())
}

#[tokio::test]
async fn accounting_rejects_malformed_cursor_and_unbounded_admission() -> Result<(), Box<dyn Error>>
{
    let (fs, authority, objects) = fixture();
    let workspace = fs
        .create_workspace(
            "cursor-accounting",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    workspace.write_text("/file", "a").await?;
    let authority_id = volume_authority_id(workspace.id().volume_id());
    let cancellation = CancellationToken::new();
    let first = accounting_page(
        &authority,
        &objects,
        authority_id,
        AccountingCursor::genesis(),
        1,
        BUDGET,
        &cancellation,
    )
    .await?;
    for cursor in [
        AccountingCursor {
            digest: Digest::ZERO,
            ..first.next
        },
        AccountingCursor {
            settled_at_micros: first.next.settled_at_micros + 1,
            ..first.next
        },
        AccountingCursor {
            sequence: Sequence::new(99),
            ..first.next
        },
    ] {
        assert!(matches!(
            accounting_page(
                &authority,
                &objects,
                authority_id,
                cursor,
                1,
                BUDGET,
                &cancellation,
            )
            .await,
            Err(AccountingError::InvalidHistory)
        ));
    }
    for cursor in [
        AccountingCursor {
            digest: first.next.digest,
            ..AccountingCursor::genesis()
        },
        AccountingCursor {
            settled_at_micros: 1,
            ..AccountingCursor::genesis()
        },
    ] {
        assert!(matches!(
            accounting_page(
                &authority,
                &objects,
                authority_id,
                cursor,
                1,
                BUDGET,
                &cancellation,
            )
            .await,
            Err(AccountingError::InvalidBounds)
        ));
    }
    for maximum in [0, 257] {
        assert!(matches!(
            accounting_page(
                &authority,
                &objects,
                authority_id,
                AccountingCursor::genesis(),
                maximum,
                BUDGET,
                &cancellation,
            )
            .await,
            Err(AccountingError::InvalidBounds)
        ));
    }
    assert!(matches!(
        accounting_page(
            &authority,
            &objects,
            authority_id,
            AccountingCursor::genesis(),
            1,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await,
        Err(AccountingError::InvalidBounds)
    ));
    Ok(())
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

#[tokio::test]
async fn accounting_checks_actual_replayed_digest_operation_and_time_before_producing_facts()
-> Result<(), Box<dyn Error>> {
    let (fs, authority, objects) = fixture();
    let workspace = fs
        .create_workspace(
            "damaged-accounting",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    workspace.write_text("/file", "actual").await?;
    let authority_id = volume_authority_id(workspace.id().volume_id());
    let durable = records(&authority, authority_id).await?;
    let later = durable
        .last()
        .ok_or("actual records missing")?
        .settled_at_micros
        .ok_or("Stream timestamp missing")?
        + 1;
    let cancellation = CancellationToken::new();
    for damage in [
        ReplayDamage::Digest,
        ReplayDamage::Operation,
        ReplayDamage::RegressingTime(later),
    ] {
        assert!(matches!(
            accounting_page(
                &DamagedReplay {
                    inner: &authority,
                    damage
                },
                &objects,
                authority_id,
                AccountingCursor::genesis(),
                2,
                BUDGET,
                &cancellation,
            )
            .await,
            Err(AccountingError::InvalidHistory)
        ));
    }
    for damage in [ReplayDamage::MissingTime, ReplayDamage::ZeroTime] {
        assert!(matches!(
            accounting_page(
                &DamagedReplay {
                    inner: &authority,
                    damage
                },
                &objects,
                authority_id,
                AccountingCursor::genesis(),
                2,
                BUDGET,
                &cancellation,
            )
            .await,
            Err(AccountingError::MissingTime)
        ));
    }
    Ok(())
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
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::storage::ObjectResult<()> {
        crate::AsyncObjectStore::put(&self.inner, object_id, bytes, budget, cancellation).await
    }

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::storage::ObjectResult<crate::storage::ObjectRead> {
        crate::AsyncObjectStore::read(&self.inner, object_id, maximum_bytes, budget, cancellation)
            .await
    }

    async fn read_many(
        &self,
        requests: &[crate::storage::ObjectReadRequest],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::storage::ObjectResult<Vec<crate::storage::ObjectRead>> {
        crate::AsyncObjectStore::read_many(&self.inner, requests, budget, cancellation).await
    }

    async fn contains(
        &self,
        object_id: ObjectId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::storage::ObjectResult<bool> {
        crate::AsyncObjectStore::contains(&self.inner, object_id, budget, cancellation).await
    }

    async fn historical_generation_evidence(
        &self,
        authority_id: AuthorityId,
        record: &DurableCommit,
        maximum_bytes: u64,
        _maximum_objects: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::storage::ObjectResult<Option<crate::kernel::HistoricalGenerationEvidence>> {
        use crate::storage::{ObjectFailure, ObjectStoreError};
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        if self.evidence.bytes.len() as u64 > maximum_bytes {
            return Err(ObjectFailure::before_work(ObjectStoreError::Rejected(
                "original evidence exceeds requested byte bound".to_owned(),
            )));
        }
        let value = (authority_id == self.authority_id && record.digest == self.original_digest)
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

#[path = "accounting/archive.rs"]
mod archive;
