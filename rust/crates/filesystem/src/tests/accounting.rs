use crate::accounting::{
    AccountingCursor, AccountingError, AccountingOperationClass, AccountingTransition,
    accounting_page,
};
use crate::distributed::StreamAuthorityStore;
use crate::foundation::{AuthorityId, Digest, DurableCommit, Epoch, Head, OperationId, ProposedCommit, Sequence};
use crate::kernel::{RetentionKind, retention_authority_id, volume_authority_id};
use crate::memory::MemoryAuthorityStore;
use crate::storage::{
    AppendOutcome, AuthorityResult, AuthorityStore, AuthorityStoreError, CreateAuthorityOutcome,
    FenceOutcome, ObjectId, ObjectKind, ReplayLimit,
};
use crate::{AsyncAuthorityStore, CancellationToken, Fs, GenerationForkSource, IdempotencyKey, MemoryFs, MemoryObjectBackend, WorkBudget};
use std::error::Error;
use std::sync::Arc;

fn budget() -> WorkBudget {
    WorkBudget {
        authority_records_read: 4096,
        authority_records_appended: 4096,
        authority_bytes_read: 16 * 1024 * 1024,
        authority_bytes_written: 16 * 1024 * 1024,
        object_probes: 4096,
        backend_read_operations: 4096,
        backend_write_operations: 4096,
        durability_operations: 4096,
        page_reads: 4096,
        page_writes: 4096,
        object_bytes_read: 16 * 1024 * 1024,
        object_bytes_written: 16 * 1024 * 1024,
        bytes_hashed: 16 * 1024 * 1024,
        bytes_copied: 16 * 1024 * 1024,
        bytes_encoded: 16 * 1024 * 1024,
        source_bytes_read: 16 * 1024 * 1024,
        source_path_components: 4096,
        source_entries_visited: 4096,
        output_bytes: 16 * 1024 * 1024,
        items_examined: 4096,
        items_returned: 4096,
        allocation_operations: 4096,
        peak_allocation_bytes: 16 * 1024 * 1024,
        materializations: 4096,
    }
}

fn fixture() -> (MemoryFs, StreamAuthorityStore<acyclic_stream::MemoryStream>, MemoryObjectBackend) {
    let stream = Arc::new(acyclic_stream::MemoryStream::default());
    let (objects, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
    let objects = Arc::new(objects);
    let fs = Fs::from_memory_providers(Arc::clone(&stream), Arc::clone(&objects), bucket.clone());
    (fs, StreamAuthorityStore::new(stream), crate::LogicalObjectStore::new(objects, bucket))
}

fn root(id: crate::GenerationId) -> ObjectId {
    ObjectId { kind: ObjectKind::GenerationRoot, digest: id.digest() }
}

async fn records(
    authority: &StreamAuthorityStore<acyclic_stream::MemoryStream>,
    authority_id: AuthorityId,
) -> Result<Vec<DurableCommit>, Box<dyn Error>> {
    Ok(authority.replay(
        authority_id, Sequence::GENESIS,
        ReplayLimit { records: 256, payload_bytes: 1024 * 1024 },
        budget(), &CancellationToken::new(),
    ).await?.value)
}

#[tokio::test]
async fn dense_accounting_pages_preserve_old_generations_and_cursor_after_later_append()
-> Result<(), Box<dyn Error>> {
    let (fs, authority, objects) = fixture();
    let workspace = fs.create_workspace("dense-accounting").await?;
    workspace.write_text("/file", "old").await?;
    let old = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id();
    workspace.write_text("/file", "newer").await?;
    let newer = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id();
    let authority_id = volume_authority_id(workspace.id().volume_id());
    let cancellation = CancellationToken::new();
    let first = accounting_page(
        &authority, &objects, authority_id, AccountingCursor::genesis(), 1, budget(), &cancellation,
    ).await?;
    assert_eq!(first.facts.len(), 1);
    assert_eq!(first.next.sequence, Sequence::new(1));
    assert_eq!(first.watermark_sequence, Sequence::new(3));
    assert!(!first.complete);
    workspace.write_text("/file", "latest!").await?;
    let latest = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id();
    let second = accounting_page(
        &authority, &objects, authority_id, first.next, 2, budget(), &cancellation,
    ).await?;
    assert_eq!(second.facts.len(), 2);
    assert_eq!(second.next.sequence, Sequence::new(3));
    assert_eq!(second.watermark_sequence, Sequence::new(4));
    assert!(!second.complete);
    assert_eq!(second.facts[0].transition, AccountingTransition::Published {
        volume_id: workspace.id().volume_id(), generation_root: root(old),
        logical_regular_file_bytes: 3,
    });
    assert_eq!(second.facts[1].transition, AccountingTransition::Published {
        volume_id: workspace.id().volume_id(), generation_root: root(newer),
        logical_regular_file_bytes: 5,
    });
    let last = authority.accounting_page(
        &objects, authority_id, second.next, 1, budget(), &cancellation,
    ).await?;
    assert!(last.complete);
    assert_eq!(last.facts.len(), 1);
    assert_eq!(last.facts[0].transition, AccountingTransition::Published {
        volume_id: workspace.id().volume_id(), generation_root: root(latest),
        logical_regular_file_bytes: 7,
    });
    let durable = records(&authority, authority_id).await?;
    assert_eq!(durable.len(), 4);
    for (fact, record) in first.facts.iter().chain(&second.facts).chain(&last.facts).zip(&durable) {
        assert_eq!(fact.sequence, record.sequence);
        assert_eq!(fact.operation_id, record.operation_id);
        assert_eq!(fact.commit_digest, record.digest);
        assert_eq!(Some(fact.settled_at_micros), record.settled_at_micros);
        assert!(fact.settled_at_micros > 0);
        assert_eq!(fact.operation_class, AccountingOperationClass::CustomerMutation);
    }
    assert_eq!(last.next.digest, durable[3].digest);
    assert_eq!(Some(last.watermark_settled_at_micros), durable[3].settled_at_micros);
    assert!(first.work.authority_records_read > 0);
    assert!(second.work.object_bytes_read > 0);
    first.work.verify(budget())?;
    second.work.verify(budget())?;
    last.work.verify(budget())?;
    let empty = authority.accounting_page(
        &objects, authority_id, last.next, 2, budget(), &cancellation,
    ).await?;
    assert!(empty.complete);
    assert!(empty.facts.is_empty());
    assert_eq!(empty.next, last.next);
    let replayed = accounting_page(
        &authority, &objects, authority_id, first.next, 2, budget(), &cancellation,
    ).await?;
    assert_eq!(replayed.facts, second.facts);
    assert_eq!(replayed.next, second.next);
    Ok(())
}

#[tokio::test]
async fn accounting_projects_retention_and_deleted_workspace_without_reopening_retired_authority()
-> Result<(), Box<dyn Error>> {
    let (fs, authority, objects) = fixture();
    let workspace = fs.create_workspace("retained-accounting").await?;
    workspace.write_text("/file", "retained").await?;
    let generation = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id();
    workspace.checkpoint("checkpoint").await?;
    workspace.pin("pin").await?;
    let cancellation = CancellationToken::new();
    for (kind, label) in [(RetentionKind::Checkpoint, "checkpoint"), (RetentionKind::Pin, "pin")] {
        let authority_id = retention_authority_id(workspace.id().volume_id(), kind, label);
        let page = authority.accounting_page(
            &objects, authority_id, AccountingCursor::genesis(), 1, budget(), &cancellation,
        ).await?;
        assert!(page.complete);
        assert_eq!(page.facts.len(), 1);
        assert_eq!(page.facts[0].transition, AccountingTransition::Retained {
            volume_id: workspace.id().volume_id(), owner_volume_id: workspace.id().volume_id(),
            kind, generation_root: root(generation), logical_regular_file_bytes: 8,
        });
        assert_eq!(page.facts[0].operation_class, AccountingOperationClass::CustomerMutation);
        assert_eq!(Some(page.facts[0].settled_at_micros), records(&authority, authority_id).await?[0].settled_at_micros);
    }
    let fork = workspace.fork(
        "accounting-fork",
        crate::ForkOptions::from_generation(workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?, IdempotencyKey::new()),
    ).await?;
    let fork_retention = retention_authority_id(
        workspace.id().volume_id(), RetentionKind::ForkBase,
        &hex::encode(fork.id().volume_id().into_bytes()),
    );
    let retained = authority.accounting_page(
        &objects, fork_retention, AccountingCursor::genesis(), 1, budget(), &cancellation,
    ).await?;
    assert!(retained.complete);
    assert_eq!(retained.facts.len(), 1);
    assert_eq!(retained.facts[0].transition, AccountingTransition::Retained {
        volume_id: workspace.id().volume_id(), owner_volume_id: fork.id().volume_id(),
        kind: RetentionKind::ForkBase, generation_root: root(generation),
        logical_regular_file_bytes: 8,
    });
    assert_eq!(retained.facts[0].operation_class, AccountingOperationClass::InternalForkRetention);
    let authority_id = volume_authority_id(workspace.id().volume_id());
    let before = authority.accounting_page(
        &objects, authority_id, AccountingCursor::genesis(), 2, budget(), &cancellation,
    ).await?;
    assert!(before.complete);
    workspace.delete(IdempotencyKey::new()).await?;
    let deleted = authority.accounting_page(
        &objects, authority_id, before.next, 1, budget(), &cancellation,
    ).await?;
    assert!(deleted.complete);
    assert_eq!(deleted.facts.len(), 1);
    assert_eq!(deleted.facts[0].transition, AccountingTransition::WorkspaceDeleted {
        volume_id: workspace.id().volume_id(),
    });
    assert_eq!(deleted.facts[0].operation_class, AccountingOperationClass::CustomerMutation);
    for failure in [
        authority.head(authority_id, budget(), &cancellation).await.err(),
        authority.fence(authority_id, Head {
            epoch: Epoch::GENESIS, sequence: deleted.next.sequence, digest: deleted.next.digest,
        }, budget(), &cancellation).await.err(),
    ] {
        assert!(matches!(failure.ok_or("retired ordinary authority operation succeeded")?.error, AuthorityStoreError::Retired));
    }
    let repeated = authority.accounting_page(
        &objects, authority_id, before.next, 1, budget(), &cancellation,
    ).await?;
    assert_eq!(repeated.facts, deleted.facts);
    assert_eq!(repeated.next, deleted.next);
    Ok(())
}

#[tokio::test]
async fn accounting_requires_original_objects_and_authoritative_time_not_offline_defaults()
-> Result<(), Box<dyn Error>> {
    let (fs, authority, objects) = fixture();
    let workspace = fs.create_workspace("fail-closed-accounting").await?;
    workspace.write_text("/file", "original").await?;
    workspace.write_text("/file", "different current head").await?;
    let authority_id = volume_authority_id(workspace.id().volume_id());
    let cancellation = CancellationToken::new();
    // A different actual Objects provider has none of the original closure.
    let (absent, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
    let absent = crate::LogicalObjectStore::new(Arc::new(absent), bucket);
    assert!(matches!(accounting_page(
        &authority, &absent, authority_id, AccountingCursor::genesis(), 2, budget(), &cancellation,
    ).await, Err(AccountingError::Proof(_))));
    // Simulate collection of old closures while preserving the complete current
    // closure in an actual independent Objects provider.
    let latest = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id();
    let current = crate::kernel::prove_generation_closure_async(
        &objects, root(latest),
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
        budget(), &cancellation,
    ).await?;
    for object in current.objects {
        let bytes = crate::AsyncObjectStore::read(
            &objects, object, 16 * 1024 * 1024, budget(), &cancellation,
        ).await?.value.bytes;
        crate::AsyncObjectStore::put(&absent, object, bytes, budget(), &cancellation).await?;
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
        &authority, &absent, authority_id, cursor(&durable[1])?, 1, budget(), &cancellation,
    ).await?;
    assert!(present.complete);
    assert_eq!(present.facts[0].transition, AccountingTransition::Published {
        volume_id: workspace.id().volume_id(), generation_root: root(latest),
        logical_regular_file_bytes: 22,
    });
    assert!(matches!(accounting_page(
        &authority, &absent, authority_id, cursor(&durable[0])?, 1, budget(), &cancellation,
    ).await, Err(AccountingError::Proof(_))));
    let initial = records(&authority, authority_id).await?.remove(0);
    let offline = MemoryAuthorityStore::default();
    let CreateAuthorityOutcome::Created(head) = AuthorityStore::create_authority(
        &offline, authority_id, initial.epoch, budget(),
    )?.value else {
        return Err("offline fixture authority already existed".into());
    };
    let AppendOutcome::Committed(committed) = AuthorityStore::compare_and_append(
        &offline, authority_id, initial.epoch, head,
        ProposedCommit { operation_id: initial.operation_id, fingerprint: initial.fingerprint, payload: initial.payload },
        budget(),
    )?.value else {
        return Err("offline actual Memory authority did not commit".into());
    };
    assert_eq!(committed.settled_at_micros, None);
    assert!(matches!(accounting_page(
        &offline, &objects, authority_id, AccountingCursor::genesis(), 1, budget(), &cancellation,
    ).await, Err(AccountingError::MissingTime)));
    Ok(())
}

#[tokio::test]
async fn accounting_rejects_malformed_cursor_and_unbounded_admission()
-> Result<(), Box<dyn Error>> {
    let (fs, authority, objects) = fixture();
    let workspace = fs.create_workspace("cursor-accounting").await?;
    workspace.write_text("/file", "a").await?;
    let authority_id = volume_authority_id(workspace.id().volume_id());
    let cancellation = CancellationToken::new();
    let first = accounting_page(
        &authority, &objects, authority_id, AccountingCursor::genesis(), 1, budget(), &cancellation,
    ).await?;
    for cursor in [
        AccountingCursor { digest: Digest::ZERO, ..first.next },
        AccountingCursor { settled_at_micros: first.next.settled_at_micros + 1, ..first.next },
        AccountingCursor { sequence: Sequence::new(99), ..first.next },
    ] {
        assert!(matches!(accounting_page(
            &authority, &objects, authority_id, cursor, 1, budget(), &cancellation,
        ).await, Err(AccountingError::InvalidHistory)));
    }
    for cursor in [
        AccountingCursor { digest: first.next.digest, ..AccountingCursor::genesis() },
        AccountingCursor { settled_at_micros: 1, ..AccountingCursor::genesis() },
    ] {
        assert!(matches!(accounting_page(
            &authority, &objects, authority_id, cursor, 1, budget(), &cancellation,
        ).await, Err(AccountingError::InvalidBounds)));
    }
    for maximum in [0, 257] {
        assert!(matches!(accounting_page(
            &authority, &objects, authority_id, AccountingCursor::genesis(), maximum, budget(), &cancellation,
        ).await, Err(AccountingError::InvalidBounds)));
    }
    assert!(matches!(accounting_page(
        &authority, &objects, authority_id, AccountingCursor::genesis(), 1, WorkBudget::UNBOUNDED, &cancellation,
    ).await, Err(AccountingError::InvalidBounds)));
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
    async fn fork_generation_authority(
        &self, source: GenerationForkSource, destination_authority: AuthorityId,
        operation_id: OperationId, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> AuthorityResult<CreateAuthorityOutcome> {
        self.inner.fork_generation_authority(source, destination_authority, operation_id, budget, cancellation).await
    }

    async fn create_authority(
        &self, authority_id: AuthorityId, genesis_epoch: Epoch,
        budget: WorkBudget, cancellation: &CancellationToken,
    ) -> AuthorityResult<CreateAuthorityOutcome> {
        self.inner.create_authority(authority_id, genesis_epoch, budget, cancellation).await
    }

    async fn head(
        &self, authority_id: AuthorityId, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> AuthorityResult<Head> {
        self.inner.head(authority_id, budget, cancellation).await
    }

    async fn compare_and_append(
        &self, authority_id: AuthorityId, epoch: Epoch, expected: Head,
        commit: ProposedCommit, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> AuthorityResult<AppendOutcome> {
        self.inner.compare_and_append(authority_id, epoch, expected, commit, budget, cancellation).await
    }

    async fn replay(
        &self, authority_id: AuthorityId, after: Sequence, limit: ReplayLimit,
        budget: WorkBudget, cancellation: &CancellationToken,
    ) -> AuthorityResult<Vec<DurableCommit>> {
        let mut receipt = self.inner.replay(authority_id, after, limit, budget, cancellation).await?;
        for record in &mut receipt.value {
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
        Ok(receipt)
    }

    async fn fence(
        &self, authority_id: AuthorityId, expected: Head, budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<FenceOutcome> {
        self.inner.fence(authority_id, expected, budget, cancellation).await
    }

    async fn find_operation(
        &self, authority_id: AuthorityId, operation_id: OperationId, budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<Option<DurableCommit>> {
        self.inner.find_operation(authority_id, operation_id, budget, cancellation).await
    }
}

#[tokio::test]
async fn accounting_checks_actual_replayed_digest_operation_and_time_before_producing_facts()
-> Result<(), Box<dyn Error>> {
    let (fs, authority, objects) = fixture();
    let workspace = fs.create_workspace("damaged-accounting").await?;
    workspace.write_text("/file", "actual").await?;
    let authority_id = volume_authority_id(workspace.id().volume_id());
    let durable = records(&authority, authority_id).await?;
    let later = durable.last().ok_or("actual records missing")?
        .settled_at_micros.ok_or("Stream timestamp missing")? + 1;
    let cancellation = CancellationToken::new();
    for damage in [ReplayDamage::Digest, ReplayDamage::Operation, ReplayDamage::RegressingTime(later)] {
        assert!(matches!(accounting_page(
            &DamagedReplay { inner: &authority, damage }, &objects, authority_id,
            AccountingCursor::genesis(), 2, budget(), &cancellation,
        ).await, Err(AccountingError::InvalidHistory)));
    }
    for damage in [ReplayDamage::MissingTime, ReplayDamage::ZeroTime] {
        assert!(matches!(accounting_page(
            &DamagedReplay { inner: &authority, damage }, &objects, authority_id,
            AccountingCursor::genesis(), 2, budget(), &cancellation,
        ).await, Err(AccountingError::MissingTime)));
    }
    Ok(())
}

async fn prove_original(
    objects: &MemoryObjectBackend,
    root: ObjectId,
    config: crate::model::VolumeConfig,
) -> Result<crate::kernel::GenerationProof, Box<dyn Error>> {
    Ok(crate::kernel::prove_generation_closure_async(
        objects, root,
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
        budget(), &CancellationToken::new(),
    ).await?)
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
        &self, object_id: ObjectId, bytes: acyclic_stream::OwnedBytes, budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::storage::ObjectResult<()> {
        crate::AsyncObjectStore::put(&self.inner, object_id, bytes, budget, cancellation).await
    }

    async fn read(
        &self, object_id: ObjectId, maximum_bytes: u64, budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::storage::ObjectResult<crate::storage::ObjectRead> {
        crate::AsyncObjectStore::read(&self.inner, object_id, maximum_bytes, budget, cancellation).await
    }

    async fn read_many(
        &self, requests: &[crate::storage::ObjectReadRequest], budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::storage::ObjectResult<Vec<crate::storage::ObjectRead>> {
        crate::AsyncObjectStore::read_many(&self.inner, requests, budget, cancellation).await
    }

    async fn contains(
        &self, object_id: ObjectId, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> crate::storage::ObjectResult<bool> {
        crate::AsyncObjectStore::contains(&self.inner, object_id, budget, cancellation).await
    }

    async fn historical_generation_evidence(
        &self, authority_id: AuthorityId, record: &DurableCommit,
        maximum_bytes: u64, _maximum_objects: u64, budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::storage::ObjectResult<Option<crate::kernel::HistoricalGenerationEvidence>> {
        use crate::storage::{ObjectFailure, ObjectReceipt, ObjectStoreError};
        cancellation.check()
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
        work.verify(budget).map_err(|error| ObjectFailure::new(error.into(), work))?;
        Ok(ObjectReceipt { value, work })
    }
}

#[tokio::test]
async fn accounting_uses_protected_original_archive_after_old_closure_is_absent()
-> Result<(), Box<dyn Error>> {
    let (fs, authority, objects) = fixture();
    let workspace = fs.create_workspace("original-archive-accounting").await?;
    workspace.write_text("/file", "original").await?;
    let authority_id = volume_authority_id(workspace.id().volume_id());
    let original_records = records(&authority, authority_id).await?;
    let initial = &original_records[0];
    let original = &original_records[1];
    let created = crate::kernel::decode_volume_created(&initial.payload, 4096)?;
    let original_root = root(workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id());
    // Capture the actual original root and complete proof before advancing head.
    let proof = prove_original(&objects, original_root, created.config).await?;
    assert_eq!(proof.logical_file_bytes, 8);
    let append = original_append(authority_id, original);
    let request = crate::kernel::PublishGenerationRequest {
        authority_id,
        volume_id: workspace.id().volume_id(),
        epoch: append.epoch,
        expected: append.expected,
        operation_id: append.commit.operation_id,
        generation_root: original_root,
    };
    let evidence = crate::kernel::HistoricalGenerationEvidence::from(
        crate::kernel::encode_generation_publication_evidence(
            &request, &proof, &append, created.config, 64 * 1024,
        )?,
    );
    // Initial/retention codec must not disguise a publication's original request.
    assert!(matches!(
        crate::kernel::encode_original_generation_evidence(
            &proof, &append, created.config, 64 * 1024,
        ),
        Err(crate::kernel::PublicationEvidenceError::Mismatch)
    ));
    workspace.write_text("/file", "a much larger current generation").await?;
    let current_records = records(&authority, authority_id).await?;
    let current = &current_records[2];
    let current_root = root(workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id());
    assert_ne!(current_root, original_root);
    let current_proof = prove_original(&objects, current_root, created.config).await?;
    let current_append = original_append(authority_id, current);
    let current_evidence = crate::kernel::HistoricalGenerationEvidence::from(
        crate::kernel::encode_generation_publication_evidence(
            &crate::kernel::PublishGenerationRequest {
                epoch: current_append.epoch,
                expected: current_append.expected,
                operation_id: current_append.commit.operation_id,
                generation_root: current_root,
                ..request
            },
            &current_proof, &current_append, created.config, 64 * 1024,
        )?,
    );
    let (empty, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
    let mut archive = OriginalArchive {
        inner: crate::LogicalObjectStore::new(Arc::new(empty), bucket),
        authority_id,
        original_digest: original.digest,
        evidence,
    };
    let cancellation = CancellationToken::new();
    assert!(!crate::AsyncObjectStore::contains(
        &archive.inner, original_root, budget(), &cancellation,
    ).await?.value);
    assert!(!crate::AsyncObjectStore::contains(
        &archive.inner, current_root, budget(), &cancellation,
    ).await?.value);
    let cursor = AccountingCursor {
        sequence: initial.sequence,
        digest: initial.digest,
        settled_at_micros: initial.settled_at_micros.ok_or("initial Stream timestamp missing")?,
    };
    let page = accounting_page(
        &authority, &archive, authority_id, cursor, 1, budget(), &cancellation,
    ).await?;
    assert_eq!(page.facts.len(), 1);
    assert!(!page.complete);
    assert_eq!(page.facts[0].transition, AccountingTransition::Published {
        volume_id: workspace.id().volume_id(), generation_root: original_root,
        logical_regular_file_bytes: 8,
    });
    assert_eq!(page.facts[0].operation_id, original.operation_id);
    assert_eq!(Some(page.facts[0].settled_at_micros), original.settled_at_micros);
    assert_eq!(page.watermark_digest, current.digest);
    // A valid archive of the current head cannot serve an older committed fact.
    archive.evidence = current_evidence;
    assert!(matches!(accounting_page(
        &authority, &archive, authority_id, cursor, 1, budget(), &cancellation,
    ).await, Err(AccountingError::Proof(_))));
    Ok(())
}

#[tokio::test]
async fn original_initial_and_retention_evidence_bind_actual_stream_records_and_reject_sha_forgery()
-> Result<(), Box<dyn Error>> {
    let (fs, authority, objects) = fixture();
    let workspace = fs.create_workspace("initial-retention-original").await?;
    let authority_id = volume_authority_id(workspace.id().volume_id());
    let initial = records(&authority, authority_id).await?.remove(0);
    let created = crate::kernel::decode_volume_created(&initial.payload, 4096)?;
    let initial_proof = prove_original(&objects, created.initial_generation_root, created.config).await?;
    let initial_append = original_append(authority_id, &initial);
    let initial_evidence = crate::kernel::encode_original_generation_evidence(
        &initial_proof, &initial_append, created.config, 64 * 1024,
    )?;
    workspace.write_text("/file", "retained original").await?;
    workspace.checkpoint("original").await?;
    let retained_authority = retention_authority_id(
        workspace.id().volume_id(), RetentionKind::Checkpoint, "original",
    );
    let retained_record = records(&authority, retained_authority).await?.remove(0);
    let retained = crate::kernel::decode_retention_created(&retained_record.payload, 4096)?;
    let retained_proof = prove_original(&objects, retained.generation_root, retained.config).await?;
    let retained_append = original_append(retained_authority, &retained_record);
    let retained_evidence = crate::kernel::encode_original_generation_evidence(
        &retained_proof, &retained_append, retained.config, 64 * 1024,
    )?;
    for (authority_id, record, proof, append, evidence) in [
        (authority_id, initial, initial_proof, initial_append, initial_evidence),
        (retained_authority, retained_record, retained_proof, retained_append, retained_evidence),
    ] {
        let projection = crate::kernel::decode_historical_generation_evidence(
            &evidence, authority_id, &record, 64 * 1024, 4096,
        )?;
        assert_eq!(projection.append, append);
        assert_eq!(projection.root, proof.root);
        assert_eq!(projection.manifest.objects, proof.objects);
        assert_eq!(projection.logical_file_bytes, proof.logical_file_bytes);
        let mut forged_digest = evidence.clone();
        forged_digest.sha256[0] ^= 1;
        assert!(matches!(
            crate::kernel::decode_historical_generation_evidence(
                &forged_digest, authority_id, &record, 64 * 1024, 4096,
            ),
            Err(crate::kernel::PublicationEvidenceError::Mismatch)
        ));
        let mut forged_bytes = evidence.clone();
        let mut bytes = forged_bytes.bytes.to_vec();
        let last = bytes.len() - 1;
        bytes[last] ^= 1;
        forged_bytes.bytes = bytes::Bytes::from(bytes);
        assert!(matches!(
            crate::kernel::decode_historical_generation_evidence(
                &forged_bytes, authority_id, &record, 64 * 1024, 4096,
            ),
            Err(crate::kernel::PublicationEvidenceError::Mismatch)
        ));
    }
    Ok(())
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
            let workspace = fs.create_workspace("property-accounting").await?;
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
                    &objects, authority_id, cursor, page_size, budget(), &cancellation,
                ).await?;
                assert!(!page.facts.is_empty());
                assert!(page.facts.len() <= page_size as usize);
                assert_eq!(page.watermark_sequence, original_records.last().ok_or("no actual records")?.sequence);
                assert_eq!(page.facts[0].sequence.get(), cursor.sequence.get() + 1);
                page.work.verify(budget())?;
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
                    &objects, authority_id, anchor, facts.len() as u32, budget(), &cancellation,
                ).await?;
                assert_eq!(replay.facts, facts);
            }
            let terminal = authority.accounting_page(
                &objects, authority_id, cursor, 2, budget(), &cancellation,
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
            let failure = authority.fence(
                authority_id,
                Head {
                    epoch: later_record.epoch,
                    sequence: terminal.next.sequence,
                    digest: terminal.next.digest,
                },
                budget(), &cancellation,
            ).await.err().ok_or("retired fence unexpectedly succeeded")?;
            assert!(matches!(failure.error, AuthorityStoreError::Retired));
            Ok(())
        });
        proptest::prop_assert!(result.is_ok(), "{:?}", result.err());
    }
}
