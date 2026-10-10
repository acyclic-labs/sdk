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
    let old = workspace.head().await?.id();
    workspace.write_text("/file", "newer").await?;
    let newer = workspace.head().await?.id();
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
    let latest = workspace.head().await?.id();
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
    let generation = workspace.head().await?.id();
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
        crate::ForkOptions::from_generation(workspace.head().await?, IdempotencyKey::new()),
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
    let latest = workspace.head().await?.id();
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
