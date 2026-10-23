use super::*;
use crate::kernel::{GenerationProof, HistoricalGenerationEvidence, PublishGenerationRequest};
use crate::storage::{ObjectRead, ObjectReadRequest, ObjectStoreError};

pub(super) async fn prove(
    objects: &MemoryObjectBackend,
    root: ObjectId,
    config: crate::model::VolumeConfig,
) -> Result<GenerationProof, Box<dyn Error>> {
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

async fn evidence(
    objects: &MemoryObjectBackend,
    id: AuthorityId,
    record: &DurableCommit,
    root: ObjectId,
    config: crate::model::VolumeConfig,
) -> Result<HistoricalGenerationEvidence, Box<dyn Error>> {
    let proof = prove(objects, root, config).await?;
    let append = crate::GuardedAppend {
        authority_id: id,
        epoch: record.epoch,
        expected: Head {
            epoch: record.epoch,
            sequence: Sequence::new(record.sequence.get() - 1),
            digest: record.previous_digest,
        },
        commit: ProposedCommit {
            publication: None,
            operation_id: record.operation_id,
            fingerprint: record.fingerprint,
            payload: record.payload.clone(),
        },
        permit: crate::PublicationPermit::Unrestricted,
    };
    Ok(
        if crate::kernel::decode_published_generation::<crate::kernel::CanonicalDecodeError>(
            &record.payload,
            4096,
            |_| Ok(()),
        )
        .is_ok()
        {
            crate::kernel::encode_generation_publication_evidence(
                &PublishGenerationRequest {
                    authority_id: id,
                    volume_id: proof.root.volume_id,
                    epoch: record.epoch,
                    expected: append.expected,
                    operation_id: record.operation_id,
                    generation_root: root,
                },
                &proof,
                &append,
                config,
                64 * 1024,
            )?
            .into()
        } else {
            crate::kernel::encode_original_generation_evidence(&proof, &append, config, 64 * 1024)?
        },
    )
}

struct Archive {
    empty: MemoryObjectBackend,
    authority: AuthorityId,
    digest: Digest,
    evidence: HistoricalGenerationEvidence,
}

impl AsyncObjectStore for Archive {
    async fn put(
        &self,
        id: ObjectId,
        bytes: acyclic_stream::OwnedBytes,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        self.empty.put(id, bytes, work).await
    }
    async fn read(
        &self,
        id: ObjectId,
        maximum: u64,
        work: &mut AuthorityWork<'_>,
    ) -> Result<ObjectRead, ObjectStoreError> {
        self.empty.read(id, maximum, work).await
    }
    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        work: &mut AuthorityWork<'_>,
    ) -> Result<(Vec<ObjectRead>, crate::AuthorityAllocation), ObjectStoreError> {
        self.empty.read_many(requests, work).await
    }
    async fn contains(
        &self,
        id: ObjectId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<bool, ObjectStoreError> {
        self.empty.contains(id, work).await
    }
    async fn historical_generation_evidence(
        &self,
        id: AuthorityId,
        record: &DurableCommit,
        maximum: u64,
        _: u64,
        work: &mut AuthorityWork<'_>,
    ) -> Result<Option<HistoricalGenerationEvidence>, ObjectStoreError> {
        work.cancellation
            .check()
            .map_err(|_| ObjectStoreError::Cancelled)?;
        if self.evidence.bytes.len() as u64 > maximum {
            return Err(ObjectStoreError::TooLarge {
                observed: self.evidence.bytes.len() as u64,
                maximum,
            });
        }
        let value =
            (id == self.authority && record.digest == self.digest).then(|| self.evidence.clone());
        work.accept(Ok(crate::OperationReceipt {
            work: crate::WorkCounters {
                items_examined: 1,
                items_returned: u64::from(value.is_some()),
                ..crate::WorkCounters::UNCHARGED
            },
            value,
        }))
    }
}

#[tokio::test]
async fn archives_bind_original_publication_creation_and_retention_and_reject_forgery()
-> Result<(), Box<dyn Error>> {
    let (fs, authority, objects) = fixture();
    let cancel = CancellationToken::new();
    let workspace = fs
        .create_workspace("archived-accounting", B, &cancel)
        .await?
        .value;
    let config = workspace.volume_config();
    let id = volume_authority_id(workspace.id().volume_id());
    let initial = records(&authority, id).await?.remove(0);
    let initial_root = root(workspace.head(B, &cancel).await?.value.id());
    workspace.write_text("/file", "original").await?;
    let original = records(&authority, id).await?.remove(1);
    let original_root = root(workspace.head(B, &cancel).await?.value.id());
    workspace.checkpoint("original").await?;
    let retention_id = retention_authority_id(
        workspace.id().volume_id(),
        RetentionKind::Checkpoint,
        "original",
    );
    let retention = records(&authority, retention_id).await?.remove(0);
    workspace
        .write_text("/file", "a much larger current generation")
        .await?;
    let current = records(&authority, id)
        .await?
        .pop()
        .ok_or("current record missing")?;
    let current_root = root(workspace.head(B, &cancel).await?.value.id());
    let current_evidence = evidence(&objects, id, &current, current_root, config).await?;
    for (authority_id, record, original_root, position, size) in [
        (id, &initial, initial_root, AccountingCursor::genesis(), 0),
        (id, &original, original_root, cursor(&initial)?, 8),
        (
            retention_id,
            &retention,
            original_root,
            AccountingCursor::genesis(),
            8,
        ),
    ] {
        let (empty, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
        let genuine = evidence(&objects, authority_id, record, original_root, config).await?;
        let mut archive = Archive {
            empty: crate::LogicalObjectStore::new(Arc::new(empty), bucket),
            authority: authority_id,
            digest: record.digest,
            evidence: genuine.clone(),
        };
        assert!(
            !archive
                .contains(original_root, &mut AuthorityWork::new(BUDGET, &cancel))
                .await?
        );
        assert!(
            !archive
                .contains(current_root, &mut AuthorityWork::new(BUDGET, &cancel))
                .await?
        );
        let actual = page(&authority, &archive, authority_id, position, 1).await?;
        assert_eq!(actual.facts.len(), 1);
        assert_eq!(actual.facts[0].operation_id, record.operation_id);
        assert_eq!(
            Some(actual.facts[0].settled_at_micros),
            record.settled_at_micros
        );
        match actual.facts[0].transition {
            AccountingTransition::Published {
                generation_root,
                logical_regular_file_bytes,
                ..
            }
            | AccountingTransition::Retained {
                generation_root,
                logical_regular_file_bytes,
                ..
            } => {
                assert_eq!(generation_root, original_root);
                assert_eq!(logical_regular_file_bytes, size);
            }
            _ => return Err("unexpected archived transition".into()),
        }
        for tamper in 0..3 {
            archive.evidence = genuine.clone();
            match tamper {
                0 => archive.evidence.sha256[0] ^= 1,
                1 => {
                    let mut bytes = archive.evidence.bytes.to_vec();
                    *bytes.last_mut().ok_or("empty evidence")? ^= 1;
                    archive.evidence.bytes = bytes::Bytes::from(bytes);
                }
                _ => archive.evidence = current_evidence.clone(),
            }
            assert!(matches!(
                page(&authority, &archive, authority_id, position, 1).await,
                Err(AccountingError::Proof(_))
            ));
        }
    }
    Ok(())
}
