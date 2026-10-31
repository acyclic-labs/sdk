//! Original evidence writers consume a closure proved through the real adapter.
use super::*;
use acyclic_fs::{
    AsyncObjectStore, AuthorityWork, CancellationToken, FileId, GenerationId, GuardedAppend,
    HashedObject, Head, Lifecycle, LogicalObjectStore, ObjectId, ObjectKind, ProposedCommit,
    PublicationPermit, VolumeConfig, VolumeId, WorkBudget,
};
use acyclic_stream::OwnedBytes;
use std::sync::Arc;

type Store = LogicalObjectStore<acyclic_objects::MemoryObjects>;

async fn put(
    store: &Store,
    kind: ObjectKind,
    bytes: Vec<u8>,
    cancellation: &CancellationToken,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let object = HashedObject::new(kind, OwnedBytes::from(bytes));
    let id = object.object_id();
    store
        .put_hashed(
            object,
            &mut AuthorityWork::new(WorkBudget::UNBOUNDED, cancellation),
        )
        .await?;
    Ok(id)
}

pub(super) async fn seeds() -> Result<Vec<(&'static str, Vec<u8>)>, Box<dyn std::error::Error>> {
    let volume_id = VolumeId::from_bytes([81; 16]);
    let root_file_id = FileId::from_bytes([82; 16]);
    let config = VolumeConfig::portable(Lifecycle::Durable);
    let limits = DecodeLimits::for_volume(config);
    let cancellation = CancellationToken::new();
    let store = LogicalObjectStore::new(Arc::new(acyclic_objects::MemoryObjects::memory()))
        .with_object_limit(limits.maximum_object_bytes)?;
    let allocation = super::super::allocation_error;
    let metadata = put(
        &store,
        ObjectKind::Metadata,
        encode_file_metadata(FileMetadata::default(), |_| Ok(()), allocation)?,
        &cancellation,
    )
    .await?;
    let tree = put(
        &store,
        ObjectKind::TreePage,
        encode_tree_page(
            &TreePage::Leaf(Vec::new()),
            limits.maximum_page_items,
            |_| Ok(()),
            allocation,
        )?,
        &cancellation,
    )
    .await?;
    let table = put(
        &store,
        ObjectKind::FileTablePage,
        encode_file_table_page(
            &FileTablePage::Leaf(vec![FileRecord {
                file_id: root_file_id,
                kind: FileKind::Directory,
                link_count: 1,
                metadata,
                payload: FilePayload::Directory { entries: tree },
            }]),
            limits.maximum_page_items,
            |_| Ok(()),
            allocation,
        )?,
        &cancellation,
    )
    .await?;
    let root = GenerationRoot {
        volume_id,
        root_file_id,
        file_table: table,
        parents: Vec::new(),
        required_features: 0,
    };
    let root_id = put(
        &store,
        ObjectKind::GenerationRoot,
        encode_generation_root(&root, |_| Ok(()), allocation)?,
        &cancellation,
    )
    .await?;
    let proof = prove_generation_closure(
        &store,
        root_id,
        ClosureLimits {
            decode: limits,
            maximum_objects: config.limits.maximum_objects_per_generation,
            maximum_files: config.limits.maximum_files_per_generation,
            maximum_object_bytes: config.limits.maximum_generation_bytes,
            profile: config.profile,
            symbolic_links: config.symbolic_links,
            hard_links: config.hard_links,
            sparse_files: config.sparse_files,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await?;
    assert_eq!(proof.generation_id, GenerationId::from_root(root_id));
    let mut outputs = Vec::new();
    for (name, retention) in [
        ("historical-volume-original", None),
        (
            "historical-checkpoint-original",
            Some(RetentionKind::Checkpoint),
        ),
        ("historical-pin-original", Some(RetentionKind::Pin)),
        ("historical-fork-original", Some(RetentionKind::ForkBase)),
    ] {
        let (authority_id, payload) = if let Some(kind) = retention {
            (
                retention_authority_id(volume_id, kind, "original"),
                encode_retention_created(
                    &RetentionCreated {
                        volume_id,
                        kind,
                        label: "original".into(),
                        generation_root: root_id,
                        config,
                    },
                    |_| Ok::<_, acyclic_fs::FsError>(()),
                    allocation,
                )?,
            )
        } else {
            (
                volume_authority_id(volume_id),
                encode_volume_created(
                    VolumeCreated {
                        volume_id,
                        initial_generation_root: root_id,
                        config,
                    },
                    |_| Ok::<_, acyclic_fs::FsError>(()),
                    allocation,
                )?,
            )
        };
        let append = GuardedAppend {
            authority_id,
            epoch: Epoch::GENESIS,
            expected: Head::genesis(Epoch::GENESIS),
            commit: ProposedCommit {
                operation_id: OperationId::from_bytes(authority_id.into_bytes()),
                fingerprint: Digest::from_bytes(Sha256::digest(&payload).into()),
                payload: payload.into(),
                publication: None,
            },
            permit: PublicationPermit::Unrestricted,
        };
        let evidence = encode_original_generation_evidence(&proof, &append, config, u64::MAX)?;
        // This is a preparation/context control, not a claimed authority commit
        // or protected provider receipt. Use the original writer's digest.
        let mut record = DurableCommit {
            epoch: append.epoch,
            sequence: Sequence::new(1),
            operation_id: append.commit.operation_id,
            fingerprint: append.commit.fingerprint,
            previous_digest: append.expected.digest,
            digest: Digest::ZERO,
            payload: append.commit.payload.clone(),
            settled_at_micros: None,
        };
        record.digest = digest(authority_id, &record);
        let expected = HistoricalGenerationProjection {
            append,
            manifest: GenerationExportManifest {
                volume_id,
                config,
                generation_root: root_id,
                generation_id: proof.generation_id,
                objects: proof.objects.clone(),
                file_count: proof.file_count,
            },
            root: proof.root.clone(),
            logical_file_bytes: proof.logical_file_bytes,
        };
        verify(&evidence, authority_id, &record, &expected);
        outputs.push((name, evidence.bytes.to_vec()));
    }
    Ok(outputs)
}
