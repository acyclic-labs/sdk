use acyclic_fs::kernel::{
    CanonicalDecodeError, DecodeLimits, FileKind, FileMetadata, FilePayload, FileRecord,
    FileTablePage, GenerationRoot, MergeGenerationOutcome, MergeGenerationRequest, MetadataField,
    encode_file_metadata, encode_file_table_page, encode_generation_root, file_metadata_id,
    file_table_page_id, merge_generation_async,
};
use acyclic_fs::{
    AsyncObjectStore, AuthorityWork, CancellationToken, FileId, LogicalObjectStore, ObjectId,
    ObjectKind, VolumeId,
};
use acyclic_stream::OwnedBytes;
use divan::Bencher;
use std::collections::BTreeMap;
use std::sync::Arc;

type Store = LogicalObjectStore<acyclic_objects::v1::MemoryObjects>;

fn fail<T>(error: impl std::fmt::Debug) -> T {
    eprintln!("merge-diff benchmark failed: {error:?}");
    std::process::exit(1);
}

struct Fixture {
    store: Store,
    request: MergeGenerationRequest,
}

async fn put(store: &Store, id: ObjectId, bytes: Vec<u8>) {
    store
        .put(
            id,
            OwnedBytes::from(bytes),
            &mut AuthorityWork::new(
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &CancellationToken::new(),
            ),
        )
        .await
        .unwrap_or_else(fail);
}

async fn table(store: &Store, count: u8, metadata: ObjectId, changed: bool) -> ObjectId {
    let records = (1..=count)
        .map(|key| FileRecord {
            file_id: FileId::from_bytes([key; 16]),
            kind: FileKind::Fifo,
            link_count: if changed { 2 } else { 1 },
            metadata,
            payload: FilePayload::Empty,
        })
        .collect();
    let page = FileTablePage::Leaf(records);
    let id = file_table_page_id(&page, u32::from(count)).unwrap_or_else(fail);
    let bytes = encode_file_table_page(
        &page,
        u32::from(count),
        |_| Ok::<_, CanonicalDecodeError>(()),
        |_| CanonicalDecodeError::AllocationFailed,
    )
    .unwrap_or_else(fail);
    put(store, id, bytes).await;
    id
}

async fn generation(store: &Store, root: &GenerationRoot) -> ObjectId {
    let bytes = encode_generation_root(
        root,
        |_| Ok::<_, CanonicalDecodeError>(()),
        |_| CanonicalDecodeError::AllocationFailed,
    )
    .unwrap_or_else(fail);
    let id = ObjectId {
        kind: ObjectKind::GenerationRoot,
        digest: acyclic_fs::storage::object_digest(ObjectKind::GenerationRoot, &bytes),
    };
    put(store, id, bytes).await;
    id
}

impl Fixture {
    async fn new(count: u8) -> Self {
        let (objects, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
        let store = Store::new(Arc::new(objects), bucket);
        let metadata = FileMetadata::default();
        let original_metadata = file_metadata_id(metadata).unwrap_or_else(fail);
        let changed_metadata = FileMetadata {
            modified_ns: MetadataField::Value(1),
            ..metadata
        };
        let changed_id = file_metadata_id(changed_metadata).unwrap_or_else(fail);
        for (id, value) in [
            (original_metadata, metadata),
            (changed_id, changed_metadata),
        ] {
            put(
                &store,
                id,
                encode_file_metadata(
                    value,
                    |_| Ok::<_, CanonicalDecodeError>(()),
                    |_| CanonicalDecodeError::AllocationFailed,
                )
                .unwrap_or_else(fail),
            )
            .await;
        }
        let root = |file_table| GenerationRoot {
            volume_id: VolumeId::from_bytes([1; 16]),
            root_file_id: FileId::from_bytes([1; 16]),
            file_table,
            parents: Vec::new(),
            required_features: 0,
        };
        let base = root(table(&store, count, original_metadata, false).await);
        let ours = root(table(&store, count, original_metadata, true).await);
        // Both sides change every record in different fields. This forces both
        // full leaf diffs and the real merge/checkpoint path.
        let theirs = root(table(&store, count, changed_id, false).await);
        let base_generation = generation(&store, &base).await;
        Self {
            request: MergeGenerationRequest {
                base_generation,
                ours_generation: None,
                theirs_generation: generation(&store, &theirs).await,
                base,
                ours,
                theirs,
                retain_theirs_parent: true,
                maximum_changes: u32::from(count),
                maximum_conflicts: u32::from(count),
                resolutions: BTreeMap::new(),
            },
            store,
        }
    }
}

#[divan::bench(args = [1, 8, 64], threads = [1, 4], sample_count = 32, sample_size = 4)]
fn ordered_leaf_merge(bencher: Bencher, count: u8) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap_or_else(fail);
    let fixture = runtime.block_on(Fixture::new(count));
    let cancellation = CancellationToken::new();
    let operation = || {
        let result = runtime
            .block_on(merge_generation_async(
                &fixture.store,
                fixture.request.clone(),
                DecodeLimits::default(),
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &cancellation,
            ))
            .unwrap_or_else(fail);
        if !matches!(result.value, MergeGenerationOutcome::Prepared { .. }) {
            fail::<()>("fixture must prepare a merge");
        }
        result
    };
    let receipt = operation();
    if std::env::var_os("ACYCLIC_BENCH_WORK").is_some() {
        eprintln!("ordered_leaf_merge count={count} work={:?}", receipt.work);
    }
    let markers = std::env::var_os("ACYCLIC_BENCH_MARKERS").is_some();
    if markers {
        eprintln!("ACYCLIC_BENCH_BOUNDARY name=ordered_leaf_merge count={count} begin=true");
    }
    bencher.bench(operation);
    if markers {
        eprintln!("ACYCLIC_BENCH_BOUNDARY name=ordered_leaf_merge count={count} begin=false");
    }
}
