use acyclic_fs::kernel::{
    CanonicalDecodeError, Child, DecodeLimits, FileKind, FileMetadata, FilePayload, FileRecord,
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

#[derive(Clone, Copy, PartialEq)]
enum Workload {
    Dense,
    Append,
    Interleaved,
    Random,
    Sparse,
}

async fn table(
    store: &Store,
    records: Vec<FileRecord>,
    singleton: bool,
    internal: bool,
) -> ObjectId {
    async fn page(store: &Store, value: FileTablePage) -> ObjectId {
        let id = file_table_page_id(&value, 192).unwrap_or_else(fail);
        let bytes = encode_file_table_page(
            &value,
            192,
            |_| Ok::<_, CanonicalDecodeError>(()),
            |_| CanonicalDecodeError::AllocationFailed,
        )
        .unwrap_or_else(fail);
        put(store, id, bytes).await;
        id
    }
    if !internal {
        return page(store, FileTablePage::Leaf(records)).await;
    }
    let mut children = Vec::new();
    for chunk in records.chunks(if singleton { 1 } else { records.len() }) {
        children.push(Child {
            first: chunk[0].file_id,
            page: page(store, FileTablePage::Leaf(chunk.to_vec())).await,
        });
    }
    page(store, FileTablePage::Internal(children)).await
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
    async fn new(count: u8, workload: Workload) -> Self {
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
        let base_keys: Vec<_> = match workload {
            Workload::Interleaved => (1..=count * 2).filter(|key| key % 2 == 1).collect(),
            Workload::Random => (1..=count * 3).filter(|key| key % 3 == 1).collect(),
            _ => (1..=count).collect(),
        };
        let ours_keys = match workload {
            Workload::Dense => base_keys.clone(),
            Workload::Append | Workload::Interleaved => (1..=count * 2).collect(),
            Workload::Random => (1..=count * 3)
                .filter(|key| {
                    *key == 1 || (u32::from(*key).wrapping_mul(2654435761).rotate_left(13) % 3 != 0)
                })
                .collect(),
            Workload::Sparse => vec![1, count],
        };
        let records = |keys: &[u8], side: u8| {
            keys.iter()
                .map(|key| FileRecord {
                    file_id: FileId::from_bytes([*key; 16]),
                    kind: FileKind::Fifo,
                    link_count: if side == 1 && workload == Workload::Dense {
                        2
                    } else {
                        1
                    },
                    metadata: if side == 2 && (workload == Workload::Dense || *key == 1) {
                        changed_id
                    } else {
                        original_metadata
                    },
                    payload: FilePayload::Empty,
                })
                .collect()
        };
        let branch = workload == Workload::Sparse;
        let base = root(table(&store, records(&base_keys, 0), branch, branch).await);
        let ours = root(table(&store, records(&ours_keys, 1), false, branch).await);
        let theirs = root(table(&store, records(&base_keys, 2), branch, branch).await);
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
                maximum_changes: u32::from(count) * if workload == Workload::Dense { 1 } else { 3 },
                maximum_conflicts: u32::from(count)
                    * if workload == Workload::Dense { 1 } else { 3 },
                resolutions: BTreeMap::new(),
            },
            store,
        }
    }
}

#[divan::bench(args = [1, 8, 64], threads = [1, 4], sample_count = 32, sample_size = 4)]
fn ordered_leaf_merge(bencher: Bencher, count: u8) {
    measure(bencher, count, Workload::Dense, "ordered_leaf_merge");
}

#[divan::bench(args = [8, 64], threads = [1, 4], sample_count = 32, sample_size = 4)]
fn append_leaf_merge(bencher: Bencher, count: u8) {
    measure(bencher, count, Workload::Append, "append_leaf_merge");
}

#[divan::bench(args = [8, 64], threads = [1, 4], sample_count = 32, sample_size = 4)]
fn interleaved_leaf_merge(bencher: Bencher, count: u8) {
    measure(
        bencher,
        count,
        Workload::Interleaved,
        "interleaved_leaf_merge",
    );
}

#[divan::bench(args = [8, 64], threads = [1, 4], sample_count = 32, sample_size = 4)]
fn random_leaf_merge(bencher: Bencher, count: u8) {
    measure(bencher, count, Workload::Random, "random_leaf_merge");
}

#[divan::bench(args = [8, 64], threads = [1, 4], sample_count = 32, sample_size = 4)]
fn sparse_leaf_merge(bencher: Bencher, count: u8) {
    measure(bencher, count, Workload::Sparse, "sparse_leaf_merge");
}

fn measure(bencher: Bencher, count: u8, workload: Workload, name: &str) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap_or_else(fail);
    let fixture = runtime.block_on(Fixture::new(count, workload));
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
        eprintln!("{name} count={count} work={:?}", receipt.work);
    }
    let markers = std::env::var_os("ACYCLIC_BENCH_MARKERS").is_some();
    if markers {
        eprintln!("ACYCLIC_BENCH_BOUNDARY name={name} count={count} begin=true");
    }
    bencher.bench(operation);
    if markers {
        eprintln!("ACYCLIC_BENCH_BOUNDARY name={name} count={count} begin=false");
    }
}
