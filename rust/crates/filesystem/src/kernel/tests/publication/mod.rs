use super::*;
use crate::GuardedAppend;
use crate::async_storage::AsyncAuthorityStore;
use crate::async_storage::AsyncObjectStore;
use crate::foundation::FileId;
use crate::foundation::Sequence;
use crate::kernel::{
    DecodeLimits, FileKind, FileMetadata, FilePayload, FileRecord, FileTablePage, GenerationRoot,
    TreePage, encode_file_metadata, encode_file_table_page, encode_generation_root,
    encode_tree_page,
};
use crate::kernel::{
    Extent, ExtentKind, ExtentPage, GenerationExportManifestError, LogicalName, NameEncoding,
    PublicationEvidenceError, TreeEntry, decode_generation_publication_evidence,
    encode_extent_page, encode_generation_publication_evidence, prove_generation_closure,
};
use crate::memory::{MemoryAuthorityStore, MemoryObjectStore};
use crate::model::{Lifecycle, VolumeConfig};
use crate::storage::{FenceOutcome, ObjectKind, object_digest};
use sha2::{Digest as _, Sha256};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
fn accept<T>(_: T) -> Result<(), CanonicalDecodeError> {
    Ok(())
}
fn allocation_failed(_: std::collections::TryReserveError) -> CanonicalDecodeError {
    CanonicalDecodeError::AllocationFailed
}
async fn created(authority: &MemoryAuthorityStore, id: AuthorityId) -> TestResult<Head> {
    let crate::storage::CreateAuthorityOutcome::Created(head) = authority
        .create_authority(
            id,
            Epoch::GENESIS,
            &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()),
            None::<&mut ()>,
        )
        .await
        .map_err(|error| error.into_error(std::convert::identity))?
    else {
        return Err("authority existed".into());
    };
    Ok(head)
}
async fn initialized(
    objects: &MemoryObjectStore,
    authority: &MemoryAuthorityStore,
    volume_id: VolumeId,
) -> TestResult<Head> {
    use crate::model::{
        CaseSensitivity, ConcurrencyMode, FilesystemProfile, UnicodePolicy, VolumeLimits,
    };
    let authority_id = volume_authority_id(volume_id);
    created(authority, authority_id).await?;
    let creation = crate::kernel::VolumeCreated {
        volume_id,
        config: VolumeConfig {
            profile: FilesystemProfile::Portable,
            concurrency: ConcurrencyMode::Optimistic,
            lifecycle: Lifecycle::Ephemeral,
            case_sensitivity: CaseSensitivity::Sensitive,
            unicode: UnicodePolicy::Preserve,
            symbolic_links: true,
            hard_links: true,
            sparse_files: true,
            limits: VolumeLimits::default(),
        },
        initial_generation_root: fixture(objects, volume_id).await?,
    };
    let payload = crate::kernel::encode_volume_created(
        creation,
        |_| Ok::<_, crate::kernel::VolumeCreatedError>(()),
        |_| CanonicalDecodeError::AllocationFailed.into(),
    )?;
    authority
        .create_first_record(
            authority_id,
            ProposedCommit {
                operation_id: OperationId::from_bytes(volume_id.into_bytes()),
                fingerprint: Digest::from_bytes(Sha256::digest(&payload).into()),
                payload: Bytes::from(payload),
                publication: None,
            },
            &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()),
            None::<&mut ()>,
        )
        .await
        .map_err(|error| error.into_error(std::convert::identity))?;
    read_head(authority, authority_id).await
}
async fn read_head(authority: &MemoryAuthorityStore, id: AuthorityId) -> TestResult<Head> {
    Ok(authority
        .head(
            id,
            &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()),
        )
        .await?)
}
async fn fenced(
    authority: &MemoryAuthorityStore,
    id: AuthorityId,
    expected: Head,
) -> TestResult<FenceOutcome> {
    Ok(authority
        .fence(
            id,
            expected,
            &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()),
        )
        .await?)
}

async fn put(store: &MemoryObjectStore, kind: ObjectKind, bytes: Vec<u8>) -> TestResult<ObjectId> {
    let object = ObjectId {
        kind,
        digest: object_digest(kind, &bytes),
    };
    store
        .put(
            object,
            acyclic_stream::OwnedBytes::from(bytes),
            &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()),
        )
        .await?;
    Ok(object)
}

async fn fixture(objects: &MemoryObjectStore, volume_id: VolumeId) -> TestResult<ObjectId> {
    let metadata = metadata_object(objects).await?;
    Ok(generation(
        objects,
        volume_id,
        FileId::from_bytes([3; 16]),
        metadata,
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .await?[2])
}
async fn metadata_object(objects: &MemoryObjectStore) -> TestResult<ObjectId> {
    put(
        objects,
        ObjectKind::Metadata,
        encode_file_metadata(FileMetadata::default(), accept, allocation_failed)?,
    )
    .await
}
async fn generation(
    objects: &MemoryObjectStore,
    volume_id: VolumeId,
    root_file_id: FileId,
    metadata: ObjectId,
    entries: Vec<TreeEntry>,
    mut files: Vec<FileRecord>,
    parents: Vec<crate::GenerationId>,
) -> TestResult<[ObjectId; 3]> {
    let tree = put(
        objects,
        ObjectKind::TreePage,
        encode_tree_page(&TreePage::Leaf(entries), 8, accept, allocation_failed)?,
    )
    .await?;
    files.insert(
        0,
        FileRecord {
            file_id: root_file_id,
            kind: FileKind::Directory,
            link_count: 1,
            metadata,
            payload: FilePayload::Directory { entries: tree },
        },
    );
    let table = put(
        objects,
        ObjectKind::FileTablePage,
        encode_file_table_page(&FileTablePage::Leaf(files), 8, accept, allocation_failed)?,
    )
    .await?;
    let root = put(
        objects,
        ObjectKind::GenerationRoot,
        encode_generation_root(
            &GenerationRoot {
                volume_id,
                root_file_id,
                file_table: table,
                parents,
                required_features: 0,
            },
            accept,
            allocation_failed,
        )?,
    )
    .await?;
    Ok([tree, table, root])
}
fn regular(
    file_id: FileId,
    metadata: ObjectId,
    logical_bytes: u64,
    extents: ObjectId,
) -> FileRecord {
    FileRecord {
        file_id,
        kind: FileKind::Regular,
        link_count: 1,
        metadata,
        payload: FilePayload::Regular {
            logical_bytes,
            extents,
        },
    }
}
fn entry(name: &[u8], file_id: FileId) -> TestResult<TreeEntry> {
    Ok(TreeEntry {
        name: LogicalName::new(NameEncoding::Utf8, name.to_vec(), 255)?,
        file_id,
        kind: FileKind::Regular,
    })
}

fn limits() -> ClosureLimits {
    ClosureLimits {
        decode: DecodeLimits::default(),
        maximum_objects: 16,
        maximum_files: 16,
        maximum_object_bytes: 64 * 1024,
        profile: crate::model::FilesystemProfile::Portable,
        symbolic_links: true,
        hard_links: true,
        sparse_files: true,
    }
}

mod api;
mod codec;
mod evidence;
#[cfg(not(target_arch = "wasm32"))]
mod native;
