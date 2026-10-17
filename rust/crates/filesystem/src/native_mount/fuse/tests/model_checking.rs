use super::*;
use crate::native_mount::*;

// Inode-only invalidation never calls into content. Unexpected source I/O is
// a failure, while the core, meter, notifications and waits remain production.
struct NoContent;

macro_rules! unused_content_methods {
    ($($name:ident($($arg:ident: $ty:ty),* $(,)?) -> $result:ty;)*) => {
        $(fn $name(&self, $($arg: $ty),*) -> $result {
            let _ = ($($arg,)*);
            unreachable!("inode invalidation unexpectedly accessed content")
        })*
    };
}

impl MountFilesystem for NoContent {
    unused_content_methods! {
        authorize_with_meter(requests: &[(crate::access::Action, &MountPath)], work: &NativeMaterializationWork) -> Result<(), MountSourceError>;
        lookup_with_meter(path: &MountPath, work: &NativeMaterializationWork) -> Result<Option<MountLookup>, MountSourceError>;
        lookup_pinned_with_meter(path: &MountPath, work: &NativeMaterializationWork) -> Result<Option<(MountLookup, Option<MountContentPin>)>, MountSourceError>;
        observe_view_with_meter(observer: std::sync::Weak<dyn ViewObserver>, work: &NativeMaterializationWork) -> Result<(), MountSourceError>;
        fence_changes_with_meter(work: &NativeMaterializationWork) -> Result<(), MountSourceError>;
        acquire_binding_lease_with_meter(expected_epoch: Option<u64>, work: &NativeMaterializationWork) -> Result<Box<dyn Send>, MountSourceError>;
        capture_host_paths_with_meter(root: &Path, paths: &[MountPath], expected_root_identity: crate::NativeRootIdentity, work: &NativeMaterializationWork) -> Result<(), MountSourceError>;
        capture_host_subtree_with_meter(root: &Path, path: &MountPath, expected_root_identity: crate::NativeRootIdentity, work: &NativeMaterializationWork) -> Result<(), MountSourceError>;
        read_pinned_with_meter(path: &MountPath, pin: MountContentPin, range: crate::ByteRange, piece: u32, sink: &mut NativeMountContentSink<'_>, work: &NativeMaterializationWork) -> Result<(), MountSourceError>;
        read_link(path: &MountPath, work: &NativeMaterializationWork) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError>;
        read_range_with_meter(path: &MountPath, offset: u64, length: u32, work: &NativeMaterializationWork) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError>;
        read_directory_with_meter(path: &MountPath, cursor: Option<&[u8]>, maximum_entries: u32, work: &NativeMaterializationWork) -> Result<MountDirectoryPage, MountSourceError>;
        remove(path: &MountPath, expected: Option<FileId>, work: &NativeMaterializationWork) -> Result<(), MountSourceError>;
        rename(source: &MountPath, destination: &MountPath, replace: bool, work: &NativeMaterializationWork) -> Result<(), MountSourceError>;
        set_attributes(path: &MountPath, metadata: FileMetadata, logical_bytes: Option<u64>, work: &NativeMaterializationWork) -> Result<(), MountSourceError>;
        flush(work: &NativeMaterializationWork) -> Result<(), MountSourceError>;
        open_file_with_meter(path: &MountPath, work: &NativeMaterializationWork) -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError>;
        open_file_with_lookup_with_meter(path: &MountPath, access: crate::native_mount::MountOpenAccess, work: &NativeMaterializationWork) -> Result<(crate::native_mount::MountOpenClaim, MountLookup), MountSourceError>;
        open_created_with_meter(path: &MountPath, created: &MountLookup, work: &NativeMaterializationWork) -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError>;
        detach_file_with_meter(path: &MountPath, work: &NativeMaterializationWork) -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError>;
        create_file(path: &MountPath, metadata: FileMetadata, absent_since: Option<ViewStamp>, work: &NativeMaterializationWork) -> Result<MountLookup, MountSourceError>;
        create_directory(path: &MountPath, metadata: FileMetadata, work: &NativeMaterializationWork) -> Result<MountLookup, MountSourceError>;
        create_symbolic_link(path: &MountPath, target: crate::kernel::OwnedSourceChunk, metadata: FileMetadata, work: &NativeMaterializationWork) -> Result<MountLookup, MountSourceError>;
        create_special(path: &MountPath, kind: MountNodeKind, device: Option<(u32, u32)>, metadata: FileMetadata, work: &NativeMaterializationWork) -> Result<MountLookup, MountSourceError>;
        read_attribute(path: &MountPath, name: &[u8], work: &NativeMaterializationWork) -> Result<Option<crate::kernel::OwnedSourceChunk>, MountSourceError>;
        list_attributes_with_meter(path: &MountPath, cursor: Option<&[u8]>, maximum_entries: u32, work: &NativeMaterializationWork) -> Result<(MountAttributePage, crate::NativeAllocation), MountSourceError>;
        write_attribute(path: &MountPath, name: &[u8], value: crate::kernel::OwnedSourceChunk, mode: MountAttributeWriteMode, work: &NativeMaterializationWork) -> Result<(), MountSourceError>;
        remove_attribute(path: &MountPath, name: &[u8], work: &NativeMaterializationWork) -> Result<(), MountSourceError>;
        hard_link(source: &MountPath, destination: &MountPath, work: &NativeMaterializationWork) -> Result<(), MountSourceError>;
        folded_path_with_meter(path: &MountPath, work: &NativeMaterializationWork) -> Result<Option<(MountPath, u64)>, MountSourceError>;
        view_is_stable_with_meter(work: &NativeMaterializationWork) -> Result<bool, MountSourceError>;
        view_stamp_with_meter(work: &NativeMaterializationWork) -> Result<Option<ViewStamp>, MountSourceError>;
        binding_epoch_with_meter(work: &NativeMaterializationWork) -> Result<Option<u64>, MountSourceError>;
        unchanged_since_with_meter(path: &MountPath, file_id: Option<FileId>, stamp: ViewStamp, work: &NativeMaterializationWork) -> Result<bool, MountSourceError>;
        reported_unchanged_since_with_meter(path: &MountPath, file_id: Option<FileId>, stamp: ViewStamp, work: &NativeMaterializationWork) -> Result<bool, MountSourceError>;
        shutdown_with_meter(work: &NativeMaterializationWork) -> Result<bool, MountSourceError>;
        node_unchanged_since_with_meter(file_id: FileId, stamp: ViewStamp, work: &NativeMaterializationWork) -> Result<bool, MountSourceError>;
        listing_unchanged_since_with_meter(path: &MountPath, stamp: ViewStamp, work: &NativeMaterializationWork) -> Result<bool, MountSourceError>;
        lookup(path: &MountPath) -> Result<Option<MountLookup>, MountSourceError>;
        open_file(path: &MountPath) -> Result<Arc<dyn MountOpenFile>, MountSourceError>;
        detach_file(path: &MountPath) -> Result<Arc<dyn MountOpenFile>, MountSourceError>;
        read_range(path: &MountPath, offset: u64, length: u32) -> Result<acyclic_stream::OwnedBytes, MountSourceError>;
        seek(path: &MountPath, offset: u64, target: MountSeekTarget) -> Result<Option<u64>, MountSourceError>;
        read_directory(path: &MountPath, cursor: Option<&[u8]>, maximum_entries: u32) -> Result<MountDirectoryPage, MountSourceError>;
        list_attributes(path: &MountPath, cursor: Option<&[u8]>, maximum_entries: u32) -> Result<MountAttributePage, MountSourceError>;
        write_range(path: &MountPath, offset: u64, bytes: Bytes) -> Result<(), MountSourceError>;
        resize(path: &MountPath, logical_bytes: u64) -> Result<(), MountSourceError>;
        allocate_range(path: &MountPath, offset: u64, length: u64, operation: MountRangeAllocation) -> Result<(), MountSourceError>;
        clone_range(source: &MountPath, source_offset: u64, destination: &MountPath, destination_offset: u64, length: u64) -> Result<(), MountSourceError>;
        capture_host_path(source_root: &Path, path: &MountPath) -> Result<(), MountSourceError>;
        capture_host_paths(source_root: &Path, paths: &[MountPath]) -> Result<(), MountSourceError>;
        capture_host_subtree(source_root: &Path, path: &MountPath) -> Result<(), MountSourceError>;
    }
}

pub(in crate::native_mount::fuse) fn fixture()
-> (Arc<ProjectionCore>, crate::NativeMaterializationWork) {
    let work = crate::NativeMaterializationWork::new(
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .unwrap();
    let core = ProjectionCore::new(
        Arc::new(NoContent),
        directory(FileId::new()),
        None,
        false,
        0,
        0,
        &work,
    )
    .unwrap();
    (core, work)
}
