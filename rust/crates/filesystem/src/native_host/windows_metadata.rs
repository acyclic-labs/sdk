use super::*;
use crate::kernel::{FileMetadata, MetadataField};
use windows::Wdk::Storage::FileSystem::FileBasicInformation;

pub(crate) fn apply_metadata<W: NativeHostWork<Error = DemandError>>(
    file: &cap_std::fs::File,
    metadata: FileMetadata,
    work: &mut W,
) -> Result<(), DemandError> {
    work.check()?;
    if !matches!(
        metadata,
        FileMetadata {
            posix_mode: MetadataField::Unavailable,
            posix_uid: MetadataField::Unavailable,
            posix_gid: MetadataField::Unavailable,
            posix_flags: MetadataField::Unavailable,
            named_attributes: MetadataField::Unavailable,
            acl: MetadataField::Unavailable,
            security_descriptor: MetadataField::Unavailable,
            ..
        }
    ) {
        return Err(NativeMetadataError("unsupported Windows metadata").into());
    }
    if metadata == FileMetadata::default() {
        return Ok(());
    }
    let current = windows_stat_information(file, work)?;
    let mut desired = FILE_BASIC_INFO {
        CreationTime: current.CreationTime,
        LastAccessTime: current.LastAccessTime,
        LastWriteTime: current.LastWriteTime,
        ChangeTime: current.ChangeTime,
        FileAttributes: current.FileAttributes,
    };
    if let MetadataField::Value(attributes) = metadata.windows_attributes {
        // Readonly, hidden, system, archive, normal, temporary, and not-content-indexed.
        const SETTABLE: u32 = 0x21a7;
        // Every known structural attribute remains unchanged.
        const STRUCTURAL: u32 = 0x5fdc50;
        // Sparseness is how this host stores the file's holes, chosen by
        // whoever wrote it; it stays as the file has it.
        let requested = attributes & !SPARSE;
        if attributes == 0
            || requested & !(SETTABLE | STRUCTURAL) != 0
            || requested & 0x80 != 0 && requested != 0x80
            || requested & STRUCTURAL != current.FileAttributes & STRUCTURAL
        {
            return Err(NativeMetadataError("Windows attributes").into());
        }
        desired.FileAttributes = if requested == 0 { 0x80 } else { requested };
    }
    for (requested, value) in [
        (metadata.created_ns, &mut desired.CreationTime),
        (metadata.accessed_ns, &mut desired.LastAccessTime),
        (metadata.modified_ns, &mut desired.LastWriteTime),
        (metadata.changed_ns, &mut desired.ChangeTime),
    ] {
        if let MetadataField::Value(nanoseconds) = requested {
            if nanoseconds % 100 != 0 {
                return Err(NativeMetadataError("sub-100ns timestamp").into());
            }
            *value = unix_nanoseconds_to_filetime(nanoseconds);
        }
    }
    work.attempt_io(WRITE, || {
        windows_set_information(file, &desired, FileBasicInformation)
    })?;
    let observed = windows_stat_information(file, work)?;
    for (requested, expected, actual) in [
        (
            metadata.created_ns,
            desired.CreationTime,
            observed.CreationTime,
        ),
        (
            metadata.accessed_ns,
            desired.LastAccessTime,
            observed.LastAccessTime,
        ),
        (
            metadata.modified_ns,
            desired.LastWriteTime,
            observed.LastWriteTime,
        ),
        (metadata.changed_ns, desired.ChangeTime, observed.ChangeTime),
    ] {
        if matches!(requested, MetadataField::Value(_)) && expected != actual {
            return Err(NativeMetadataError("Windows timestamp").into());
        }
    }
    if let MetadataField::Value(attributes) = metadata.windows_attributes
        && exact_windows_attributes(observed.FileAttributes) != exact_windows_attributes(attributes)
    {
        return Err(NativeMetadataError("Windows attributes").into());
    }
    Ok(())
}

/// The sparse-file attribute: a storage representation of holes, not file
/// metadata a checkout can demand.
const SPARSE: u32 = 0x200;

/// The attributes a materialization reproduces exactly: everything but
/// sparseness, with `NORMAL` (no other attribute) as the empty set.
const fn exact_windows_attributes(attributes: u32) -> u32 {
    let attributes = attributes & !SPARSE;
    if attributes == 0x80 { 0 } else { attributes }
}
