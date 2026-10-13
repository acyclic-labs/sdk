use super::*;
use crate::kernel::{FileMetadata, MetadataField};
use windows::Wdk::Storage::FileSystem::FileBasicInformation;

pub(crate) fn apply_windows_metadata<W: NativeHostWork<Error = DemandError>>(
    file: cap_std::fs::File,
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
    let current = windows_stat_information(&file, work)?;
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
        const STRUCTURAL: u32 = 0x5fde50;
        if attributes == 0
            || attributes & !(SETTABLE | STRUCTURAL) != 0
            || attributes & 0x80 != 0 && attributes != 0x80
            || attributes & STRUCTURAL != current.FileAttributes & STRUCTURAL
        {
            return Err(NativeMetadataError("Windows attributes").into());
        }
        desired.FileAttributes = attributes;
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
        windows_set_information(&file, &desired, FileBasicInformation)
    })?;
    let observed = windows_stat_information(&file, work)?;
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
    if matches!(metadata.windows_attributes, MetadataField::Value(_))
        && observed.FileAttributes != desired.FileAttributes
    {
        return Err(NativeMetadataError("Windows attributes").into());
    }
    Ok(())
}
