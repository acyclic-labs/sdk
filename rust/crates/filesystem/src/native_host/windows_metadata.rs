use super::*;

#[cfg(windows)]
#[cfg(any(feature = "native-mount", test))]
pub(crate) fn apply_windows_metadata<W: crate::demand::SourceDemandReadWork>(
    file: cap_std::fs::File,
    metadata: crate::kernel::FileMetadata,
    work: &mut W,
) -> Result<(), DemandError> {
    work.check()?;
    reject_unsupported_windows_metadata(metadata)?;
    if metadata == crate::kernel::FileMetadata::default() {
        return Ok(());
    }
    let current = query_windows_basic_info(&file, work)?;
    let desired = desired_windows_basic_info(metadata, current)?;
    work.attempt_io(
        WorkCounters {
            backend_write_operations: 1,
            ..WorkCounters::UNCHARGED
        },
        || set_windows_basic_info(&file, desired),
    )?;
    verify_windows_basic_info(metadata, desired, query_windows_basic_info(&file, work)?)
        .map_err(Into::into)
}

#[cfg(windows)]
#[cfg(any(feature = "native-mount", test))]
pub(super) fn reject_unsupported_windows_metadata(
    metadata: crate::kernel::FileMetadata,
) -> Result<(), NativeMetadataError> {
    use crate::kernel::MetadataField;
    reject_metadata(&[
        (
            !matches!(metadata.posix_mode, MetadataField::Unavailable),
            "posix mode",
        ),
        (
            !matches!(metadata.posix_uid, MetadataField::Unavailable),
            "POSIX owner",
        ),
        (
            !matches!(metadata.posix_gid, MetadataField::Unavailable),
            "POSIX group",
        ),
        (
            !matches!(metadata.posix_flags, MetadataField::Unavailable),
            "POSIX flags",
        ),
        (
            !matches!(metadata.named_attributes, MetadataField::Unavailable),
            "named attributes",
        ),
        (!matches!(metadata.acl, MetadataField::Unavailable), "ACL"),
        (
            !matches!(metadata.security_descriptor, MetadataField::Unavailable),
            "security descriptor",
        ),
    ])?;
    Ok(())
}

#[cfg(windows)]
#[cfg(any(feature = "native-mount", test))]
pub(super) fn desired_windows_basic_info(
    metadata: crate::kernel::FileMetadata,
    current: FILE_BASIC_INFO,
) -> Result<FILE_BASIC_INFO, NativeMetadataError> {
    use crate::kernel::MetadataField;
    const SETTABLE_ATTRIBUTES: u32 = 0x0000_0001 // readonly
        | 0x0000_0002 // hidden
        | 0x0000_0004 // system
        | 0x0000_0020 // archive
        | 0x0000_0080 // normal
        | 0x0000_0100 // temporary
        | 0x0000_2000; // not content indexed
    const STRUCTURAL_ATTRIBUTES: u32 = 0x0000_0010 // directory
        | 0x0000_0040 // device
        | 0x0000_0200 // sparse
        | 0x0000_0400 // reparse point
        | 0x0000_0800 // compressed
        | 0x0000_1000 // offline
        | 0x0000_4000 // encrypted
        | 0x0000_8000 // integrity stream
        | 0x0001_0000 // virtual
        | 0x0002_0000 // no scrub data
        | 0x0004_0000 // EA / recall on open
        | 0x0008_0000 // pinned
        | 0x0010_0000 // unpinned
        | 0x0040_0000; // recall on data access
    const KNOWN_ATTRIBUTES: u32 = SETTABLE_ATTRIBUTES | STRUCTURAL_ATTRIBUTES;
    let attributes = match metadata.windows_attributes {
        MetadataField::Unavailable => current.FileAttributes,
        MetadataField::Value(attributes) => {
            if attributes == 0
                || attributes & !KNOWN_ATTRIBUTES != 0
                || attributes & 0x80 != 0 && attributes != 0x80
                || attributes & STRUCTURAL_ATTRIBUTES
                    != current.FileAttributes & STRUCTURAL_ATTRIBUTES
            {
                return Err(NativeMetadataError("Windows attributes"));
            }
            attributes
        }
    };
    Ok(FILE_BASIC_INFO {
        CreationTime: metadata_time(metadata.created_ns, current.CreationTime)?,
        LastAccessTime: metadata_time(metadata.accessed_ns, current.LastAccessTime)?,
        LastWriteTime: metadata_time(metadata.modified_ns, current.LastWriteTime)?,
        ChangeTime: metadata_time(metadata.changed_ns, current.ChangeTime)?,
        FileAttributes: attributes,
    })
}

#[cfg(windows)]
#[cfg(any(feature = "native-mount", test))]
pub(super) fn verify_windows_basic_info(
    metadata: crate::kernel::FileMetadata,
    desired: FILE_BASIC_INFO,
    observed: FILE_BASIC_INFO,
) -> Result<(), NativeMetadataError> {
    use crate::kernel::MetadataField;
    for (field, expected, actual, required) in [
        (
            "creation time",
            desired.CreationTime,
            observed.CreationTime,
            !matches!(metadata.created_ns, MetadataField::Unavailable),
        ),
        (
            "access time",
            desired.LastAccessTime,
            observed.LastAccessTime,
            !matches!(metadata.accessed_ns, MetadataField::Unavailable),
        ),
        (
            "modification time",
            desired.LastWriteTime,
            observed.LastWriteTime,
            !matches!(metadata.modified_ns, MetadataField::Unavailable),
        ),
        (
            "change time",
            desired.ChangeTime,
            observed.ChangeTime,
            !matches!(metadata.changed_ns, MetadataField::Unavailable),
        ),
    ] {
        if required && expected != actual {
            return Err(NativeMetadataError(field));
        }
    }
    if matches!(metadata.windows_attributes, MetadataField::Value(_))
        && observed.FileAttributes != desired.FileAttributes
    {
        return Err(NativeMetadataError("Windows attributes"));
    }
    Ok(())
}

#[cfg(windows)]
#[cfg(any(feature = "native-mount", test))]
pub(super) fn metadata_time(
    field: crate::kernel::MetadataField<i64>,
    current: i64,
) -> Result<i64, NativeMetadataError> {
    let crate::kernel::MetadataField::Value(nanoseconds) = field else {
        return Ok(current);
    };
    if nanoseconds % 100 != 0 {
        return Err(NativeMetadataError("sub-100ns timestamp"));
    }
    Ok(unix_nanoseconds_to_filetime(nanoseconds))
}
