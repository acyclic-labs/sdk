use super::*;

/// Complete customer-visible facts for one path without following links.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkspaceStat {
    /// Stable path-independent identity.
    pub file_id: crate::FileId,
    /// Exact filesystem object kind.
    pub kind: FileKind,
    /// Number of namespace bindings to this identity.
    pub link_count: u64,
    /// Logical bytes for regular files and symbolic links.
    pub logical_bytes: Option<u64>,
    /// Complete scalar cross-profile metadata and explicit opaque-payload presence.
    pub metadata: WorkspaceMetadata,
}

/// Customer-visible metadata without storage object identities.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkspaceMetadata {
    /// POSIX permission/type bits.
    pub posix_mode: Option<u32>,
    /// POSIX numeric owner identity.
    pub posix_uid: Option<u32>,
    /// POSIX numeric group identity.
    pub posix_gid: Option<u32>,
    /// POSIX inode flags.
    pub posix_flags: Option<u64>,
    /// Windows file-attribute bitset.
    pub windows_attributes: Option<u32>,
    /// Creation/birth time in signed Unix-epoch nanoseconds.
    pub created_ns: Option<i64>,
    /// Last content-modification time in signed Unix-epoch nanoseconds.
    pub modified_ns: Option<i64>,
    /// Last access time in signed Unix-epoch nanoseconds.
    pub accessed_ns: Option<i64>,
    /// Last metadata-change time in signed Unix-epoch nanoseconds.
    pub changed_ns: Option<i64>,
    /// Whether authenticated named attributes exist.
    pub has_named_attributes: bool,
    /// Whether an authenticated ACL exists.
    pub has_acl: bool,
    /// Whether an authenticated Windows security descriptor exists.
    pub has_security_descriptor: bool,
}

impl WorkspaceMetadata {
    pub(crate) fn from_engine(metadata: FileMetadata) -> Self {
        Self {
            posix_mode: metadata_value(metadata.posix_mode),
            posix_uid: metadata_value(metadata.posix_uid),
            posix_gid: metadata_value(metadata.posix_gid),
            posix_flags: metadata_value(metadata.posix_flags),
            windows_attributes: metadata_value(metadata.windows_attributes),
            created_ns: metadata_value(metadata.created_ns),
            modified_ns: metadata_value(metadata.modified_ns),
            accessed_ns: metadata_value(metadata.accessed_ns),
            changed_ns: metadata_value(metadata.changed_ns),
            has_named_attributes: matches!(metadata.named_attributes, MetadataField::Value(_)),
            has_acl: matches!(metadata.acl, MetadataField::Value(_)),
            has_security_descriptor: matches!(
                metadata.security_descriptor,
                MetadataField::Value(_)
            ),
        }
    }
}

const fn metadata_value<T: Copy>(field: MetadataField<T>) -> Option<T> {
    match field {
        MetadataField::Unavailable => None,
        MetadataField::Value(value) => Some(value),
    }
}

