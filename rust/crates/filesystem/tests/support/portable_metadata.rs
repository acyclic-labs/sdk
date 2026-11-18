use acyclic_fs_format::{
    identity::Digest,
    metadata::{FileMetadata, MetadataField},
    object_reference::{ObjectId, ObjectKind},
};

pub(super) fn metadata() -> FileMetadata {
    FileMetadata {
        posix_mode: MetadataField::Value(0o100_644),
        posix_uid: MetadataField::Value(1000),
        posix_gid: MetadataField::Value(1000),
        posix_flags: MetadataField::Unavailable,
        windows_attributes: MetadataField::Unavailable,
        created_ns: MetadataField::Unavailable,
        modified_ns: MetadataField::Value(-1),
        accessed_ns: MetadataField::Value(0),
        changed_ns: MetadataField::Unavailable,
        named_attributes: MetadataField::Unavailable,
        acl: MetadataField::Value(ObjectId {
            storage: None,
            kind: ObjectKind::Blob,
            digest: Digest::from_bytes([3; 32]),
        }),
        security_descriptor: MetadataField::Unavailable,
    }
}
