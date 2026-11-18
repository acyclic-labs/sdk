use acyclic_fs::kernel::{AttributeClass, AttributeEntry, AttributeName};
use acyclic_fs_format::{
    identity::Digest,
    object_reference::{ObjectId, ObjectKind},
};

pub(crate) fn name(value: impl AsRef<[u8]>) -> Result<AttributeName, Box<dyn std::error::Error>> {
    Ok(AttributeName::new(
        AttributeClass::PosixXattr,
        value.as_ref().to_vec(),
    )?)
}

pub(crate) fn entry(
    value: impl AsRef<[u8]>,
    value_bytes: u64,
    byte: u8,
) -> Result<AttributeEntry, Box<dyn std::error::Error>> {
    Ok(AttributeEntry {
        name: name(value)?,
        value_bytes,
        value: ObjectId {
            storage: None,
            kind: ObjectKind::Blob,
            digest: Digest::from_bytes([byte; 32]),
        },
    })
}
