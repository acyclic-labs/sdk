use acyclic_fs_format::{
    identity::{Digest, FileId},
    object_reference::ObjectStorage,
};

pub(super) fn association(byte: u8) -> Option<ObjectStorage> {
    (byte & 1 != 0).then_some(ObjectStorage {
        locator: Digest::from_bytes([byte.wrapping_add(1); 32]),
        root_file_id: FileId::from_bytes([byte; 16]),
        origin_generation: Digest::from_bytes([byte; 32]),
    })
}
