use acyclic_fs::kernel::GenerationRoot;
use acyclic_fs_format::{
    identity::{Digest, FileId, VolumeId},
    object_reference::{GenerationId, ObjectId, ObjectKind, ObjectStorage},
};

pub(super) fn root() -> GenerationRoot {
    GenerationRoot {
        volume_id: VolumeId::from_bytes([1; 16]),
        root_file_id: FileId::from_bytes([2; 16]),
        file_table: ObjectId {
            storage: Some(ObjectStorage {
                locator: Digest::from_bytes([5; 32]),
                root_file_id: FileId::from_bytes([2; 16]),
                origin_generation: Digest::from_bytes([6; 32]),
            }),
            kind: ObjectKind::FileTablePage,
            digest: Digest::from_bytes([3; 32]),
        },
        parents: vec![GenerationId::new(Digest::from_bytes([4; 32]))],
        required_features: 0,
    }
}
