use acyclic_fs::GenerationExportManifest;
use acyclic_fs_format::model::{Lifecycle, VolumeConfig};
use acyclic_fs_format::{
    identity::{Digest, VolumeId},
    object_reference::{GenerationId, ObjectId, ObjectKind},
};

pub(super) fn manifest() -> GenerationExportManifest {
    let root = ObjectId {
        storage: None,
        kind: ObjectKind::GenerationRoot,
        digest: Digest::from_bytes([9; 32]),
    };
    GenerationExportManifest {
        volume_id: VolumeId::from_bytes([3; 16]),
        config: VolumeConfig::portable(Lifecycle::Ephemeral),
        generation_root: root,
        generation_id: GenerationId::from_root(root),
        objects: vec![
            ObjectId {
                storage: None,
                kind: ObjectKind::BlobChunk,
                digest: Digest::from_bytes([1; 32]),
            },
            root,
        ],
        file_count: 1,
    }
}
