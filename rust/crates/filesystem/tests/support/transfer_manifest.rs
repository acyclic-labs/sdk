use acyclic_fs::{GenerationExportManifest, Lifecycle, VolumeConfig, object_digest};
use acyclic_fs_format::{
    identity::VolumeId,
    object_reference::{GenerationId, ObjectId, ObjectKind},
};

pub(super) fn fixture() -> (GenerationExportManifest, [acyclic_stream::OwnedBytes; 2]) {
    let chunk = acyclic_stream::OwnedBytes::from_static(b"transfer-chunk");
    let root_body = acyclic_stream::OwnedBytes::from_static(b"transfer-root");
    let chunk_id = ObjectId {
        storage: None,
        kind: ObjectKind::BlobChunk,
        digest: object_digest(&chunk),
    };
    let root_id = ObjectId {
        storage: None,
        kind: ObjectKind::GenerationRoot,
        digest: object_digest(&root_body),
    };
    (
        GenerationExportManifest {
            volume_id: VolumeId::from_bytes([7; 16]),
            config: VolumeConfig::portable(Lifecycle::Ephemeral),
            generation_root: root_id,
            generation_id: GenerationId::from_root(root_id),
            objects: vec![chunk_id, root_id],
            file_count: 1,
        },
        [chunk, root_body],
    )
}
