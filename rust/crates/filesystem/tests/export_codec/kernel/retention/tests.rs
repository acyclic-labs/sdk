use acyclic_fs::kernel::{
    CanonicalDecodeError, decode_workspace_deleted, encode_workspace_deleted,
};
use acyclic_fs_format::identity::VolumeId;

#[test]
fn workspace_deletion_round_trip_is_canonical() -> Result<(), Box<dyn std::error::Error>> {
    let volume_id = VolumeId::from_bytes([8; 16]);
    let encoded = encode_workspace_deleted(
        volume_id,
        |_| Ok::<_, CanonicalDecodeError>(()),
        |_| CanonicalDecodeError::AllocationFailed,
    )?;
    assert_eq!(decode_workspace_deleted(&encoded, 1024)?, volume_id);
    assert!(decode_workspace_deleted(&encoded, 1).is_err());
    let mut trailing = encoded;
    trailing.push(0);
    assert!(decode_workspace_deleted(&trailing, 1024).is_err());
    Ok(())
}
