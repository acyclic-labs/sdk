mod tests {
    use crate::{
        NativeRootIdentity,
        native_checkpoint::codec::{WindowsUsnCheckpoint, WindowsUsnError},
    };
    #[test]
    fn checkpoint_encoding_is_canonical_versioned_and_bounded() {
        let checkpoint = WindowsUsnCheckpoint {
            root_identity: NativeRootIdentity::from_bytes([7; 16]),
            journal_id: 19,
            next_usn: 23,
        };
        let bytes = checkpoint.to_bytes();
        assert!(matches!(
            WindowsUsnCheckpoint::from_bytes(&bytes),
            Ok(decoded) if decoded == checkpoint
        ));
        let truncated = bytes.get(..39).expect("checkpoint length");
        assert!(matches!(
            WindowsUsnCheckpoint::from_bytes(truncated),
            Err(WindowsUsnError::InvalidCheckpoint)
        ));
        for version in [0, 2, u8::MAX] {
            let mut wrong_version = bytes.get(..8).expect("checkpoint header").to_vec();
            *wrong_version.last_mut().expect("version byte") = version;
            assert!(matches!(
                WindowsUsnCheckpoint::from_bytes(&wrong_version),
                Err(WindowsUsnError::UnsupportedVersion)
            ));
        }
        let mut impossible = bytes;
        impossible
            .get_mut(32..40)
            .expect("checkpoint USN")
            .copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(matches!(
            WindowsUsnCheckpoint::from_bytes(&impossible),
            Err(WindowsUsnError::InvalidCheckpoint)
        ));
    }
}
