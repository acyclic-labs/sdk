fn frame_checksum(length: &[u8; 4], command: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"acyclic-stream-local-frame-v1\0");
    hasher.update(length);
    hasher.update(command);
    hasher.finalize().into()
}
