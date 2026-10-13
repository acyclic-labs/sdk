/// Creates `root` if it is missing and takes its exclusive lock.
fn lock_root(root: &Path, limits: LocalStreamLimits) -> Result<File, LocalStreamError> {
    std::fs::create_dir_all(root)?;
    sync_directory(root, limits.durability)?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(root.join(LOCK_FILE))?;
    lock.try_lock_exclusive().map_err(|error| {
        if acyclic_native_runtime::is_exclusive_lock_contention(&error) {
            LocalStreamError::AlreadyOpen
        } else {
            LocalStreamError::Io(error)
        }
    })?;
    Ok(lock)
}

/// The largest frame the journal holds: length prefix, commit timestamp, command
fn maximum_frame_bytes() -> usize {
    4 + FRAME_TIME_BYTES + MAX_COMMAND_BYTES + FRAME_CHECKSUM_BYTES
}

/// The bytes of the frame whose length prefix is `length_bytes`.
fn frame_bytes(length_bytes: &[u8; 4]) -> u64 {
    4 + u64::from(u32::from_le_bytes(*length_bytes)) + FRAME_CHECKSUM_BYTES as u64
}

/// The commit timestamp and command length a frame's prefix declares, if the
fn command_length(length_bytes: [u8; 4]) -> Option<usize> {
    usize::try_from(u32::from_le_bytes(length_bytes))
        .ok()
        .filter(|length| {
            (FRAME_TIME_BYTES + 1..=FRAME_TIME_BYTES + MAX_COMMAND_BYTES).contains(length)
        })
}
