/// Creates `root` if it is missing and claims it exclusively.
fn lock_root(
    root: &Path,
    limits: LocalStreamLimits,
) -> Result<acyclic_native_runtime::StoreClaim, LocalStreamError> {
    std::fs::create_dir_all(root)?;
    sync_directory(root, limits.durability)?;
    acyclic_native_runtime::StoreClaim::acquire(&root.join(LOCK_FILE)).map_err(|error| {
        if acyclic_native_runtime::is_exclusive_lock_contention(&error) {
            LocalStreamError::AlreadyOpen
        } else {
            LocalStreamError::Io(error)
        }
    })
}

/// The largest frame the journal holds: length prefix, commit timestamp, command
fn maximum_frame_bytes() -> usize {
    usize::try_from(JOURNAL_FRAME.maximum_frame_bytes()).unwrap_or(usize::MAX)
}

/// The bytes of the frame whose length prefix is `length_bytes`.
fn frame_bytes(length_bytes: &[u8; 4]) -> u64 {
    JOURNAL_FRAME.frame_bytes(u64::from(u32::from_le_bytes(*length_bytes)))
}
