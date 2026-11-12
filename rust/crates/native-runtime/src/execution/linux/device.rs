pub(super) const NULL_DEVICE: u64 = 0x103;

/// Linux device numbers and file types identify the held object, not its name.
pub(super) fn character_device(mode: u32, device: u64, expected: u64) -> bool {
    mode & 0o170_000 == 0o020_000 && device == expected
}
