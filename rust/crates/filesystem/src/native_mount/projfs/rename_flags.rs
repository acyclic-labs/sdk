use super::*;

pub(super) fn replacement_flags(read_only: bool) -> u32 {
    FILE_RENAME_REPLACE_IF_EXISTS
        | FILE_RENAME_POSIX_SEMANTICS
        | if read_only {
            FILE_RENAME_IGNORE_READONLY_ATTRIBUTE
        } else {
            0
        }
}
