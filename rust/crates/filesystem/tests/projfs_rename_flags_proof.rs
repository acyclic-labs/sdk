//! Standalone Kani proof of the production ProjFS rename flag selector.
#![cfg(kani)]

// NT ABI constants supplied by the Windows SDK in the production parent.
const FILE_RENAME_REPLACE_IF_EXISTS: u32 = 0x1;
const FILE_RENAME_POSIX_SEMANTICS: u32 = 0x2;
const FILE_RENAME_IGNORE_READONLY_ATTRIBUTE: u32 = 0x40;

#[path = "../src/native_mount/projfs/rename_flags.rs"]
mod rename_flags;

#[kani::proof]
#[kani::unwind(1)]
fn readonly_replacement_preserves_base_flags_and_adds_only_the_required_attribute_flag() {
    let read_only: bool = kani::any();
    let flags = rename_flags::replacement_flags(read_only);
    assert_eq!(flags & 0x3, 0x3);
    assert_eq!(flags & 0x40 != 0, read_only);
    assert_eq!(flags & !0x43, 0);
}
