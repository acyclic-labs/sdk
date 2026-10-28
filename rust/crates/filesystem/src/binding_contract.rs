//! Transport-independent scalar semantics shared by filesystem bindings.
//!
//! Runtime adapters retain their original error type, selected resource, work,
//! cancellation and native syscall scheduling.

use crate::NamedAttributeWriteMode;
use crate::kernel::{AttributeClass, FileKind};

mod metadata;
pub use metadata::{metadata_from_proto, metadata_to_proto};
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use metadata::{output_u32, output_u64};

/// Parses the public named-attribute class without widening its accepted set.
pub fn attribute_class(value: &str) -> Result<AttributeClass, &'static str> {
    match value {
        "posix-xattr" => Ok(AttributeClass::PosixXattr),
        "windows-stream" => Ok(AttributeClass::WindowsStream),
        "mac-resource-fork" => Ok(AttributeClass::MacResourceFork),
        _ => Err("unknown named-attribute class"),
    }
}

/// Returns the public spelling of one exact named-attribute class.
#[must_use]
pub const fn attribute_class_name(value: AttributeClass) -> &'static str {
    match value {
        AttributeClass::PosixXattr => "posix-xattr",
        AttributeClass::WindowsStream => "windows-stream",
        AttributeClass::MacResourceFork => "mac-resource-fork",
    }
}

/// Parses an attribute write's existing creation/replacement condition.
pub fn attribute_write_mode(value: &str) -> Result<NamedAttributeWriteMode, &'static str> {
    match value {
        "upsert" => Ok(NamedAttributeWriteMode::Upsert),
        "create" => Ok(NamedAttributeWriteMode::Create),
        "replace" => Ok(NamedAttributeWriteMode::Replace),
        _ => Err("unknown named-attribute write mode"),
    }
}

/// Parses a special kind that has no device identifier or reparse payload.
pub fn empty_special_kind(value: &str) -> Result<FileKind, &'static str> {
    match value {
        "fifo" => Ok(FileKind::Fifo),
        "socket" => Ok(FileKind::Socket),
        "mount-boundary" => Ok(FileKind::MountBoundary),
        _ => Err("unknown empty special-file kind"),
    }
}

/// Parses a special kind whose caller must supply a device identifier.
pub fn device_kind(value: &str) -> Result<FileKind, &'static str> {
    match value {
        "character-device" => Ok(FileKind::CharacterDevice),
        "block-device" => Ok(FileKind::BlockDevice),
        _ => Err("unknown device kind"),
    }
}
