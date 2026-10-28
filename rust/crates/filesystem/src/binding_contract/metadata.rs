use crate::WorkspaceMetadata;
use crate::kernel::{FileMetadata, MetadataField};
use acyclic_fs_wire::wire::filesystem::v2 as wire;

macro_rules! optional_scalar {
    ($input:ident, $output:ident, $wire:ident, $module:ident, $scalar:ty) => {
        fn $input(value: Option<wire::$wire>) -> Result<MetadataField<$scalar>, &'static str> {
            match value.and_then(|value| value.value) {
                None | Some(wire::$module::Value::Unavailable(true)) => {
                    Ok(MetadataField::Unavailable)
                }
                Some(wire::$module::Value::Present(value)) => Ok(MetadataField::Value(value)),
                Some(wire::$module::Value::Unavailable(false)) => {
                    Err("unavailable marker must be true")
                }
            }
        }

        pub(crate) fn $output(value: Option<$scalar>) -> wire::$wire {
            wire::$wire {
                value: Some(value.map_or(
                    wire::$module::Value::Unavailable(true),
                    wire::$module::Value::Present,
                )),
            }
        }
    };
}

optional_scalar!(input_u32, output_u32, OptionalU32, optional_u32, u32);
optional_scalar!(input_u64, output_u64, OptionalU64, optional_u64, u64);
optional_scalar!(input_i64, output_i64, OptionalI64, optional_i64, i64);

/// Decodes scalar metadata, requiring opaque bodies to be supplied separately.
pub fn metadata_from_proto(value: Option<wire::Metadata>) -> Result<FileMetadata, &'static str> {
    let value = value.unwrap_or_default();
    if value.has_named_attributes || value.has_acl || value.has_security_descriptor {
        return Err("opaque metadata bodies are required separately");
    }
    Ok(FileMetadata {
        posix_mode: input_u32(value.posix_mode)?,
        posix_uid: input_u32(value.posix_uid)?,
        posix_gid: input_u32(value.posix_gid)?,
        posix_flags: input_u64(value.posix_flags)?,
        windows_attributes: input_u32(value.windows_attributes)?,
        created_ns: input_i64(value.created_ns)?,
        modified_ns: input_i64(value.modified_ns)?,
        accessed_ns: input_i64(value.accessed_ns)?,
        changed_ns: input_i64(value.changed_ns)?,
        named_attributes: MetadataField::Unavailable,
        acl: MetadataField::Unavailable,
        security_descriptor: MetadataField::Unavailable,
    })
}

/// Projects scalar metadata while retaining explicit opaque-body presence facts.
#[must_use]
pub fn metadata_to_proto(value: WorkspaceMetadata) -> wire::Metadata {
    wire::Metadata {
        posix_mode: Some(output_u32(value.posix_mode)),
        posix_uid: Some(output_u32(value.posix_uid)),
        posix_gid: Some(output_u32(value.posix_gid)),
        posix_flags: Some(output_u64(value.posix_flags)),
        windows_attributes: Some(output_u32(value.windows_attributes)),
        created_ns: Some(output_i64(value.created_ns)),
        modified_ns: Some(output_i64(value.modified_ns)),
        accessed_ns: Some(output_i64(value.accessed_ns)),
        changed_ns: Some(output_i64(value.changed_ns)),
        has_named_attributes: value.has_named_attributes,
        has_acl: value.has_acl,
        has_security_descriptor: value.has_security_descriptor,
    }
}
