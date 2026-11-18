use acyclic_fs::kernel::{FileKind, LogicalName, NameEncoding, TreeEntry};
use acyclic_fs_format::identity::FileId;

pub(crate) fn name(value: &str) -> Result<LogicalName, Box<dyn std::error::Error>> {
    Ok(LogicalName::new(
        NameEncoding::Utf8,
        value.as_bytes().to_vec(),
    )?)
}

pub(crate) fn entry(
    value: &str,
    byte: u8,
    kind: FileKind,
) -> Result<TreeEntry, Box<dyn std::error::Error>> {
    Ok(TreeEntry {
        name: name(value)?,
        file_id: FileId::from_bytes([byte; 16]),
        kind,
    })
}

pub(crate) fn wide_entry(
    value: u16,
    kind: FileKind,
) -> Result<TreeEntry, Box<dyn std::error::Error>> {
    entry(
        &format!("wide-{value:04}"),
        u8::try_from(value & 0xff)?,
        kind,
    )
}
