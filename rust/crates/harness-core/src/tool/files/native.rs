use super::*;

pub(in crate::tool) fn invalid(reason: &'static str) -> Error {
    Error::NativeBoundary {
        code: crate::wire::ErrorCode::Invalid,
        reason,
    }
}

macro_rules! record {
        ($ty:ty, $value:ident, $($bytes:expr),+ $(,)?) => {
            impl crate::filesystem::UploadRecord for $ty {
                fn diagnostic_label_bytes() -> u64 { crate::tool::TOOL_SERDE_DIAGNOSTIC_LABEL_BYTES }
                fn logical_owned_bytes(&$value) -> Result<u64> {
                    let mut bytes = 0_u64;
                    $(bytes = bytes.checked_add($bytes as u64)
                        .ok_or_else(|| invalid("file tool DTO ownership overflow"))?;)+
                    Ok(bytes)
                }
            }
        };
    }
record!(
    ReadFileInput,
    self,
    self.path.capacity(),
    self.version.as_ref().map_or(0, String::capacity),
    selection_bytes(&self.selection)
);
record!(
    DirectoryReadResult,
    self,
    self.entries.capacity() * std::mem::size_of::<DirectoryEntry>(),
    self.entries.iter().try_fold(0u64, |n, entry| {
        n.checked_add(entry.name.capacity() as u64)
            .and_then(|n| n.checked_add(entry.path.capacity() as u64))
            .and_then(|n| n.checked_add(entry.version.capacity() as u64))
            .ok_or_else(|| invalid("directory result ownership overflow"))
    })?
);
record!(
    WriteFileInput,
    self,
    self.path.capacity(),
    self.if_version.as_ref().map_or(0, String::capacity),
    self.content.capacity()
);
record!(
    EditFileInput,
    self,
    self.path.capacity(),
    self.version.as_ref().map_or(0, String::capacity),
    self.old_text.capacity(),
    self.new_text.capacity(),
    self.notebook.as_ref().map_or(0, |edit| match edit {
        NotebookEdit::Replace {
            cell,
            source,
            cell_type,
        } => cell_bytes(cell) + source.capacity() + cell_type.as_ref().map_or(0, String::capacity),
        NotebookEdit::Insert {
            cell,
            id,
            cell_type,
            source,
            ..
        } => cell_bytes(cell) + id.capacity() + cell_type.capacity() + source.capacity(),
        NotebookEdit::Delete { cell } => cell_bytes(cell),
    })
);

fn cell_bytes(cell: &NotebookCell) -> usize {
    match cell {
        NotebookCell::Index { id, .. } => id.as_ref().map_or(0, String::capacity),
        NotebookCell::Id { id } => id.capacity(),
    }
}
record!(
    PatchFileInput,
    self,
    self.file.logical_owned_bytes()?,
    self.diff.capacity()
);
record!(FileResult, self, self.file.logical_owned_bytes()?);
record!(FileReadSource, self, self.logical_owned_bytes()?);
record!(
    crate::conversation::FilePathSelection,
    self,
    self.logical_owned_bytes()?
);
record!(
    ReadFileResult,
    self,
    self.file.logical_owned_bytes()?,
    self.text.capacity(),
    selection_bytes(&self.media),
    self.outputs.capacity() * std::mem::size_of::<NotebookOutput>(),
    self.outputs.iter().try_fold(0u64, |total, output| {
        total
            .checked_add(output.file.logical_owned_bytes()?)
            .ok_or_else(|| invalid("notebook output ownership overflow"))
    })?
);

fn selection_bytes(selection: &Option<ReadSelection>) -> usize {
    match selection {
        Some(ReadSelection::Directory { after, .. }) => after.as_ref().map_or(0, String::capacity),
        _ => 0,
    }
}
