use super::*;

use super::*;
use crate::filesystem::UploadRecord;

fn total(file: &FileRef, additional: usize) -> Result<u64> {
    file.logical_owned_bytes()?
        .checked_add(additional as u64)
        .ok_or_else(|| super::super::files::native::invalid("text tool DTO ownership overflow"))
}

impl UploadRecord for ReadInput {
    fn diagnostic_label_bytes() -> u64 {
        crate::tool::tool_serde_diagnostic_label_bytes()
    }
    fn logical_owned_bytes(&self) -> Result<u64> {
        total(&self.file, 0)
    }
}
impl UploadRecord for SearchInput {
    fn diagnostic_label_bytes() -> u64 {
        crate::tool::tool_serde_diagnostic_label_bytes()
    }
    fn logical_owned_bytes(&self) -> Result<u64> {
        total(&self.file, self.query.capacity())
    }
}
impl UploadRecord for ReadResult {
    fn diagnostic_label_bytes() -> u64 {
        crate::tool::tool_serde_diagnostic_label_bytes()
    }
    fn logical_owned_bytes(&self) -> Result<u64> {
        total(&self.file, self.selection.text.capacity())
    }
}
impl UploadRecord for SearchResult {
    fn diagnostic_label_bytes() -> u64 {
        crate::tool::tool_serde_diagnostic_label_bytes()
    }
    fn logical_owned_bytes(&self) -> Result<u64> {
        let positions = self
            .matches
            .matches
            .capacity()
            .checked_mul(std::mem::size_of::<TextRange>())
            .and_then(|n| n.checked_add(self.query.capacity()))
            .ok_or_else(|| {
                super::super::files::native::invalid("search result ownership overflow")
            })?;
        total(&self.file, positions)
    }
}
