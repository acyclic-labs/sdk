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
record!(ReadFileInput, self, self.path.capacity());
record!(
    ReadFileResult,
    self,
    self.file.logical_owned_bytes()?,
    self.text.capacity()
);
record!(
    WriteFileInput,
    self,
    self.path.capacity(),
    self.text.capacity(),
    self.media_type.as_ref().map_or(0, String::capacity),
    self.display_name.as_ref().map_or(0, String::capacity)
);
record!(
    EditFileInput,
    self,
    self.path.capacity(),
    self.old_string.capacity(),
    self.new_string.capacity()
);
record!(
    PatchFileInput,
    self,
    self.file.logical_owned_bytes()?,
    self.diff.capacity()
);
record!(FileResult, self, self.file.logical_owned_bytes()?);
