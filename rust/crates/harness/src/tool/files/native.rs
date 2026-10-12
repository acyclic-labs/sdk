use super::*;

pub(in crate::tool) fn invalid(reason: &'static str) -> Error {
    Error::NativeBoundary {
        code: crate::wire::ErrorCode::Invalid,
        reason,
    }
}

pub(in crate::tool) fn context(
    context: &ToolContext,
    invocation: &ToolInvocation,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    work.remaining()?;
    if context.operation_id() != invocation.operation_id || context.call_id() != invocation.call_id
    {
        return Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            reason: "file/text tool requires its exact admitted call context",
        });
    }
    if !crate::contract::is_valid_component_label(&invocation.name)
        || invocation.call_id.is_empty()
        || invocation.call_id.len() as u64 > crate::COMPONENT_LABEL_MAX_BYTES as u64
        || invocation.call_id.chars().any(char::is_control)
        || invocation.call_id.contains('/')
        || invocation.call_id.contains('\\')
        || invocation.call_id == "."
        || invocation.call_id == ".."
    {
        return Err(invalid("tool call identity is invalid"));
    }
    Ok(())
}

pub(in crate::tool) fn public_path(
    path: &str,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    crate::conversation::validate_content_path(
        path,
        &mut Some(&mut crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(work)?,
        )),
    )?;
    if crate::conversation::is_internal_path(path) {
        return Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            reason: "file tools cannot access internal storage",
        });
    }
    Ok(())
}

pub(in crate::tool) fn bound<T: Serialize>(
    value: &T,
    maximum: u64,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    let io = crate::filesystem::PreparationIo::from_native_work(work)?;
    let diagnostic_bytes = crate::executor::json::sdk_json_diagnostic_bytes();
    io.retain_owned(diagnostic_bytes)?;
    let custody = io.into_native_custody(diagnostic_bytes)?;
    struct Counter<'a> {
        remaining: u64,
        work: &'a acyclic_fs::NativeMaterializationWork,
        denial: Option<Error>,
    }
    impl std::io::Write for Counter<'_> {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let Some(remaining) = self.remaining.checked_sub(bytes.len() as u64) else {
                self.denial = Some(invalid("JSON exceeds declared byte bound"));
                return Err(std::io::ErrorKind::WriteZero.into());
            };
            if let Err(cause) = self.work.charge(acyclic_fs::WorkCounters {
                bytes_encoded: bytes.len() as u64,
                ..Default::default()
            }) {
                self.denial = Some(super::super::edit::native_error(cause));
                return Err(std::io::ErrorKind::Interrupted.into());
            }
            self.remaining = remaining;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter {
        remaining: maximum,
        work,
        denial: None,
    };
    let encoded = serde_json::to_writer(&mut counter, value);
    if let Some(error) = counter.denial.take() {
        drop(encoded);
        return Err(error);
    }
    encoded.map_err(|source| Error::NativeJson {
        code: crate::wire::ErrorCode::Invalid,
        source,
        custody: Some(custody),
    })
}

pub(in crate::tool) fn summary_reference(
    file: &FileRef,
    maximum: usize,
    arguments: std::fmt::Arguments<'_>,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(Value, crate::NativeCustody)> {
    use std::fmt::Write as _;
    let mut allocation =
        super::super::edit::AlgorithmWork::native(work, std::mem::size_of::<String>())?;
    let mut text = String::new();
    allocation.reserve_string(&mut text, maximum)?;
    struct FixedText<'a> {
        text: &'a mut String,
        work: &'a super::super::edit::AlgorithmWork,
        denial: Option<Error>,
    }
    impl std::fmt::Write for FixedText<'_> {
        fn write_str(&mut self, value: &str) -> std::fmt::Result {
            if value.len() > self.text.capacity() - self.text.len() {
                self.denial = Some(invalid("file summary exceeds its preclaimed capacity"));
                return Err(std::fmt::Error);
            }
            if let Err(error) = self.work.copy(value.len()) {
                self.denial = Some(error);
                return Err(std::fmt::Error);
            }
            self.text.push_str(value);
            Ok(())
        }
    }
    let mut writer = FixedText {
        text: &mut text,
        work: &allocation,
        denial: None,
    };
    if writer.write_fmt(arguments).is_err() {
        return Err(writer
            .denial
            .take()
            .unwrap_or_else(|| invalid("file summary formatting failed")));
    }
    drop(writer);
    super::super::schema::project_reference(file, &text, Some(work))
}

macro_rules! record {
        ($ty:ty, $value:ident, $($bytes:expr),+ $(,)?) => {
            impl crate::filesystem::UploadRecord for $ty {
                fn diagnostic_label_bytes() -> u64 { crate::tool::tool_serde_diagnostic_label_bytes() }
                fn logical_owned_bytes(&$value) -> Result<u64> {
                    let mut bytes = 0_u64;
                    $(bytes = bytes.checked_add($bytes as u64)
                        .ok_or_else(|| invalid("file tool DTO ownership overflow"))?;)+
                    Ok(bytes)
                }
            }
        };
    }
record!(ReadFileInput, self, self.file.logical_owned_bytes()?);
record!(
    WriteFileInput,
    self,
    self.path.capacity(),
    self.text.capacity(),
    self.media_type.capacity(),
    self.display_name.capacity()
);
record!(
    EditFileInput,
    self,
    self.file.logical_owned_bytes()?,
    self.old_text.capacity(),
    self.new_text.capacity()
);
record!(
    PatchFileInput,
    self,
    self.file.logical_owned_bytes()?,
    self.diff.capacity()
);
record!(FileResult, self, self.file.logical_owned_bytes()?);
