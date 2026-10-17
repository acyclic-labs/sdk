//! Project-instruction identity and its single provider projection.
use super::*;
use crate::conversation::ContentIo;

/// Canonical instruction identity, derived from the authenticated source path.
/// Display labels never determine a filename or directory.
#[derive(Clone, Copy)]
pub struct InstructionSource<'a>(&'a FileRef);

impl<'a> InstructionSource<'a> {
    /// Borrows the immutable file carrying volume, source path and descriptor.
    pub fn new(file: &'a FileRef) -> Self {
        Self(file)
    }

    /// Canonical source filename.
    pub fn filename(self) -> &'a str {
        self.0.path().rsplit('/').next().unwrap_or(self.0.path())
    }

    /// Canonical volume-relative directory; a root file belongs to `.`.
    pub fn directory(self) -> &'a str {
        self.0
            .path()
            .rsplit_once('/')
            .map_or(".", |(directory, _)| directory)
    }

    /// Exact UTF-8 frame length, before reading its body.
    pub fn framed_bytes(self) -> Result<u64> {
        let overhead = "#  instructions for \n\n<INSTRUCTIONS>\n\n</INSTRUCTIONS>".len() as u64;
        self.0
            .descriptor()
            .byte_length()
            .checked_add(overhead)
            .and_then(|size| size.checked_add(self.filename().len() as u64))
            .and_then(|size| size.checked_add(self.directory().len() as u64))
            .ok_or_else(|| Error::Invalid("project instruction size overflows".into()))
    }

    /// Reserves the entire frame before any body read or allocation.
    pub fn reserve(self, remaining: &mut u64, io: Option<&ContentIo>) -> Result<u64> {
        *remaining = remaining.checked_sub(self.framed_bytes()?).ok_or_else(|| {
            crate::executor::stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "project instructions exceed render bound",
            )
        })?;
        Ok(self.0.descriptor().byte_length())
    }

    /// Frames already authenticated UTF-8 bytes, preserving them verbatim.
    pub fn render(self, bytes: &[u8], io: Option<&ContentIo>) -> Result<String> {
        if bytes.len() as u64 != self.0.descriptor().byte_length() {
            return Err(crate::executor::stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "instruction body length differs from its source",
            ));
        }
        let body = std::str::from_utf8(bytes).map_err(|_| {
            crate::executor::stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "project instructions are not UTF-8",
            )
        })?;
        crate::executor::with_history_io(io, |work| {
            crate::executor::preparation_format(
                format_args!(
                    "# {} instructions for {}\n\n<INSTRUCTIONS>\n{body}\n</INSTRUCTIONS>",
                    self.filename(),
                    self.directory()
                ),
                work,
            )
        })
    }

    pub(crate) fn same_content(self, other: &FileRef) -> bool {
        self.0.volume() == other.volume()
            && self.0.path() == other.path()
            && self.0.descriptor().sha256() == other.descriptor().sha256()
            && self.0.descriptor().byte_length() == other.descriptor().byte_length()
    }
}

pub(crate) fn project_instruction(
    file: &FileRef,
    io: Option<&PreparationIo>,
) -> Result<ModelMessage> {
    Ok(ModelMessage {
        role: ModelRole::User,
        content: ModelContent::Part(ModelContentPart::File {
            file: clone_file(file, io)?,
            policy: FileProjectionPolicy::ProjectInstructions,
        }),
    })
}

// Both discovery entrypoints call this preflight before cloning any instruction.
pub(crate) fn instruction_reservation(
    files: &[FileRef],
    maximum: u64,
    io: Option<&PreparationIo>,
) -> Result<u64> {
    let mut remaining = maximum;
    for file in files {
        let metadata = crate::executor::with_history_io(io, |work| {
            crate::executor::json::json_byte_length(file, remaining, work)
        })?;
        let overhead =
            r#"{"role":"user","content":{"kind":"file","file":,"policy":"project_instructions"}}"#
                .len() as u64
                + 1;
        remaining = remaining
            .checked_sub(metadata)
            .and_then(|size| size.checked_sub(overhead))
            .ok_or_else(|| {
                crate::executor::stock_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "instruction metadata exceeds render bound",
                )
            })?;
        InstructionSource::new(file).reserve(&mut remaining, io)?;
    }
    Ok(maximum - remaining)
}
