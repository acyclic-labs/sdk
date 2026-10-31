//! One immutable range producer for byte and ordinal projections.
use super::*;

pub(super) struct Window {
    pub source: FileReadSource,
    pub bytes: acyclic_stream::OwnedBytes,
    // The source must be dropped before the credit that owns it.
    _hold: crate::NativeCustody,
}

pub(super) fn custody(
    io: Option<&crate::executor::PreparationIo>,
    retained: u64,
) -> Result<crate::NativeCustody> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    return io.map(|io| io.take_native_custody(retained)).transpose();
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    {
        let _ = (io, retained);
        Ok(None)
    }
}

impl Window {
    pub async fn first(
        context: &ToolContext,
        input: &ReadFileInput,
        range: acyclic_fs::ByteRange,
        work: crate::NativeWork<'_>,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Self> {
        let original = input
            .version
            .as_deref()
            .map(|version| {
                super::version::decode_tool_version(Some(context), &input.path, version, work)
            })
            .transpose()?;
        let original = match original {
            Some((FileReadSource::Content(file), hold)) => {
                let range = content_range(&file, range, work)?;
                let bytes = context.read_file_range(&file, range, io).await?;
                return Ok(Self {
                    source: FileReadSource::Content(file),
                    bytes,
                    _hold: hold,
                });
            }
            original => original,
        };
        let expected = original.as_ref().and_then(|(source, _)| match source {
            FileReadSource::Authored(file) => Some(file),
            FileReadSource::Content(_) => None,
        });
        let hold;
        let (source, bytes, retained) = context
            .read_tool_path_range(
                &input.path,
                expected.map(|file| &file.generation),
                range,
                io,
            )
            .await?;
        hold = custody(io, retained)?;
        if expected.is_some_and(|expected| expected != &source) {
            return Err(executor::rejection(
                work,
                "range belongs to another source selection",
            ));
        }
        Ok(Self {
            source: FileReadSource::Authored(source),
            bytes,
            _hold: hold,
        })
    }

    pub async fn next(
        &self,
        context: &ToolContext,
        path: &str,
        range: acyclic_fs::ByteRange,
        work: crate::NativeWork<'_>,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<acyclic_stream::OwnedBytes> {
        match &self.source {
            FileReadSource::Content(file) => {
                context
                    .read_file_range(file, content_range(file, range, work)?, io)
                    .await
            }
            FileReadSource::Authored(original) => {
                let _hold;
                let (selected, bytes, retained) = context
                    .read_tool_path_range(path, Some(&original.generation), range, io)
                    .await?;
                _hold = custody(io, retained)?;
                if selected != *original {
                    return Err(executor::rejection(
                        work,
                        "range belongs to another source selection",
                    ));
                }
                Ok(bytes)
            }
        }
    }
}

fn content_range(
    file: &FileRef,
    range: acyclic_fs::ByteRange,
    work: crate::NativeWork<'_>,
) -> Result<acyclic_fs::ByteRange> {
    let remaining = file
        .descriptor()
        .byte_length()
        .checked_sub(range.offset)
        .ok_or_else(|| executor::rejection(work, "range exceeds source"))?;
    Ok(acyclic_fs::ByteRange {
        length: range.length.min(remaining),
        ..range
    })
}
