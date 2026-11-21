//! One immutable range producer for byte and ordinal projections.
use super::*;

pub(super) struct Window<'io> {
    pub source: FileReadSource,
    pub bytes: crate::conversation::Custody,
    // The source must be dropped before the credit that owns it.
    _hold: WindowHold<'io>,
}

enum WindowHold<'io> {
    Authored {
        _hold: Option<crate::conversation::ContentHold<'io>>,
    },
    Content {
        _hold: acyclic_fs::NativeCustody,
    },
}

/// The bytes at `range` of `bytes`, for offsets carried as source `u64`s.
pub(super) fn span(bytes: &[u8], range: std::ops::Range<u64>) -> Option<&[u8]> {
    bytes.get(span_range(bytes.len(), range)?)
}

fn span_range(length: usize, range: std::ops::Range<u64>) -> Option<std::ops::Range<usize>> {
    let start = usize::try_from(range.start).ok()?;
    let end = usize::try_from(range.end).ok()?;
    (start <= end && end <= length).then_some(start..end)
}

pub(super) fn custody(
    io: Option<&crate::executor::PreparationIo>,
    retained: u64,
) -> Result<acyclic_fs::NativeCustody> {
    #[cfg(not(target_arch = "wasm32"))]
    return io.map(|io| io.take_native_custody(retained)).transpose();
    #[cfg(target_arch = "wasm32")]
    {
        let _ = (io, retained);
        Ok(None)
    }
}

impl<'io> Window<'io> {
    /// The in-memory range holding source bytes `source`, when this window,
    /// which begins at source offset `position`, already covers all of them.
    pub fn resident_range(
        &self,
        position: u64,
        source: std::ops::Range<u64>,
    ) -> Option<std::ops::Range<usize>> {
        span_range(
            self.bytes.len(),
            source.start.checked_sub(position)?..source.end.checked_sub(position)?,
        )
    }

    /// The resident bytes of source range `source`; see [`Self::resident_range`].
    pub fn resident(&self, position: u64, source: std::ops::Range<u64>) -> Option<&[u8]> {
        self.bytes.get(self.resident_range(position, source)?)
    }

    pub async fn first(
        context: &ToolContext,
        input: &ReadFileInput,
        original: super::source::ReadSource,
        range: acyclic_fs::ByteRange,
        work: acyclic_fs::NativeWork<'_>,
        io: Option<&'io crate::executor::PreparationIo>,
    ) -> Result<Self> {
        let original = match original.source {
            Some(super::version::SelectedVersion::Content(file)) => {
                let range = content_range(&file, range);
                let bytes = context.read_file_range(&file, range, io).await?;
                return Ok(Self {
                    source: FileReadSource::Content(file),
                    bytes,
                    _hold: WindowHold::Content {
                        _hold: original.custody,
                    },
                });
            }
            source => super::source::ReadSource {
                source,
                custody: original.custody,
            },
        };
        let expected = original.source.as_ref().and_then(|source| match source {
            super::version::SelectedVersion::Authored(file) => Some(file),
            super::version::SelectedVersion::Content(_) => None,
        });
        let (source, bytes, hold) = context
            .read_tool_path_range(
                &input.path,
                expected.map(|file| &file.generation),
                range,
                io,
            )
            .await?;
        if expected.is_some_and(|expected| !expected.matches_source(&source)) {
            return Err(executor::rejection(
                work,
                "range belongs to another source selection",
            ));
        }
        Ok(Self {
            source: FileReadSource::Authored(source),
            bytes,
            _hold: WindowHold::Authored { _hold: hold },
        })
    }

    pub async fn next(
        &self,
        context: &ToolContext,
        path: &str,
        range: acyclic_fs::ByteRange,
        work: acyclic_fs::NativeWork<'_>,
        io: Option<&'io crate::executor::PreparationIo>,
    ) -> Result<crate::conversation::Custody> {
        match &self.source {
            FileReadSource::Content(file) => {
                context
                    .read_file_range(file, content_range(file, range), io)
                    .await
            }
            FileReadSource::Authored(original) => {
                let (selected, bytes, _hold) = context
                    .read_tool_path_range(path, Some(&original.generation), range, io)
                    .await?;
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

fn content_range(file: &FileRef, range: acyclic_fs::ByteRange) -> acyclic_fs::ByteRange {
    let offset = range.offset.min(file.descriptor().byte_length());
    let remaining = file.descriptor().byte_length() - offset;
    acyclic_fs::ByteRange {
        offset,
        length: range.length.min(remaining),
    }
}
