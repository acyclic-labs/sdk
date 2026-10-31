//! Line selection keeps only the selected byte window, including long lines.
use super::*;

struct Lines {
    offset: u64,
    limit: Option<u64>,
    line: u64,
    done: bool,
}

struct Selection {
    interval: std::ops::Range<usize>,
    consumed: usize,
}

impl Lines {
    fn finish(&mut self, unterminated: bool) {
        if self.line < self.offset && unterminated {
            self.line += 1;
        }
    }

    fn select(
        &mut self,
        bytes: &[u8],
        position: u64,
        mut boundary: impl FnMut(u64, u64) -> Result<bool>,
    ) -> Result<Selection> {
        let mut start = (self.line >= self.offset).then_some(0);
        if start.is_some() && self.limit == Some(0) {
            self.done = true;
            return Ok(Selection {
                interval: 0..0,
                consumed: 0,
            });
        }
        for (index, byte) in bytes.iter().enumerate() {
            if *byte != b'\n' {
                continue;
            }
            self.line += 1;
            let page_full = boundary(self.line, position + index as u64 + 1)?;
            if self.line == self.offset && start.is_none() {
                start = Some(index + 1);
            }
            if start.is_some()
                && self
                    .limit
                    .is_some_and(|limit| self.line - self.offset >= limit)
            {
                self.done = true;
                return Ok(Selection {
                    interval: start.unwrap_or(index + 1)..index + 1,
                    consumed: index + 1,
                });
            }
            if page_full {
                return Ok(Selection {
                    interval: start.unwrap_or(index + 1)..index + 1,
                    consumed: index + 1,
                });
            }
        }
        Ok(Selection {
            interval: start.unwrap_or(bytes.len())..bytes.len(),
            consumed: bytes.len(),
        })
    }
}

pub(super) async fn read(
    context: &ToolContext,
    input: &ReadFileInput,
    offset: u64,
    limit: Option<u64>,
    work: crate::NativeWork<'_>,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<(ToolResult, crate::NativeCustody)> {
    let maximum = context.effective_scope().limits().render_bytes;
    let quantum = maximum
        .min(crate::tool::output::VIEW_BYTES)
        .min(context.effective_scope().limits().file_bytes);
    if quantum == 0 {
        return Err(executor::rejection(
            work,
            "line projection requires source byte credit",
        ));
    }
    let destination = super::index::OrdinalIndex::destination(context, io)?;
    let mut window = super::window::Window::first(
        context,
        input,
        acyclic_fs::ByteRange {
            offset: 0,
            length: if destination.is_some() && offset != 0 {
                0
            } else {
                quantum
            },
        },
        work,
        io,
    )
    .await?;
    let _version_hold;
    let (version, version_hold) = super::version::encode_source(&window.source, work)?;
    _version_hold = version_hold;
    let mut index = destination
        .filter(|destination| destination.volume() != window.source.volume())
        .map(|destination| {
            super::index::OrdinalIndex::new(
                context,
                destination,
                &version,
                window.source.byte_length(),
                work,
            )
        })
        .transpose()?;
    let (line, mut position) = if let Some(index) = &mut index {
        index.seek(offset, work, io).await?
    } else {
        (0, 0)
    };
    if window.bytes.is_empty() && window.source.byte_length() != 0 {
        window.bytes = window
            .next(
                context,
                &input.path,
                acyclic_fs::ByteRange {
                    offset: position,
                    length: quantum,
                },
                work,
                io,
            )
            .await?;
    }
    let budget = super::read::encoded_text_budget(&version, &input.path, maximum, work, io)?;
    let mut hold = edit::allocation(work, std::mem::size_of::<Vec<u8>>())?;
    let mut selected = Vec::new();
    let mut selector = Lines {
        offset,
        limit,
        line,
        done: false,
    };
    let mut start = None;
    loop {
        edit::charge(&hold, 0, window.bytes.len() as u64)?;
        let mut consumed = 0;
        while consumed < window.bytes.len() || (window.bytes.is_empty() && consumed == 0) {
            let remaining = &window.bytes[consumed..];
            // One scan both selects the view and records ordinal boundaries. A
            // full index page yields to its original publication before continuing.
            let selection = selector.select(remaining, position, |ordinal, position| {
                index
                    .as_mut()
                    .map_or(Ok(false), |index| index.record(ordinal, position))
            })?;
            if selector.line >= offset && start.is_none() {
                start = Some(position + selection.interval.start as u64);
            }
            let bytes = remaining
                .get(selection.interval)
                .ok_or_else(|| executor::rejection(work, "invalid line interval"))?;
            let capacity = usize::try_from(budget.saturating_add(3)).unwrap_or(usize::MAX);
            let shown = bytes
                .get(..bytes.len().min(capacity.saturating_sub(selected.len())))
                .ok_or_else(|| executor::rejection(work, "invalid line window"))?;
            let length = selected
                .len()
                .checked_add(shown.len())
                .ok_or_else(|| executor::rejection(work, "line window size overflow"))?;
            edit::reserve_vec(&mut hold, &mut selected, length)?;
            edit::charge(&hold, shown.len() as u64, 0)?;
            selected.extend_from_slice(shown);
            position += selection.consumed as u64;
            consumed += selection.consumed;
            if let Some(index) = &mut index {
                index.publish(io).await?;
            }
            if position == window.source.byte_length() {
                let unterminated = window.bytes.last().is_some_and(|byte| *byte != b'\n');
                if unterminated && let Some(index) = &mut index {
                    index.record(selector.line + 1, position)?;
                    index.publish(io).await?;
                }
                selector.finish(unterminated);
                if start.is_none() && selector.line >= offset {
                    start = Some(position);
                }
            }
            if selector.done
                || selected.len() >= capacity
                || position == window.source.byte_length()
            {
                break;
            }
            if selection.consumed == 0 {
                return Err(executor::rejection(work, "source range made no progress"));
            }
        }
        let capacity = usize::try_from(budget.saturating_add(3)).unwrap_or(usize::MAX);
        if selector.done || selected.len() >= capacity || position == window.source.byte_length() {
            break;
        }
        if window.bytes.is_empty() {
            return Err(executor::rejection(work, "source range made no progress"));
        }
        window.bytes = acyclic_stream::OwnedBytes::new();
        let bytes = window
            .next(
                context,
                &input.path,
                acyclic_fs::ByteRange {
                    offset: position,
                    length: quantum,
                },
                work,
                io,
            )
            .await?;
        window.bytes = bytes;
    }
    let start = start.ok_or_else(|| executor::rejection(work, "line offset exceeds source"))?;
    let truncated = start + (selected.len() as u64) < window.source.byte_length();
    let text = super::read::page_text(&selected, selected.is_empty(), truncated, work)?;
    let shown = crate::tools::bounds::prefix(text, usize::try_from(budget).unwrap_or(usize::MAX));
    let end = start + shown.len() as u64;
    #[derive(Serialize)]
    struct Read<'a> {
        file: FileReadSource,
        text: &'a str,
        page: ReadPage,
    }
    let result = super::value(
        &Read {
            page: ReadPage {
                start,
                end,
                next: (end < window.source.byte_length()).then_some(end),
                offset,
            },
            file: window.source,
            text: shown,
        },
        u64::MAX,
        work,
    )?;
    Ok((ToolResult { value: result.0 }, result.1))
}

#[cfg(test)]
mod checks {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        #[test]
        fn chunk_boundaries_preserve_line_selection(
            lines in prop::collection::vec("[^\\n]{0,80}\\n", 0..30),
            tail in "[^\\n]{0,80}",
            offset in 0usize..35, limit in prop::option::of(0usize..35), chunk in 1usize..120,
            page_lines in 1u64..10,
        ) {
            let source = lines.concat() + &tail;
            let mut selector = Lines { offset: offset as u64, limit: limit.map(|n| n as u64), line: 0, done: false };
            let mut selected = Vec::new();
            let mut position = 0;
            for bytes in source.as_bytes().chunks(chunk) {
                let mut consumed = 0;
                while consumed < bytes.len() {
                    let selection = selector.select(&bytes[consumed..], position, |line, byte| {
                        assert_eq!(source.as_bytes()[byte as usize - 1], b'\n');
                        Ok(line % page_lines == 0)
                    }).unwrap();
                    selected.extend_from_slice(&bytes[consumed + selection.interval.start..consumed + selection.interval.end]);
                    consumed += selection.consumed;
                    position += selection.consumed as u64;
                    if selector.done { break; }
                    prop_assert!(selection.consumed > 0);
                }
                if selector.done { break; }
            }
            selector.finish(!tail.is_empty());
            let count = source.split_inclusive('\n').count();
            if offset <= count {
                let expected: String = source.split_inclusive('\n').skip(offset).take(limit.unwrap_or(usize::MAX)).collect();
                prop_assert_eq!(selected, expected.into_bytes());
                prop_assert!(selector.line >= offset as u64);
            } else {
                prop_assert!(selector.line < offset as u64);
            }
        }
    }
}
