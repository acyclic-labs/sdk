//! Line selection keeps only the selected byte window, including long lines.
use super::*;

struct Lines {
    offset: u64,
    limit: Option<u64>,
    line: u64,
    done: bool,
}

impl Lines {
    fn finish(&mut self, unterminated: bool) {
        if self.line < self.offset && unterminated {
            self.line += 1;
        }
    }

    fn select(&mut self, bytes: &[u8]) -> std::ops::Range<usize> {
        let mut start = (self.line >= self.offset).then_some(0);
        if start.is_some() && self.limit == Some(0) {
            self.done = true;
            return 0..0;
        }
        for (index, byte) in bytes.iter().enumerate() {
            if *byte != b'\n' {
                continue;
            }
            self.line += 1;
            if self.line == self.offset && start.is_none() {
                start = Some(index + 1);
            }
            if start.is_some()
                && self
                    .limit
                    .is_some_and(|limit| self.line - self.offset >= limit)
            {
                self.done = true;
                return start.unwrap_or(index + 1)..index + 1;
            }
        }
        start.unwrap_or(bytes.len())..bytes.len()
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
    let mut window = super::window::Window::first(
        context,
        input,
        acyclic_fs::ByteRange {
            offset: 0,
            length: quantum,
        },
        work,
        io,
    )
    .await?;
    let (version, _version_hold) = super::version::encode_source(&window.source, work)?;
    let budget = super::read::encoded_text_budget(&version, &input.path, maximum, work, io)?;
    let mut hold = edit::allocation(work, std::mem::size_of::<Vec<u8>>())?;
    let mut selected = Vec::new();
    let mut selector = Lines {
        offset,
        limit,
        line: 0,
        done: false,
    };
    let mut position = 0;
    let mut start = None;
    loop {
        edit::charge(&hold, 0, window.bytes.len() as u64)?;
        let interval = selector.select(&window.bytes);
        if selector.line >= offset && start.is_none() {
            start = Some(position + interval.start as u64);
        }
        let bytes = window
            .bytes
            .get(interval)
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
        position += window.bytes.len() as u64;
        if position == window.source.byte_length() {
            selector.finish(window.bytes.last().is_some_and(|byte| *byte != b'\n'));
            if start.is_none() && selector.line >= offset {
                start = Some(position);
            }
        }
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
        ) {
            let source = lines.concat() + &tail;
            let mut selector = Lines { offset: offset as u64, limit: limit.map(|n| n as u64), line: 0, done: false };
            let mut selected = Vec::new();
            for bytes in source.as_bytes().chunks(chunk) {
                let interval = selector.select(bytes);
                selected.extend_from_slice(&bytes[interval]);
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
