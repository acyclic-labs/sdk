use super::*;

pub(super) fn closed(source: &str, events: &[(Event<'_>, Range<usize>)]) -> bool {
    let Some((Event::Start(Tag::CodeBlock(kind)), opening)) = events.first() else {
        return false;
    };
    let Some(end) = events
        .iter()
        .position(|(event, _)| matches!(event, Event::End(TagEnd::CodeBlock)))
    else {
        return false;
    };
    if events
        .get(end + 1..)
        .unwrap_or_default()
        .iter()
        .any(|(event, _)| !matches!(event, Event::End(_)))
    {
        return true;
    }
    let CodeBlockKind::Fenced(_) = kind else {
        return false;
    };
    let row = source.get(opening.start..).unwrap_or_default();
    let Some((marker, _, _, _)) = code::opening(row) else {
        return false;
    };
    let opening_end = opening.start + row.find('\n').map_or(row.len(), |end| end + 1);
    let text_end = events
        .get(1..end)
        .unwrap_or_default()
        .iter()
        .filter_map(|(event, range)| matches!(event, Event::Text(_)).then_some(range.end))
        .max()
        .unwrap_or(opening_end);
    // Only the parser's consumed closing delimiter can contain fence markers
    // beyond its last code-text event. EOF indentation contains no markers.
    source
        .get(text_end..opening.end)
        .is_some_and(|tail| tail.as_bytes().contains(&marker))
}

impl Markdown {
    pub(super) fn start_fence(
        &mut self,
        source: &str,
        (base, prefix): (usize, usize),
        events: &[(Event<'_>, Range<usize>)],
        out: &mut render::Renderer,
        stack: &[Container],
    ) -> bool {
        let Some((Event::Start(tag @ Tag::CodeBlock(kind)), range)) = events.first() else {
            return false;
        };
        let code_line = events.get(1).and_then(|(event, _)| match event {
            Event::Text(text) => text.split_inclusive('\n').next(),
            _ => None,
        });
        let row = source.get(range.start..).unwrap_or_default();
        let (info, indented, marker, count, indent) = match kind {
            CodeBlockKind::Fenced(info) => {
                let Some((marker, count, indent, _)) = code::opening(row) else {
                    return false;
                };
                (info.as_ref(), false, marker, count, indent)
            }
            CodeBlockKind::Indented if row.contains('\n') => ("", true, 0, 0, 4),
            CodeBlockKind::Indented => return false,
        };
        out.event(&Event::Start(tag.clone()));
        let mut code = code::highlighter(info.split_whitespace().next().unwrap_or_default());
        if indented && let Some(line) = code_line {
            out.block_line(code::highlight(&mut code, line));
        }
        self.state = out.checkpoint();
        self.lines.append(&mut out.lines);
        let containers = context_prefix(stack);
        self.context = stack.to_vec();
        let length = row.find('\n').map_or(0, |position| position + 1);
        self.offset = if indented {
            let row_start = line_start(source, range.start);
            let length = source
                .get(row_start..)
                .unwrap_or_default()
                .find('\n')
                .map_or(0, |end| end + 1);
            base + row_start.saturating_sub(prefix) + length
        } else {
            base + range.start.saturating_sub(prefix) + length
        };
        self.set_stable_lines(self.lines.len());
        self.fence = Some(Fence {
            indented,
            pending: String::new(),
            marker,
            count,
            indent,
            opening: source
                .get(line_start(source, range.start)..range.start)
                .unwrap_or_default()
                .to_owned(),
            parser_opening: if indented {
                let container = stack
                    .iter()
                    .rev()
                    .find(|container| !container.continuation.is_empty());
                let continuation = container.map_or("", |container| {
                    container.continuation.trim_end_matches('\n')
                });
                let space = if container
                    .is_some_and(|container| matches!(container.end, TagEnd::BlockQuote(_)))
                {
                    " "
                } else {
                    ""
                };
                format!("{containers}{continuation}{space}    x\n")
            } else {
                format!(
                    "{containers}{}",
                    source
                        .get(line_start(source, range.start)..range.start + length)
                        .unwrap_or_default()
                )
            },
            code,
        });
        self.references.commit(self.offset);
        true
    }

    pub(super) fn advance_fence(&mut self) -> bool {
        let Some(mut fence) = self.fence.take() else {
            return true;
        };
        let mut out = self.state.checkpoint();
        let tail = self.source.get(self.offset..).unwrap_or_default();
        for raw in tail.split_inclusive('\n') {
            #[cfg(test)]
            if (fence.indented && !indented_blank(raw, &fence))
                || (!fence.indented && !fence.opening.is_empty())
            {
                self.parsed_bytes += fence.parser_opening.len() + fence.pending.len() + raw.len();
            }
            let decoded = fence_line(raw, &fence);
            if matches!(decoded, FenceLine::Outside) {
                out.event(&Event::End(TagEnd::CodeBlock));
                self.state = out.checkpoint();
                self.lines.extend(out.lines);
                if self.context.is_empty() {
                    self.lines.push(Line::default());
                }
                self.set_stable_lines(self.lines.len());
                return true;
            }
            if !raw.ends_with('\n') {
                break;
            }
            self.offset += raw.len();
            if matches!(decoded, FenceLine::Blank) {
                fence.pending.push_str(raw);
                continue;
            }
            if matches!(decoded, FenceLine::Close) {
                out.event(&Event::End(TagEnd::CodeBlock));
                self.state = out.checkpoint();
                self.lines.extend(out.lines);
                if self.context.is_empty() {
                    self.lines.push(Line::default());
                }
                self.set_stable_lines(self.lines.len());
                return true;
            }
            if let FenceLine::Text(line) = decoded {
                for line in line.split_inclusive('\n') {
                    out.block_line(code::highlight(&mut fence.code, line));
                }
                fence.pending.clear();
            }
        }
        self.lines.extend(out.lines);
        self.set_stable_lines(self.lines.len());
        let raw = self.source.get(self.offset..).unwrap_or_default();
        #[cfg(test)]
        if (fence.indented && !indented_blank(raw, &fence))
            || (!fence.indented && !fence.opening.is_empty())
        {
            self.parsed_bytes += fence.parser_opening.len() + fence.pending.len() + raw.len();
        }
        let mut preview = self.state.checkpoint();
        if let FenceLine::Text(content) = fence_line(raw, &fence)
            && !content.is_empty()
        {
            let mut code = fence.code.clone();
            for line in content.split_inclusive('\n') {
                preview.block_line(code::highlight(&mut code, line));
            }
        }
        preview.block_line(code::footer());
        self.lines.extend(preview.lines);
        self.fence = Some(fence);
        false
    }
}

enum FenceLine<'a> {
    Text(std::borrow::Cow<'a, str>),
    Blank,
    Close,
    Outside,
}

fn fence_line<'a>(line: &'a str, fence: &Fence) -> FenceLine<'a> {
    if fence.indented {
        if indented_blank(line, fence) {
            return FenceLine::Blank;
        }
        let source = format!("{}{}{line}", fence.parser_opening, fence.pending);
        let mut text = String::new();
        let mut inside = false;
        #[cfg(test)]
        references::PARSED_BYTES.with(|work| work.set(work.get() + source.len()));
        for event in Parser::new_ext(&source, options()) {
            match event {
                Event::Start(Tag::CodeBlock(_)) => inside = true,
                Event::Text(content) if inside => text.push_str(&content),
                Event::End(TagEnd::CodeBlock) => break,
                _ => {}
            }
        }
        return match text.strip_prefix("x\n") {
            Some("") | None => FenceLine::Outside,
            Some(text) => FenceLine::Text(text.to_owned().into()),
        };
    }
    if fence.opening.is_empty() {
        if !line.ends_with('\n')
            && line
                .trim_matches(|c: char| c.is_ascii_whitespace())
                .is_empty()
        {
            let source = format!("{}{line}", fence.parser_opening);
            #[cfg(test)]
            references::PARSED_BYTES.with(|work| work.set(work.get() + source.len()));
            let text = Parser::new_ext(&source, options())
                .filter_map(|event| match event {
                    Event::Text(text) => Some(text.to_string()),
                    _ => None,
                })
                .collect::<String>();
            return FenceLine::Text(text.into());
        }
        return if code::closing(line, fence.marker, fence.count) {
            FenceLine::Close
        } else {
            FenceLine::Text(code::dedent(line, fence.indent))
        };
    }
    let source = format!("{}{line}", fence.parser_opening);
    let mut inside = false;
    let mut text = String::new();
    #[cfg(test)]
    references::PARSED_BYTES.with(|work| work.set(work.get() + source.len()));
    for (event, range) in Parser::new_ext(&source, options()).into_offset_iter() {
        match event {
            Event::Start(Tag::CodeBlock(_)) => inside = true,
            Event::Text(content) if inside => text.push_str(&content),
            Event::End(TagEnd::CodeBlock) => {
                if source.get(range.end..).is_some_and(|rest| {
                    !rest
                        .trim_matches(|character: char| character.is_ascii_whitespace())
                        .is_empty()
                }) {
                    return FenceLine::Outside;
                }
                return if text.is_empty() && !line.is_empty() {
                    FenceLine::Close
                } else {
                    FenceLine::Text(text.into())
                };
            }
            _ => {}
        }
    }
    FenceLine::Outside
}

fn indented_blank(line: &str, fence: &Fence) -> bool {
    line.trim_matches(|c: char| c.is_ascii_whitespace())
        .is_empty()
        || (fence.parser_opening.contains('>')
            && line
                .trim_matches(|c: char| c.is_ascii_whitespace() || c == '>')
                .is_empty())
}
