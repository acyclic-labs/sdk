use super::*;

pub(super) fn context_event(
    event: &Event<'_>,
    range: &Range<usize>,
    prefix: usize,
    leaf: &mut bool,
) -> bool {
    if let Event::Start(tag) = event
        && range.start < prefix
    {
        *leaf = is_leaf(tag) && range.end <= prefix;
        return true;
    }
    if *leaf {
        if let Event::End(end) = event
            && is_leaf_end(*end)
        {
            *leaf = false;
        }
        return true;
    }
    if !matches!(event, Event::End(_)) && range.end <= prefix {
        return true;
    }
    false
}

pub(super) fn container(source: &str, position: usize, tag: &Tag<'_>) -> Container {
    if !matches!(tag, Tag::Item | Tag::BlockQuote(_) | Tag::HtmlBlock) {
        return Container {
            end: tag.to_end(),
            opening: String::new(),
            continuation: String::new(),
        };
    }
    let line = source
        .get(line_start(source, position)..)
        .unwrap_or_default();
    // Item ranges include indentation before the marker. It is part of the
    // container prefix, not the marker's padding; retaining only that padding
    // would turn the synthetic item into a paragraph at the next checkpoint.
    let before = position - line_start(source, position);
    let before = before
        + line
            .get(before..)
            .unwrap_or_default()
            .bytes()
            // With a partially consumed tab, the parser subtracts indentation
            // columns from a byte offset; an item range can include its quote.
            .take_while(|byte| {
                matches!(byte, b' ' | b'\t') || (matches!(tag, Tag::Item) && *byte == b'>')
            })
            .count();
    let parent: String = line
        .get(..before)
        .unwrap_or_default()
        .chars()
        .map(|character| {
            if character == '>' || character.is_ascii_whitespace() {
                character
            } else {
                ' '
            }
        })
        .collect();
    if matches!(tag, Tag::HtmlBlock) {
        return Container {
            end: TagEnd::HtmlBlock,
            opening: String::new(),
            continuation: format!(
                "{parent}{}\n",
                html_opening(line.get(before..).unwrap_or_default())
            ),
        };
    }
    let marker = match tag {
        Tag::Item => {
            let row = line
                .get(before..)
                .unwrap_or_default()
                .split('\n')
                .next()
                .unwrap_or_default();
            let end = row.find([' ', '\t']).unwrap_or(row.len());
            let rest = row.get(end..).unwrap_or_default();
            let column = columns(line.get(..before + end).unwrap_or_default(), 0);
            let whitespace = rest.len() - rest.trim_start_matches([' ', '\t']).len();
            let padding = columns(rest.get(..whitespace).unwrap_or_default(), column) - column;
            // Empty items always use one column, even with trailing whitespace.
            // Nonempty items use one to four columns; excess starts code content.
            // Expand tabs at their original column before replacing the marker.
            let padding = if whitespace == rest.len() || !(1..=4).contains(&padding) {
                1
            } else {
                padding
            };
            format!(
                "{}{}",
                row.get(..end).unwrap_or_default(),
                " ".repeat(padding)
            )
        }
        Tag::BlockQuote(_) => ">".to_owned(),
        _ => {
            return Container {
                end: tag.to_end(),
                opening: String::new(),
                continuation: String::new(),
            };
        }
    };
    let opening = format!(
        "{parent}{marker}{}\n",
        if matches!(tag, Tag::Item) { "x" } else { "" }
    );
    let continuation = if matches!(tag, Tag::Item) {
        format!(
            "{parent}{}\n",
            marker
                .chars()
                .map(|c| if c.is_ascii_whitespace() { c } else { ' ' })
                .collect::<String>()
        )
    } else {
        opening.clone()
    };
    Container {
        end: tag.to_end(),
        opening,
        continuation,
    }
}

fn columns(source: &str, start: usize) -> usize {
    #[cfg(test)]
    work::scan(source.len());
    source.bytes().fold(start, |column, byte| {
        column + if byte == b'\t' { 4 - column % 4 } else { 1 }
    })
}

fn html_opening(source: &str) -> &'static str {
    let source = source.trim_start_matches([' ', '\t']);
    if source.starts_with("<!--") {
        "<!--"
    } else if source.starts_with("<![CDATA[") {
        "<![CDATA["
    } else if source.starts_with("<?") {
        "<?"
    } else if source.starts_with("<!") {
        "<!A"
    } else {
        let name = source
            .strip_prefix('<')
            .unwrap_or_default()
            .split(|c: char| !c.is_ascii_alphanumeric() && c != '-')
            .next()
            .unwrap_or_default();
        if name.eq_ignore_ascii_case("script") {
            "<script>"
        } else if name.eq_ignore_ascii_case("pre") {
            "<pre>"
        } else if name.eq_ignore_ascii_case("style") {
            "<style>"
        } else if name.eq_ignore_ascii_case("textarea") {
            "<textarea>"
        }
        // Once opened, HTML kinds 6 and 7 have identical blank-line termination.
        else {
            "<div>"
        }
    }
}

pub(super) fn html_closed(source: &str, range: &Range<usize>, parents: &[Container]) -> bool {
    if source.as_bytes().get(range.end.saturating_sub(1)) != Some(&b'\n') {
        return false;
    }
    let prefix = context_prefix(parents);
    let row = parents
        .iter()
        .rev()
        .find(|parent| !parent.continuation.is_empty())
        .map_or("", |parent| parent.continuation.trim_end_matches('\n'));
    let raw = source.get(range.clone()).unwrap_or_default();
    let end = prefix.len() + row.len() + raw.len();
    let probe = format!("{prefix}{row}{raw}{row}x\n");
    #[cfg(test)]
    {
        work::copy(probe.len());
        references::PARSED_BYTES.with(|work| work.set(work.get() + probe.len()));
    }
    Parser::new_ext(&probe, options())
        .into_offset_iter()
        .any(|(event, range)| matches!(event, Event::End(TagEnd::HtmlBlock)) && range.end <= end)
}

pub(super) fn context_prefix(context: &[Container]) -> String {
    let mut prefix = String::new();
    for container in context {
        prefix.push_str(&container.opening);
        if container.end == TagEnd::Item {
            // The placeholder paragraph must end before opening a child: an
            // ordered child starting at zero (or >1) cannot interrupt it. Use
            // the container's blank prefix to keep enclosing quotes open too.
            prefix.push_str(&container.continuation);
        }
    }
    if let Some(container) = context
        .iter()
        .rev()
        .find(|container| !container.continuation.is_empty())
    {
        if container.end != TagEnd::Item {
            prefix.push_str(&container.continuation);
        }
    }
    prefix
}
