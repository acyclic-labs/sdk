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
    let before = position - line_start(source, position);
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
    let length = match tag {
        Tag::Item => {
            let marker = line.get(before..).unwrap_or_default();
            let end = marker.find([' ', '\t', '\n']).unwrap_or(marker.len());
            let padding = marker
                .get(end..)
                .unwrap_or_default()
                .bytes()
                .take_while(|byte| matches!(byte, b' ' | b'\t'))
                .count();
            before
                + end
                + if (1..=4).contains(&padding) {
                    padding
                } else {
                    usize::from(padding > 0)
                }
        }
        Tag::BlockQuote(_) => before + 1,
        _ => {
            return Container {
                end: tag.to_end(),
                opening: String::new(),
                continuation: String::new(),
            };
        }
    };
    let marker = line.get(before..length).unwrap_or_default();
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
    let mut prefix: String = context
        .iter()
        .map(|container| container.opening.as_str())
        .collect();
    if let Some(container) = context
        .iter()
        .rev()
        .find(|container| !container.continuation.is_empty())
    {
        prefix.push_str(&container.continuation);
    }
    prefix
}
