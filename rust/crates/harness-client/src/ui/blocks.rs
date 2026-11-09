//! CommonMark fences only. Kinds and attributes remain renderer-owned data.
use super::{Error, Result};
use pulldown_cmark::{CodeBlockKind, Event, Parser, Tag, TagEnd};
use std::{collections::BTreeMap, ops::Range};

/// Protective document bound, independent of question count or kind.
pub const MAX_DOCUMENT_BYTES: usize = 64 * 1024 * 1024;
/// Protective fence-info bound against hostile headers.
pub const MAX_HEADER_BYTES: usize = 64 * 1024;

/// One inert fenced input, including unknown future kinds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InputBlock {
    /// Renderer hint; never dispatched as a command.
    pub kind: String,
    /// Uninterpreted `key=value` tokens, in canonical key order.
    pub attributes: BTreeMap<String, String>,
    /// CommonMark's literal code-block text.
    pub body: String,
    /// Original UTF-8 source range, including the fence.
    pub range: Range<usize>,
}

/// Parse all input fences without executing content or following links.
pub fn parse(markdown: &str) -> Result<Vec<InputBlock>> {
    if markdown.len() > MAX_DOCUMENT_BYTES {
        return Err(Error::Capacity);
    }
    let mut blocks = Vec::new();
    let mut current = None;
    for (event, range) in Parser::new(markdown).into_offset_iter() {
        match event {
            Event::Start(Tag::CodeBlock(CodeBlockKind::Fenced(info))) => {
                let mut tokens = info.split_whitespace();
                if tokens.next() != Some("input") {
                    continue;
                }
                if info.len() > MAX_HEADER_BYTES {
                    return Err(Error::Capacity);
                }
                let kind = tokens.next().ok_or(Error::Invalid("input kind missing"))?;
                let mut attributes = BTreeMap::new();
                for token in tokens {
                    let (key, value) = token
                        .split_once('=')
                        .filter(|(key, _)| !key.is_empty())
                        .ok_or(Error::Invalid("expected key=value"))?;
                    if attributes
                        .insert(key.to_owned(), value.to_owned())
                        .is_some()
                    {
                        return Err(Error::Invalid("duplicate input attribute"));
                    }
                }
                current = Some(InputBlock {
                    kind: kind.to_owned(),
                    attributes,
                    body: String::new(),
                    range,
                });
            }
            Event::Text(text) => {
                if let Some(block) = &mut current {
                    block.body.push_str(&text);
                }
            }
            Event::End(TagEnd::CodeBlock) => {
                if let Some(mut block) = current.take() {
                    block.range.end = range.end;
                    blocks.push(block);
                }
            }
            _ => {}
        }
    }
    Ok(blocks)
}

/// Substitute only actual `ui:default` fences, preserving surrounding bytes.
pub(super) fn embed(markdown: &str, mut default: impl FnMut() -> Result<String>) -> Result<String> {
    if markdown.len() > MAX_DOCUMENT_BYTES {
        return Err(Error::Capacity);
    }
    let mut spans = Vec::new();
    let mut start = None;
    for (event, range) in Parser::new(markdown).into_offset_iter() {
        match event {
            Event::Start(Tag::CodeBlock(CodeBlockKind::Fenced(info))) if info == "ui:default" => {
                start = Some(range.start);
            }
            Event::End(TagEnd::CodeBlock) => {
                if let Some(start) = start.take() {
                    spans.push(start..range.end);
                }
            }
            _ => {}
        }
    }
    let mut output = String::new();
    let mut end = 0;
    let mut projected = None;
    for span in spans {
        output.push_str(
            markdown
                .get(end..span.start)
                .ok_or(Error::Invalid("fence range"))?,
        );
        let body = match &projected {
            Some(body) => body,
            None => projected.insert(default()?),
        };
        if output.len().saturating_add(body.len()) > MAX_DOCUMENT_BYTES {
            return Err(Error::Capacity);
        }
        output.push_str(body);
        end = span.end;
    }
    output.push_str(markdown.get(end..).ok_or(Error::Invalid("fence range"))?);
    if output.len() > MAX_DOCUMENT_BYTES {
        return Err(Error::Capacity);
    }
    Ok(output)
}
