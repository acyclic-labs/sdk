//! File citations parsed independently of filesystem access.

use std::ops::Range;

mod presentation;
pub use presentation::{Excerpt, Resolution, ResolutionError};

/// Citation presentation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Kind {
    /// A path chip: `:file[path]`.
    Inline,
    /// A code excerpt: `::file[path]{attributes}`.
    Block,
}

/// Inclusive, one-based source lines.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Lines {
    /// First included line.
    pub start: usize,
    /// Last included line.
    pub end: usize,
}

/// Valid syntax. Paths are logical task-view paths, never OS paths.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Target {
    /// Path as authored, with surrounding whitespace removed.
    pub path: String,
    /// Optional inclusive line selection.
    pub lines: Option<Lines>,
    /// Optional zero-based, end-exclusive character selection.
    pub chars: Option<Range<usize>>,
    /// Requested symbol; resolution may report unsupported.
    pub symbol: Option<String>,
}

/// Invalid syntax remains ordinary literal text.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Directive {
    raw: String,
    kind: Kind,
    target: Option<Target>,
}

impl Directive {
    /// Parse one complete citation, preserving its exact serialization.
    pub fn parse(raw: &str, kind: Kind) -> Self {
        Self {
            raw: raw.to_owned(),
            kind,
            target: parse(raw, kind),
        }
    }

    /// Exact authored bytes, including invalid syntax and unknown attributes.
    pub fn raw(&self) -> &str {
        &self.raw
    }

    /// Inline chip or block excerpt.
    pub fn kind(&self) -> Kind {
        self.kind
    }

    /// Validated syntax, or `None` for literal invalid text.
    pub fn target(&self) -> Option<&Target> {
        self.target.as_ref()
    }
}

fn integer(raw: &str) -> Option<usize> {
    (!raw.is_empty() && raw.bytes().all(|byte| byte.is_ascii_digit()))
        .then(|| raw.parse().ok())
        .flatten()
}

fn lines(raw: &str, shorthand: bool) -> Option<Lines> {
    let raw = if shorthand {
        raw.strip_prefix('L')?
    } else {
        raw
    };
    let (start, end) = raw.split_once('-').map_or((raw, raw), |(a, b)| {
        (
            a,
            if shorthand {
                b.strip_prefix('L').unwrap_or(b)
            } else {
                b
            },
        )
    });
    let (start, end) = (integer(start)?, integer(end)?);
    (start > 0 && end >= start).then_some(Lines { start, end })
}

fn parse(raw: &str, kind: Kind) -> Option<Target> {
    let prefix = match kind {
        Kind::Inline => ":file[",
        Kind::Block => "::file[",
    };
    let (body, suffix) = raw.strip_prefix(prefix)?.split_once(']')?;
    if body.contains(['[', '\n', '\r']) {
        return None;
    }
    let (path, selection) = body
        .split_once('#')
        .map_or((body, None), |(a, b)| (a, Some(b)));
    let path = path.trim();
    if path.is_empty() {
        return None;
    }
    let mut target = Target {
        path: path.to_owned(),
        lines: match selection {
            Some(raw) => Some(lines(raw, true)?),
            None => None,
        },
        chars: None,
        symbol: None,
    };
    if suffix.is_empty() {
        return Some(target);
    }
    if kind != Kind::Block {
        return None;
    }
    let attrs = suffix.strip_prefix('{')?.strip_suffix('}')?;
    if attrs.contains(['{', '}']) {
        return None;
    }
    for attr in attrs
        .split(|c: char| c == ',' || c.is_ascii_whitespace())
        .filter(|part| !part.is_empty())
    {
        let Some((key, value)) = attr.split_once('=') else {
            continue;
        };
        match key {
            "lines" => target.lines = Some(lines(value, false)?),
            "chars" => {
                let (start, end) = value.split_once(':')?;
                let (start, end) = (integer(start)?, integer(end)?);
                if end < start {
                    return None;
                }
                target.chars = Some(start..end);
            }
            "symbol" => target.symbol = (!value.is_empty()).then(|| value.to_owned()),
            _ => {}
        }
    }
    Some(target)
}

/// A completed citation's position in the original text.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Located {
    /// Original UTF-8 byte range.
    pub range: Range<usize>,
    /// Parsed citation or literal invalid syntax.
    pub directive: Directive,
}

#[derive(Clone, Debug)]
struct Prefix {
    start: usize,
    matched: usize,
    block: bool,
}

#[derive(Clone, Debug)]
enum Pending {
    Body { start: usize, kind: Kind },
    Closed { start: usize },
    Attributes { start: usize },
}

/// Append-only syntax recognition. Pending bytes are visited once as they arrive;
/// completed citations are parsed once, and ordinary text is left untouched.
#[derive(Clone, Debug, Default)]
pub struct Stream {
    source: String,
    nodes: Vec<Located>,
    pending: Option<Pending>,
    prefix: Option<Prefix>,
    #[cfg(test)]
    swallow_invalid: bool,
    escaped: bool,
    #[cfg(test)]
    work: usize,
}

impl Stream {
    /// Append arbitrary UTF-8 chunks. A block's optional attributes remain
    /// provisional after `]` until the next byte or the end of the message.
    pub fn push(&mut self, chunk: &str) {
        let offset = self.source.len();
        self.source.push_str(chunk);
        for (relative, byte) in chunk.bytes().enumerate() {
            let position = offset + relative;
            #[cfg(test)]
            {
                self.work += 1;
            }
            let escaped = self.escaped;
            self.escaped = byte == b'\\' && !escaped;
            let opened = self.recognize(position, byte, escaped);
            let pending = self.pending.take();
            match pending {
                Some(Pending::Body { start, kind }) => {
                    let invalid = matches!(byte, b'[' | b'\n' | b'\r');
                    #[cfg(test)]
                    let invalid = invalid && !self.swallow_invalid;
                    if invalid {
                        // The source retains every abandoned byte. A nested
                        // prefix is already recognized when its '[' invalidates
                        // this body, so it becomes the next candidate directly.
                        self.pending = opened;
                    } else if byte == b']' {
                        if kind == Kind::Block {
                            self.pending = Some(Pending::Closed { start });
                        } else {
                            self.complete(start, position + 1, kind);
                        }
                    } else {
                        self.pending = Some(Pending::Body { start, kind });
                    }
                }
                Some(Pending::Closed { start }) => {
                    if byte == b'{' {
                        self.pending = Some(Pending::Attributes { start });
                    } else {
                        self.complete(start, position, Kind::Block);
                        self.pending = opened;
                    }
                }
                Some(Pending::Attributes { start }) => {
                    if byte == b'}' {
                        self.complete(start, position + 1, Kind::Block);
                    } else {
                        self.pending = Some(Pending::Attributes { start });
                    }
                }
                None => self.pending = opened,
            }
        }
    }

    // Prefix recognition runs beside an active body. A second `:file[` makes
    // that body invalid, but its opening colon must not be lost on recovery.
    fn recognize(&mut self, position: usize, byte: u8, escaped: bool) -> Option<Pending> {
        if let Some(Prefix {
            start,
            matched,
            block,
        }) = self.prefix.take()
        {
            if matched == 1 && byte == b':' {
                self.prefix = Some(Prefix {
                    start: if block { position - 1 } else { start },
                    matched,
                    block: true,
                });
                return None;
            }
            if b"file[".get(matched - 1) == Some(&byte) {
                if matched == 5 {
                    return Some(Pending::Body {
                        start,
                        kind: if block { Kind::Block } else { Kind::Inline },
                    });
                }
                self.prefix = Some(Prefix {
                    start,
                    matched: matched + 1,
                    block,
                });
                return None;
            }
        }
        if byte == b':' && !escaped {
            self.prefix = Some(Prefix {
                start: position,
                matched: 1,
                block: false,
            });
        }
        None
    }

    fn complete(&mut self, start: usize, end: usize, kind: Kind) {
        let raw = &self.source[start..end];
        #[cfg(test)]
        {
            self.work += raw.len();
        }
        self.nodes.push(Located {
            range: start..end,
            directive: Directive::parse(raw, kind),
        });
    }

    /// Completed nodes; any unfinished citation remains literal provisional text.
    pub fn nodes(&self) -> &[Located] {
        &self.nodes
    }

    /// Original bytes, including provisional and invalid syntax.
    pub fn source(&self) -> &str {
        &self.source
    }

    /// Finalize the message. An optional attribute list can no longer follow `]`.
    /// Incomplete prefixes, brackets and attributes remain ordinary raw text.
    pub fn finish(&mut self) {
        self.prefix = None;
        if let Some(Pending::Closed { start }) = self.pending.take() {
            self.complete(start, self.source.len(), Kind::Block);
        }
    }
}

#[cfg(test)]
#[path = "../tests/directives.rs"]
mod tests;
