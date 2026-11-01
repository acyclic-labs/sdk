//! Incremental spans on the codec's lexical pass. The cursor owns only its
//! bounded nesting stack; strings and skipped values never become allocations.
use super::*;
use std::cell::RefCell;

#[cfg(test)]
#[path = "spans_checks.rs"]
mod checks;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SpanKind {
    Object,
    Array,
    String,
    Scalar,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum SpanEvent {
    Key {
        depth: u32,
        start: u64,
        end: u64,
    },
    Start {
        depth: u32,
        start: u64,
        kind: SpanKind,
    },
    End {
        depth: u32,
        start: u64,
        end: u64,
        kind: SpanKind,
    },
}

#[derive(Clone, Copy)]
enum Expect {
    KeyOrEnd,
    Key,
    Colon,
    ValueOrEnd,
    Value,
    CommaOrEnd,
}

#[derive(Clone, Copy)]
struct Frame {
    kind: SpanKind,
    start: u64,
    expect: Expect,
}

#[derive(Clone, Copy)]
enum Atom {
    Literal(&'static [u8], usize),
    Minus,
    Zero,
    Integer,
    Dot,
    Fraction,
    Exponent,
    ExponentSign,
    ExponentDigits,
}

impl Atom {
    fn first(byte: u8) -> Option<Self> {
        Some(match byte {
            b'n' => Self::Literal(b"null", 1),
            b't' => Self::Literal(b"true", 1),
            b'f' => Self::Literal(b"false", 1),
            b'-' => Self::Minus,
            b'0' => Self::Zero,
            b'1'..=b'9' => Self::Integer,
            _ => return None,
        })
    }

    fn byte(self, byte: u8) -> Option<Self> {
        use Atom::*;
        Some(match (self, byte) {
            (Literal(text, position), byte) if text.get(position) == Some(&byte) => {
                Literal(text, position + 1)
            }
            (Minus, b'0') => Zero,
            (Minus, b'1'..=b'9') | (Integer, b'0'..=b'9') => Integer,
            (Zero | Integer, b'.') => Dot,
            (Dot | Fraction, b'0'..=b'9') => Fraction,
            (Zero | Integer | Fraction, b'e' | b'E') => Exponent,
            (Exponent, b'+' | b'-') => ExponentSign,
            (Exponent | ExponentSign | ExponentDigits, b'0'..=b'9') => ExponentDigits,
            _ => return None,
        })
    }

    fn complete(self) -> bool {
        match self {
            Self::Literal(text, position) => text.len() == position,
            Self::Zero | Self::Integer | Self::Fraction | Self::ExponentDigits => true,
            _ => false,
        }
    }
}

#[derive(Default)]
struct StringSyntax {
    escape: bool,
    hex_left: u8,
    hex: u16,
    high_surrogate: bool,
    low_escape: u8,
    low_hex: bool,
    utf8_left: u8,
    lower: u8,
    upper: u8,
}

impl StringSyntax {
    fn byte(&mut self, byte: u8) -> bool {
        if self.utf8_left != 0 {
            if byte < self.lower || byte > self.upper {
                return false;
            }
            self.utf8_left -= 1;
            self.lower = 0x80;
            self.upper = 0xbf;
            return true;
        }
        if self.hex_left != 0 {
            let digit = match byte {
                b'0'..=b'9' => byte - b'0',
                b'a'..=b'f' => byte - b'a' + 10,
                b'A'..=b'F' => byte - b'A' + 10,
                _ => return false,
            };
            self.hex = (self.hex << 4) | u16::from(digit);
            self.hex_left -= 1;
            if self.hex_left == 0 {
                if self.low_hex {
                    if !(0xdc00..=0xdfff).contains(&self.hex) {
                        return false;
                    }
                    self.low_hex = false;
                    self.high_surrogate = false;
                } else if (0xd800..=0xdbff).contains(&self.hex) {
                    self.high_surrogate = true;
                    self.low_escape = 2;
                } else if (0xdc00..=0xdfff).contains(&self.hex) {
                    return false;
                }
            }
            return true;
        }
        if self.low_escape != 0 {
            let expected = if self.low_escape == 2 { b'\\' } else { b'u' };
            if byte != expected {
                return false;
            }
            self.low_escape -= 1;
            if self.low_escape == 0 {
                self.hex = 0;
                self.hex_left = 4;
                self.low_hex = true;
            }
            return true;
        }
        if self.escape {
            self.escape = false;
            return match byte {
                b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't' => true,
                b'u' => {
                    self.hex_left = 4;
                    self.hex = 0;
                    true
                }
                _ => false,
            };
        }
        match byte {
            0..=0x1f => false,
            b'\\' => {
                self.escape = true;
                true
            }
            0x20..=0x7f => true,
            0xc2..=0xdf => self.utf8(1, 0x80, 0xbf),
            0xe0 => self.utf8(2, 0xa0, 0xbf),
            0xe1..=0xec | 0xee..=0xef => self.utf8(2, 0x80, 0xbf),
            0xed => self.utf8(2, 0x80, 0x9f),
            0xf0 => self.utf8(3, 0x90, 0xbf),
            0xf1..=0xf3 => self.utf8(3, 0x80, 0xbf),
            0xf4 => self.utf8(3, 0x80, 0x8f),
            _ => false,
        }
    }

    fn utf8(&mut self, count: u8, lower: u8, upper: u8) -> bool {
        self.utf8_left = count;
        self.lower = lower;
        self.upper = upper;
        true
    }

    fn complete(&self) -> bool {
        !self.escape && self.hex_left == 0 && !self.high_surrogate && self.utf8_left == 0
    }
}

/// Bounded incremental framing, shared by range consumers of ordinary JSON.
/// Events are source-byte spans. Typed field/cell decoding stays on the same
/// codec after the consumer selects those spans.
#[derive(Default)]
pub(crate) struct JsonSpans {
    lexical: admission::Lexical,
    frames: arrayvec::ArrayVec<Frame, 256>,
    position: u64,
    root_started: bool,
    root_done: bool,
    token_start: u64,
    key: bool,
    atom: Option<Atom>,
    string: StringSyntax,
}

impl JsonSpans {
    /// Resume a validated immutable array at an indexed element boundary.
    /// Its producer has already validated the document and retained its bounds.
    pub fn array_at(position: u64, opening: u64) -> Self {
        let mut cursor = Self::default();
        cursor.position = position;
        cursor.root_started = true;
        cursor.lexical.depth = 1;
        cursor.frames.push(Frame {
            kind: SpanKind::Array,
            start: opening,
            expect: Expect::ValueOrEnd,
        });
        cursor
    }

    fn invalid(code: Option<crate::wire::ErrorCode>) -> Error {
        input_error(JsonInputError::Shape, code)
    }

    fn start(&mut self, kind: SpanKind, code: Option<crate::wire::ErrorCode>) -> Result<bool> {
        let key = if let Some(frame) = self.frames.last_mut() {
            match frame.expect {
                Expect::KeyOrEnd | Expect::Key if kind == SpanKind::String => {
                    frame.expect = Expect::Colon;
                    true
                }
                Expect::ValueOrEnd | Expect::Value => {
                    frame.expect = Expect::CommaOrEnd;
                    false
                }
                _ => return Err(Self::invalid(code)),
            }
        } else if !self.root_started {
            self.root_started = true;
            false
        } else {
            return Err(Self::invalid(code));
        };
        Ok(key)
    }

    fn end(
        &mut self,
        kind: SpanKind,
        start: u64,
        end: u64,
        emit: &mut dyn FnMut(SpanEvent) -> Result<bool>,
    ) -> Result<bool> {
        if self.frames.is_empty() {
            self.root_done = true;
        }
        emit(SpanEvent::End {
            depth: self.frames.len() as u32,
            kind,
            start,
            end,
        })
    }

    /// Returns consumed bytes. Admit each input buffer with `Read` before its
    /// first feed; resuming after an event reuses that admission. A yield completes
    /// the current byte and all its events before returning.
    pub fn feed(
        &mut self,
        bytes: &[u8],
        bounds: JsonBounds,
        code: Option<crate::wire::ErrorCode>,
        charge: &mut dyn FnMut(JsonCodecCharge) -> Result<()>,
        emit: &mut dyn FnMut(SpanEvent) -> Result<bool>,
    ) -> Result<usize> {
        let charge = RefCell::new(charge);
        for (index, byte) in bytes.iter().copied().enumerate() {
            if self.position >= bounds.maximum_bytes {
                return Err(boundary("JSON input exceeds declared byte bound", code));
            }
            let mut yielded = false;
            if self.lexical.string {
                if !self.string.byte(byte) {
                    return Err(Self::invalid(code));
                }
                self.lexical.byte(byte, bounds, code, &charge)?;
                self.position += 1;
                if !self.lexical.string {
                    if !self.string.complete() {
                        return Err(Self::invalid(code));
                    }
                    yielded |= if self.key {
                        emit(SpanEvent::Key {
                            depth: self.frames.len() as u32,
                            start: self.token_start,
                            end: self.position,
                        })?
                    } else {
                        self.end(SpanKind::String, self.token_start, self.position, emit)?
                    };
                }
            } else {
                let delimiter = matches!(byte, b' ' | b'\n' | b'\r' | b'\t' | b',' | b']' | b'}');
                if let Some(atom) = self.atom {
                    if delimiter {
                        if !atom.complete() {
                            return Err(Self::invalid(code));
                        }
                        self.atom = None;
                        yielded |=
                            self.end(SpanKind::Scalar, self.token_start, self.position, emit)?;
                    } else {
                        self.atom = Some(atom.byte(byte).ok_or_else(|| Self::invalid(code))?);
                        self.lexical.byte(byte, bounds, code, &charge)?;
                        self.position += 1;
                        continue;
                    }
                }
                match byte {
                    b' ' | b'\n' | b'\r' | b'\t' => {}
                    b':' => {
                        let frame = self.frames.last_mut().ok_or_else(|| Self::invalid(code))?;
                        if !matches!(frame.expect, Expect::Colon) {
                            return Err(Self::invalid(code));
                        }
                        frame.expect = Expect::Value;
                    }
                    b',' => {
                        let frame = self.frames.last_mut().ok_or_else(|| Self::invalid(code))?;
                        if !matches!(frame.expect, Expect::CommaOrEnd) {
                            return Err(Self::invalid(code));
                        }
                        frame.expect = if frame.kind == SpanKind::Object {
                            Expect::Key
                        } else {
                            Expect::Value
                        };
                    }
                    b']' | b'}' => {
                        let frame = self.frames.pop().ok_or_else(|| Self::invalid(code))?;
                        let kind = if byte == b']' {
                            SpanKind::Array
                        } else {
                            SpanKind::Object
                        };
                        if frame.kind != kind
                            || !matches!(
                                frame.expect,
                                Expect::KeyOrEnd | Expect::ValueOrEnd | Expect::CommaOrEnd
                            )
                        {
                            return Err(Self::invalid(code));
                        }
                        yielded |= self.end(kind, frame.start, self.position + 1, emit)?;
                    }
                    _ => {
                        let kind = match byte {
                            b'{' => SpanKind::Object,
                            b'[' => SpanKind::Array,
                            b'"' => SpanKind::String,
                            _ => SpanKind::Scalar,
                        };
                        self.key = self.start(kind, code)?;
                        self.token_start = self.position;
                        if !self.key {
                            yielded |= emit(SpanEvent::Start {
                                depth: self.frames.len() as u32,
                                start: self.position,
                                kind,
                            })?;
                        }
                        match kind {
                            SpanKind::Object | SpanKind::Array => {
                                if self.frames.len() as u32 > bounds.maximum_depth {
                                    return Err(input_error(JsonInputError::Depth, code));
                                }
                                self.frames
                                    .try_push(Frame {
                                        kind,
                                        start: self.position,
                                        expect: if kind == SpanKind::Object {
                                            Expect::KeyOrEnd
                                        } else {
                                            Expect::ValueOrEnd
                                        },
                                    })
                                    .map_err(|_| input_error(JsonInputError::Depth, code))?;
                            }
                            SpanKind::String => self.string = StringSyntax::default(),
                            SpanKind::Scalar => {
                                self.atom =
                                    Some(Atom::first(byte).ok_or_else(|| Self::invalid(code))?)
                            }
                        }
                    }
                }
                self.lexical.byte(byte, bounds, code, &charge)?;
                self.position += 1;
            }
            if yielded {
                return Ok(index + 1);
            }
        }
        Ok(bytes.len())
    }

    pub fn finish(
        &mut self,
        code: Option<crate::wire::ErrorCode>,
        emit: &mut dyn FnMut(SpanEvent) -> Result<bool>,
    ) -> Result<()> {
        if let Some(atom) = self.atom.take() {
            if !atom.complete() {
                return Err(Self::invalid(code));
            }
            self.end(SpanKind::Scalar, self.token_start, self.position, emit)?;
        }
        if !self.root_done || !self.frames.is_empty() || self.lexical.string {
            return Err(input_error(JsonInputError::Incomplete, code));
        }
        Ok(())
    }
}
