//! Incremental spans on the codec's lexical pass. The cursor owns only its
//! bounded nesting stack; strings and skipped values never become allocations.
use crate::tokens::{Atom, StringSyntax};
use crate::{
    JsonBounds, JsonInputError, RECORD_MAXIMUM_DEPTH,
    lexical::Lexer,
    policy::{Policy, Unfunded},
};
use std::cell::RefCell;

#[cfg(test)]
#[path = "spans_checks.rs"]
mod checks;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Kind of one original JSON value span.
pub enum SpanKind {
    /// An object container.
    Object,
    /// An array container.
    Array,
    /// A string value.
    String,
    /// A number, boolean or null.
    Scalar,
}

#[derive(Clone, Copy, Debug)]
/// An original source boundary emitted before the cursor yields.
pub enum SpanEvent {
    /// An original object key, including its quotes.
    Key {
        /// Zero-based container depth.
        depth: u32,
        /// Original starting byte offset.
        start: u64,
        /// Exclusive original ending byte offset.
        end: u64,
    },
    /// The first byte of an original value.
    Start {
        /// Zero-based container depth.
        depth: u32,
        /// Original starting byte offset.
        start: u64,
        /// Original value kind.
        kind: SpanKind,
    },
    /// The exclusive ending byte of an original value.
    End {
        /// Zero-based container depth.
        depth: u32,
        /// Original starting byte offset.
        start: u64,
        /// Exclusive original ending byte offset.
        end: u64,
        /// Original value kind.
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

/// Bounded incremental framing, shared by range consumers of ordinary JSON.
/// Events are source-byte spans. Typed field/cell decoding stays on the same
/// codec after the consumer selects those spans.
#[derive(Default)]
pub struct JsonSpans<P: Policy = Unfunded> {
    lexical: Lexer<false, P>,
    // Container depths are zero-based, so an empty container at the accepted
    // maximum depth still needs its own frame above the root frame.
    frames: arrayvec::ArrayVec<Frame, { RECORD_MAXIMUM_DEPTH as usize + 1 }>,
    position: u64,
    root_started: bool,
    root_done: bool,
    sequence: bool,
    token_start: u64,
    key: bool,
    atom: Option<Atom>,
    string: StringSyntax,
}

impl<P: Policy> JsonSpans<P> {
    /// A sequence of complete JSON values, including JSONL, on the same lexical
    /// pass and cumulative byte/item admission as an ordinary document.
    pub fn sequence() -> Self {
        Self {
            sequence: true,
            ..Self::default()
        }
    }

    /// Resume a validated immutable array at an indexed element boundary.
    /// Its producer has already validated the document and retained its bounds.
    pub fn array_at(position: u64, opening: u64) -> Self {
        let mut cursor = Self::default();
        cursor.position = position;
        cursor.root_started = true;
        cursor.lexical.state.depth = 1;
        cursor.frames.push(Frame {
            kind: SpanKind::Array,
            start: opening,
            expect: Expect::ValueOrEnd,
        });
        cursor
    }

    fn invalid(code: P::Context) -> P::Error {
        P::input(JsonInputError::Shape, code)
    }

    fn start(&mut self, kind: SpanKind, code: P::Context) -> Result<bool, P::Error> {
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
        } else if !self.root_started || self.sequence && self.root_done {
            self.root_started = true;
            self.root_done = false;
            false
        } else {
            return Err(Self::invalid(code));
        };
        Ok(key)
    }

    fn end(
        &mut self,
        /// Original value kind.
        kind: SpanKind,
        /// Original starting byte offset.
        start: u64,
        /// Exclusive original ending byte offset.
        end: u64,
        emit: &mut dyn FnMut(SpanEvent) -> Result<bool, P::Error>,
    ) -> Result<bool, P::Error> {
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
        code: P::Context,
        charge: &mut dyn FnMut(P::Charge) -> Result<(), P::Error>,
        emit: &mut dyn FnMut(SpanEvent) -> Result<bool, P::Error>,
    ) -> Result<usize, P::Error> {
        let charge = RefCell::new(charge);
        for (index, byte) in bytes.iter().copied().enumerate() {
            if bounds
                .maximum_bytes
                .is_some_and(|maximum| self.position >= maximum)
            {
                return Err(P::boundary("JSON input exceeds declared byte bound", code));
            }
            let next_position = self
                .position
                .checked_add(1)
                .ok_or_else(|| P::boundary("JSON byte position overflow", code))?;
            let mut yielded = false;
            if self.lexical.string {
                if !self.string.byte(byte) {
                    return Err(Self::invalid(code));
                }
                self.lexical.byte(byte, bounds, code, &charge)?;
                self.position = next_position;
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
                        self.position = next_position;
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
                        yielded |= self.end(kind, frame.start, next_position, emit)?;
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
                                    return Err(P::input(JsonInputError::Depth, code));
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
                                    .map_err(|_| P::input(JsonInputError::Depth, code))?;
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
                self.position = next_position;
            }
            if yielded {
                return Ok(index + 1);
            }
        }
        Ok(bytes.len())
    }

    /// Refuse an unfinished value after the original source ends.
    pub fn finish(
        &mut self,
        code: P::Context,
        emit: &mut dyn FnMut(SpanEvent) -> Result<bool, P::Error>,
    ) -> Result<(), P::Error> {
        if let Some(atom) = self.atom.take() {
            if !atom.complete() {
                return Err(Self::invalid(code));
            }
            self.end(SpanKind::Scalar, self.token_start, self.position, emit)?;
        }
        if !(self.root_done || self.sequence && !self.root_started)
            || !self.frames.is_empty()
            || self.lexical.string
        {
            return Err(P::input(JsonInputError::Incomplete, code));
        }
        Ok(())
    }
}
