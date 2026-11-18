//! The single original-byte counter and scratch tracker used by JSON codecs.
use crate::{
    JsonBounds, JsonInputError,
    policy::{Policy, Unfunded},
};
use std::cell::RefCell;

type Charge<'a, P> =
    RefCell<&'a mut dyn FnMut(<P as Policy>::Charge) -> Result<(), <P as Policy>::Error>>;

/// Read-only observations of an original-byte pass.
#[derive(Default)]
pub struct State {
    /// Original values and keys admitted so far.
    pub items: u64,
    /// Original object and array openings.
    pub containers: u64,
    /// Original array openings.
    pub arrays: usize,
    /// Current container depth.
    pub depth: u32,
    /// The cursor is inside a string token.
    pub string: bool,
    /// Deepest original opening when scratch accounting is enabled.
    pub maximum_depth: u32,
    escape: bool,
    atom: bool,
    string_bytes: u64,
    escaped_string: bool,
    longest_string: u64,
    longest_escaped: u64,
}
/// Admission records scratch bounds in its existing lexical pass. Encoders
/// and array indexing use the same lexer with that accounting compiled out.
#[derive(Default)]
pub struct Lexer<const TRACK_SCRATCH: bool, P: Policy = Unfunded> {
    pub(crate) state: State,
    policy: std::marker::PhantomData<fn() -> P>,
}
impl<const TRACK_SCRATCH: bool, P: Policy> Lexer<TRACK_SCRATCH, P> {
    /// Longest original token or nesting stack requiring decoder scratch.
    pub fn scratch_source_bytes(&self, labels: bool) -> u64 {
        u64::from(self.state.maximum_depth).max(if labels {
            self.state.longest_string
        } else {
            self.state.longest_escaped
        })
    }
    /// Admit one original byte and charge an item before increasing its count.
    pub fn byte(
        &mut self,
        byte: u8,
        bounds: JsonBounds,
        code: P::Context,
        charge: &Charge<'_, P>,
    ) -> Result<bool, P::Error> {
        if self.state.string {
            if TRACK_SCRATCH {
                self.state.string_bytes += 1;
            }
            if self.state.escape {
                self.state.escape = false;
            } else if byte == b'\\' {
                self.state.escape = true;
                if TRACK_SCRATCH {
                    self.state.escaped_string = true;
                }
            } else if byte == b'"' {
                self.state.string = false;
                if TRACK_SCRATCH {
                    self.state.longest_string =
                        self.state.longest_string.max(self.state.string_bytes);
                    if self.state.escaped_string {
                        self.state.longest_escaped =
                            self.state.longest_escaped.max(self.state.string_bytes);
                    }
                }
            }
            return Ok(false);
        }
        let depth = self.state.depth;
        let item = match byte {
            b'"' => {
                self.state.string = true;
                if TRACK_SCRATCH {
                    self.state.string_bytes = 0;
                    self.state.escaped_string = false;
                }
                self.state.atom = false;
                true
            }
            b'[' | b'{' => {
                self.state.containers += 1;
                self.state.arrays += usize::from(byte == b'[');
                self.state.depth += 1;
                if TRACK_SCRATCH {
                    self.state.maximum_depth = self.state.maximum_depth.max(self.state.depth);
                }
                self.state.atom = false;
                true
            }
            b']' | b'}' => {
                self.state.depth = self
                    .depth
                    .checked_sub(1)
                    .ok_or_else(|| P::input(JsonInputError::Nesting, code))?;
                self.state.atom = false;
                false
            }
            b':' | b',' | b' ' | b'\n' | b'\r' | b'\t' => {
                self.state.atom = false;
                false
            }
            _ => {
                let first = !self.state.atom;
                self.state.atom = true;
                first
            }
        };
        if item {
            if bounds
                .maximum_items
                .is_some_and(|maximum| self.state.items >= maximum)
            {
                return Err(P::boundary("JSON exceeds declared structural bound", code));
            }
            if depth > bounds.maximum_depth {
                return Err(P::input(JsonInputError::Depth, code));
            }
            charge.borrow_mut()(P::item())?;
            self.state.items = self
                .state
                .items
                .checked_add(1)
                .ok_or_else(|| P::overflow("JSON item count overflow"))?;
        }
        Ok(item)
    }
}

impl<const TRACK_SCRATCH: bool, P: Policy> std::ops::Deref for Lexer<TRACK_SCRATCH, P> {
    type Target = State;
    #[inline]
    fn deref(&self) -> &State {
        &self.state
    }
}
