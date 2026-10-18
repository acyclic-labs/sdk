use super::*;
use std::cell::RefCell;
type Charge<'a> = RefCell<&'a mut dyn FnMut(JsonCodecCharge) -> Result<()>>;

#[derive(Default)]
pub(super) struct Lexical {
    pub(super) items: u64,
    pub(super) containers: u64,
    pub(super) arrays: usize,
    pub(super) depth: u32,
    pub(super) string: bool,
    escape: bool,
    atom: bool,
}
impl Lexical {
    pub(super) fn byte(
        &mut self,
        byte: u8,
        bounds: JsonBounds,
        code: Option<crate::wire::ErrorCode>,
        charge: &Charge<'_>,
    ) -> Result<bool> {
        if self.string {
            if self.escape {
                self.escape = false;
            } else if byte == b'\\' {
                self.escape = true;
            } else if byte == b'"' {
                self.string = false;
            }
            return Ok(false);
        }
        let depth = self.depth;
        let item = match byte {
            b'"' => {
                self.string = true;
                self.atom = false;
                true
            }
            b'[' | b'{' => {
                self.containers += 1;
                self.arrays += usize::from(byte == b'[');
                self.depth += 1;
                self.atom = false;
                true
            }
            b']' | b'}' => {
                self.depth = self
                    .depth
                    .checked_sub(1)
                    .ok_or_else(|| input_error(JsonInputError::Nesting, code))?;
                self.atom = false;
                false
            }
            b':' | b',' | b' ' | b'\n' | b'\r' | b'\t' => {
                self.atom = false;
                false
            }
            _ => {
                let first = !self.atom;
                self.atom = true;
                first
            }
        };
        if item {
            if self.items >= bounds.maximum_items {
                return Err(boundary("JSON exceeds declared structural bound", code));
            }
            if depth > bounds.maximum_depth {
                return Err(input_error(JsonInputError::Depth, code));
            }
            charge.borrow_mut()(JsonCodecCharge::Work { bytes: 0, items: 1 })?;
            self.items += 1;
        }
        Ok(item)
    }
}
