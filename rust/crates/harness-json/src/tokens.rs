//! Incremental scalar syntax on the original JSON bytes.

/// A JSON number or literal with a validated prefix and private syntax state.
#[derive(Clone, Copy)]
pub struct Atom(AtomState);

#[derive(Clone, Copy)]
enum AtomState {
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
    /// Start a scalar token at its original first byte.
    pub fn first(byte: u8) -> Option<Self> {
        Some(Self(match byte {
            b'n' => AtomState::Literal(b"null", 1),
            b't' => AtomState::Literal(b"true", 1),
            b'f' => AtomState::Literal(b"false", 1),
            b'-' => AtomState::Minus,
            b'0' => AtomState::Zero,
            b'1'..=b'9' => AtomState::Integer,
            _ => return None,
        }))
    }

    /// Consume one scalar byte; absence refuses the token.
    pub fn byte(self, byte: u8) -> Option<Self> {
        use AtomState::*;
        Some(Self(match (self.0, byte) {
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
        }))
    }

    /// Whether the token can end at this original byte boundary.
    pub fn complete(self) -> bool {
        match self.0 {
            AtomState::Literal(text, position) => text.len() == position,
            AtomState::Zero
            | AtomState::Integer
            | AtomState::Fraction
            | AtomState::ExponentDigits => true,
            _ => false,
        }
    }
}

/// Escape and UTF-8 validation for one JSON string, without source ownership.
#[derive(Default)]
pub struct StringSyntax {
    escape: bool,
    hex_left: u8,
    utf8_left: u8,
    lower: u8,
    upper: u8,
}

impl StringSyntax {
    /// Consume a byte inside a JSON string; false refuses the token.
    pub fn byte(&mut self, byte: u8) -> bool {
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
            if !byte.is_ascii_hexdigit() {
                return false;
            }
            self.hex_left -= 1;
            return true;
        }
        if self.escape {
            self.escape = false;
            return match byte {
                b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't' => true,
                b'u' => {
                    self.hex_left = 4;
                    true
                }
                _ => false,
            };
        }
        match byte {
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

    /// Whether the string can end without an unfinished escape or UTF-8 scalar.
    pub fn complete(&self) -> bool {
        !self.escape && self.hex_left == 0 && self.utf8_left == 0
    }
}
