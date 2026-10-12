#[derive(Clone, Copy)]
enum Escape {
    Plain,
    Slash,
    Hex(u8, u16),
    LowSlash(u16),
    LowU(u16),
    LowHex(u16, u8, u16),
}
#[derive(Clone, Copy)]
pub(super) struct Decoder {
    escape: Escape,
    utf8_left: u8,
    utf8_low: u8,
    utf8_high: u8,
}
#[derive(Clone, Copy)]
pub(super) struct Decoded {
    pub(super) bytes: [u8; 4],
    pub(super) len: u8,
    pub(super) end: bool,
    pub(super) invalid: bool,
}
impl Decoded {
    const fn empty() -> Self {
        Self {
            bytes: [0; 4],
            len: 0,
            end: false,
            invalid: false,
        }
    }
    fn byte(byte: u8) -> Self {
        Self {
            bytes: [byte, 0, 0, 0],
            len: 1,
            ..Self::empty()
        }
    }
    fn scalar(scalar: u32) -> Self {
        let mut decoded = Self::empty();
        if let Some(character) = char::from_u32(scalar) {
            decoded.len = character.encode_utf8(&mut decoded.bytes).len() as u8;
        } else {
            decoded.invalid = true;
        }
        decoded
    }
}
impl Decoder {
    pub(super) const fn new() -> Self {
        Self {
            escape: Escape::Plain,
            utf8_left: 0,
            utf8_low: 0x80,
            utf8_high: 0xbf,
        }
    }
    fn hex(byte: u8) -> Option<u16> {
        match byte {
            b'0'..=b'9' => Some((byte - b'0') as u16),
            b'a'..=b'f' => Some((byte - b'a' + 10) as u16),
            b'A'..=b'F' => Some((byte - b'A' + 10) as u16),
            _ => None,
        }
    }
    pub(super) fn push(&mut self, byte: u8) -> Decoded {
        let invalid = || Decoded {
            invalid: true,
            ..Decoded::empty()
        };
        match self.escape {
            Escape::Plain => {
                if self.utf8_left != 0 {
                    if !(self.utf8_low..=self.utf8_high).contains(&byte) {
                        return invalid();
                    }
                    self.utf8_left -= 1;
                    self.utf8_low = 0x80;
                    self.utf8_high = 0xbf;
                    return Decoded::byte(byte);
                }
                match byte {
                    b'"' => Decoded {
                        end: true,
                        ..Decoded::empty()
                    },
                    b'\\' => {
                        self.escape = Escape::Slash;
                        Decoded::empty()
                    }
                    0..=0x1f => invalid(),
                    0x20..=0x7f => Decoded::byte(byte),
                    0xc2..=0xdf => {
                        self.utf8_left = 1;
                        Decoded::byte(byte)
                    }
                    0xe0..=0xef => {
                        self.utf8_left = 2;
                        if byte == 0xe0 {
                            self.utf8_low = 0xa0;
                        }
                        if byte == 0xed {
                            self.utf8_high = 0x9f;
                        }
                        Decoded::byte(byte)
                    }
                    0xf0..=0xf4 => {
                        self.utf8_left = 3;
                        if byte == 0xf0 {
                            self.utf8_low = 0x90;
                        }
                        if byte == 0xf4 {
                            self.utf8_high = 0x8f;
                        }
                        Decoded::byte(byte)
                    }
                    _ => invalid(),
                }
            }
            Escape::Slash => {
                self.escape = Escape::Plain;
                match byte {
                    b'"' | b'\\' | b'/' => Decoded::byte(byte),
                    b'b' => Decoded::byte(8),
                    b'f' => Decoded::byte(12),
                    b'n' => Decoded::byte(b'\n'),
                    b'r' => Decoded::byte(b'\r'),
                    b't' => Decoded::byte(b'\t'),
                    b'u' => {
                        self.escape = Escape::Hex(0, 0);
                        Decoded::empty()
                    }
                    _ => invalid(),
                }
            }
            Escape::Hex(count, value) => {
                let Some(digit) = Self::hex(byte) else {
                    self.escape = Escape::Plain;
                    return invalid();
                };
                let value = (value << 4) | digit;
                if count != 3 {
                    self.escape = Escape::Hex(count + 1, value);
                    return Decoded::empty();
                }
                self.escape = Escape::Plain;
                if (0xd800..=0xdbff).contains(&value) {
                    self.escape = Escape::LowSlash(value);
                    Decoded::empty()
                } else {
                    Decoded::scalar(value as u32)
                }
            }
            Escape::LowSlash(high) => {
                if byte != b'\\' {
                    self.escape = Escape::Plain;
                    return invalid();
                }
                self.escape = Escape::LowU(high);
                Decoded::empty()
            }
            Escape::LowU(high) => {
                if byte != b'u' {
                    self.escape = Escape::Plain;
                    return invalid();
                }
                self.escape = Escape::LowHex(high, 0, 0);
                Decoded::empty()
            }
            Escape::LowHex(high, count, value) => {
                let Some(digit) = Self::hex(byte) else {
                    self.escape = Escape::Plain;
                    return invalid();
                };
                let value = (value << 4) | digit;
                if count != 3 {
                    self.escape = Escape::LowHex(high, count + 1, value);
                    return Decoded::empty();
                }
                self.escape = Escape::Plain;
                if !(0xdc00..=0xdfff).contains(&value) {
                    return invalid();
                }
                Decoded::scalar(0x10000 + (((high as u32) - 0xd800) << 10) + value as u32 - 0xdc00)
            }
        }
    }
}
