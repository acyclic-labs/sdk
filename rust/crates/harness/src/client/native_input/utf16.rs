/// Console UTF-16 to UTF-8, with a high surrogate carried across reads.
/// Unpaired surrogates become U+FFFD; a Ctrl+Z unit ends the input.
#[derive(Default)]
pub(super) struct Utf16 {
    high: Option<u16>,
    pub(super) ended: bool,
}

impl Utf16 {
    /// Encode `units` into `out`, which must hold three bytes per unit plus
    /// three; returns the bytes written.
    pub(super) fn decode(&mut self, units: &[u16], out: &mut [u8]) -> usize {
        if self.ended {
            return 0;
        }
        let mut written = 0;
        let mut put = |c: char, out: &mut [u8]| {
            written += c.encode_utf8(&mut out[written..]).len();
        };
        if units.is_empty() {
            if self.high.take().is_some() {
                put(char::REPLACEMENT_CHARACTER, out);
            }
            self.ended = true;
        }
        for &unit in units {
            if unit == 0x1a {
                if self.high.take().is_some() {
                    put(char::REPLACEMENT_CHARACTER, out);
                }
                self.ended = true;
                break;
            }
            match (self.high.take(), unit) {
                (Some(high), 0xdc00..=0xdfff) => {
                    let scalar =
                        0x10000 + ((u32::from(high) - 0xd800) << 10) + (u32::from(unit) - 0xdc00);
                    put(
                        char::from_u32(scalar).unwrap_or(char::REPLACEMENT_CHARACTER),
                        out,
                    );
                }
                (high, 0xd800..=0xdbff) => {
                    if high.is_some() {
                        put(char::REPLACEMENT_CHARACTER, out);
                    }
                    self.high = Some(unit);
                }
                (high, unit) => {
                    if high.is_some() {
                        put(char::REPLACEMENT_CHARACTER, out);
                    }
                    put(
                        char::from_u32(u32::from(unit)).unwrap_or(char::REPLACEMENT_CHARACTER),
                        out,
                    );
                }
            }
        }
        written
    }
}
