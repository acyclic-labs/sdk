//! A partial generated marker block is still an unresolved merge.
#[derive(Default)]
pub(super) struct Scanner {
    prefix: [u8; 8],
    length: usize,
    separator: bool,
    found: bool,
}

impl Scanner {
    pub(super) fn new() -> Self {
        Self {
            separator: true,
            ..Self::default()
        }
    }

    pub(super) fn push(&mut self, bytes: &[u8]) -> bool {
        for &byte in bytes {
            if byte == b'\n' {
                self.found |= self.separator && self.length >= 7;
                self.length = 0;
                self.separator = true;
            } else {
                self.separator &= if self.length < 7 {
                    byte == b'='
                } else {
                    byte == b'\r'
                };
                if self.length < 8 {
                    self.prefix[self.length] = byte;
                    self.length += 1;
                    if self.length == 8 {
                        self.found |=
                            matches!(&self.prefix[..7], b"<<<<<<<" | b"|||||||" | b">>>>>>>")
                                && matches!(byte, b' ' | b'\t');
                    }
                }
            }
            if self.found {
                return true;
            }
        }
        false
    }

    pub(super) fn finish(&self) -> bool {
        self.found || (self.separator && self.length >= 7)
    }
}
