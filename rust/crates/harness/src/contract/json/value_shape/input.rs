use super::decoder::Decoder;
use super::*;
use crate::contract::json::lexical::Scan;
type Charge<'a> = &'a mut dyn FnMut(JsonCodecCharge) -> Result<()>;
type ReadResult = std::result::Result<Option<u8>, CodecError>;
pub(super) trait Input {
    fn next(&mut self, charge: Charge<'_>) -> ReadResult;
    fn peek(&mut self, charge: Charge<'_>) -> ReadResult;
    fn decoded_bytes(&self) -> u64 {
        0
    }
    fn valid(&self) -> bool {
        true
    }
    fn whitespace(&mut self, charge: Charge<'_>) -> ReadResult {
        loop {
            let byte = self.peek(charge)?;
            if !matches!(byte, Some(b' ' | b'\r' | b'\n' | b'\t')) {
                return Ok(byte);
            }
            self.next(charge)?;
        }
    }
}
pub(super) struct Slice<'a> {
    bytes: &'a [u8],
    position: usize,
    bounds: Option<JsonBounds>,
    pub(super) scan: Scan,
}
impl<'a> Slice<'a> {
    pub(super) fn new(bytes: &'a [u8], bounds: Option<JsonBounds>) -> Self {
        Self {
            bytes,
            position: 0,
            bounds,
            scan: Scan::default(),
        }
    }
}
impl Input for Slice<'_> {
    fn next(&mut self, charge: Charge<'_>) -> ReadResult {
        let byte = self.bytes.get(self.position).copied();
        if let Some(byte) = byte {
            if let Some(bounds) = self.bounds {
                self.scan
                    .push_inline(&[byte], bounds, &mut |event| charge(event))?;
            } else {
                charge(JsonCodecCharge::Inspect { items: 1 })?;
            }
            self.position += 1;
        }
        Ok(byte)
    }
    fn peek(&mut self, _: Charge<'_>) -> ReadResult {
        Ok(self.bytes.get(self.position).copied())
    }
    fn decoded_bytes(&self) -> u64 {
        if self.bounds.is_none() {
            self.position as u64
        } else {
            0
        }
    }
}
pub(super) struct StringInput<'a> {
    input: &'a mut dyn Input,
    decoder: Decoder,
    bytes: [u8; 4],
    position: u8,
    length: u8,
    finished: bool,
    valid: bool,
    decoded: u64,
    pub(super) inspect: bool,
}
impl<'a> StringInput<'a> {
    pub(super) fn new(input: &'a mut dyn Input) -> Self {
        Self {
            input,
            decoder: Decoder::new(),
            bytes: [0; 4],
            position: 0,
            length: 0,
            finished: false,
            valid: false,
            decoded: 0,
            inspect: false,
        }
    }
}
impl Input for StringInput<'_> {
    fn next(&mut self, charge: Charge<'_>) -> ReadResult {
        let byte = self.peek(charge)?;
        if byte.is_some() {
            if self.inspect {
                charge(JsonCodecCharge::Inspect { items: 1 })?;
            }
            self.position += 1;
        }
        Ok(byte)
    }
    fn peek(&mut self, charge: Charge<'_>) -> ReadResult {
        while self.position == self.length && !self.finished {
            let Some(byte) = self.input.next(charge)? else {
                self.finished = true;
                break;
            };
            let decoded = self.decoder.push(byte);
            if decoded.end || decoded.invalid {
                self.finished = true;
                self.valid = decoded.end && !decoded.invalid;
                break;
            }
            self.bytes = decoded.bytes;
            self.position = 0;
            self.length = decoded.len;
            self.decoded += u64::from(decoded.len);
        }
        Ok(if self.position < self.length {
            Some(self.bytes[self.position as usize])
        } else {
            None
        })
    }
    fn decoded_bytes(&self) -> u64 {
        self.decoded
    }
    fn valid(&self) -> bool {
        self.valid
    }
}
#[derive(Clone, Copy)]
pub(super) struct Token {
    bytes: [u8; 64],
    length: usize,
}
impl Token {
    pub(super) const EMPTY: Self = Self {
        bytes: [0; 64],
        length: 0,
    };
    pub(super) fn bytes(&self) -> &[u8] {
        if self.length <= self.bytes.len() {
            &self.bytes[..self.length]
        } else {
            b""
        }
    }
    pub(super) fn from_str(value: &str) -> Self {
        let mut token = Self::EMPTY;
        token.length = value.len();
        if value.len() <= token.bytes.len() {
            token.bytes[..value.len()].copy_from_slice(value.as_bytes());
        }
        token
    }
    pub(super) fn read(
        input: &mut dyn Input,
        charge: Charge<'_>,
    ) -> std::result::Result<(Self, bool), CodecError> {
        let mut string = StringInput::new(input);
        let mut token = Self::EMPTY;
        while let Some(byte) = string.next(charge)? {
            if token.length < token.bytes.len() {
                token.bytes[token.length] = byte;
            }
            token.length = token.length.saturating_add(1);
        }
        Ok((token, string.valid))
    }
}
