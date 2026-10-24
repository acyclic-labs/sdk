//! Typed refusals encoded in place, so every claimed request gets an answer.
//!
//! A refusal is written into a fixed inline frame and never allocates. A
//! request refused for its own exhausted budget still reaches the client as a
//! typed refusal, never as a missing response that looks like a timeout.

use super::*;
use std::fmt::Write as _;

/// The inline capacity reserved for one refusal: its fixed fields, the
/// longest request identity and the longest code leave the rest to a message,
/// which is cut at a character boundary when it does not fit.
pub(crate) const REFUSAL_FRAME_BYTES: usize = 1024;

/// One control response frame: a response encoded on its funded ledger or
/// replayed from the journal, or a refusal held in its reserved inline frame.
pub(crate) struct ControlFrame {
    encoded: Vec<u8>,
    refusal: Option<RefusalFrame>,
}

impl ControlFrame {
    pub(crate) const fn encoded(encoded: Vec<u8>) -> Self {
        Self {
            encoded,
            refusal: None,
        }
    }

    pub(crate) const fn refusal(frame: RefusalFrame) -> Self {
        Self {
            encoded: Vec::new(),
            refusal: Some(frame),
        }
    }

    #[cfg(test)]
    pub(crate) const fn is_refusal(&self) -> bool {
        self.refusal.is_some()
    }
}

impl std::ops::Deref for ControlFrame {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        match &self.refusal {
            Some(frame) => frame.as_bytes(),
            None => &self.encoded,
        }
    }
}

impl PartialEq for ControlFrame {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}

impl std::fmt::Debug for ControlFrame {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&String::from_utf8_lossy(self))
    }
}

/// `{"version":2,"requestId":…,"ok":false,"code":…,"error":…}` in place.
pub(crate) struct RefusalFrame {
    bytes: [u8; REFUSAL_FRAME_BYTES],
    length: usize,
}

/// Room kept for the closing `"}` while the message is written.
const CLOSING: &[u8] = b"\"}";

impl RefusalFrame {
    pub(crate) fn new(request_id: Option<&control_protocol::RequestId>, error: &Error) -> Self {
        let mut frame = Self {
            bytes: [0; REFUSAL_FRAME_BYTES],
            length: 0,
        };
        frame.raw(br#"{"version":2,"#);
        if let Some(request_id) = request_id {
            // Request identities are validated ASCII identifiers.
            frame.raw(br#""requestId":""#);
            frame.raw(request_id.as_str().as_bytes());
            frame.raw(br#"","#);
        }
        frame.raw(br#""ok":false,"code":""#);
        frame.raw(error.kind().as_bytes());
        frame.raw(br#"","error":""#);
        // A full frame ends the message early; the frame stays valid JSON.
        let _ = write!(Escaped(&mut frame), "{error}");
        frame.raw(CLOSING);
        frame
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        self.bytes.get(..self.length).unwrap_or_default()
    }

    /// Appends `bytes` whole, or nothing when they would not fit.
    fn raw(&mut self, bytes: &[u8]) -> bool {
        let end = self.length.saturating_add(bytes.len());
        match self.bytes.get_mut(self.length..end) {
            Some(slot) => {
                slot.copy_from_slice(bytes);
                self.length = end;
                true
            }
            None => false,
        }
    }
}

/// Writes a JSON string body, stopping before the closing bytes' room.
struct Escaped<'a>(&'a mut RefusalFrame);

impl std::fmt::Write for Escaped<'_> {
    fn write_str(&mut self, text: &str) -> std::fmt::Result {
        for character in text.chars() {
            let mut buffer = [0_u8; 6];
            let encoded: &[u8] = match character {
                '"' => br#"\""#,
                '\\' => br"\\",
                character if u32::from(character) < 0x20 => {
                    const HEX: &[u8; 16] = b"0123456789abcdef";
                    let value = u32::from(character) as usize;
                    buffer = [
                        b'\\',
                        b'u',
                        b'0',
                        b'0',
                        HEX.get(value >> 4).copied().unwrap_or(b'0'),
                        HEX.get(value & 15).copied().unwrap_or(b'0'),
                    ];
                    &buffer
                }
                character => character.encode_utf8(&mut buffer).as_bytes(),
            };
            let room = REFUSAL_FRAME_BYTES - CLOSING.len();
            if self.0.length.saturating_add(encoded.len()) > room || !self.0.raw(encoded) {
                return Err(std::fmt::Error);
            }
        }
        Ok(())
    }
}
