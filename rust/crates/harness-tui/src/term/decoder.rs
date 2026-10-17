use std::io;

use super::{PROBE_LIMIT, Probe, ProbeReplies};
use crate::key_event::{
    InputEvent, KeyCode, KeyEvent, KeyEventKind, KeyEventState, KeyModifiers, MouseButton,
    MouseEvent, MouseEventKind,
};

#[cfg(all(any(unix, windows), not(acyclic_harness_tui_loom)))]
/// Quiet interval after which a pending lone Escape is a key, not a prefix.
pub(crate) const ESCAPE_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(30);

/// Owns framing from the first startup byte through the last input byte.
/// Replies, including late/partial multiplexer replies, never become keys.
#[derive(Default)]
pub struct Decoder {
    probe: Probe,
    text: Vec<u8>,
    alt: bool,
    paste: Option<Paste>,
    cr: bool,
    events: Vec<InputEvent>,
}

#[derive(Default)]
struct Paste {
    bytes: Vec<u8>,
    ending: usize,
    rejected: bool,
}

impl Paste {
    fn push(&mut self, byte: u8) -> bool {
        const END: &[u8] = b"\x1b[201~";
        if !self.rejected {
            self.bytes.push(byte);
        }
        self.ending = if END.get(self.ending) == Some(&byte) {
            self.ending + 1
        } else {
            usize::from(byte == 0x1b)
        };
        // A candidate terminator is not part of the payload. Hold at most the
        // limit plus six candidate bytes, then drain without retaining bytes.
        if !self.rejected && self.bytes.len().saturating_sub(self.ending) > PROBE_LIMIT {
            self.rejected = true;
            self.bytes.clear();
        }
        self.ending == END.len()
    }
}

impl Decoder {
    /// Terminal replies received so far. Capabilities can be cached at startup
    /// while this same decoder continues filtering replies for the session.
    pub fn replies(&self) -> &ProbeReplies {
        &self.probe.replies
    }

    /// Consume an arbitrary transport chunk. Drain typed events with `take_events`.
    pub fn feed(&mut self, bytes: &[u8]) -> io::Result<()> {
        let mut failure = None;
        for &byte in bytes {
            let result = self.byte(byte);
            if let Err(error) = result {
                failure.get_or_insert(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }

    // A rejected unit is consumed completely before returning its error. The
    // rest of this transport chunk is still classified and retained as events.
    fn byte(&mut self, byte: u8) -> io::Result<()> {
        if let Some(paste) = &mut self.paste {
            if paste.push(byte) {
                let mut paste = self.paste.take().ok_or(io::ErrorKind::InvalidData)?;
                if paste.rejected {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "bracketed paste exceeds input limit",
                    ));
                }
                paste.bytes.truncate(paste.bytes.len() - 6);
                let text =
                    String::from_utf8(paste.bytes).map_err(|_| io::ErrorKind::InvalidData)?;
                self.events.push(InputEvent::Paste(text));
            }
            return Ok(());
        }
        if self.probe.idle() && byte != 0x1b {
            self.input(&[byte])?;
        } else {
            self.probe.feed(&[byte]);
            self.drain_input()?;
        }
        Ok(())
    }

    #[cfg(all(any(unix, windows), not(acyclic_harness_tui_loom)))]
    /// A lone Escape awaits `ESCAPE_TIMEOUT` before `finish_input` resolves it.
    pub(crate) fn escape_pending(&self) -> bool {
        self.probe.pending_escape()
    }

    /// Take only classified input; there is no raw startup replay API.
    pub fn take_events(&mut self) -> Vec<InputEvent> {
        self.drain_events().collect()
    }

    /// Drain classified input in place, retaining the event buffer.
    pub(crate) fn drain_events(&mut self) -> std::vec::Drain<'_, InputEvent> {
        self.events.drain(..)
    }

    /// Resolve a lone legacy Escape at an input deadline. Startup detection
    /// never calls this; partial CSI/OSC/DCS/APC replies remain quarantined.
    pub fn finish_input(&mut self) -> io::Result<()> {
        self.probe.finish_input();
        self.drain_input()
    }

    fn drain_input(&mut self) -> io::Result<()> {
        let mut failure = None;
        while let Some(frame) = self.probe.input.pop_front() {
            let result = self.input(&frame);
            self.probe.recycle(frame);
            if let Err(error) = result {
                failure.get_or_insert(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }

    fn input(&mut self, frame: &[u8]) -> io::Result<()> {
        if frame == b"\x1b[200~" {
            self.text.clear();
            self.alt = false;
            self.cr = false;
            self.paste = Some(Paste::default());
            return Ok(());
        }
        if frame == b"\x1b[I" {
            self.events.push(InputEvent::Focus(true));
            return Ok(());
        }
        if frame == b"\x1b[O" {
            self.events.push(InputEvent::Focus(false));
            return Ok(());
        }
        if let Some(mouse) = mouse(frame) {
            self.events.push(InputEvent::Mouse(mouse));
            return Ok(());
        }
        if frame.starts_with(b"\x1b[") || frame.starts_with(b"\x1bO") {
            if let Some(key) = decode_key(frame, self.probe.replies.keyboard_flags.is_some()) {
                self.events.push(InputEvent::Key(key));
            }
            return Ok(());
        }
        // OSC/DCS/APC frames are terminal traffic, recognized or otherwise.
        if frame.starts_with(b"\x1b]") || frame.starts_with(b"\x1bP") || frame.starts_with(b"\x1b_")
        {
            return Ok(());
        }
        if frame == b"\x1b" {
            self.events.push(InputEvent::Key(KeyEvent::new(
                KeyCode::Esc,
                KeyModifiers::NONE,
            )));
            return Ok(());
        }
        let bytes = if let Some(bytes) = frame.strip_prefix(b"\x1b") {
            self.alt = true;
            bytes
        } else {
            frame
        };
        for &byte in bytes {
            if self.text.is_empty() && byte.is_ascii() {
                self.character(char::from(byte))?;
                continue;
            }
            self.text.push(byte);
            match std::str::from_utf8(&self.text) {
                Ok(text) => {
                    let c = text.chars().next().ok_or(io::ErrorKind::InvalidData)?;
                    self.character(c)?;
                    self.text.clear();
                }
                Err(error) if error.error_len().is_none() && self.text.len() < 4 => {}
                Err(_) => {
                    self.text.clear();
                    self.alt = false;
                    self.cr = false;
                    // The byte that exposed a malformed prefix can itself
                    // begin valid input. Do not lose that resynchronization.
                    if byte.is_ascii() {
                        self.character(char::from(byte))?;
                    } else if (0xc2..=0xf4).contains(&byte) {
                        self.text.push(byte);
                    }
                    return Err(io::ErrorKind::InvalidData.into());
                }
            }
        }
        Ok(())
    }

    fn character(&mut self, c: char) -> io::Result<()> {
        if c == '\n' && self.cr {
            self.cr = false;
            self.alt = false;
            return Ok(());
        }
        self.cr = c == '\r';
        let (code, modifiers) = match c {
            '\r' | '\n' => (KeyCode::Enter, KeyModifiers::NONE),
            '\t' => (KeyCode::Tab, KeyModifiers::NONE),
            '\0' => (KeyCode::Null, KeyModifiers::NONE),
            '\u{7f}' | '\u{8}' => (KeyCode::Backspace, KeyModifiers::NONE),
            '\u{1}'..='\u{1a}' => (
                KeyCode::Char(char::from_u32(u32::from(c) + 96).ok_or(io::ErrorKind::InvalidData)?),
                KeyModifiers::CONTROL,
            ),
            '\u{1c}'..='\u{1f}' => (
                KeyCode::Char(char::from_u32(u32::from(c) + 64).ok_or(io::ErrorKind::InvalidData)?),
                KeyModifiers::CONTROL,
            ),
            _ => (KeyCode::Char(c), KeyModifiers::NONE),
        };
        self.events.push(InputEvent::Key(KeyEvent::new(
            code,
            modifiers
                | if self.alt {
                    KeyModifiers::ALT
                } else {
                    KeyModifiers::NONE
                },
        )));
        self.alt = false;
        Ok(())
    }
}

fn key_number(code: KeyCode) -> Option<u32> {
    Some(match code {
        KeyCode::Char(c) if !c.is_control() => u32::from(c),
        KeyCode::Null => 0,
        KeyCode::Enter => 13,
        KeyCode::Tab => 9,
        KeyCode::Esc => 27,
        KeyCode::Backspace => 127,
        KeyCode::CapsLock => 57358,
        KeyCode::ScrollLock => 57359,
        KeyCode::NumLock => 57360,
        KeyCode::PrintScreen => 57361,
        KeyCode::Pause => 57362,
        KeyCode::Menu => 57363,
        KeyCode::F(n @ 13..=35) => 57363 + u32::from(n),
        _ => return None,
    })
}

fn numbered_key(number: u32) -> Option<KeyCode> {
    Some(match number {
        0 => KeyCode::Null,
        9 => KeyCode::Tab,
        13 => KeyCode::Enter,
        27 => KeyCode::Esc,
        127 => KeyCode::Backspace,
        57358 => KeyCode::CapsLock,
        57359 => KeyCode::ScrollLock,
        57360 => KeyCode::NumLock,
        57361 => KeyCode::PrintScreen,
        57362 => KeyCode::Pause,
        57363 => KeyCode::Menu,
        57376..=57398 => KeyCode::F(u8::try_from(number - 57363).ok()?),
        57344..=63743 => return None,
        _ => KeyCode::Char(char::from_u32(number).filter(|c| !c.is_control())?),
    })
}

/// Encode the representable kitty key-event domain, rejecting unsupported
/// physical-key metadata instead of silently changing the event's identity.
pub fn encode_key(key: KeyEvent) -> io::Result<Vec<u8>> {
    if key.state.contains(KeyEventState::KEYPAD) {
        return Err(io::ErrorKind::Unsupported.into());
    }
    let (code, action) = match key.code {
        KeyCode::Insert => (2, '~'),
        KeyCode::Delete => (3, '~'),
        KeyCode::PageUp => (5, '~'),
        KeyCode::PageDown => (6, '~'),
        KeyCode::Left => (1, 'D'),
        KeyCode::Right => (1, 'C'),
        KeyCode::Up => (1, 'A'),
        KeyCode::Down => (1, 'B'),
        KeyCode::Home => (1, 'H'),
        KeyCode::End => (1, 'F'),
        KeyCode::F(1) => (1, 'P'),
        KeyCode::F(2) => (1, 'Q'),
        KeyCode::F(3) => (13, '~'),
        KeyCode::F(4) => (1, 'S'),
        KeyCode::F(n @ 5..=12) => {
            let number = [15, 17, 18, 19, 20, 21, 23, 24]
                .get(usize::from(n - 5))
                .copied()
                .ok_or(io::ErrorKind::Unsupported)?;
            (number, '~')
        }
        _ => (key_number(key.code).ok_or(io::ErrorKind::Unsupported)?, 'u'),
    };
    // Reserved kitty functional code points cannot also encode literal text.
    if matches!(key.code, KeyCode::Char(c) if (57344..=63743).contains(&u32::from(c))) {
        return Err(io::ErrorKind::Unsupported.into());
    }
    let mut modifiers = 0;
    for (flag, bit) in [
        (KeyModifiers::SHIFT, 1),
        (KeyModifiers::ALT, 2),
        (KeyModifiers::CONTROL, 4),
        (KeyModifiers::SUPER, 8),
        (KeyModifiers::HYPER, 16),
        (KeyModifiers::META, 32),
    ] {
        if key.modifiers.contains(flag) {
            modifiers |= bit;
        }
    }
    if key.state.contains(KeyEventState::CAPS_LOCK) {
        modifiers |= 64;
    }
    if key.state.contains(KeyEventState::NUM_LOCK) {
        modifiers |= 128;
    }
    let kind = match key.kind {
        KeyEventKind::Press => 1,
        KeyEventKind::Repeat => 2,
        KeyEventKind::Release => 3,
    };
    Ok(format!("\x1b[{code};{}:{kind}{action}", modifiers + 1).into_bytes())
}

fn modifiers(value: u16) -> Option<(KeyModifiers, KeyEventState)> {
    let flags = value.checked_sub(1)?;
    if flags > 255 {
        return None;
    }
    let mut modifiers = KeyModifiers::NONE;
    for (bit, flag) in [
        (1, KeyModifiers::SHIFT),
        (2, KeyModifiers::ALT),
        (4, KeyModifiers::CONTROL),
        (8, KeyModifiers::SUPER),
        (16, KeyModifiers::HYPER),
        (32, KeyModifiers::META),
    ] {
        if flags & bit != 0 {
            modifiers |= flag;
        }
    }
    let mut state = KeyEventState::NONE;
    if flags & 64 != 0 {
        state |= KeyEventState::CAPS_LOCK;
    }
    if flags & 128 != 0 {
        state |= KeyEventState::NUM_LOCK;
    }
    Some((modifiers, state))
}

fn decode_key(frame: &[u8], kitty: bool) -> Option<KeyEvent> {
    let text = std::str::from_utf8(frame).ok()?;
    let body = text
        .strip_prefix("\x1b[")
        .or_else(|| text.strip_prefix("\x1bO"))?;
    let action = body.chars().last()?;
    let parameters = body.get(..body.len().checked_sub(1)?)?;
    let mut fields = parameters.split(';');
    let first = fields.next()?;
    let modifier_field = fields.next().unwrap_or("1");
    let kitty = kitty || action == 'u' || modifier_field.contains(':');
    let mut modifier = modifier_field.split(':');
    let (mut modifiers, state) = modifiers(modifier.next()?.parse().ok()?)?;
    if !kitty && modifiers.contains(KeyModifiers::SUPER) {
        modifiers.remove(KeyModifiers::SUPER);
        modifiers.insert(KeyModifiers::META);
    }
    let kind = match modifier.next().unwrap_or("1") {
        "1" => KeyEventKind::Press,
        "2" => KeyEventKind::Repeat,
        "3" => KeyEventKind::Release,
        _ => return None,
    };
    let code = match action {
        'u' => {
            if fields.next().is_some() {
                return None;
            }
            numbered_key(first.split(':').next()?.parse().ok()?)?
        }
        '~' if first == "27" => numbered_key(fields.next()?.parse().ok()?)?,
        '~' => match first {
            "1" | "7" => KeyCode::Home,
            "4" | "8" => KeyCode::End,
            "2" => KeyCode::Insert,
            "3" => KeyCode::Delete,
            "5" => KeyCode::PageUp,
            "6" => KeyCode::PageDown,
            "11" => KeyCode::F(1),
            "12" => KeyCode::F(2),
            "13" => KeyCode::F(3),
            "14" => KeyCode::F(4),
            "15" => KeyCode::F(5),
            "17" => KeyCode::F(6),
            "18" => KeyCode::F(7),
            "19" => KeyCode::F(8),
            "20" => KeyCode::F(9),
            "21" => KeyCode::F(10),
            "23" => KeyCode::F(11),
            "24" => KeyCode::F(12),
            "25" => KeyCode::F(13),
            "26" => KeyCode::F(14),
            "28" => KeyCode::F(15),
            "29" => KeyCode::F(16),
            "31" => KeyCode::F(17),
            "32" => KeyCode::F(18),
            "33" => KeyCode::F(19),
            "34" => KeyCode::F(20),
            _ => return None,
        },
        'A' => KeyCode::Up,
        'B' => KeyCode::Down,
        'C' => KeyCode::Right,
        'D' => KeyCode::Left,
        'H' => KeyCode::Home,
        'F' => KeyCode::End,
        'P' => KeyCode::F(1),
        'Q' => KeyCode::F(2),
        'R' => KeyCode::F(3),
        'S' => KeyCode::F(4),
        'Z' => {
            modifiers |= KeyModifiers::SHIFT;
            KeyCode::BackTab
        }
        _ => return None,
    };
    Some(KeyEvent {
        code,
        modifiers,
        kind,
        state,
    })
}

fn mouse(frame: &[u8]) -> Option<MouseEvent> {
    let text = std::str::from_utf8(frame).ok()?.strip_prefix("\x1b[<")?;
    let release = text.ends_with('m');
    if !release && !text.ends_with('M') {
        return None;
    }
    let mut fields = text.get(..text.len().checked_sub(1)?)?.split(';');
    let flags: u16 = fields.next()?.parse().ok()?;
    let column = fields.next()?.parse::<u16>().ok()?.checked_sub(1)?;
    let row = fields.next()?.parse::<u16>().ok()?.checked_sub(1)?;
    if fields.next().is_some() || flags > 255 {
        return None;
    }
    let mut modifiers = KeyModifiers::NONE;
    if flags & 4 != 0 {
        modifiers |= KeyModifiers::SHIFT;
    }
    if flags & 8 != 0 {
        modifiers |= KeyModifiers::ALT;
    }
    if flags & 16 != 0 {
        modifiers |= KeyModifiers::CONTROL;
    }
    let button = match flags & 3 {
        0 => MouseButton::Left,
        1 => MouseButton::Middle,
        _ => MouseButton::Right,
    };
    let kind = if flags & 64 != 0 {
        match flags & 3 {
            0 => MouseEventKind::ScrollUp,
            1 => MouseEventKind::ScrollDown,
            2 => MouseEventKind::ScrollLeft,
            _ => MouseEventKind::ScrollRight,
        }
    } else if release {
        MouseEventKind::Up(button)
    } else if flags & 32 != 0 {
        if flags & 3 == 3 {
            MouseEventKind::Moved
        } else {
            MouseEventKind::Drag(button)
        }
    } else {
        MouseEventKind::Down(button)
    };
    Some(MouseEvent {
        kind,
        column,
        row,
        modifiers,
    })
}
