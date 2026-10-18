use std::collections::{BTreeMap, VecDeque};
use std::io;
use std::time::{Duration, Instant};

use super::{
    Capabilities, ColourDepth, Environment, ImageProtocol, KeyboardProtocol, ModeState, Multiplexer,
};

/// Startup query deadline, shared by the entire batch, not by each query.
pub const PROBE_TIMEOUT: Duration = Duration::from_millis(100);
/// Bound both hostile reply strings and startup input buffering.
pub const PROBE_LIMIT: usize = 16 * 1024;
const GRAPHICS_ID: &str = "314159";
const INDEPENDENT_MODES: [u16; 5] = [2026, 2027, 1004, 2004, 1049];

fn modes() -> impl Iterator<Item = u16> {
    INDEPENDENT_MODES
        .into_iter()
        .chain(super::restore::MOUSE_PROTOCOLS)
        .chain(super::restore::MOUSE_ENCODINGS)
}

/// Polling transport; implementations must honor the supplied absolute deadline.
pub trait ProbeIo {
    /// Native initialization established bounded margins and SU/SD support.
    fn native_scroll_regions(&self) -> bool {
        false
    }

    /// Write and flush the entire batch.
    fn send(&mut self, bytes: &[u8], deadline: Instant) -> io::Result<()>;
    /// Return available bytes, or zero at EOF/deadline. Must not block past it.
    fn receive(&mut self, bytes: &mut [u8], deadline: Instant) -> io::Result<usize>;
    /// Install the initial mode snapshot before setup, when the transport owns
    /// a terminal lifecycle. Scripted transports need no lifecycle state.
    fn detected(&mut self, _caps: &Capabilities) -> io::Result<()> {
        Ok(())
    }
}

/// Validated query replies accumulated by the whole-stream decoder.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProbeReplies {
    /// Primary device attributes were received.
    pub sentinel: bool,
    /// Query-derived terminal name and version.
    pub identity: Option<String>,
    /// Reported private modes, including explicit unsupported responses.
    pub modes: BTreeMap<u16, ModeState>,
    /// Current kitty keyboard flags; zero still means supported.
    pub keyboard_flags: Option<u16>,
    /// OSC 11 background.
    pub background: Option<[u8; 3]>,
    /// XTGETTCAP RGB result.
    pub rgb: Option<bool>,
    /// XTGETTCAP Smulx result.
    pub smulx: Option<bool>,
    /// Kitty graphics query result.
    pub graphics: Option<bool>,
    /// DA1 sixel attribute.
    pub sixel: Option<bool>,
    /// At least one validated terminal reply arrived.
    pub answered: bool,
}

impl ProbeReplies {
    /// Use environment capability guesses only when no valid reply arrived.
    pub fn capabilities(&self, env: &Environment) -> Capabilities {
        let answered = self.answered
            || self.sentinel
            || self.identity.is_some()
            || self.keyboard_flags.is_some()
            || self.background.is_some()
            || self.rgb.is_some()
            || self.smulx.is_some()
            || self.graphics.is_some()
            || !self.modes.is_empty();
        let mut caps = if answered {
            env.transport()
        } else {
            env.fallback()
        };
        if let Some(identity) = &self.identity {
            caps.apply_identity(identity);
        }
        caps.modes.clone_from(&self.modes);
        if self.keyboard_flags.is_some() {
            caps.keyboard = KeyboardProtocol::Kitty;
        }
        if let Some(rgb) = self.rgb {
            caps.colour = if rgb {
                ColourDepth::TrueColour
            } else if caps.colour == ColourDepth::TrueColour {
                ColourDepth::Indexed256
            } else {
                caps.colour
            };
        }
        if let Some(smulx) = self.smulx {
            caps.styled_underline = smulx;
        }
        caps.background = self.background;
        caps.images = match (self.graphics, self.sixel) {
            (Some(true), _) => ImageProtocol::Kitty,
            (_, _) if caps.images == ImageProtocol::Iterm2 => ImageProtocol::Iterm2,
            (_, Some(true)) => ImageProtocol::Sixel,
            (Some(false), _) if caps.images == ImageProtocol::Kitty => ImageProtocol::Placeholder,
            _ => caps.images,
        };
        caps.passthrough = caps.multiplexer == Multiplexer::None
            || self.background.is_some()
            || self.rgb.is_some()
            || self.smulx.is_some()
            || self.graphics.is_some();
        if caps.multiplexer != Multiplexer::None && !caps.passthrough {
            caps.images = ImageProtocol::Placeholder;
            caps.clipboard = false;
            caps.notification = super::NotificationProtocol::Bell;
        }
        if env.0.contains_key("NO_COLOR") || caps.dumb {
            caps.colour = ColourDepth::Monochrome;
        }
        if caps.dumb {
            caps.scroll_regions = false;
        }
        caps
    }
}

/// Streaming reply decoder. VTE interprets CSI, OSC and DCS; the small framing
/// layer retains unrelated input and adds APC, which VTE does not dispatch.
#[derive(Default)]
pub struct Probe {
    /// Decoded replies, including those received after the startup sentinel.
    pub replies: ProbeReplies,
    frame: Vec<u8>,
    state: u8,
    pub(crate) input: VecDeque<Vec<u8>>,
    discarding: bool,
    string_kind: u8,
}

impl Probe {
    pub(crate) fn pending_escape(&self) -> bool {
        self.frame == b"\x1b"
    }

    pub(crate) fn idle(&self) -> bool {
        self.state == 0
    }

    pub(crate) fn recycle(&mut self, mut frame: Vec<u8>) {
        if self.frame.is_empty() && frame.capacity() > self.frame.capacity() {
            frame.clear();
            self.frame = frame;
        }
    }
    /// One batch, with DA1 last so its response acts as a completion sentinel.
    pub fn batch(multiplexer: Multiplexer) -> Vec<u8> {
        let mut batch = b"\x1b[?u".to_vec();
        for mode in modes() {
            batch.extend_from_slice(format!("\x1b[?{mode}$p").as_bytes());
        }
        for query in [
            b"\x1b[>0q".as_slice(),
            b"\x1b]11;?\x07",
            b"\x1bP+q524742;536d756c78\x1b\\",
            b"\x1b_Gi=314159,s=1,v=1,a=q,t=d,f=24;AAAA\x1b\\",
        ] {
            // CSI XTVERSION goes to the outer terminal; OSC/DCS/APC go inward.
            if query.starts_with(b"\x1b[") {
                batch.extend_from_slice(query);
            } else if multiplexer != Multiplexer::Screen
                || !query.windows(2).any(|x| x == b"\x1b\\")
            {
                batch.extend_from_slice(&multiplexer.wrap(query));
            }
        }
        batch.extend_from_slice(b"\x1b[c");
        batch
    }

    /// Classify a bounded chunk, including replies received after DA1. Partial
    /// control frames remain here across startup and input reads.
    pub fn feed(&mut self, bytes: &[u8]) -> usize {
        let mut used = 0;
        for &byte in bytes {
            used += 1;
            if self.frame.len() == PROBE_LIMIT {
                self.frame.clear();
                self.discarding = true;
            }
            if !self.discarding {
                self.frame.push(byte);
            }
            if self.state != 0 && matches!(byte, 0x18 | 0x1a) {
                self.finish(false);
                continue;
            }
            match self.state {
                0 if byte == 0x1b => self.state = 1,
                0 => self.finish(false),
                1 => match byte {
                    b'[' => self.state = 2,
                    b']' | b'P' | b'_' => {
                        self.string_kind = byte;
                        self.state = 3;
                    }
                    b'O' => self.state = 5,
                    _ => self.finish(false),
                },
                2 if (0x40..=0x7e).contains(&byte) => self.finish(true),
                2 if byte == 0x1b => {
                    self.frame.pop();
                    self.finish(false);
                    self.frame.push(byte);
                    self.state = 1;
                }
                3 if byte == 7 && self.string_kind == b']' => self.finish(true),
                3 if byte == 0x1b => self.state = 4,
                4 if byte == 0x1b && self.frame.starts_with(b"\x1bPtmux;") => self.state = 3,
                4 if byte == b'\\' => self.finish(true),
                4 if byte != 0x1b => {
                    self.frame.pop();
                    self.frame.pop();
                    self.finish(false);
                    self.frame.extend_from_slice(&[0x1b, byte]);
                    self.state = match byte {
                        b'[' => 2,
                        b']' | b'P' | b'_' => {
                            self.string_kind = byte;
                            3
                        }
                        _ => {
                            self.finish(false);
                            0
                        }
                    };
                }
                _ => {}
            }
            if self.state == 5 && self.frame.len() == 3 {
                self.finish(false);
            }
        }
        used
    }

    /// Resolve only an isolated Escape key. Partial replies stay quarantined
    /// until their terminator arrives, even after the startup deadline.
    pub fn finish_input(&mut self) {
        if self.frame == b"\x1b" {
            self.finish(false);
        }
    }

    fn finish(&mut self, parse: bool) {
        if self.discarding {
            self.discarding = false;
            self.frame.clear();
            self.state = 0;
            return;
        }
        if self.frame.starts_with(b"\x1bPtmux;") && parse {
            let frame = std::mem::take(&mut self.frame);
            self.state = 0;
            let body = frame
                .get(7..frame.len().saturating_sub(2))
                .unwrap_or_default();
            let mut previous_escape = false;
            let mut decoded = Vec::new();
            for &byte in body {
                if byte == 0x1b && !previous_escape {
                    previous_escape = true;
                    continue;
                }
                decoded.push(byte);
                previous_escape = false;
            }
            // A tmux envelope is terminal traffic, never user input. One level
            // is sufficient; nested envelopes are discarded without recursion.
            if !decoded.starts_with(b"\x1bPtmux;") {
                self.feed(&decoded);
            }
            return;
        }
        let mut handler = ReplyHandler {
            replies: &mut self.replies,
            recognized: false,
            dcs: Vec::new(),
            kind: 0,
        };
        if parse {
            if self.frame.starts_with(b"\x1b_G") {
                if let Some(body) = self.frame.get(3..self.frame.len().saturating_sub(2))
                    && let Ok(body) = std::str::from_utf8(body)
                    && let Some((header, status)) = body.split_once(';')
                    && header
                        .split(',')
                        .any(|field| field == format!("i={GRAPHICS_ID}"))
                {
                    handler.replies.graphics = Some(status == "OK");
                    handler.recognized = true;
                }
            } else {
                let mut parser = vte::Parser::new();
                parser.advance(&mut handler, &self.frame);
            }
        }
        if handler.recognized {
            self.replies.answered = true;
        } else {
            self.input.push_back(std::mem::take(&mut self.frame));
        }
        self.frame.clear();
        self.state = 0;
    }
}

struct ReplyHandler<'a> {
    replies: &'a mut ProbeReplies,
    recognized: bool,
    dcs: Vec<u8>,
    kind: u8,
}

impl vte::Perform for ReplyHandler<'_> {
    fn csi_dispatch(
        &mut self,
        params: &vte::Params,
        intermediates: &[u8],
        ignore: bool,
        action: char,
    ) {
        if ignore {
            return;
        }
        if params.iter().any(|parameter| parameter.len() != 1) {
            return;
        }
        let values: Vec<_> = params
            .iter()
            .filter_map(|parameter| match parameter {
                [value] => Some(*value),
                _ => None,
            })
            .collect();
        match (action, intermediates, values.as_slice()) {
            ('c', b"?", [_, attributes @ ..]) => {
                self.replies.sentinel = true;
                self.replies.sixel = Some(attributes.contains(&4));
            }
            ('u', b"?", [flags]) => self.replies.keyboard_flags = Some(*flags),
            ('y', b"?$", [mode, value]) if modes().any(|known| known == *mode) && *value <= 4 => {
                let state = match value {
                    0 => ModeState::Unsupported,
                    1 => ModeState::Set,
                    2 => ModeState::Reset,
                    3 => ModeState::AlwaysSet,
                    _ => ModeState::AlwaysReset,
                };
                self.replies.modes.insert(*mode, state);
            }
            _ => return,
        }
        self.recognized = true;
    }

    fn osc_dispatch(&mut self, params: &[&[u8]], _bell_terminated: bool) {
        if let [b"11", rgb] = params
            && let Some(rgb) = parse_rgb(rgb)
        {
            self.replies.background = Some(rgb);
            self.recognized = true;
        }
    }

    fn hook(&mut self, params: &vte::Params, intermediates: &[u8], ignore: bool, action: char) {
        if ignore {
            return;
        }
        self.kind = match (action, intermediates) {
            ('|', b">") => 1,
            ('r', b"+") => match params.iter().next() {
                Some([1]) => 2,
                Some([0]) => 3,
                _ => 0,
            },
            _ => 0,
        };
    }

    fn put(&mut self, byte: u8) {
        self.dcs.push(byte);
    }

    fn unhook(&mut self) {
        match self.kind {
            1 => {
                if let Ok(identity) = std::str::from_utf8(&self.dcs)
                    && !identity.is_empty()
                    && identity.chars().all(|c| !c.is_control())
                {
                    self.replies.identity = Some(identity.to_owned());
                    self.recognized = true;
                }
            }
            2 | 3 => {
                for field in self.dcs.split(|&byte| byte == b';') {
                    let name = field.split(|&byte| byte == b'=').next().unwrap_or_default();
                    if name.eq_ignore_ascii_case(b"524742") {
                        self.replies.rgb = Some(self.kind == 2);
                        self.recognized = true;
                    }
                    if name.eq_ignore_ascii_case(b"536d756c78") {
                        self.replies.smulx = Some(self.kind == 2);
                        self.recognized = true;
                    }
                }
            }
            _ => {}
        }
    }
}

fn parse_rgb(bytes: &[u8]) -> Option<[u8; 3]> {
    let text = std::str::from_utf8(bytes).ok()?.strip_prefix("rgb:")?;
    let mut components = text.split('/');
    let mut rgb = [0; 3];
    for component in &mut rgb {
        let hex = components.next()?;
        if hex.is_empty() || hex.len() > 4 {
            return None;
        }
        let value = u32::from_str_radix(hex, 16).ok()?;
        let max = (1u32 << (hex.len() * 4)) - 1;
        *component = u8::try_from(value * 255 / max).ok()?;
    }
    components.next().is_none().then_some(rgb)
}

/// Run the single batch on the caller's controlling terminal. The returned
/// value is the session cache; callers must not probe again during rendering.
pub fn detect(
    io: &mut impl ProbeIo,
    env: &Environment,
) -> io::Result<(Capabilities, super::Decoder)> {
    let mut decoder = super::Decoder::default();
    if env.get("TERM") == "dumb" {
        return Ok((decoder.replies().capabilities(env), decoder));
    }
    let deadline = Instant::now() + PROBE_TIMEOUT;
    let multiplexer = env.transport().multiplexer;
    if let Err(error) = io.send(&Probe::batch(multiplexer), deadline) {
        // Even an empty restoration journal can follow a partially sent OSC
        // or mux envelope. Give cleanup one bounded nonblocking retry window,
        // including when the original startup deadline has already expired.
        let mut cancel = vec![0x18];
        if multiplexer != Multiplexer::None {
            cancel.extend_from_slice(&multiplexer.wrap(b"\x18"));
        }
        let _ = io.send(&cancel, Instant::now() + Duration::from_millis(1));
        return Err(error);
    }
    let mut buffer = [0; 1024];
    let mut total = 0;
    // An outer DA1 is not an inner completion marker. Collect mux replies up
    // to the same absolute deadline, irrespective of their relative ordering.
    while Instant::now() < deadline
        && (multiplexer != Multiplexer::None || !decoder.replies().sentinel)
        && total < PROBE_LIMIT
    {
        let available = buffer.len().min(PROBE_LIMIT - total);
        let count = match io.receive(
            buffer
                .get_mut(..available)
                .ok_or(io::ErrorKind::InvalidInput)?,
            deadline,
        ) {
            Err(error) if error.kind() == io::ErrorKind::TimedOut => break,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                std::thread::sleep(
                    Duration::from_millis(1)
                        .min(deadline.saturating_duration_since(Instant::now())),
                );
                continue;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => result?,
        };
        if count == 0 {
            break;
        }
        let bytes = buffer.get(..count).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "probe transport exceeded buffer",
            )
        })?;
        total += count;
        decoder.feed(bytes)?;
    }
    let mut caps = decoder.replies().capabilities(env);
    caps.scroll_regions |= io.native_scroll_regions();
    io.detected(&caps)?;
    Ok((caps, decoder))
}
