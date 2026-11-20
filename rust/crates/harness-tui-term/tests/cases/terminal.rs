//! Terminal profiles, decoder properties, restoration and PTY transport.
#![cfg(not(target_arch = "wasm32"))]

use std::io::{self, Write};
use std::time::Instant;

use acyclic_harness_tui_core::key_event::{InputEvent, KeyCode, KeyEvent, KeyModifiers};
use acyclic_harness_tui_term::*;
use alacritty_terminal::event::EventListener;
use alacritty_terminal::grid::Dimensions;
use alacritty_terminal::index::{Column, Line, Point};
use alacritty_terminal::term::{Config, Term, TermMode};
use alacritty_terminal::vte::ansi::Processor;
use proptest::prelude::*;
use serde::Deserialize;

fn keys(text: &str) -> Vec<InputEvent> {
    text.chars()
        .map(|c| InputEvent::Key(KeyEvent::new(KeyCode::Char(c), KeyModifiers::NONE)))
        .collect()
}

#[derive(Deserialize)]
struct Profile {
    name: String,
    term: String,
    identity: String,
    kitty: bool,
    sixel: bool,
    rgb: bool,
    smulx: bool,
    modes: bool,
    #[serde(default)]
    grapheme: bool,
    image: String,
    keyboard: String,
    colour: String,
}

fn image_protocol(name: &str) -> Option<ImageProtocol> {
    match name {
        "Kitty" => Some(ImageProtocol::Kitty),
        "Iterm2" => Some(ImageProtocol::Iterm2),
        "Sixel" => Some(ImageProtocol::Sixel),
        "Placeholder" => Some(ImageProtocol::Placeholder),
        _ => None,
    }
}

fn keyboard_protocol(name: &str) -> Option<KeyboardProtocol> {
    match name {
        "Kitty" => Some(KeyboardProtocol::Kitty),
        "ModifyOtherKeys" => Some(KeyboardProtocol::ModifyOtherKeys),
        "Legacy" => Some(KeyboardProtocol::Legacy),
        _ => None,
    }
}

fn colour_depth(name: &str) -> Option<ColourDepth> {
    match name {
        "TrueColour" => Some(ColourDepth::TrueColour),
        "Indexed256" => Some(ColourDepth::Indexed256),
        "Ansi16" => Some(ColourDepth::Ansi16),
        "Monochrome" => Some(ColourDepth::Monochrome),
        _ => None,
    }
}

impl Profile {
    fn replies(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        if !self.identity.is_empty() {
            bytes.extend_from_slice(format!("\x1bP>|{}\x1b\\", self.identity).as_bytes());
        }
        if self.kitty {
            bytes.extend_from_slice(b"\x1b[?0u");
        }
        for mode in [
            2026, 2027, 1004, 2004, 1049, 9, 1000, 1001, 1002, 1003, 1005, 1006, 1015, 1016,
        ] {
            let state = if self.modes && (mode != 2027 || self.grapheme) {
                2
            } else {
                0
            };
            bytes.extend_from_slice(format!("\x1b[?{mode};{state}$y").as_bytes());
        }
        if self.name != "screen" {
            bytes.extend_from_slice(
                format!(
                    "\x1bP{}+r524742\x1b\\\x1bP{}+r536d756c78=1b\x1b\\",
                    u8::from(self.rgb),
                    u8::from(self.smulx)
                )
                .as_bytes(),
            );
        }
        let graphics = if self.image == "Kitty" {
            "OK"
        } else {
            "ENOTSUP"
        };
        if self.name != "screen" {
            bytes.extend_from_slice(format!("\x1b_Gi=314159;{graphics}\x1b\\").as_bytes());
        }
        bytes.extend_from_slice(b"\x1b]11;rgb:1010/2020/3030\x1b\\");
        bytes.extend_from_slice(if self.sixel {
            b"\x1b[?62;4c"
        } else {
            b"\x1b[?62;1c"
        });
        bytes
    }

    fn env(&self) -> Environment {
        let mut env = Environment([("TERM".into(), self.term.clone())].into());
        if self.name == "tmux" {
            env.0.insert("TMUX".into(), "session".into());
        }
        if self.name == "screen" {
            env.0.insert("STY".into(), "session".into());
        }
        env
    }
}

#[derive(Clone, Copy)]
struct Size;
impl Dimensions for Size {
    fn total_lines(&self) -> usize {
        10
    }
    fn screen_lines(&self) -> usize {
        10
    }
    fn columns(&self) -> usize {
        40
    }
}
#[derive(Clone, Copy)]
struct Events;
impl EventListener for Events {}

fn oracle() -> Term<Events> {
    Term::new(Config::default(), &Size, Events)
}

#[test]
fn profile_detection_and_setup_matrix() -> Result<(), Box<dyn std::error::Error>> {
    let profiles: Vec<Profile> = serde_json::from_str(include_str!("../profiles.json"))?;
    assert_eq!(profiles.len(), 14);
    for profile in profiles {
        let mut responder = Script {
            sent: Vec::new(),
            replies: profile.replies(),
            reads: 0,
            native_scroll: false,
        };
        let (detected, _) = detect(&mut responder, &profile.env())?;
        assert_eq!(responder.sent, Probe::batch(detected.multiplexer));
        for chunk_size in [1, 2, 7, 1024] {
            let mut decoder = Decoder::default();
            for chunk in profile.replies().chunks(chunk_size) {
                decoder.feed(chunk)?;
            }
            assert!(decoder.take_events().is_empty());
            let probe = decoder.replies();
            assert!(probe.sentinel, "{}", profile.name);
            let caps = probe.capabilities(&profile.env());
            assert_eq!(caps, detected, "{}", profile.name);
            assert_eq!(
                Some(caps.images),
                image_protocol(&profile.image),
                "{}",
                profile.name
            );
            assert_eq!(
                Some(caps.keyboard),
                keyboard_protocol(&profile.keyboard),
                "{}",
                profile.name
            );
            assert_eq!(
                Some(caps.colour),
                colour_depth(&profile.colour),
                "{}",
                profile.name
            );
            assert_eq!(caps.styled_underline, profile.smulx, "{}", profile.name);
            assert_eq!(caps.background, Some([16, 32, 48]));
            let mut bytes = Vec::new();
            {
                let mut guard = RestoreGuard::new(&mut bytes, &caps);
                guard.setup(ScreenMode::Alternate)?;
                guard.writer().write_all(b"hello")?;
                guard.restore()?;
            }
            let mut expected = Vec::new();
            if profile.modes {
                expected.extend_from_slice(b"\x1b[?1004h\x1b[?2004h");
                if profile.grapheme {
                    expected.extend_from_slice(b"\x1b[?2027h");
                }
                expected.extend_from_slice(b"\x1b[?1049h\x1b[?1006h\x1b[?1002h");
            }
            expected.extend_from_slice(b"hello");
            if profile.modes {
                expected.extend_from_slice(b"\x18\x1b[?1002l\x18\x1b[?1006l\x18\x1b[?1049l");
                if profile.grapheme {
                    expected.extend_from_slice(b"\x18\x1b[?2027l");
                }
                expected.extend_from_slice(b"\x18\x1b[?2004l\x18\x1b[?1004l");
            }
            assert_eq!(bytes, expected, "{}", profile.name);
            let mut term = oracle();
            let mut parser: Processor = Processor::new();
            parser.advance(&mut term, &bytes);
            assert!(!term.mode().intersects(
                TermMode::ALT_SCREEN
                    | TermMode::FOCUS_IN_OUT
                    | TermMode::BRACKETED_PASTE
                    | TermMode::MOUSE_MODE
                    | TermMode::SGR_MOUSE
            ));
            // Alternate-screen contents disappear; inline output remains.
            let first = term.grid()[Point::new(Line(0), Column(0))].c;
            assert_eq!(first, if profile.modes { ' ' } else { 'h' });
        }
    }
    Ok(())
}

struct Script {
    sent: Vec<u8>,
    replies: Vec<u8>,
    reads: usize,
    native_scroll: bool,
}
impl ProbeIo for Script {
    fn native_scroll_regions(&self) -> bool {
        self.native_scroll
    }
    fn send(&mut self, bytes: &[u8], deadline: Instant) -> io::Result<()> {
        assert!(deadline <= Instant::now() + PROBE_TIMEOUT);
        self.sent.extend_from_slice(bytes);
        Ok(())
    }
    fn receive(&mut self, buffer: &mut [u8], deadline: Instant) -> io::Result<usize> {
        assert!(deadline <= Instant::now() + PROBE_TIMEOUT);
        self.reads += 1;
        let count = buffer.len().min(self.replies.len());
        buffer
            .get_mut(..count)
            .ok_or(io::ErrorKind::InvalidInput)?
            .copy_from_slice(
                self.replies
                    .get(..count)
                    .ok_or(io::ErrorKind::InvalidInput)?,
            );
        self.replies.drain(..count);
        Ok(count)
    }
}

#[test]
fn batch_deadline_sentinel_and_replay() -> io::Result<()> {
    let mut io = Script {
        sent: Vec::new(),
        replies: b"typed\x1b[?62;4cafter".to_vec(),
        reads: 0,
        native_scroll: false,
    };
    let (_, mut decoder) = detect(&mut io, &Environment::default())?;
    assert_eq!(decoder.take_events(), keys("typedafter"));
    assert_eq!(io.reads, 1);
    assert!(io.sent.ends_with(b"\x1b[c"));
    assert_eq!(io.sent, Probe::batch(Multiplexer::None));
    Ok(())
}

#[test]
fn query_overrides_environment_and_mux_requires_inner_reply() {
    let env = Environment(
        [
            ("TERM".into(), "xterm-kitty".into()),
            ("TMUX".into(), "yes".into()),
        ]
        .into(),
    );
    let mut probe = Probe::default();
    probe.feed(b"\x1bP>|kitty\x1b\\\x1bP0+r524742\x1b\\\x1b[?2026;0$y\x1b_Gi=314159;ENOTSUP\x1b\\\x1b[?62;1c");
    let caps = probe.replies.capabilities(&env);
    assert_eq!(caps.images, ImageProtocol::Placeholder);
    assert_eq!(caps.colour, ColourDepth::Indexed256);
    assert_eq!(caps.mode(2026), ModeState::Unsupported);
    assert!(caps.passthrough);
    let caps = ProbeReplies {
        identity: Some("kitty".into()),
        ..ProbeReplies::default()
    }
    .capabilities(&env);
    assert!(!caps.passthrough);
    assert!(!caps.clipboard);
    assert_eq!(caps.images, ImageProtocol::Placeholder);
    assert_eq!(
        Multiplexer::Tmux.wrap(b"\x1b]52;c;x\x07"),
        b"\x1bPtmux;\x1b\x1b]52;c;x\x07\x1b\\"
    );
    assert_eq!(
        Multiplexer::Screen.wrap(b"\x1b]11;?\x07"),
        b"\x1bP\x1b]11;?\x07\x1b\\"
    );
}

#[test]
fn quiet_dumb_timeout_and_incomplete_input() -> io::Result<()> {
    let env = Environment(
        [
            ("TERM".into(), "dumb".into()),
            ("TERM_PROGRAM".into(), "kitty".into()),
        ]
        .into(),
    );
    let mut script = Script {
        sent: Vec::new(),
        replies: Vec::new(),
        reads: 0,
        native_scroll: false,
    };
    let (caps, _) = detect(&mut script, &env)?;
    assert!(caps.dumb);
    assert_eq!(caps.colour, ColourDepth::Monochrome);
    assert!(!caps.hyperlinks);
    assert!(script.sent.is_empty());
    assert_eq!(script.reads, 0);
    script.replies = b"text\x1b]unfinished".to_vec();
    let (_, mut decoder) = detect(&mut script, &Environment::default())?;
    assert_eq!(decoder.take_events(), keys("text"));
    assert!(!decoder.replies().sentinel);
    Ok(())
}

#[test]
fn malformed_replies_do_not_swallow_user_input() -> io::Result<()> {
    for input in [
        b"\x1b]11;rgb:ffff/0000/0000\x18typed".as_slice(),
        b"\x1b[?2026;7:1;2$ytyped",
        b"\x1b]invalid\x1b[13;5utyped",
    ] {
        let mut decoder = Decoder::default();
        for chunk in input.chunks(2) {
            decoder.feed(chunk)?;
        }
        let mut expected = keys("typed");
        if input.ends_with(b"\x1b[13;5utyped") {
            expected.insert(
                0,
                InputEvent::Key(KeyEvent::new(KeyCode::Enter, KeyModifiers::CONTROL)),
            );
        }
        assert_eq!(decoder.take_events(), expected);
        assert!(decoder.replies().modes.is_empty());
        assert!(decoder.replies().background.is_none());
    }
    Ok(())
}

#[cfg(unix)]
#[path = "../support/status_flags.rs"]
mod status_flags;

#[cfg(unix)]
#[test]
fn controlling_tty_probe_and_os_state_restore() -> Result<(), Box<dyn std::error::Error>> {
    let _terminal = crate::terminal_owner();
    use rustix::fs::OFlags;
    use rustix::termios::{LocalModes, tcgetattr};
    use status_flags::settable_status_flags;
    let pty = rustix_openpty::openpty(None, None)?;
    let mut controller = std::fs::File::from(pty.controller);
    let user = std::fs::File::from(pty.user);
    let observer = user.try_clone()?;
    let original = tcgetattr(&observer)?;
    let original_flags = settable_status_flags(&observer)?;
    {
        let mut tty = Tty::from_file(user)?;
        assert!(
            !tcgetattr(&observer)?
                .local_modes
                .contains(LocalModes::ICANON)
        );
        assert!(settable_status_flags(&observer)?.contains(OFlags::NONBLOCK));
        controller.write_all(b"typed\x1b[?0u\x1b[?62;4c")?;
        let (caps, mut decoder) = detect(&mut tty, &Environment::default())?;
        assert_eq!(caps.keyboard, KeyboardProtocol::Kitty);
        assert_eq!(caps.images, ImageProtocol::Sixel);
        assert_eq!(decoder.take_events(), keys("typed"));
    }
    let restored = tcgetattr(&observer)?;
    assert_eq!(restored.input_modes, original.input_modes);
    assert_eq!(restored.output_modes, original.output_modes);
    assert_eq!(restored.control_modes, original.control_modes);
    assert_eq!(restored.local_modes, original.local_modes);
    assert_eq!(
        format!("{:?}", restored.special_codes),
        format!("{:?}", original.special_codes)
    );
    assert_eq!(restored.input_speed(), original.input_speed());
    assert_eq!(restored.output_speed(), original.output_speed());
    assert_eq!(settable_status_flags(&observer)?, original_flags);
    Ok(())
}

#[test]
fn restore_is_reusable_and_preserves_permanent_and_unknown_modes() -> io::Result<()> {
    let mut caps = Capabilities::default();
    caps.modes.insert(1004, ModeState::AlwaysSet);
    caps.modes.insert(2004, ModeState::AlwaysReset);
    caps.modes.insert(1049, ModeState::Reset);
    let mut bytes = Vec::new();
    {
        let mut guard = RestoreGuard::new(&mut bytes, &caps);
        for _ in 0..2 {
            guard.setup(ScreenMode::Alternate)?;
            guard.restore()?;
            guard.restore()?;
        }
    }
    assert_eq!(
        bytes,
        b"\x1b[?1049h\x18\x1b[?1049l\x1b[?1049h\x18\x1b[?1049l"
    );
    Ok(())
}

#[test]
fn alternate_screen_reports_sgr_wheel_and_inline_keeps_native_scrolling() -> io::Result<()> {
    let setup = |screen, modes: &[(u16, ModeState)]| -> io::Result<Vec<u8>> {
        let mut caps = Capabilities::default();
        for mode in [1049, 9, 1000, 1001, 1003, 1005, 1015, 1016] {
            caps.modes.insert(mode, ModeState::Reset);
        }
        caps.modes.extend(modes.iter().copied());
        let mut bytes = Vec::new();
        RestoreGuard::new(&mut bytes, &caps).setup(screen)?;
        Ok(bytes)
    };
    let both = [(1006, ModeState::Reset), (1002, ModeState::Reset)];
    // Alternate screen: SGR encoding before button-event reports, undone in
    // reverse, so no legacy X10 report can ever precede the SGR switch.
    assert_eq!(
        setup(ScreenMode::Alternate, &both)?,
        b"\x1b[?1049h\x1b[?1006h\x1b[?1002h\x18\x1b[?1002l\x18\x1b[?1006l\x18\x1b[?1049l"
    );
    // Inline: the terminal's own scrollback and selection keep the wheel.
    assert_eq!(setup(ScreenMode::Inline, &both)?, b"");
    // Negative controls: either mode alone would emit undecodable legacy
    // reports or nothing useful, so neither is enabled.
    for partial in [
        [(1006, ModeState::Reset), (1002, ModeState::Unsupported)],
        [(1006, ModeState::Unknown), (1002, ModeState::Reset)],
    ] {
        assert_eq!(
            setup(ScreenMode::Alternate, &partial)?,
            b"\x1b[?1049h\x18\x1b[?1049l"
        );
    }
    // Mouse reporting needs a confirmed alternate screen: with 1049 unknown,
    // unsupported or permanently reset, both supported reporting modes stay
    // off, because reports would replace the primary screen's own wheel.
    for primary in [
        ModeState::Unknown,
        ModeState::Unsupported,
        ModeState::AlwaysReset,
    ] {
        assert_eq!(
            setup(ScreenMode::Alternate, &[(1049, primary), both[0], both[1]])?,
            b""
        );
    }
    // An alternate screen already active (or permanent) is confirmed as is.
    for active in [ModeState::Set, ModeState::AlwaysSet] {
        assert_eq!(
            setup(ScreenMode::Alternate, &[(1049, active), both[0], both[1]])?,
            b"[?1006h[?1002h[?1002l[?1006l"
        );
    }
    // What the terminal then reports decodes to wheel notches, burst intact.
    let mut decoder = Decoder::default();
    decoder.feed(b"\x1b[<64;5;7M\x1b[<64;5;7M\x1b[<65;5;7M")?;
    let kinds: Vec<_> = decoder
        .take_events()
        .into_iter()
        .map(|event| match event {
            InputEvent::Mouse(mouse) => Some((mouse.kind, mouse.column, mouse.row)),
            _ => None,
        })
        .collect();
    use acyclic_harness_tui_core::key_event::MouseEventKind::{ScrollDown, ScrollUp};
    assert_eq!(
        kinds,
        [
            Some((ScrollUp, 4, 6)),
            Some((ScrollUp, 4, 6)),
            Some((ScrollDown, 4, 6))
        ]
    );
    Ok(())
}

/// A transient failure after an arbitrary byte, including inside a CSI.
struct FailOnce {
    bytes: Vec<u8>,
    at: usize,
    failed: bool,
}
impl Write for FailOnce {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if !self.failed && self.bytes.len() == self.at {
            self.failed = true;
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        let count = if self.failed {
            bytes.len()
        } else {
            bytes.len().min(self.at - self.bytes.len())
        };
        self.bytes
            .extend_from_slice(bytes.get(..count).ok_or(io::ErrorKind::InvalidInput)?);
        Ok(count)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Start states: a subset of the independent modes and at most one mouse
/// protocol, as a terminal can actually be (selecting one replaces another).
fn start_states() -> impl Iterator<Item = u8> {
    (0..128u8).filter(|initial| (initial >> 4).count_ones() <= 1)
}

fn restored_modes(initial: u8, at: usize) -> (TermMode, TermMode) {
    let mut caps = Capabilities::default();
    for mode in [9, 1001, 1005, 1015, 1016] {
        caps.modes.insert(mode, ModeState::Reset);
    }
    let mut original = Vec::new();
    for (index, mode) in [1004, 2004, 1049, 1006, 1002, 1000, 1003]
        .into_iter()
        .enumerate()
    {
        let set = initial & (1 << index) != 0;
        caps.modes.insert(
            mode,
            if set {
                ModeState::Set
            } else {
                ModeState::Reset
            },
        );
        if set {
            original.extend_from_slice(format!("\x1b[?{mode}h").as_bytes());
        }
    }
    let mut term = oracle();
    let mut parser: Processor = Processor::new();
    parser.advance(&mut term, &original);
    let before = *term.mode();
    let mut writer = FailOnce {
        bytes: Vec::new(),
        at,
        failed: false,
    };
    {
        let mut guard = RestoreGuard::new(&mut writer, &caps);
        let _ = guard.setup(ScreenMode::Alternate);
    }
    parser.advance(&mut term, &writer.bytes);
    (before, *term.mode())
}

#[test]
fn every_mode_subset_and_failure_boundary_restores() {
    // Exhaust every initial subset and byte boundary of setup AND teardown,
    // including a transient failure partway through a restoration sequence.
    // Start states include an inherited click (1000) or any-event (1003)
    // protocol that ours displaces and restoration selects again.
    for initial in start_states() {
        for at in 0..110 {
            let (before, after) = restored_modes(initial, at);
            assert_eq!(before, after, "initial={initial} failure_at={at}");
        }
    }
}

#[test]
fn mouse_restoration_oracle_rejects_a_lost_selection_and_preserves_unknown_modes() -> io::Result<()>
{
    // Negative control: resetting only ours leaves the inherited click
    // reporting off, which the oracle detects.
    let mut term = oracle();
    let mut parser: Processor = Processor::new();
    parser.advance(&mut term, b"\x1b[?1000h");
    let before = *term.mode();
    parser.advance(&mut term, b"\x1b[?1002h\x1b[?1002l");
    assert_ne!(*term.mode(), before);
    // An unknown protocol state could be an active selection: reporting is
    // not enabled at all.
    let mut caps = Capabilities::default();
    for mode in [1049, 9, 1001, 1002, 1003, 1005, 1006, 1015, 1016] {
        caps.modes.insert(mode, ModeState::Reset);
    }
    let mut bytes = Vec::new();
    RestoreGuard::new(&mut bytes, &caps).setup(ScreenMode::Alternate)?;
    assert_eq!(bytes, b"\x1b[?1049h\x18\x1b[?1049l");
    Ok(())
}

fn keyboard_rank(protocol: KeyboardProtocol) -> u8 {
    match protocol {
        KeyboardProtocol::Legacy => 0,
        KeyboardProtocol::ModifyOtherKeys => 1,
        KeyboardProtocol::Kitty => 2,
    }
}

/// One evidence key and the capability it alone establishes.
fn single_evidence()
-> impl Strategy<Value = (&'static str, &'static str, ColourDepth, KeyboardProtocol)> {
    prop::sample::select(vec![
        (
            "TERM",
            "linux",
            ColourDepth::Ansi16,
            KeyboardProtocol::Legacy,
        ),
        (
            "TERM",
            "xterm-256color",
            ColourDepth::Indexed256,
            KeyboardProtocol::ModifyOtherKeys,
        ),
        (
            "COLORTERM",
            "24bit",
            ColourDepth::TrueColour,
            KeyboardProtocol::Legacy,
        ),
        (
            "TERM_PROGRAM",
            "WezTerm",
            ColourDepth::TrueColour,
            KeyboardProtocol::Kitty,
        ),
        (
            "WT_SESSION",
            "session",
            ColourDepth::TrueColour,
            KeyboardProtocol::Legacy,
        ),
        (
            "KITTY_WINDOW_ID",
            "1",
            ColourDepth::TrueColour,
            KeyboardProtocol::Kitty,
        ),
    ])
}

fn fallback_environment() -> impl Strategy<Value = Environment> {
    let term = prop::sample::select(vec![
        "",
        "dumb",
        "linux",
        "ansi",
        "xterm-256color",
        "screen-256color",
        "tmux-256color",
        "xterm-kitty",
        "alacritty",
        "foot",
    ]);
    let colorterm = prop::sample::select(vec!["", "truecolor", "24bit", "yes"]);
    let program = prop::sample::select(vec![
        "",
        "WezTerm",
        "kitty",
        "ghostty",
        "iTerm2",
        "Apple_Terminal",
        "tmux",
        "screen",
        "vscode",
        "alacritty",
        "foot",
        "Windows Terminal",
    ]);
    let flags = (
        any::<bool>(),
        any::<bool>(),
        any::<bool>(),
        any::<bool>(),
        any::<bool>(),
    );
    (term, colorterm, program, flags).prop_map(
        |(term, colorterm, program, (wt, kitty, tmux, sty, ssh))| {
            let mut env = Environment::default();
            if !term.is_empty() {
                env.0.insert("TERM".into(), term.into());
            }
            if !colorterm.is_empty() {
                env.0.insert("COLORTERM".into(), colorterm.into());
            }
            if !program.is_empty() {
                env.0.insert("TERM_PROGRAM".into(), program.into());
            }
            if wt {
                env.0.insert("WT_SESSION".into(), "session".into());
            }
            if kitty {
                env.0.insert("KITTY_WINDOW_ID".into(), "1".into());
            }
            if tmux {
                env.0.insert("TMUX".into(), "session".into());
            }
            if sty {
                env.0.insert("STY".into(), "session".into());
            }
            if ssh {
                env.0.insert("SSH_CONNECTION".into(), "remote".into());
            }
            env
        },
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]

    // Property: one evidence key alone establishes its documented capability,
    // the fallback ladder is monotone in its evidence, and transport is
    // derived from the environment alone.
    #[test]
    fn fallback_ladders_are_monotone_and_transport_is_derived(
        env in fallback_environment(),
        (key, value, colour, keyboard) in single_evidence(),
    ) {
        let single = Environment([(key.into(), value.into())].into());
        let caps = ProbeReplies::default().capabilities(&single);
        prop_assert_eq!(caps.colour, colour, "{}={}", key, value);
        prop_assert_eq!(caps.keyboard, keyboard, "{}={}", key, value);

        let caps = ProbeReplies::default().capabilities(&env);
        let term = env.0.get("TERM").map_or("", String::as_str);
        let overridden = env.0.contains_key("NO_COLOR") || term == "dumb";
        if overridden {
            prop_assert_eq!(caps.colour, ColourDepth::Monochrome);
        }
        if term == "dumb" {
            prop_assert_eq!(caps.keyboard, KeyboardProtocol::Legacy);
            prop_assert!(!caps.scroll_regions);
        }
        if !overridden {
            if matches!(
                env.0.get("COLORTERM").map_or("", String::as_str),
                "truecolor" | "24bit"
            ) {
                prop_assert_eq!(caps.colour, ColourDepth::TrueColour);
            }
            if term.contains("256color") {
                prop_assert!(caps.colour >= ColourDepth::Indexed256);
            }
            if env.0.contains_key("KITTY_WINDOW_ID") {
                prop_assert_eq!(caps.keyboard, KeyboardProtocol::Kitty);
                prop_assert_eq!(caps.colour, ColourDepth::TrueColour);
            } else if env.0.contains_key("WT_SESSION") {
                prop_assert_eq!(caps.colour, ColourDepth::TrueColour);
            }
        }
        // Adding weaker evidence never reduces a known capability.
        let mut stronger = env.clone();
        stronger.0.insert("TERM".into(), "xterm-256color".into());
        stronger.0.insert("COLORTERM".into(), "truecolor".into());
        prop_assert!(ProbeReplies::default().capabilities(&stronger).colour >= caps.colour);
        let mut keyboard_evidence = env.clone();
        keyboard_evidence
            .0
            .insert("TERM_PROGRAM".into(), "WezTerm".into());
        prop_assert!(
            keyboard_rank(ProbeReplies::default().capabilities(&keyboard_evidence).keyboard)
                >= keyboard_rank(caps.keyboard)
        );
        // Transport is derived from the environment alone; passthrough needs
        // an inner reply, which no reply here provides.
        prop_assert_eq!(caps.remote, env.0.keys().any(|key| key.starts_with("SSH_")));
        let multiplexer = if env.0.contains_key("TMUX") {
            Multiplexer::Tmux
        } else if env.0.contains_key("STY") {
            Multiplexer::Screen
        } else {
            Multiplexer::None
        };
        prop_assert_eq!(caps.multiplexer, multiplexer);
        prop_assert_eq!(caps.passthrough, multiplexer == Multiplexer::None);
    }

    #[test]
    fn reply_round_trip_and_chunk_invariance(mode in prop::sample::select(vec![2026u16,2027,1004,2004,1049,9,1000,1001,1002,1003,1005,1006,1015,1016]),
        state in 0u8..5, flags in any::<u16>(), rgb in any::<[u8; 3]>(), chunk in 1usize..80) {
        let [r, g, b] = rgb;
        let encoded = format!("\x1b[?{mode};{state}$y\x1b[?{flags}u\x1b]11;rgb:{r:02x}/{g:02x}/{b:02x}\x07\x1b[?62;4c");
        let mut whole = Probe::default();
        whole.feed(encoded.as_bytes());
        let mut split = Probe::default();
        for bytes in encoded.as_bytes().chunks(chunk) { split.feed(bytes); }
        prop_assert_eq!(&whole.replies, &split.replies);
        prop_assert_eq!(split.replies.keyboard_flags, Some(flags));
        prop_assert_eq!(split.replies.background, Some(rgb));
        let states = [ModeState::Unsupported, ModeState::Set, ModeState::Reset, ModeState::AlwaysSet, ModeState::AlwaysReset];
        prop_assert_eq!(split.replies.modes.get(&mode), states.get(usize::from(state)));
        prop_assert!(split.replies.sentinel);
    }

    #[test]
    fn arbitrary_input_is_bounded_and_chunk_independent(bytes in prop::collection::vec(any::<u8>(), 0..20_000), chunk in 1usize..150) {
        let mut whole = Probe::default();
        whole.feed(&bytes);
        whole.finish_input();
        let mut split = Probe::default();
        for bytes in bytes.chunks(chunk) { split.feed(bytes); }
        split.finish_input();
        prop_assert_eq!(&whole.replies, &split.replies);
    }
}

#[path = "../terminal/mouse.rs"]
mod mouse;
#[test]
fn scrolling_requires_a_screen_oracle_and_dumb_always_disables_it() {
    for native in [false, true] {
        for term in ["", "dumb"] {
            let mut script = Script {
                sent: Vec::new(),
                replies: Vec::new(),
                reads: 0,
                native_scroll: native,
            };
            let env = Environment([("TERM".into(), term.into())].into());
            let (caps, _) = detect(&mut script, &env).unwrap();
            assert_eq!(caps.scroll_regions, native && term != "dumb");
        }
    }
    for identity in [
        "alacritty",
        "tmux",
        "screen",
        "kitty",
        "wezterm",
        "ghostty",
        "iTerm2",
        "foot",
        "Windows Terminal",
        "xterm",
        "xterm-256color",
        "Apple_Terminal",
        "VTE(7800)",
        "konsole",
        "",
        "vt100",
        "unknown",
    ] {
        for term in ["", "dumb"] {
            let caps = ProbeReplies {
                identity: Some(identity.into()),
                ..ProbeReplies::default()
            }
            .capabilities(&Environment([("TERM".into(), term.into())].into()));
            assert_eq!(
                caps.scroll_regions,
                term != "dumb" && ["alacritty", "tmux", "screen"].contains(&identity),
                "{identity} {term}"
            );
        }
    }
}
