//! Terminal profiles, decoder properties, restoration and PTY transport.
#![cfg(feature = "tui")]

use std::io::{self, Write};
use std::time::Instant;

use acyclic_harness_tui::key_event::{InputEvent, KeyCode, KeyEvent, KeyModifiers};
use acyclic_harness_tui::term::*;
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

impl Profile {
    fn replies(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        if !self.identity.is_empty() {
            bytes.extend_from_slice(format!("\x1bP>|{}\x1b\\", self.identity).as_bytes());
        }
        if self.kitty {
            bytes.extend_from_slice(b"\x1b[?0u");
        }
        for mode in [2026, 2027, 1004, 2004, 1049] {
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
    let profiles: Vec<Profile> = serde_json::from_str(include_str!("profiles.json"))?;
    assert_eq!(profiles.len(), 14);
    for profile in profiles {
        let mut responder = Script {
            sent: Vec::new(),
            replies: profile.replies(),
            reads: 0,
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
                format!("{:?}", caps.images),
                profile.image,
                "{}",
                profile.name
            );
            assert_eq!(
                format!("{:?}", caps.keyboard),
                profile.keyboard,
                "{}",
                profile.name
            );
            assert_eq!(
                format!("{:?}", caps.colour),
                profile.colour,
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
                expected.extend_from_slice(b"\x1b[?1049h");
            }
            expected.extend_from_slice(b"hello");
            if profile.modes {
                expected.extend_from_slice(b"\x18\x1b[?1049l");
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
                TermMode::ALT_SCREEN | TermMode::FOCUS_IN_OUT | TermMode::BRACKETED_PASTE
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
}
impl ProbeIo for Script {
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
fn environment_fallback_ladders_and_policy() {
    let cases = [
        (
            vec![("TERM", "linux")],
            ColourDepth::Ansi16,
            KeyboardProtocol::Legacy,
        ),
        (
            vec![("TERM", "xterm-256color")],
            ColourDepth::Indexed256,
            KeyboardProtocol::ModifyOtherKeys,
        ),
        (
            vec![("COLORTERM", "24bit")],
            ColourDepth::TrueColour,
            KeyboardProtocol::Legacy,
        ),
        (
            vec![("TERM_PROGRAM", "WezTerm")],
            ColourDepth::TrueColour,
            KeyboardProtocol::Kitty,
        ),
        (
            vec![("TERM_PROGRAM", "WezTerm"), ("TERM", "xterm-256color")],
            ColourDepth::TrueColour,
            KeyboardProtocol::Kitty,
        ),
        (
            vec![("WT_SESSION", "session"), ("TERM", "xterm-256color")],
            ColourDepth::TrueColour,
            KeyboardProtocol::Legacy,
        ),
        (
            vec![("KITTY_WINDOW_ID", "1"), ("TERM", "screen-256color")],
            ColourDepth::TrueColour,
            KeyboardProtocol::Kitty,
        ),
        (
            vec![("TERM", "screen-256color"), ("COLORTERM", "truecolor")],
            ColourDepth::TrueColour,
            KeyboardProtocol::Legacy,
        ),
        (
            vec![("WT_SESSION", "session")],
            ColourDepth::TrueColour,
            KeyboardProtocol::Legacy,
        ),
        (
            vec![("KITTY_WINDOW_ID", "1")],
            ColourDepth::TrueColour,
            KeyboardProtocol::Kitty,
        ),
        (
            vec![("TERM_PROGRAM", "kitty"), ("NO_COLOR", "1")],
            ColourDepth::Monochrome,
            KeyboardProtocol::Kitty,
        ),
    ];
    for (variables, colour, keyboard) in cases {
        let env = Environment(
            variables
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        );
        let caps = ProbeReplies::default().capabilities(&env);
        assert_eq!(caps.colour, colour);
        assert_eq!(caps.keyboard, keyboard);
    }
    let env = Environment(
        [
            ("STY".into(), "screen".into()),
            ("SSH_CONNECTION".into(), "remote".into()),
        ]
        .into(),
    );
    let caps = ProbeReplies::default().capabilities(&env);
    assert!(caps.remote);
    assert_eq!(caps.multiplexer, Multiplexer::Screen);
    assert!(!caps.passthrough);
}

#[test]
fn profile_environment_colour_evidence_matrix() -> Result<(), Box<dyn std::error::Error>> {
    let profiles: Vec<Profile> = serde_json::from_str(include_str!("profiles.json"))?;
    for profile in profiles {
        let base = Environment([("TERM_PROGRAM".into(), profile.identity.clone())].into());
        let known = ProbeReplies::default().capabilities(&base).colour;
        for term in ["ansi", "xterm-256color", "screen-256color"] {
            for colorterm in ["", "truecolor", "24bit"] {
                let mut env = base.clone();
                env.0.insert("TERM".into(), term.into());
                env.0.insert("COLORTERM".into(), colorterm.into());
                let expected = if !colorterm.is_empty() {
                    ColourDepth::TrueColour
                } else if term.contains("256color") {
                    known.max(ColourDepth::Indexed256)
                } else {
                    known
                };
                assert_eq!(
                    ProbeReplies::default().capabilities(&env).colour,
                    expected,
                    "{} TERM={term} COLORTERM={colorterm}",
                    profile.name
                );
            }
        }
    }
    Ok(())
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
#[test]
fn controlling_tty_probe_and_os_state_restore() -> Result<(), Box<dyn std::error::Error>> {
    use rustix::fs::{OFlags, fcntl_getfl};
    use rustix::termios::{LocalModes, tcgetattr};
    let pty = rustix_openpty::openpty(None, None)?;
    let mut controller = std::fs::File::from(pty.controller);
    let user = std::fs::File::from(pty.user);
    let observer = user.try_clone()?;
    let original = tcgetattr(&observer)?;
    let original_flags = fcntl_getfl(&observer)?;
    {
        let mut tty = Tty::from_file(user)?;
        assert!(
            !tcgetattr(&observer)?
                .local_modes
                .contains(LocalModes::ICANON)
        );
        assert!(fcntl_getfl(&observer)?.contains(OFlags::NONBLOCK));
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
    assert_eq!(fcntl_getfl(&observer)?, original_flags);
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

fn restored_modes(initial: u8, at: usize) -> (TermMode, TermMode) {
    let mut caps = Capabilities::default();
    let mut original = Vec::new();
    for (index, mode) in [1004, 2004, 1049].into_iter().enumerate() {
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
    for initial in 0..8 {
        for at in 0..65 {
            let (before, after) = restored_modes(initial, at);
            assert_eq!(before, after, "initial={initial} failure_at={at}");
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]

    // Adding weaker TERM evidence cannot reduce a known colour capability.
    #[test]
    fn term_fallback_colour_is_monotone(
        program in prop::sample::select(vec!["WezTerm", "kitty", "ghostty", "iTerm2", "alacritty", "foot", "vscode", "Apple_Terminal", "tmux", "screen", ""]),
        term in prop::sample::select(vec!["ansi", "xterm-256color", "screen-256color", "tmux-256color"]),
        colorterm in prop::sample::select(vec!["", "truecolor", "24bit"]),
    ) {
        let mut env = Environment([("TERM_PROGRAM".into(), program.into())].into());
        let known = ProbeReplies::default().capabilities(&env).colour;
        env.0.insert("TERM".into(), term.into());
        env.0.insert("COLORTERM".into(), colorterm.into());
        prop_assert!(ProbeReplies::default().capabilities(&env).colour >= known);
    }

    #[test]
    fn restore_after_arbitrary_failure(initial in 0u8..8, at in 0usize..65) {
        let (before, after) = restored_modes(initial, at);
        prop_assert_eq!(after, before);
    }

    #[test]
    fn reply_round_trip_and_chunk_invariance(mode in prop_oneof![Just(2026u16), Just(2027), Just(1004), Just(2004)],
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
