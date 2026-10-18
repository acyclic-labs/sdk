use super::*;
use alacritty_terminal::vte::{Params, Parser, Perform};

const PROTOCOLS: [u16; 5] = [9, 1000, 1001, 1002, 1003];
const ENCODINGS: [u16; 4] = [1005, 1006, 1015, 1016];

fn mutable_modes() -> Capabilities {
    let mut caps = Capabilities::default();
    for mode in [1004, 2004, 2027, 1049]
        .into_iter()
        .chain(PROTOCOLS)
        .chain(ENCODINGS)
    {
        caps.modes.insert(mode, ModeState::Reset);
    }
    caps
}

#[test]
fn alternate_setup_owns_each_undo_without_copying() {
    let caps = mutable_modes();
    let mut bytes = Vec::with_capacity(256);
    let mut guard = RestoreGuard::new(&mut bytes, &caps);
    let allocation = acyclic_test_support::measure(|| guard.setup(ScreenMode::Alternate).unwrap());
    guard.restore().unwrap();
    drop(guard);
    assert_eq!(
        bytes,
        b"\x1b[?1004h\x1b[?2004h\x1b[?2027h\x1b[?1049h\x1b[?1006h\x1b[?1002h\x18\x1b[?1002l\x18\x1b[?1006l\x18\x1b[?1049l\x18\x1b[?2027l\x18\x1b[?2004l\x18\x1b[?1004l"
    );
    assert_restored(0, 0, &bytes);
    assert_eq!(
        (allocation.count_total, allocation.bytes_total),
        (16, 504),
        "alternate setup never copies an owned undo"
    );
}

#[test]
fn warmed_nested_switches_reuse_and_compact_the_owned_journal() {
    for count in [32, 64] {
        let caps = mutable_modes();
        let mut bytes = Vec::with_capacity(4096);
        let mut guard = RestoreGuard::new(&mut bytes, &caps);
        guard.enable_mode(1006).unwrap();
        guard.enable_mode(1016).unwrap();
        // The second switch grows the pending union once. Later switches reuse
        // its capacity; only the existing device SET formatting allocates.
        let allocation = acyclic_test_support::measure(|| {
            for mode in [1005, 1015, 1006, 1016].into_iter().cycle().take(count) {
                guard.enable_mode(mode).unwrap();
            }
        });
        guard.restore().unwrap();
        drop(guard);
        let mut expected = b"\x1b[?1006h\x1b[?1016h".to_vec();
        for mode in [1005, 1015, 1006, 1016].into_iter().cycle().take(count) {
            write!(expected, "\x1b[?{mode}h").unwrap();
        }
        expected.extend_from_slice(b"\x18\x1b[?1016l");
        assert_eq!(bytes, expected);
        assert_restored(0, 0, &bytes);
        assert_eq!(
            (allocation.count_total, allocation.bytes_total),
            (count as u64, 8 * count as u64),
            "nested switches allocate only their device SET"
        );
    }
}

// The xterm spec defines two exclusive selections. Alacritty's model does not
// implement every encoding, so drive this independent state with its VTE parser.
#[derive(Default, Debug, PartialEq, Eq)]
struct Mouse {
    protocol: u16,
    encoding: u16,
}
impl Perform for Mouse {
    fn csi_dispatch(&mut self, params: &Params, intermediate: &[u8], ignore: bool, action: char) {
        if ignore || intermediate != b"?" || !matches!(action, 'h' | 'l') {
            return;
        }
        for parameter in params {
            let Some(&mode) = parameter.first() else {
                continue;
            };
            let target = if PROTOCOLS.contains(&mode) {
                &mut self.protocol
            } else if ENCODINGS.contains(&mode) {
                &mut self.encoding
            } else {
                continue;
            };
            if action == 'h' {
                *target = mode;
            } else if *target == mode {
                *target = 0;
            }
        }
    }
}

fn output(protocol: u16, encoding: u16, fail: usize, nested: bool) -> Vec<u8> {
    let mut caps = Capabilities::default();
    for mode in [2027, 1004, 2004, 1049]
        .into_iter()
        .chain(PROTOCOLS)
        .chain(ENCODINGS)
    {
        caps.modes.insert(
            mode,
            if mode == protocol || mode == encoding {
                ModeState::Set
            } else {
                ModeState::Reset
            },
        );
    }
    let mut writer = FailOnce {
        bytes: Vec::new(),
        at: fail,
        failed: false,
    };
    {
        let mut guard = RestoreGuard::new(&mut writer, &caps);
        if guard.setup(ScreenMode::Alternate).is_ok() && nested {
            for mode in [1003, 1016, 9, 1005, 1001, 1015] {
                if guard.enable_mode(mode).is_err() {
                    assert_restored(protocol, encoding, &guard.writer().bytes);
                    break;
                }
            }
            if guard.restore().is_ok() {
                let _ = guard.setup(ScreenMode::Alternate);
            }
        }
    }
    writer.bytes
}

#[test]
fn nested_switch_failure_restores_before_any_later_setup() {
    for group in [&PROTOCOLS[..], &ENCODINGS[..]] {
        for &first in group {
            for &second in group {
                if first == second {
                    continue;
                }
                for offset in 0..=format!("\x1b[?{second}h").len() {
                    let mut caps = Capabilities::default();
                    for &mode in group {
                        caps.modes.insert(mode, ModeState::Reset);
                    }
                    let mut writer = FailOnce {
                        bytes: Vec::new(),
                        at: usize::MAX,
                        failed: false,
                    };
                    {
                        let mut guard = RestoreGuard::new(&mut writer, &caps);
                        guard.enable_mode(first).unwrap();
                        let at = guard.writer().bytes.len() + offset;
                        guard.writer().at = at;
                        if guard.enable_mode(second).is_ok() {
                            guard.restore().ok();
                            // A failure during explicit restoration is retried.
                            guard.restore().unwrap();
                        }
                        assert_restored(0, 0, &guard.writer().bytes);
                    }
                    assert_restored(0, 0, &writer.bytes);
                }
            }
        }
    }
}

#[test]
#[should_panic(expected = "both mouse selections must be restored")]
fn negative_control_replacing_pending_selection_before_set_completes() {
    let mut caps = Capabilities::default();
    for mode in ENCODINGS {
        caps.modes.insert(mode, ModeState::Reset);
    }
    let mut writer = FailOnce {
        bytes: Vec::new(),
        at: usize::MAX,
        failed: false,
    };
    let mut guard = RestoreGuard::new(&mut writer, &caps);
    guard.enable_mode(1006).unwrap();
    guard.writer().at = guard.writer().bytes.len();
    assert!(guard.enable_mode(1016).is_err());
    let mut bytes = guard.writer().bytes.clone();
    // Reintroduce the old journal's premature replacement in the real output.
    let reset = b"\x1b[?1006l";
    let at = bytes.windows(reset.len()).position(|b| b == reset).unwrap();
    bytes.drain(at..at + reset.len());
    assert_restored(0, 0, &bytes);
}

fn assert_restored(protocol: u16, encoding: u16, bytes: &[u8]) {
    let mut state = Mouse { protocol, encoding };
    let mut parser: Parser = Parser::new();
    parser.advance(&mut state, bytes);
    assert_eq!(
        state,
        Mouse { protocol, encoding },
        "both mouse selections must be restored"
    );
}

#[test]
fn every_mouse_selection_and_failure_boundary_restores() {
    for protocol in std::iter::once(0).chain(PROTOCOLS) {
        for encoding in std::iter::once(0).chain(ENCODINGS) {
            for nested in [false, true] {
                let complete = output(protocol, encoding, usize::MAX, nested);
                assert_restored(protocol, encoding, &complete);
                for fail in 0..=complete.len() {
                    assert_restored(
                        protocol,
                        encoding,
                        &output(protocol, encoding, fail, nested),
                    );
                }
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    #[test]
    fn arbitrary_displaced_selection_and_partial_write_restore(
        protocol in prop::sample::select(vec![0u16,9,1000,1001,1002,1003]),
        encoding in prop::sample::select(vec![0u16,1005,1006,1015,1016]),
        fail in 0usize..256, nested in any::<bool>(),
    ) {
        assert_restored(protocol, encoding, &output(protocol, encoding, fail, nested));
    }
}

#[test]
#[should_panic(expected = "both mouse selections must be restored")]
fn negative_control_resetting_only_sgr_loses_the_displaced_encoding() {
    let mut bytes = output(1000, 1016, usize::MAX, false);
    let undo = b"\x1b[?1016h";
    let at = bytes
        .windows(undo.len())
        .position(|bytes| bytes == undo)
        .unwrap();
    bytes.splice(at..at + undo.len(), b"\x1b[?1006l".iter().copied());
    assert_restored(1000, 1016, &bytes);
}

#[test]
fn an_unknown_or_permanent_selection_prevents_displacement() {
    for unknown in PROTOCOLS.into_iter().chain(ENCODINGS) {
        if [1002, 1006].contains(&unknown) {
            continue;
        }
        for state in [ModeState::Unknown, ModeState::AlwaysSet] {
            let mut caps = Capabilities::default();
            for mode in std::iter::once(1049).chain(PROTOCOLS).chain(ENCODINGS) {
                caps.modes.insert(mode, ModeState::Reset);
            }
            caps.modes.insert(unknown, state);
            let mut bytes = Vec::new();
            RestoreGuard::new(&mut bytes, &caps)
                .setup(ScreenMode::Alternate)
                .unwrap();
            assert_eq!(
                bytes, b"\x1b[?1049h\x18\x1b[?1049l",
                "unknown={unknown} state={state:?}"
            );
        }
    }
}

#[test]
fn an_already_selected_encoding_needs_no_displacement_or_reselection() {
    for encoding in [ModeState::Set, ModeState::AlwaysSet] {
        let mut caps = Capabilities::default();
        caps.modes.insert(1049, ModeState::Reset);
        for mode in PROTOCOLS {
            caps.modes.insert(mode, ModeState::Reset);
        }
        // Other encoding states are unknown, but switching is unnecessary:
        // the selected SGR encoding already proves its exclusive selection.
        caps.modes.insert(1006, encoding);
        let mut bytes = Vec::new();
        RestoreGuard::new(&mut bytes, &caps)
            .setup(ScreenMode::Alternate)
            .unwrap();
        assert_eq!(
            bytes,
            b"\x1b[?1049h\x1b[?1002h\x18\x1b[?1002l\x18\x1b[?1049l"
        );
        assert_restored(0, 1006, &bytes);
    }
}

fn assert_complete_queries(bytes: &[u8]) {
    for mode in PROTOCOLS.into_iter().chain(ENCODINGS) {
        let query = format!("\x1b[?{mode}$p");
        assert_eq!(
            bytes
                .windows(query.len())
                .filter(|bytes| *bytes == query.as_bytes())
                .count(),
            1,
            "every exclusive mouse selection is queried exactly once"
        );
    }
}
#[test]
fn negotiation_queries_every_exclusive_selection() {
    assert_complete_queries(&Probe::batch(Multiplexer::None));
}
#[test]
#[should_panic(expected = "every exclusive mouse selection is queried exactly once")]
fn negative_control_omitting_a_displaced_encoding_query_is_detected() {
    let mut batch = Probe::batch(Multiplexer::None);
    let query = b"\x1b[?1016$p";
    let at = batch
        .windows(query.len())
        .position(|bytes| bytes == query)
        .unwrap();
    batch.drain(at..at + query.len());
    assert_complete_queries(&batch);
}
