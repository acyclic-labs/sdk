//! Production negotiation under reordered replies, failed sends and short reads.
#![cfg(feature = "tui")]

use acyclic_harness_tui::term::Multiplexer;
use alacritty_terminal::event::EventListener;
use alacritty_terminal::grid::Dimensions;
use alacritty_terminal::index::{Column, Line, Point};
use alacritty_terminal::term::{Config, Term};
use alacritty_terminal::vte::ansi::Processor;
use proptest::prelude::*;
use proptest::test_runner::{Config as PropertyConfig, TestRunner};

#[path = "support/negotiation.rs"]
mod negotiation;

struct Size;
impl Dimensions for Size {
    fn total_lines(&self) -> usize {
        4
    }
    fn screen_lines(&self) -> usize {
        4
    }
    fn columns(&self) -> usize {
        80
    }
}
struct Events;
impl EventListener for Events {}

fn visible_prompt(bytes: &[u8], mux: Multiplexer, at: usize) {
    let mut term = Term::new(Config::default(), &Size, Events);
    let mut parser: Processor = Processor::new();
    parser.advance(&mut term, bytes);
    assert_eq!(
        term.grid()[Point::new(Line(0), Column(0))].c,
        'Z',
        "{mux:?} failure={at}"
    );
}

#[test]
fn every_query_send_failure_boundary_is_cancelled_and_leaves_a_visible_prompt() {
    for mux in [Multiplexer::None, Multiplexer::Tmux, Multiplexer::Screen] {
        for at in 0..=negotiation::batch_len(mux) {
            let mut bytes = negotiation::run(mux, &[], 100, Some(at), 0);
            bytes.push(b'Z');
            visible_prompt(&bytes, mux, at);
        }
    }
}

#[cfg(unix)]
#[test]
fn real_pty_query_failure_boundaries_restore_a_visible_prompt()
-> Result<(), Box<dyn std::error::Error>> {
    use acyclic_harness_tui::term::{Environment, ProbeIo, Tty, detect};
    use rustix::fs::{OFlags, fcntl_getfl, fcntl_setfl};
    use std::fs::File;
    use std::io::{self, Read, Write};
    use std::time::Instant;
    struct FailSend<'a> {
        tty: &'a mut Tty,
        at: Option<usize>,
    }
    impl ProbeIo for FailSend<'_> {
        fn send(&mut self, bytes: &[u8], deadline: Instant) -> io::Result<()> {
            if let Some(at) = self.at.take() {
                self.tty.send(&bytes[..at], deadline)?;
                Err(io::ErrorKind::BrokenPipe.into())
            } else {
                self.tty.send(bytes, deadline)
            }
        }
        fn receive(&mut self, bytes: &mut [u8], deadline: Instant) -> io::Result<usize> {
            self.tty.receive(bytes, deadline)
        }
    }
    for mux in [Multiplexer::None, Multiplexer::Tmux, Multiplexer::Screen] {
        for at in 0..=negotiation::batch_len(mux) {
            let pty = rustix_openpty::openpty(None, None)?;
            let mut controller = File::from(pty.controller);
            fcntl_setfl(&controller, fcntl_getfl(&controller)? | OFlags::NONBLOCK)?;
            let mut observer = File::from(pty.user);
            let mut tty = Tty::from_file(observer.try_clone()?)?;
            let key = match mux {
                Multiplexer::None => "TERM",
                Multiplexer::Tmux => "TMUX",
                Multiplexer::Screen => "STY",
            };
            let env = Environment([(key.into(), "session".into())].into());
            let mut fail = FailSend {
                tty: &mut tty,
                at: Some(at),
            };
            assert!(
                matches!(detect(&mut fail, &env), Err(error) if error.kind() == io::ErrorKind::BrokenPipe)
            );
            drop(tty);
            observer.write_all(b"Z")?;
            let mut bytes = Vec::new();
            let mut buffer = [0; 1024];
            loop {
                match controller.read(&mut buffer) {
                    Ok(0) => break,
                    Ok(count) => bytes.extend_from_slice(&buffer[..count]),
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                    Err(error) => return Err(error.into()),
                }
            }
            visible_prompt(&bytes, mux, at);
        }
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn negotiation_is_invariant_under_ordering_interleaving_failure_and_read_limits(
        ranks in any::<[u8; 8]>(), read_max in 1usize..2048, screen in any::<bool>(), failure in prop::option::of(any::<u16>()),
    ) {
        let mux = if screen { Multiplexer::Screen } else { Multiplexer::Tmux };
        let failure = failure.map(|at| usize::from(at) % (negotiation::batch_len(mux) + 1));
        negotiation::run(mux, &ranks, read_max, failure, 0);
    }
}

#[test]
fn negative_controls_detect_early_completion_missing_cancel_and_overread() {
    for broken in 1..=3 {
        let config = PropertyConfig {
            cases: 8,
            failure_persistence: None,
            ..PropertyConfig::default()
        };
        let witness = Just(if broken == 2 { Some(50usize) } else { None });
        assert!(
            TestRunner::new(config)
                .run(&witness, |failure| {
                    negotiation::run(Multiplexer::Tmux, &[], 100, failure, broken);
                    Ok(())
                })
                .is_err()
        );
    }
}
