use super::*;
use std::cell::Cell;

struct Cut {
    device: Device,
    at: Rc<Cell<Option<usize>>>,
    failures: Rc<Cell<usize>>,
    panic: bool,
}
impl Write for Cut {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if let Some(at) = self.at.get() {
            let room = at.saturating_sub(self.device.0.borrow().len());
            if room == 0 {
                self.failures.set(self.failures.get() + 1);
                if self.panic {
                    std::panic::resume_unwind(Box::new("injected region writer panic"));
                }
                return Err(io::ErrorKind::BrokenPipe.into());
            }
            return self.device.write(&bytes[..room.min(bytes.len())]);
        }
        self.device.write(bytes)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct Observed<P> {
    device: P,
    effects: Rc<RefCell<Vec<FrameEffects>>>,
}
impl<P: Present> Present for Observed<P> {
    fn supports_scroll_regions(&self) -> bool {
        self.device.supports_scroll_regions()
    }
    fn outside_writes(&mut self) -> Option<u64> {
        self.device.outside_writes()
    }
    fn present(&mut self, frame: &mut Vec<u8>, effects: FrameEffects) -> io::Result<()> {
        self.effects.borrow_mut().push(effects);
        self.device.present(frame, effects)
    }
    fn present_if_current(
        &mut self,
        frame: &mut Vec<u8>,
        effects: FrameEffects,
        expected: Option<u64>,
    ) -> io::Result<Option<u64>> {
        self.effects.borrow_mut().push(effects);
        self.device.present_if_current(frame, effects, expected)
    }
}

fn probe() -> Vec<u8> {
    // A full-screen line feed must remove Z from the first row. The footer
    // also lets the native oracle acknowledge consumption of the entire probe.
    format!("\x1b[1;1HZ\x1b[{HEIGHT};1H\nfixed footer").into_bytes()
}

fn recovered(panic: bool, at: usize) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
    let size = Size::new(WIDTH, HEIGHT);
    let device = Device::default();
    let limit = Rc::new(Cell::new(None));
    let failures = Rc::new(Cell::new(0));
    let effects = Rc::new(RefCell::new(Vec::new()));
    let guard = RestoreGuard::new(
        Cut {
            device: device.clone(),
            at: limit.clone(),
            failures: failures.clone(),
            panic,
        },
        &region_caps(true),
    );
    let mut terminal = FrameTerminal::new(
        Observed {
            device: guard,
            effects: effects.clone(),
        },
        size,
    )
    .unwrap();
    terminal
        .draw(size, |frame| paint(frame, &window(0, HEIGHT)))
        .unwrap();
    let initial = device.take();
    limit.set(Some(at));
    let result = catch_unwind(AssertUnwindSafe(|| {
        terminal.draw_scrolled(
            size,
            Some(RegionScroll {
                region: Rect::new(0, 2, WIDTH, 4),
                rows: 1,
            }),
            |frame| paint(frame, &window(1, HEIGHT)),
        )
    }));
    if panic {
        assert!(result.is_err());
        assert_eq!(failures.get(), 1);
    } else {
        assert_eq!(
            result.unwrap().unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
        assert!(failures.get() >= 2, "both the frame and abort must fail");
    }
    let partial = device.take();
    assert!(partial.starts_with(b"\x1b[3;6r"));
    limit.set(None);
    terminal
        .draw(size, |frame| paint(frame, &window(1, HEIGHT)))
        .unwrap();
    let recovery = device.take();
    assert!(
        effects.borrow().last().unwrap().margins,
        "recovery must journal its margin reset"
    );
    let clear = recovery.windows(4).position(|b| b == b"\x1b[2J").unwrap();
    assert!(
        recovery[..clear].windows(3).any(|b| b == b"\x1b[r"),
        "reset before repaint"
    );
    let mut control = Vec::new();
    let mut offset = 0;
    while offset < recovery.len() {
        if recovery[offset..].starts_with(b"\x1b[r") {
            offset += 3;
        } else {
            control.push(recovery[offset]);
            offset += 1;
        }
    }
    // Observe before terminal/guard Drop can repair the device and mask a bug.
    let bytes = [initial.clone(), partial.clone(), recovery, probe()].concat();
    let control = [initial, partial, control, probe()].concat();
    let expected = [per_command(&[window(1, HEIGHT)]).concat(), probe()].concat();
    (bytes, control, expected)
}

#[test]
fn failed_abort_and_writer_unwind_reset_margins_before_same_size_repaint() {
    for panic in [false, true] {
        // Every cut after DECSTBM and through the following SU command.
        for at in b"\x1b[3;6r".len()..=b"\x1b[3;6r\x1b[1S".len() {
            let (bytes, control, expected) = recovered(panic, at);
            assert_eq!(
                screens(&[bytes]),
                screens(&[expected.clone()]),
                "panic={panic}, at={at}"
            );
            assert_ne!(
                screens(&[control]),
                screens(&[expected]),
                "omitting reset must fail the same oracle"
            );
        }
    }
}

#[cfg(target_os = "linux")]
#[test]
fn native_tmux_observes_recovered_margins_after_failed_abort_and_writer_unwind() {
    for panic in [false, true] {
        let (bytes, control, expected) = recovered(panic, b"\x1b[3;6r".len());
        let expected = native_profiles::linux::capture("tmux", &expected);
        assert_eq!(native_profiles::linux::capture("tmux", &bytes), expected);
        assert_ne!(
            native_profiles::linux::capture("tmux", &control),
            expected,
            "omitting reset must fail the native screen oracle"
        );
    }
}

#[test]
fn abandoning_a_failed_recovery_does_not_discharge_unwritten_margin_effects() {
    for panic in [false, true] {
        for abandon_render in [false, true] {
            let device = Device::default();
            let at = Rc::new(Cell::new(None));
            let effects = Rc::new(RefCell::new(Vec::new()));
            let mut guard = RestoreGuard::new(
                Cut {
                    device: device.clone(),
                    at: at.clone(),
                    failures: Rc::new(Cell::new(0)),
                    panic,
                },
                &region_caps(true),
            );
            let size = Size::new(WIDTH, HEIGHT);
            let mut terminal = FrameTerminal::new(
                Observed {
                    device: &mut guard,
                    effects: effects.clone(),
                },
                size,
            )
            .unwrap();
            terminal
                .draw(size, |frame| paint(frame, &window(0, HEIGHT)))
                .unwrap();
            at.set(Some(device.0.borrow().len() + b"\x1b[3;6r".len()));
            let failed = catch_unwind(AssertUnwindSafe(|| {
                terminal.draw_scrolled(
                    size,
                    Some(RegionScroll {
                        region: Rect::new(0, 2, WIDTH, 4),
                        rows: 1,
                    }),
                    |frame| paint(frame, &window(1, HEIGHT)),
                )
            }));
            assert!(failed.is_err() || failed.unwrap().is_err());
            at.set(None);
            if abandon_render {
                assert!(
                    catch_unwind(AssertUnwindSafe(|| {
                        terminal.draw(size, |_| {
                            std::panic::resume_unwind(Box::new("render panic"))
                        })
                    }))
                    .is_err()
                );
            }
            drop(terminal);
            assert!(
                !effects.borrow().last().unwrap().margins,
                "a cursor-only teardown must not claim a lost reset"
            );
            let before = device.take();
            guard.restore().unwrap();
            let restored = [before.clone(), device.take()].concat();
            assert!(scrolls_whole_screen(&restored));
            assert!(
                !scrolls_whole_screen(&before),
                "losing the journal must fail the screen oracle"
            );
        }
    }
}
