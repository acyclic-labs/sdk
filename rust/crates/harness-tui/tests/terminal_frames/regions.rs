use super::*;
use acyclic_harness_tui::term::RegionScroll;
use std::{cell::Cell as Count, time::Instant};

fn one_native_submission(calls: u64, writes: u64, frames: usize, idle: bool) {
    assert_eq!(
        calls,
        if idle { 0 } else { frames as u64 },
        "exact native frame submissions"
    );
    #[cfg(windows)]
    assert_eq!(writes, calls, "one native syscall per submitted frame");
    #[cfg(unix)]
    if idle {
        assert_eq!(writes, 0, "idle frames never enter the kernel");
    } else {
        // PTYs can legally split writes or fill between readiness and write.
        assert!(writes >= calls, "every submitted frame reaches the kernel");
    }
}

fn scene(offset: usize) -> impl FnOnce(&mut Frame<'_>) {
    move |frame| {
        let lines: Vec<Line<'_>> = (offset..offset + usize::from(frame.area().height))
            .map(|row| Line::raw(format!("row{row:04}")))
            .collect();
        frame.render_widget(Paragraph::new(lines), frame.area());
        frame.set_cursor_position((2, frame.area().height - 1));
    }
}

/// Native kernel/device cost, paired with the independently initialized old
/// terminal. Run in ConPTY on Windows and a drained kernel PTY on Unix.
#[test]
#[ignore = "physical terminal measurement: run via the platform host"]
fn real_terminal_region_cost() -> io::Result<()> {
    let size = Size::new(100, 40);
    let area = Rect::new(0, 0, size.width, size.height);
    let (mut output, _custody) = real_terminal()?;
    assert!(
        output.supports_scroll_regions(),
        "native profile must enable the measured path"
    );
    let mut report = String::new();
    // Keep even the intentionally unbuffered old Windows path below the suite
    // ceiling. Cell-work properties independently cover 4000/8000-cell screens.
    for frames in [2, 4] {
        for path in ["per-command", "frame", "regions"] {
            output.write_all(b"\x1b[0m\x1b[r\x1b[2J")?;
            let calls = Rc::new(Count::new(0));
            let bytes = Rc::new(Count::new(0));
            let device = Calls(&mut output, Rc::clone(&calls), Rc::clone(&bytes));
            let mut old = None;
            let mut new = None;
            if path == "per-command" {
                let mut terminal = Terminal::with_options(
                    CrosstermBackend::new(device),
                    TerminalOptions {
                        viewport: Viewport::Fixed(area),
                    },
                )?;
                terminal.draw(scene(0))?;
                old = Some(terminal);
            } else {
                let mut terminal = FrameTerminal::new(device, size)?;
                terminal.draw(size, scene(0))?;
                new = Some(terminal);
            }
            for idle in [false, true] {
                calls.set(0);
                bytes.set(0);
                let before = device_io();
                let start = Instant::now();
                let mut rendered = Ok(());
                let allocations = acyclic_test_support::measure(|| {
                    rendered = (|| -> io::Result<()> {
                        for step in 1..=frames {
                            let offset = if idle { frames } else { step };
                            if let Some(terminal) = old.as_mut() {
                                terminal.draw(scene(offset))?;
                            }
                            if let Some(terminal) = new.as_mut() {
                                let hint = (path == "regions" && !idle).then_some(RegionScroll {
                                    region: area,
                                    rows: 1,
                                });
                                terminal.draw_scrolled(size, hint, scene(offset))?;
                            }
                        }
                        Ok(())
                    })();
                });
                rendered?;
                let elapsed = start.elapsed();
                let after = device_io();
                let writes = after.writes - before.writes;
                if path == "per-command" {
                    let calls = calls.get();
                    assert!(
                        std::panic::catch_unwind(|| one_native_submission(
                            calls, writes, frames, idle
                        ))
                        .is_err(),
                        "the independent old device path must fail the same syscall oracle"
                    );
                } else {
                    one_native_submission(calls.get(), writes, frames, idle);
                }
                use std::fmt::Write as _;
                writeln!(report, "{} {path} frames={frames} idle={idle} bytes={} submissions={} writes={} waits={} allocations={} allocated_bytes={} us_per_frame={:.3}",
                    std::env::consts::OS, bytes.get(), calls.get(), after.writes-before.writes, after.waits-before.waits,
                    allocations.count_total, allocations.bytes_total, elapsed.as_secs_f64()*1e6/frames as f64).unwrap();
            }
        }
    }
    eprint!("{report}");
    if let Some(path) = std::env::var_os("ACYCLIC_TERMINAL_REGION_REPORT") {
        std::fs::write(path, report)?;
    }
    Ok(())
}
