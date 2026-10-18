use super::*;
use std::path::Path;

/// Independent instances start with a complete frame, then move a partial
/// band in both directions. Each final row is distinct, including the fixed
/// header/footer; an omitted scroll cannot match by repainting exposed rows.
fn fixtures() -> Vec<(Vec<u8>, Vec<String>)> {
    let size = Size::new(WIDTH, HEIGHT);
    let band = Rect::new(0, 1, WIDTH, HEIGHT - 2);
    let scene = |top: usize| {
        let mut rows = vec![Line::raw("fixed header")];
        rows.extend((top..top + usize::from(band.height)).map(|n| Line::raw(format!("row{n:04}"))));
        rows.push(Line::raw("fixed footer"));
        (rows, Some((2, HEIGHT - 1)))
    };
    [(0, 1), (0, 2), (4, 3), (4, 2)]
        .into_iter()
        .map(|(from, to)| {
            let last = scene(to);
            let expected = last
                .0
                .iter()
                .map(|line| {
                    let text: String = line
                        .spans
                        .iter()
                        .map(|span| span.content.as_ref())
                        .collect();
                    format!("{text:<width$}", width = usize::from(WIDTH))
                })
                .collect();
            let mut initial = scene(from);
            *initial.0.last_mut().unwrap() = Line::raw("initial footer");
            let frames = scrolled(
                &[
                    (initial, None),
                    (
                        last,
                        Some(RegionScroll {
                            region: band,
                            rows: to as isize - from as isize,
                        }),
                    ),
                ],
                size,
            );
            (frames.concat(), expected)
        })
        .collect()
}

/// Shared production bytes for local Windows ReadConsoleOutput and the native
/// Linux terminal oracles. This exports fixtures only; it makes no OS claim.
#[test]
#[ignore = "invoked by scripts/test-terminal-region-screen.ps1"]
fn export_windows_screen_fixtures() {
    let root = std::env::var_os("ACYCLIC_REGION_SCREEN_FIXTURES").expect("fixture directory");
    let root = Path::new(&root);
    std::fs::create_dir_all(root).unwrap();
    for (index, (bytes, expected)) in fixtures().into_iter().enumerate() {
        std::fs::write(root.join(format!("{index}.bin")), &bytes).unwrap();
        std::fs::write(
            root.join(format!("{index}-control.bin")),
            without_scrolling(&bytes),
        )
        .unwrap();
        std::fs::write(
            root.join(format!("{index}.json")),
            serde_json::to_vec(&expected).unwrap(),
        )
        .unwrap();
    }
}

#[cfg(target_os = "linux")]
pub(super) mod linux {
    use super::*;
    use std::process::Command;
    use std::time::{Duration, Instant};

    struct Session<'a> {
        name: String,
        tool: &'a str,
    }
    impl Drop for Session<'_> {
        fn drop(&mut self) {
            let mut command = Command::new(self.tool);
            if self.tool == "tmux" {
                command.args(["-L", &self.name, "kill-server"]);
            } else {
                command.args(["-S", &self.name, "-X", "quit"]);
            }
            let _ = command.output();
        }
    }
    fn checked(command: &mut Command) -> Vec<u8> {
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "{command:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        output.stdout
    }
    pub(crate) fn capture(tool: &str, bytes: &[u8]) -> Vec<String> {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("frame.bin");
        let dump = dir.path().join("screen.txt");
        let ready = dir.path().join("ready");
        let start = dir.path().join("start");
        let config = dir.path().join("tmux.conf");
        std::fs::write(&config, "set -g status off\n").unwrap();
        std::fs::write(&input, bytes).unwrap();
        let name = format!(
            "acyclic-region-{}-{}",
            std::process::id(),
            dir.path().file_name().unwrap().to_string_lossy()
        );
        let session = Session { name, tool };
        // Keep the process alive without echoing input into the screen.
        let shell = format!(
            "stty -echo; while [ ! -e '{}' ]; do sleep 0.01; done; cat '{}'; touch '{}'; read answer",
            start.display(),
            input.display(),
            ready.display()
        );
        if tool == "tmux" {
            checked(Command::new(tool).args([
                "-L",
                &session.name,
                "-f",
                config.to_str().unwrap(),
                "new-session",
                "-d",
                "-x",
                "24",
                "-y",
                "8",
                "sh",
                "-c",
                &shell,
            ]));
        } else {
            checked(Command::new(tool).args([
                "-c",
                "/dev/null",
                "-dmS",
                &session.name,
                "sh",
                "-c",
                &shell,
            ]));
            checked(Command::new(tool).args(["-S", &session.name, "-X", "width", "24", "8"]));
        }
        std::fs::write(start, "").unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let text = if tool == "tmux" {
                String::from_utf8(checked(Command::new(tool).args([
                    "-L",
                    &session.name,
                    "capture-pane",
                    "-p",
                    "-t",
                    "0",
                ])))
                .unwrap()
            } else {
                checked(Command::new(tool).args([
                    "-S",
                    &session.name,
                    "-X",
                    "hardcopy",
                    dump.to_str().unwrap(),
                ]));
                std::fs::read_to_string(&dump).unwrap_or_default()
            };
            // The final footer establishes that the emulator consumed the
            // complete initial frame, not just that the writer exited.
            if ready.exists() && text.lines().nth(7).is_some_and(|row| row == "fixed footer") {
                return text
                    .lines()
                    .take(8)
                    .map(|row| format!("{row:<24}"))
                    .collect();
            }
            assert!(
                Instant::now() < deadline,
                "{tool} did not consume the fixture: {text:?}"
            );
            std::thread::sleep(Duration::from_millis(2));
        }
    }
    fn oracle(tool: &str) {
        for (bytes, expected) in fixtures() {
            assert_eq!(capture(tool, &bytes), expected, "{tool} region screen");
            assert_ne!(
                capture(tool, &without_scrolling(&bytes)),
                expected,
                "removing production scroll sequences must fail the same screen oracle"
            );
        }
    }
    #[test]
    fn tmux_capture_pane_agrees_with_the_rendered_screen() {
        oracle("tmux");
    }
    #[test]
    fn screen_hardcopy_agrees_with_the_rendered_screen() {
        oracle("screen");
    }
}
