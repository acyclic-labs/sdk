//! One set of behaviour tests, run against the command type and, on
//! Windows, against `std::process::Command` too: a builder starts every
//! child alike, however often and however it is started. On Unix the
//! command type is std's, so the bodies describe the behaviour the Windows
//! type must match.

/// A program that prints its arguments on one line.
#[cfg(windows)]
const ECHO: (&str, &[&str]) = ("cmd.exe", &["/d", "/c", "echo"]);
#[cfg(unix)]
const ECHO: (&str, &[&str]) = ("echo", &[]);

/// A program that prints the variable `V`.
#[cfg(windows)]
const ECHO_V: (&str, &[&str]) = ("cmd.exe", &["/d", "/c", "echo %V%"]);
#[cfg(unix)]
const ECHO_V: (&str, &[&str]) = ("sh", &["-c", "echo $V"]);

/// A program that exits with status 3.
#[cfg(windows)]
const EXIT_3: (&str, &[&str]) = ("cmd.exe", &["/d", "/c", "exit 3"]);
#[cfg(unix)]
const EXIT_3: (&str, &[&str]) = ("sh", &["-c", "exit 3"]);

/// A program that copies its input to its output.
#[cfg(windows)]
const COPY: (&str, &[&str]) = ("findstr", &["^"]);
#[cfg(unix)]
const COPY: (&str, &[&str]) = ("cat", &[]);

/// Output text with Windows line ends folded.
fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).replace("\r\n", "\n")
}

macro_rules! bodies {
    () => {
        use super::{COPY, ECHO, ECHO_V, EXIT_3, text};
        use std::io::{Read as _, Write as _};

        fn command((program, args): (&str, &[&str])) -> Command {
            let mut command = Command::new(program);
            command.args(args);
            command
        }

        fn read_all(child: &mut std::process::ChildStdout) -> String {
            let mut bytes = Vec::new();
            child.read_to_end(&mut bytes).expect("read output");
            text(&bytes)
        }

        #[test]
        fn every_spawn_gets_its_own_pipe() {
            let mut echo = command(ECHO);
            echo.arg("hello").stdout(Stdio::piped());
            for _ in 0..3 {
                let mut child = echo.spawn().expect("spawn");
                let mut stdout = child.stdout.take().expect("a pipe for every spawn");
                assert_eq!(read_all(&mut stdout), "hello\n");
                assert!(child.wait().expect("wait").success());
            }
        }

        #[test]
        fn output_after_spawn_still_captures() {
            let mut echo = command(ECHO);
            echo.arg("hello").stdout(Stdio::piped());
            let mut child = echo.spawn().expect("spawn");
            drop(child.stdout.take());
            child.wait().expect("wait");
            let output = echo.output().expect("output");
            assert!(output.status.success());
            assert_eq!(text(&output.stdout), "hello\n");
        }

        #[test]
        fn status_is_repeatable() {
            let mut exit = command(EXIT_3);
            for _ in 0..2 {
                assert_eq!(exit.status().expect("status").code(), Some(3));
            }
            assert_eq!(exit.output().expect("output").status.code(), Some(3));
        }

        #[test]
        fn a_reused_builder_starts_with_its_current_env_and_args() {
            let mut echo = command(ECHO_V);
            echo.env("V", "first");
            assert_eq!(text(&echo.output().expect("first").stdout), "first\n");
            echo.env("V", "second");
            assert_eq!(text(&echo.output().expect("second").stdout), "second\n");
            let mut echo = command(ECHO);
            echo.arg("one");
            assert_eq!(text(&echo.output().expect("one").stdout), "one\n");
            echo.arg("two");
            assert_eq!(text(&echo.output().expect("two").stdout), "one two\n");
        }

        #[test]
        fn piped_input_reaches_every_child() {
            let mut copy = command(COPY);
            copy.stdin(Stdio::piped()).stdout(Stdio::piped());
            for _ in 0..2 {
                let mut child = copy.spawn().expect("spawn");
                let mut stdin = child.stdin.take().expect("a pipe for every spawn");
                stdin.write_all(b"b\na\n").expect("write");
                drop(stdin);
                let mut stdout = child.stdout.take().expect("output pipe");
                assert_eq!(read_all(&mut stdout), "b\na\n");
                assert!(child.wait().expect("wait").success());
            }
        }

        #[test]
        fn null_input_reads_nothing_every_time() {
            let mut copy = command(COPY);
            copy.stdin(Stdio::null()).stdout(Stdio::piped());
            for _ in 0..2 {
                // `findstr` reports finding nothing as a failure.
                assert_eq!(copy.output().expect("output").stdout, b"");
            }
        }

        #[test]
        fn inherited_and_null_output_leave_no_pipe() {
            let mut exit = command(EXIT_3);
            for stdio in [Stdio::inherit as fn() -> Stdio, Stdio::null] {
                exit.stdout(stdio()).stderr(stdio()).stdin(stdio());
                for _ in 0..2 {
                    let mut child = exit.spawn().expect("spawn");
                    assert!(child.stdin.is_none() && child.stdout.is_none());
                    assert!(child.stderr.is_none());
                    assert_eq!(child.wait().expect("wait").code(), Some(3));
                }
            }
        }

        #[test]
        fn a_file_receives_every_childs_output() {
            let directory = tempfile::tempdir().expect("directory");
            let path = directory.path().join("output");
            let file = std::fs::File::create(&path).expect("file");
            let mut echo = command(ECHO);
            echo.arg("hello").stdout(Stdio::from(file));
            for _ in 0..2 {
                assert!(echo.status().expect("status").success());
            }
            let mut written = Vec::new();
            std::fs::File::open(&path)
                .and_then(|mut file| file.read_to_end(&mut written))
                .expect("read file");
            assert_eq!(text(&written), "hello\nhello\n");
        }
    };
}

/// The workspace's command type.
mod owned {
    use crate::command::{Command, Stdio};
    bodies!();
}

/// `std` on Windows, which the command type replaces there.
#[cfg(windows)]
mod standard {
    use std::process::{Command, Stdio};
    bodies!();
}
