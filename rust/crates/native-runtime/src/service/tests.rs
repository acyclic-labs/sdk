use super::*;

#[cfg(unix)]
use std::os::fd::OwnedFd as PipeHandle;
#[cfg(windows)]
use std::os::windows::io::OwnedHandle as PipeHandle;

fn readiness_channel() -> io::Result<(ServiceReadiness, ServiceReadySignal)> {
    let (reader, writer) = io::pipe()?;
    Ok((
        ServiceReadiness(File::from(PipeHandle::from(reader))),
        ServiceReadySignal(File::from(PipeHandle::from(writer))),
    ))
}

#[test]
fn readiness_reaches_the_starter_and_closes_the_channel() -> io::Result<()> {
    let (mut starter, service) = readiness_channel()?;
    service.signal()?;
    let mut received = Vec::new();
    crate::io_progress::read_to_end(&mut starter.0, &mut received)?;
    assert_eq!(received, b"\n");
    Ok(())
}

#[test]
fn starter_exit_does_not_fail_service_startup() -> io::Result<()> {
    let (starter, service) = readiness_channel()?;
    drop(starter);
    service.signal()?;
    Ok(())
}

#[test]
fn readiness_still_reports_other_io_failures() -> io::Result<()> {
    let file = tempfile::NamedTempFile::new()?;
    let service = ServiceReadySignal(File::open(file.path())?);
    let Err(error) = service.signal() else {
        return Err(io::Error::other(
            "read-only readiness channel accepted a write",
        ));
    };
    assert_ne!(error.kind(), io::ErrorKind::BrokenPipe);
    Ok(())
}

#[test]
fn the_starter_distinguishes_readiness_failure_and_exit() -> io::Result<()> {
    let (starter, service) = readiness_channel()?;
    service.signal()?;
    assert_eq!(starter.wait()?, ServiceStartOutcome::Ready);

    let (starter, service) = readiness_channel()?;
    service.fail("native mount registry is unavailable")?;
    assert_eq!(
        starter.wait()?,
        ServiceStartOutcome::Failed("native mount registry is unavailable".to_owned())
    );

    let (starter, service) = readiness_channel()?;
    drop(service);
    assert_eq!(starter.wait()?, ServiceStartOutcome::Exited);

    // A pipe buffer may be smaller than the report: the service writes while
    // its starter reads, as two processes do.
    let (starter, service) = readiness_channel()?;
    let original = "native failure \u{03bb}".repeat(1000);
    let report = original.clone();
    let writer = std::thread::spawn(move || service.fail(&report));
    let outcome = starter.wait()?;
    writer
        .join()
        .map_err(|_| io::Error::other("failure writer panicked"))??;
    let ServiceStartOutcome::Failed(reason) = outcome else {
        return Err(io::Error::other("an oversized failure was not reported"));
    };
    assert_eq!(reason, original);
    let (starter, mut service) = readiness_channel()?;
    send(&mut service.0, FAILURE_FRAME, &[0xff])?;
    drop(service);
    assert_eq!(
        starter.wait()?,
        ServiceStartOutcome::Failed("\u{fffd}".to_owned())
    );
    Ok(())
}

#[test]
fn status_schedule_child() {
    if std::env::var_os("ACYCLIC_STATUS_SCHEDULE_CHILD").is_some() {
        std::process::exit(7);
    }
}

#[test]
fn process_status_keeps_unbounded_exit_and_rejects_invalid_deadline() -> io::Result<()> {
    let command = || -> io::Result<std::process::Command> {
        let mut command = std::process::Command::new(std::env::current_exe()?);
        command
            .args(["--exact", "service::tests::status_schedule_child"])
            .env("ACYCLIC_STATUS_SCHEDULE_CHILD", "1")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        Ok(command)
    };
    let status = process_status(&mut command()?, CaptureSchedule::Unbounded)?;
    assert_eq!(status.code(), Some(7));
    let error = process_status(&mut command()?, CaptureSchedule::Overflow)
        .expect_err("invalid caller deadline must not report child status");
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    Ok(())
}

#[test]
fn service_channels_return_first_errors_and_keep_physical_prefixes() -> io::Result<()> {
    use std::io::{Seek as _, Write as _};

    struct Reader {
        pipe: io::PipeReader,
        calls: usize,
        fail_at: usize,
    }
    impl Read for Reader {
        fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
            self.calls += 1;
            if self.calls == self.fail_at {
                return Err(io::ErrorKind::Interrupted.into());
            }
            let count = output.len().min(3);
            self.pipe.read(&mut output[..count])
        }
    }
    fn reader(fail_at: usize) -> io::Result<Reader> {
        let (pipe, writer) = io::pipe()?;
        ServiceReadySignal(File::from(PipeHandle::from(writer))).fail("retained")?;
        Ok(Reader {
            pipe,
            calls: 0,
            fail_at,
        })
    }
    struct Writer {
        file: File,
        calls: usize,
        fail_at: usize,
        zero: bool,
    }
    impl io::Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.calls += 1;
            if self.calls == self.fail_at {
                return if self.zero {
                    Ok(0)
                } else {
                    Err(io::ErrorKind::Interrupted.into())
                };
            }
            self.file.write(&bytes[..bytes.len().min(3)])
        }
        fn flush(&mut self) -> io::Result<()> {
            self.file.flush()
        }
    }

    for (fail_at, consumed) in [(1, 0), (2, 1), (3, 2)] {
        let mut source = reader(fail_at)?;
        assert_eq!(
            receive(&mut source).unwrap_err().kind(),
            io::ErrorKind::Interrupted
        );
        assert_eq!(
            source.calls, fail_at,
            "the failure ends the original operation"
        );
        let mut remainder = Vec::new();
        crate::io_progress::read_to_end(&mut source, &mut remainder)?;
        assert_eq!(remainder, b"!retained"[consumed..]);

        // Restoring the original standard-library error consumption accepts
        // the same physical pipe after its first failed read instead.
        let mut old = reader(fail_at)?;
        let mut frame = [0];
        old.read_exact(&mut frame)?;
        assert_eq!(frame, [FAILURE_FRAME]);
        let mut reason = Vec::new();
        old.read_to_end(&mut reason)?;
        assert_eq!(reason, b"retained");
        assert!(old.calls > fail_at);
    }
    #[cfg(windows)]
    let prefixes = [(1, &b""[..]), (2, &b"!re"[..]), (3, &b"!retai"[..])];
    #[cfg(not(windows))]
    let prefixes = [(1, &b""[..]), (2, &b"!"[..]), (3, &b"!ret"[..])];
    for (fail_at, prefix) in prefixes {
        for zero in [false, true] {
            let mut writer = Writer {
                file: tempfile::tempfile()?,
                calls: 0,
                fail_at,
                zero,
            };
            let error = send(&mut writer, FAILURE_FRAME, b"retained").unwrap_err();
            assert_eq!(
                error.kind(),
                if zero {
                    io::ErrorKind::WriteZero
                } else {
                    io::ErrorKind::Interrupted
                }
            );
            assert_eq!(writer.calls, fail_at);
            writer.file.rewind()?;
            let mut actual = Vec::new();
            crate::io_progress::read_to_end(&mut writer.file, &mut actual)?;
            assert_eq!(actual, prefix);
        }
    }
    let mut old = Writer {
        file: tempfile::tempfile()?,
        calls: 0,
        fail_at: 1,
        zero: false,
    };
    old.write_all(b"!retained")?;
    old.file.rewind()?;
    let mut actual = Vec::new();
    crate::io_progress::read_to_end(&mut old.file, &mut actual)?;
    assert_eq!(
        actual, b"!retained",
        "the old writer consumed the first failure"
    );
    Ok(())
}
