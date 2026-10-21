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
    starter.0.read_to_end(&mut received)?;
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
    let writer = std::thread::spawn(move || service.fail(&"x".repeat(10_000)));
    let outcome = starter.wait()?;
    writer
        .join()
        .map_err(|_| io::Error::other("failure writer panicked"))??;
    let ServiceStartOutcome::Failed(reason) = outcome else {
        return Err(io::Error::other("an oversized failure was not reported"));
    };
    assert_eq!(reason.len(), 4096);
    Ok(())
}
