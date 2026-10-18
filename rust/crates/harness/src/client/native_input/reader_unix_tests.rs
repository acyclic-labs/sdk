use rustix::fs::{OFlags, fcntl_getfl, fcntl_setfl};
use std::fs::File;
use std::io::{self, Write};
use std::time::Duration;

fn pipe() -> io::Result<(File, File)> {
    let (read, write) = io::pipe()?;
    Ok((
        File::from(std::os::fd::OwnedFd::from(read)),
        File::from(std::os::fd::OwnedFd::from(write)),
    ))
}

/// Drain the exact readiness notification before the worker can enter read.
/// The old detached worker then consumes the byte written after its drop;
/// the native owner must instead cancel and join before that write.
fn stolen_readiness_after_drop(broken: bool) -> io::Result<bool> {
    use std::io::Read;
    use std::sync::mpsc;
    let (mut read, mut write) = pipe()?;
    let mut observer = read.try_clone()?;
    let flags = fcntl_getfl(&observer)?;
    let (ready, observed) = mpsc::sync_channel::<io::Result<()>>(0);
    let (release, proceed) = mpsc::sync_channel(0);
    let (finished, exited) = mpsc::sync_channel(0);
    write.write_all(b"x")?;
    let (drop_reader, old): (Box<dyn FnOnce() + Send>, _) = if broken {
        let worker = std::thread::spawn(move || -> io::Result<()> {
            let mut input = [0; 1];
            // A competing reader consumes the announced byte.
            read.read_exact(&mut input)?;
            ready.send(Ok(())).map_err(io::Error::other)?;
            proceed.recv().map_err(io::Error::other)?;
            read.read_exact(&mut input)?;
            assert_eq!(input, *b"z");
            Ok(())
        });
        (Box::new(|| {}), Some(worker))
    } else {
        let mut thief = read.try_clone()?;
        let mut first = true;
        let reader = super::Reader::spawn_after_ready(read, move || {
            if first {
                first = false;
                // Callback is after the production native poll, before read.
                let result = thief.read_exact(&mut [0; 1]);
                let _ = ready.send(result);
                let _ = proceed.recv();
            }
        })?;
        (Box::new(move || drop(reader)), None)
    };
    // On assertion/error unwind, release callback gates before dropping the
    // owner, whose shutdown joins that callback's worker.
    let release = release;
    let observed = observed;
    observed
        .recv_timeout(Duration::from_secs(2))
        .map_err(io::Error::other)??;
    let dropper = std::thread::spawn(move || {
        drop_reader();
        let _ = finished.send(());
    });
    release.send(()).map_err(io::Error::other)?;
    exited
        .recv_timeout(Duration::from_secs(2))
        .map_err(io::Error::other)?;
    dropper.join().map_err(|_| io::ErrorKind::Other)?;
    assert_eq!(fcntl_getfl(&observer)?, flags, "shared flags changed");
    write.write_all(b"z")?;
    if let Some(old) = old {
        old.join().map_err(|_| io::ErrorKind::Other)??;
    }
    fcntl_setfl(&observer, flags | OFlags::NONBLOCK)?;
    let count = observer.read(&mut [0; 1]);
    Ok(matches!(count, Ok(1)))
}

#[test]
fn cancellation_joins_a_read_admitted_after_stolen_readiness() -> io::Result<()> {
    assert!(stolen_readiness_after_drop(false)?);
    Ok(())
}

#[test]
fn negative_control_detects_post_drop_read_after_stolen_readiness() -> io::Result<()> {
    assert!(!stolen_readiness_after_drop(true)?);
    Ok(())
}

#[test]
fn native_worker_publishes_valid_bytes_and_eof() -> io::Result<()> {
    use tokio::io::ReadBuf;
    let (read, mut write) = pipe()?;
    write.write_all(b"first\nsecond\n")?;
    drop(write);
    let mut reader = super::Reader::spawn(read)?;
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let bytes = runtime.block_on(async {
        let mut output = Vec::new();
        loop {
            let mut bytes = [0; 3];
            let mut destination = ReadBuf::new(&mut bytes);
            std::future::poll_fn(|cx| reader.poll_read(cx, &mut destination)).await?;
            if destination.filled().is_empty() {
                break;
            }
            output.extend_from_slice(destination.filled());
        }
        io::Result::Ok(output)
    });
    assert_eq!(bytes?, b"first\nsecond\n");
    Ok(())
}
