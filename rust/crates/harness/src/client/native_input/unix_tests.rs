use super::stdin::{Stdin, from_file};
use futures::StreamExt;
use rustix::fs::{OFlags, fcntl_getfl, fcntl_setfl};
use std::fs::File;
use std::io::{self, Write};
use std::time::Duration;
use tokio_util::codec::{FramedRead, LinesCodec};

fn runtime() -> io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
}

fn pipe() -> io::Result<(File, File)> {
    let (read, write) = io::pipe()?;
    Ok((
        File::from(std::os::fd::OwnedFd::from(read)),
        File::from(std::os::fd::OwnedFd::from(write)),
    ))
}

#[test]
fn pipe_input_never_changes_the_shared_description() -> io::Result<()> {
    let (read, mut write) = pipe()?;
    let observer = read.try_clone()?;
    let original = fcntl_getfl(&observer)?;
    assert!(!original.contains(OFlags::NONBLOCK));
    let runtime = runtime()?;
    let lines = runtime.block_on(async {
        let input = from_file(read)?;
        assert_eq!(
            matches!(input, Stdin::Reactor(_)),
            cfg!(target_os = "linux")
        );
        // Nothing a killed process could leave behind: the description
        // shared with the parent keeps its flags while input is followed.
        assert_eq!(fcntl_getfl(&observer)?, original);
        write.write_all(b"one\ntwo\n")?;
        drop(write);
        let lines: Vec<_> = FramedRead::new(input, LinesCodec::new()).collect().await;
        io::Result::Ok(lines)
    })?;
    let lines: Vec<String> = lines
        .into_iter()
        .collect::<Result<_, _>>()
        .map_err(io::Error::other)?;
    assert_eq!(lines, ["one", "two"]);
    assert_eq!(fcntl_getfl(&observer)?, original);
    Ok(())
}

#[test]
fn negative_control_detects_flags_set_through_a_duplicate() -> io::Result<()> {
    let (read, _write) = pipe()?;
    let original = fcntl_getfl(&read)?;
    let duplicate = read.try_clone()?;
    fcntl_setfl(&duplicate, original | OFlags::NONBLOCK)?;
    assert_ne!(
        fcntl_getfl(&read)?,
        original,
        "a duplicate shares the description"
    );
    Ok(())
}

/// Another reader takes the announced byte between the readiness edge and
/// this stream's read. The reactor path must keep waiting without
/// blocking the runtime. (The thread path cannot be raced into blocking
/// the runtime: only its own thread ever waits in a read.)
#[cfg(target_os = "linux")]
#[test]
fn input_stolen_after_readiness_leaves_the_runtime_running() -> io::Result<()> {
    let (read, mut write) = pipe()?;
    let mut thief = read.try_clone()?;
    let runtime = runtime()?;
    runtime.block_on(async {
        let mut input = from_file(read)?;
        let Stdin::Reactor(fd) = &input else {
            return Err(io::ErrorKind::Unsupported.into());
        };
        write.write_all(b"x")?;
        // Hold the edge, then let the thief consume its byte.
        fd.readable().await?.retain_ready();
        assert_eq!(io::Read::read(&mut thief, &mut [0; 1])?, 1);
        let stalled = tokio::time::timeout(
            Duration::from_millis(5),
            tokio::io::AsyncReadExt::read(&mut input, &mut [0; 1]),
        )
        .await;
        assert!(stalled.is_err(), "the stolen byte was read");
        write.write_all(b"y\n")?;
        let mut lines = FramedRead::new(input, LinesCodec::new());
        let line = lines.next().await.ok_or(io::ErrorKind::UnexpectedEof)?;
        assert_eq!(line.map_err(io::Error::other)?, "y");
        io::Result::Ok(())
    })
}

#[test]
fn negative_control_detects_a_blocking_read_after_stolen_readiness() -> io::Result<()> {
    // The removed shared path: readiness seen, byte stolen, then a read of
    // the blocking description holds its thread.
    let (read, mut write) = pipe()?;
    let mut thief = read.try_clone()?;
    write.write_all(b"x")?;
    let mut fds = [rustix::event::PollFd::new(
        &read,
        rustix::event::PollFlags::IN,
    )];
    rustix::event::poll(&mut fds, None)?;
    assert_eq!(io::Read::read(&mut thief, &mut [0; 1])?, 1);
    let mut shared = read;
    let blocked = std::thread::spawn(move || io::Read::read(&mut shared, &mut [0; 1]));
    std::thread::sleep(Duration::from_millis(5));
    assert!(!blocked.is_finished(), "the stale read returned");
    write.write_all(b"y")?;
    assert_eq!(blocked.join().map_err(|_| io::ErrorKind::Other)??, 1);
    Ok(())
}

#[test]
fn regular_files_are_read_off_the_reactor() -> io::Result<()> {
    let mut file = tempfile::tempfile()?;
    file.write_all(b"alpha\nbeta\n")?;
    std::io::Seek::rewind(&mut file)?;
    let runtime = runtime()?;
    let lines = runtime.block_on(async {
        let input = from_file(file)?;
        assert!(matches!(input, Stdin::Reader(_)));
        let lines: Vec<_> = FramedRead::new(input, LinesCodec::new()).collect().await;
        io::Result::Ok(lines)
    })?;
    let lines: Vec<String> = lines
        .into_iter()
        .collect::<Result<_, _>>()
        .map_err(io::Error::other)?;
    assert_eq!(lines, ["alpha", "beta"]);
    Ok(())
}

#[test]
fn a_stalled_dedicated_read_leaves_the_runtime_running() -> io::Result<()> {
    // A read that cannot complete stands in for a stalled filesystem.
    let (read, mut write) = pipe()?;
    let runtime = runtime()?;
    runtime.block_on(async {
        let reader = super::reader::Reader::spawn(read)?;
        let mut lines = FramedRead::new(Stdin::Reader(reader), LinesCodec::new());
        assert!(
            tokio::time::timeout(Duration::from_millis(5), lines.next())
                .await
                .is_err()
        );
        write.write_all(b"late\n")?;
        let line = lines.next().await.ok_or(io::ErrorKind::UnexpectedEof)?;
        assert_eq!(line.map_err(io::Error::other)?, "late");
        io::Result::Ok(())
    })
}

#[test]
fn negative_control_detects_a_read_that_blocks_its_thread() -> io::Result<()> {
    // The removed path read regular files inline; on a stalled source that
    // read holds its thread, which the timer here observes.
    let (mut read, mut write) = pipe()?;
    let blocked = std::thread::spawn(move || io::Read::read(&mut read, &mut [0; 8]));
    let runtime = runtime()?;
    runtime.block_on(async { tokio::time::sleep(Duration::from_millis(5)).await });
    assert!(!blocked.is_finished());
    write.write_all(b"x")?;
    assert_eq!(blocked.join().map_err(|_| io::ErrorKind::Other)??, 1);
    Ok(())
}

#[test]
fn a_nonblocking_socket_waits_for_input_instead_of_ending_it() -> io::Result<()> {
    let (read, mut write) = std::os::unix::net::UnixStream::pair()?;
    read.set_nonblocking(true)?;
    let read = File::from(std::os::fd::OwnedFd::from(read));
    let runtime = runtime()?;
    let lines = runtime.block_on(async {
        let input = from_file(read)?;
        assert!(matches!(input, Stdin::Reader(_)));
        let mut lines = FramedRead::new(input, LinesCodec::new());
        // Empty: the reader's read would block, so it waits in poll.
        assert!(
            tokio::time::timeout(Duration::from_millis(5), lines.next())
                .await
                .is_err()
        );
        write.write_all(b"late\n")?;
        drop(write);
        let lines: Vec<_> = lines.collect().await;
        io::Result::Ok(lines)
    })?;
    let lines: Vec<String> = lines
        .into_iter()
        .collect::<Result<_, _>>()
        .map_err(io::Error::other)?;
    assert_eq!(lines, ["late"]);
    Ok(())
}

#[test]
fn negative_control_detects_a_nonblocking_read_ending_input() -> io::Result<()> {
    // The removed reader loop took this error as the end of input.
    let (read, _write) = std::os::unix::net::UnixStream::pair()?;
    read.set_nonblocking(true)?;
    let mut read = File::from(std::os::fd::OwnedFd::from(read));
    let error = io::Read::read(&mut read, &mut [0; 8]).err();
    assert_eq!(
        error.map(|error| error.kind()),
        Some(io::ErrorKind::WouldBlock)
    );
    Ok(())
}

#[test]
fn devices_other_than_terminals_are_read_off_the_reactor() -> io::Result<()> {
    let runtime = runtime()?;
    runtime.block_on(async {
        for device in ["/dev/null", "/dev/random", "/dev/urandom", "/dev/zero"] {
            let input = from_file(File::open(device)?)?;
            assert!(matches!(input, Stdin::Reader(_)), "{device}");
        }
        io::Result::Ok(())
    })
}

#[cfg(target_os = "linux")]
#[test]
fn negative_control_detects_epoll_accepting_a_device() -> io::Result<()> {
    // Without the allow-list, a reopened /dev/random registers with epoll
    // and would be awaited on the reactor.
    let runtime = runtime()?;
    runtime.block_on(async {
        let device = File::open("/dev/random")?;
        assert!(tokio::io::unix::AsyncFd::try_new(device).is_ok());
        io::Result::Ok(())
    })
}

/// Child: park the dedicated reader on stdin, drop the stream and return.
/// With `joined`, a reader the process must join instead (the negative
/// control): it stays parked, so the child never exits.
#[test]
fn parked_reader_child() -> io::Result<()> {
    let Some(mode) = std::env::var_os("ACYCLIC_PARKED_READER_CHILD") else {
        return Ok(());
    };
    let stdin = File::from(std::os::fd::AsFd::as_fd(&io::stdin()).try_clone_to_owned()?);
    if mode == "joined" {
        let mut stdin = stdin;
        let reader = std::thread::spawn(move || io::Read::read(&mut stdin, &mut [0; 1]));
        let _ = reader.join();
        return Ok(());
    }
    runtime()?.block_on(async {
        let reader = super::reader::Reader::spawn(stdin)?;
        let mut lines = FramedRead::new(Stdin::Reader(reader), LinesCodec::new());
        let waited = tokio::time::timeout(Duration::from_millis(5), lines.next()).await;
        assert!(waited.is_err());
        io::Result::Ok(())
    })
}

/// Runs the child with a stdin pipe that stays open and returns whether it
/// exited before the deadline.
fn child_exits(mode: &str, within: Duration) -> io::Result<bool> {
    let mut child = std::process::Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "client::native_input::unix_tests::parked_reader_child",
            "--test-threads=1",
        ])
        .env("ACYCLIC_PARKED_READER_CHILD", mode)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()?;
    let deadline = std::time::Instant::now() + within;
    let exited = loop {
        if let Some(status) = child.try_wait()? {
            assert!(status.success());
            break true;
        }
        if std::time::Instant::now() >= deadline {
            break false;
        }
        std::thread::sleep(Duration::from_millis(2));
    };
    if !exited {
        child.kill()?;
        child.wait()?;
    }
    Ok(exited)
}

#[test]
fn a_parked_reader_never_delays_process_exit() -> io::Result<()> {
    assert!(child_exits("cancelled", Duration::from_secs(5))?);
    Ok(())
}

#[test]
fn negative_control_detects_exit_waiting_for_a_parked_reader() -> io::Result<()> {
    assert!(!child_exits("joined", Duration::from_millis(300))?);
    Ok(())
}

/// Writes one byte after `drop_reader` and reports whether it is still
/// unread once the reader has had time to take it.
fn byte_after_drop_unread(
    spawn: impl FnOnce(File) -> io::Result<Box<dyn FnOnce()>>,
) -> io::Result<bool> {
    let (read, mut write) = pipe()?;
    let probe = read.try_clone()?;
    let drop_reader = spawn(read)?;
    // Let the reader reach its wait before the stream goes away.
    std::thread::sleep(Duration::from_millis(5));
    drop_reader();
    write.write_all(b"x")?;
    std::thread::sleep(Duration::from_millis(20));
    let mut fds = [rustix::event::PollFd::new(
        &probe,
        rustix::event::PollFlags::IN,
    )];
    Ok(rustix::event::poll(&mut fds, Some(&rustix::event::Timespec::default()))? == 1)
}

#[test]
fn a_dropped_reader_reads_nothing_written_after_it() -> io::Result<()> {
    let unread = byte_after_drop_unread(|read| {
        let runtime = runtime()?;
        let reader = runtime.block_on(async { super::reader::Reader::spawn(read) })?;
        Ok(Box::new(move || drop((reader, runtime))))
    })?;
    assert!(unread);
    Ok(())
}

#[test]
fn negative_control_detects_a_dropped_reader_left_reading() -> io::Result<()> {
    // The removed reader waited inside read, which the stream's drop
    // could not interrupt: the next byte after the drop was consumed.
    let unread = byte_after_drop_unread(|mut read| {
        std::thread::spawn(move || io::Read::read(&mut read, &mut [0; 8]));
        Ok(Box::new(|| {}))
    })?;
    assert!(!unread);
    Ok(())
}
