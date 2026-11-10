//! Physical cancellation uses actual Unix I/O, without synthetic FUSE frames.

use super::super::{
    host_io::{HostIo, Received},
    packet_cancellation,
};
use super::*;
use std::fs::File;
use std::os::fd::OwnedFd;
use std::os::unix::net::UnixStream;

#[test]
fn original_cancellation_wakes_a_quiet_packet_reader() -> Result<(), Box<dyn std::error::Error>> {
    let cancellation = crate::CancellationToken::new();
    let work = cache_work(FINITE_CACHE_BUDGET, &cancellation);
    let baseline = work.live_allocation_bytes();
    let (reader, _writer) = UnixStream::pair()?;
    let descriptor: OwnedFd = reader.into();
    let io = Arc::new(HostIo::new(File::from(descriptor), &work)?);
    let reader = io.reader()?;
    let registration = packet_cancellation::register(&work, &io)?;
    let (finished, completion) = sync_channel(1);
    let worker_io = Arc::clone(&io);
    let worker = std::thread::spawn(move || {
        let mut byte = [0];
        let _ = finished.send(worker_io.receive(&reader, &mut byte));
    });
    cancellation.cancel();
    let result = completion.recv();
    // Recover the actual result only after the original worker has joined.
    io.stop();
    worker
        .join()
        .map_err(|_| std::io::Error::other("packet reader panicked"))?;
    assert_eq!(result??, Received::Wake);
    assert!(matches!(
        work.admit(crate::WorkCounters::UNCHARGED),
        Err(crate::NativeTreeMaterializationError::Cancelled)
    ));
    drop(registration);
    drop(io);
    assert_eq!(work.live_allocation_bytes(), baseline);

    // Permanent stop must wake every original registration, including readers
    // that have not entered the native wait yet. No device clone keeps it live.
    let (reader, writer) = UnixStream::pair()?;
    let descriptor: OwnedFd = reader.into();
    let io = Arc::new(HostIo::new(File::from(descriptor), &fuse_fixture_work())?);
    let readers = (0..crate::native_mount::CALLBACK_THREADS)
        .map(|_| io.reader())
        .collect::<std::io::Result<Vec<_>>>()?;
    std::thread::scope(|scope| -> Result<(), Box<dyn std::error::Error>> {
        let (finished, completion) = std::sync::mpsc::channel();
        for reader in readers {
            let io = Arc::clone(&io);
            let finished = finished.clone();
            scope.spawn(move || {
                let _ = finished.send(io.receive(&reader, &mut [0]));
            });
        }
        drop(finished);
        io.stop();
        for _ in 0..crate::native_mount::CALLBACK_THREADS {
            assert_eq!(completion.recv()??, Received::Closed);
        }
        Ok(())
    })?;
    assert_eq!(
        io.reader().err().and_then(|error| error.raw_os_error()),
        Some(libc::ENODEV)
    );
    drop(writer);

    let (reader, writer) = UnixStream::pair()?;
    let descriptor: OwnedFd = reader.into();
    let io = HostIo::new(File::from(descriptor), &fuse_fixture_work())?;
    let reader = io.reader()?;
    drop(writer);
    assert_eq!(io.receive(&reader, &mut [0])?, Received::Closed);
    Ok(())
}
