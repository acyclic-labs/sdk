//! Physical cancellation uses actual Unix I/O, without synthetic FUSE frames.

use super::super::{host_io::HostIo, packet_cancellation};
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
    let io = Arc::new(HostIo::new(Arc::new(File::from(descriptor)), &work)?);
    let registration = packet_cancellation::register(&work, &io)?;
    let (finished, completion) = sync_channel(1);
    let worker_io = Arc::clone(&io);
    let worker = std::thread::spawn(move || {
        let mut byte = [0];
        let _ = finished.send(worker_io.receive(&mut byte));
    });
    cancellation.cancel();
    let result = completion.recv_timeout(Duration::from_secs(2));
    // Even a failed wake regression must positively stop and join its reader.
    io.stop();
    worker
        .join()
        .map_err(|_| std::io::Error::other("packet reader panicked"))?;
    assert!(result??.is_none());
    assert!(matches!(
        work.admit(crate::WorkCounters::UNCHARGED),
        Err(crate::NativeTreeMaterializationError::Cancelled)
    ));
    drop(registration);
    drop(io);
    assert_eq!(work.live_allocation_bytes(), baseline);
    Ok(())
}
