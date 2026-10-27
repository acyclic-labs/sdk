//! Capture the real host conversation, then replay it without a host mount.

use super::super::packet::{OUTPUT_HEADER, PacketInput, PacketRequest};
use super::super::packet_router::PacketRouter;
use super::super::packet_session::{MAXIMUM_WRITE, PacketSession};
use super::super::{FuseProjection, NativeMountWorkScope};
use super::*;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::sync::{Condvar, PoisonError};
use std::time::{Instant, SystemTime};

thread_local! {
    // Enrollment happens on the mounting test's thread. Only its session gets
    // this recorder; unrelated concurrent mount tests cannot enter the capture.
    static RECORDING: RefCell<Option<Arc<Capture>>> = const { RefCell::new(None) };
}

#[derive(Default)]
pub(in crate::native_mount::fuse) struct Capture {
    next: AtomicU64,
    packets: Mutex<Vec<CapturedPacket>>,
    changed: Condvar,
}

#[derive(Serialize, Deserialize)]
struct CapturedPacket {
    sequence: u64,
    request: Vec<u8>,
    response: Option<Vec<u8>>,
}

impl Capture {
    pub(in crate::native_mount::fuse) fn current() -> Option<Arc<Self>> {
        RECORDING.with_borrow(Clone::clone)
    }

    pub(in crate::native_mount::fuse) fn begin(&self) -> u64 {
        self.next.fetch_add(1, Ordering::Relaxed)
    }

    pub(in crate::native_mount::fuse) fn completed(
        &self,
        sequence: u64,
        request: &[u8],
        response: Option<&[u8]>,
    ) {
        self.packets
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(CapturedPacket {
                sequence,
                request: request.to_vec(),
                response: response.map(<[u8]>::to_vec),
            });
        self.changed.notify_all();
    }

    fn wait_for_releases(&self, count: usize) -> Result<(), Box<dyn std::error::Error>> {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut packets = self.packets.lock().unwrap_or_else(PoisonError::into_inner);
        loop {
            let released = packets
                .iter()
                .filter(|packet| {
                    PacketRequest::parse(&packet.request).is_ok_and(|request| request.opcode == 18)
                })
                .count();
            if released >= count {
                return Ok(());
            }
            let remaining = deadline.checked_duration_since(Instant::now()).ok_or(
                "capture RELEASE completion timed out; recovery: rerun the live FUSE qualification on bb2",
            )?;
            packets = self
                .changed
                .wait_timeout(packets, remaining)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
    }
}

struct Recording;

impl Recording {
    fn start(capture: Arc<Capture>) -> Self {
        RECORDING.with_borrow_mut(|current| {
            assert!(current.is_none(), "one capture per mounting thread");
            *current = Some(capture);
        });
        Self
    }
}

impl Drop for Recording {
    fn drop(&mut self) {
        RECORDING.with_borrow_mut(|current| drop(current.take()));
    }
}

#[test]
#[ignore = "captures requests from a real Linux FUSE mount"]
fn kernel_packets_replay_through_the_mount_free_entry() -> Result<(), Box<dyn std::error::Error>> {
    use std::io::Write as _;

    let (source, _) = shared_sources(posix_config())?;
    let source = Arc::new(source);
    seed(&source)?;
    let capture = Arc::new(Capture::default());
    let enrollment = Recording::start(Arc::clone(&capture));
    let directory = tempfile::tempdir()?;
    let mut session = mount(source.clone(), source.volume_id()?, directory.path())?;
    drop(enrollment);
    let file = directory.path().join("packet-fixture");
    assert_eq!(std::fs::read(&file)?, b"before");
    let mut writer = std::fs::OpenOptions::new().write(true).open(&file)?;
    writer.write_all(b"after!")?;
    writer.sync_all()?;
    drop(writer);
    assert_eq!(std::fs::read(&file)?, b"after!");
    // Kernel release is asynchronous. Its real physical publication must
    // precede stop, otherwise replay would include teardown refusals.
    capture.wait_for_releases(3)?;
    assert!(session.stop()?);

    let mut packets = std::mem::take(
        &mut *capture
            .packets
            .lock()
            .unwrap_or_else(PoisonError::into_inner),
    );
    packets.sort_unstable_by_key(|packet| packet.sequence);
    replay(&packets)?;
    if let Some(path) = std::env::var_os("ACYCLIC_FS_FUSE_CAPTURE") {
        // A durable fixture is emitted only from actual successful kernel I/O
        // and successful replay. The coordinator can fetch this artifact.
        let provenance = serde_json::json!({
            "kind": "real Linux /dev/fuse requests and physically published replies",
            "captured_unix_ns": SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?.as_nanos().to_string(),
            "kernel": std::fs::read_to_string("/proc/version")?,
            "test_binary": std::env::current_exe()?,
            "test_arguments": std::env::args().collect::<Vec<_>>(),
            "entry_source_blake3": blake3::hash(include_bytes!("../packet_session.rs")).to_hex().to_string(),
            "packets": packets,
        });
        std::fs::write(path, serde_json::to_vec_pretty(&provenance)?)?;
    }
    Ok(())
}

fn seed(source: &MemorySource) -> Result<(), Box<dyn std::error::Error>> {
    source.create_file(
        &name("packet-fixture"),
        FileMetadata::default(),
        None,
        &fuse_fixture_work(),
    )?;
    source.write_range(&name("packet-fixture"), 0, Bytes::from_static(b"before"))?;
    Ok(())
}

fn replay(packets: &[CapturedPacket]) -> Result<(), Box<dyn std::error::Error>> {
    let (source, _) = shared_sources(posix_config())?;
    seed(&source)?;
    let core = projection_core(
        Arc::new(source),
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )?;
    let work = fuse_fixture_work();
    let _active = NativeMountWorkScope::new(&core, &work)?;
    let page_size = std::num::NonZeroU32::new(
        // SAFETY: sysconf has no pointer arguments or ownership effects.
        u32::try_from(unsafe { libc::sysconf(libc::_SC_PAGESIZE) })?,
    )
    .ok_or("kernel page size is zero")?;
    let protocol = PacketSession::new(
        PacketRouter::Single(FuseProjection { core }),
        MAXIMUM_WRITE,
        page_size,
    )?;
    // A short captured READ reply can come from a much larger requested read.
    // Retain the same negotiated capacity, rather than sizing from reply bytes.
    let capacity = MAXIMUM_WRITE as usize + 4096;
    let mut response = super::super::fuse_vec(capacity, &work)?;
    response.resize(capacity, 0);
    let mut seen = std::collections::BTreeSet::new();
    for packet in packets {
        let request =
            PacketRequest::parse(&packet.request).map_err(std::io::Error::from_raw_os_error)?;
        seen.insert(request.opcode);
        protocol
            .publish(&request, &mut response, |actual| {
                match (actual, packet.response.as_deref()) {
                    (None, None) => {}
                    (Some(actual), Some(expected)) => {
                        let mut actual_header = PacketInput::new(actual);
                        let mut expected_header = PacketInput::new(expected);
                        // Metadata timestamps and newly allocated source FileIds
                        // differ. The wire status, unique ID and read/write bytes
                        // must survive replay exactly.
                        assert_eq!(
                            actual_header.take(OUTPUT_HEADER),
                            expected_header.take(OUTPUT_HEADER)
                        );
                        if matches!(request.opcode, 15 | 16) {
                            assert_eq!(actual_header.remaining(), expected_header.remaining());
                        }
                    }
                    _ => {
                        return Err(NativeMountError::Lifecycle(
                            "captured no-reply shape changed during replay",
                        ));
                    }
                }
                Ok(())
            })
            .map_err(super::super::packet_dispatch::PacketFailure::into_native_error)?;
    }
    for opcode in [26, 1, 15, 16, 18] {
        assert!(
            seen.contains(&opcode),
            "real kernel capture lacks opcode {opcode}"
        );
    }
    protocol.close();
    Ok(())
}
