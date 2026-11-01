//! Actual Linux mappings/counters and the original captured kernel requests.

use super::super::vring_control::VringControl;
use super::super::vring_memory::GuestRam;
use super::*;
use std::fs::File;
use std::num::NonZeroU16;
use std::os::fd::{FromRawFd, IntoRawFd};
use std::os::unix::fs::FileExt;
use std::os::unix::net::UnixStream;
use vhost::vhost_user::message::VhostUserMemoryRegion;
use vm_memory::{Bytes, GuestAddress};
use vmm_sys_util::event::{EventFlag, new_event_consumer_and_notifier};

struct QueueGate {
    armed: AtomicBool,
    entered: Mutex<Option<SyncSender<[bool; 3]>>>,
    release: Mutex<Receiver<()>>,
    device: Weak<VringControl>,
    unwind: bool,
}

impl crate::native_mount::NativeMountCallbackGuard for QueueGate {
    fn check<'a>(
        &'a self,
        work: &'a crate::NativeMaterializationWork,
    ) -> Result<crate::native_mount::NativeMountCallbackFuture<'a>, NativeMountError> {
        crate::native_mount::NativeMountCallbackFuture::new_guarded(work, || async {
            if self.armed.load(Ordering::Acquire)
                && let Some(entered) = self
                    .entered
                    .lock()
                    .map_err(|_| std::io::Error::other("original gate lock poisoned"))?
                    .take()
            {
                let device = self
                    .device
                    .upgrade()
                    .ok_or_else(|| std::io::Error::other("original queue owner was dropped"))?;
                let unlocked = [
                    device.memory.try_write().is_ok(),
                    device.queues[0].state.try_lock().is_ok(),
                    device.queues[1].state.try_lock().is_ok(),
                ];
                entered.send(unlocked).map_err(std::io::Error::other)?;
                self.release
                    .lock()
                    .map_err(|_| std::io::Error::other("original release lock poisoned"))?
                    .recv()
                    .map_err(std::io::Error::other)?;
                if self.unwind {
                    std::panic::resume_unwind(Box::new(()));
                }
            }
            Ok::<(), std::io::Error>(())
        })
    }

    fn check_write<'a>(
        &'a self,
        work: &'a crate::NativeMaterializationWork,
    ) -> Result<crate::native_mount::NativeMountCallbackFuture<'a>, NativeMountError> {
        self.check(work)
    }
}

#[test]
fn packet_io_releases_original_guards_and_retires_exact_mapping_and_queue()
-> Result<(), Box<dyn std::error::Error>> {
    use super::super::packet::PacketRequest;
    use super::super::packet_buffers::PacketBuffers;
    use super::super::packet_router::PacketRouter;
    use super::super::packet_session::PacketSession;
    use std::num::NonZeroU32;
    use virtio_queue::desc::split::Descriptor;

    #[derive(serde::Deserialize)]
    struct Captured {
        request: Vec<u8>,
    }
    #[derive(serde::Deserialize)]
    struct Capture {
        packets: Vec<Captured>,
    }
    // The payloads are the existing real /dev/fuse capture, not authored frames.
    // rust-vmm supplies the split descriptor representation used to transport them.
    let capture: Capture = serde_json::from_str(include_str!(
        "../../../../fixtures/fuse/linux-kernel-packets.json"
    ))?;
    let captured = |opcode| {
        capture
            .packets
            .iter()
            .find(|packet| {
                PacketRequest::parse(&packet.request).is_ok_and(|request| request.opcode == opcode)
            })
            .ok_or("original captured kernel request missing")
    };
    let initialize = captured(26)?;
    let attributes = captured(3)?;
    let unique = PacketRequest::parse(&attributes.request)
        .map_err(std::io::Error::from_raw_os_error)?
        .unique;
    let original = cache_work(FINITE_CACHE_BUDGET, &crate::CancellationToken::new());
    let baseline = original.live_allocation_bytes();
    // Return, cancellation and producer unwind use the same physical path.
    for exit in 0..3 {
        let (work, _cancellation) = original.invocation_view()?;
        let (socket, _frontend) = UnixStream::pair()?;
        let device = VringControl::new(
            socket,
            NonZeroU16::new(8).ok_or("nonzero queue capacity")?,
            &work,
        )?;
        let (entered, observed) = sync_channel(1);
        let (release, resumed) = sync_channel(1);
        let guard = Arc::new(QueueGate {
            armed: AtomicBool::new(false),
            entered: Mutex::new(Some(entered)),
            release: Mutex::new(resumed),
            device: Arc::downgrade(&device),
            unwind: exit == 2,
        });
        let (source, _) = shared_sources(posix_config())?;
        let core = projection_core(
            Arc::new(source.with_native_callback_guard(guard.clone())),
            FINITE_HANDLER_POLICY,
            work.cancellation(),
        )?;
        let protocol = PacketSession::new(
            PacketRouter::Single(super::super::FuseProjection { core }),
            64 * 1024,
            NonZeroU32::new(4096).ok_or("nonzero page size")?,
        )?;
        let mut buffers = PacketBuffers::new(protocol.packet_capacity(), &work)?;
        buffers.output.resize(4096, 0);
        protocol
            .publish(
                &PacketRequest::parse(&initialize.request)
                    .map_err(std::io::Error::from_raw_os_error)?,
                &mut buffers.output,
                |_| Ok(()),
            )
            .map_err(super::super::packet_dispatch::PacketFailure::into_native_error)?;

        const USER: u64 = 0x100000;
        let file = tempfile::tempfile()?;
        file.set_len(16384)?;
        device.set_memory(
            &[VhostUserMemoryRegion {
                guest_phys_addr: 0,
                memory_size: 16384,
                user_addr: USER,
                mmap_offset: 0,
            }],
            vec![file],
        )?;
        device.set_features(super::super::vring_control::FEATURES)?;
        device.queue(1)?.set_size(8)?;
        device.set_addresses(1, USER, USER + 256, USER + 512)?;
        let (kick, _kick_producer) = counter()?;
        device.set_kick(1, Some(kick))?;
        let memory = device
            .memory
            .read()
            .map_err(|_| "memory guard poisoned")?
            .as_ref()
            .cloned()
            .ok_or("original memory missing")?;
        let ram = memory.memory();
        ram.write_slice(&attributes.request, GuestAddress(1024))?;
        ram.write_obj(
            Descriptor::new(1024, u32::try_from(attributes.request.len())?, 1, 1),
            GuestAddress(0),
        )?;
        ram.write_obj(Descriptor::new(4096, 4096, 2, 0), GuestAddress(16))?;
        ram.write_obj(0_u16, GuestAddress(516))?;
        ram.write_obj(1_u16, GuestAddress(514))?;
        device.set_enabled(1, true)?;
        guard.armed.store(true, Ordering::Release);
        drop(guard);
        let producer = Arc::clone(&device);
        let worker = std::thread::spawn(move || {
            super::super::vring_worker::batch(&producer, &protocol, 1, &mut buffers)
        });
        // A pre-dispatch error or unwind drops the sole original gate sender;
        // this receive then terminates by type rather than waiting forever.
        let unlocked = observed.recv();
        let observations = (|| -> Result<Option<(bool, u16)>, Box<dyn std::error::Error>> {
            if unlocked.is_err() {
                return Ok(None);
            }
            // Independent original queue configuration progresses during IO.
            device.queue(0)?.set_size(8)?;
            let active = device.queues[1]
                .state
                .try_lock()
                .map_err(|_| "callback retained the original queue guard")?;
            let unsettled = active.settled();
            drop(active);
            let unknown = matches!(unsettled, Err(NativeMountError::VirtioFsCompletionUnknown));
            let used = ram.read_obj::<u16>(GuestAddress(258))?;
            // Mutation of the guest's descriptor after its original snapshot
            // cannot redirect physical completion to a different output range.
            ram.write_obj(Descriptor::new(8192, 4096, 2, 0), GuestAddress(16))?;
            if exit == 1 {
                work.cancellation().cancel();
            }
            Ok(Some((unknown, used)))
        })();
        let (started, waiting) = sync_channel(1);
        let controller = Arc::clone(&device);
        let drain = std::thread::spawn(move || -> Result<u16, NativeMountError> {
            super::super::vring::probe_wait_registration(started);
            controller.drain(1)
        });
        // Registration holds the same queue mutex until the condition wait
        // releases it atomically. Completion therefore has to wake this wait.
        let waiting = waiting.recv();
        let _ = release.send(());
        let completion = worker.join();
        let drained = drain.join().map_err(|_| "original drain owner unwound")?;
        let observations = observations?;
        waiting?;
        assert_eq!(unlocked?, [true; 3]);
        assert_eq!(observations, Some((true, 0)));
        if exit == 0 {
            completion.map_err(|_| "original packet producer unwound")??;
            assert_eq!(drained?, 1);
            assert_eq!(ram.read_obj::<u16>(GuestAddress(258))?, 1);
            assert_eq!(ram.read_obj::<u64>(GuestAddress(4104))?, unique);
            assert_eq!(ram.read_obj::<u64>(GuestAddress(8200))?, 0);
        } else {
            if exit == 1 {
                let error = completion
                    .map_err(|_| "cancelled packet producer unwound")?
                    .expect_err("original cancellation cannot complete a used entry");
                assert_eq!(crate::native_mount::callback_errno(&error), libc::ECANCELED);
            } else {
                assert!(completion.is_err());
            }
            assert!(matches!(
                drained,
                Err(NativeMountError::VirtioFsCompletionUnknown)
            ));
        }
        assert!(device.memory.try_write().is_ok());
        assert!(device.queues[1].state.try_lock().is_ok());
    }
    original.charge(crate::WorkCounters::UNCHARGED)?;
    assert_eq!(original.live_allocation_bytes(), baseline);
    Ok(())
}

fn counter() -> std::io::Result<(File, File)> {
    let (consumer, notifier) =
        new_event_consumer_and_notifier(EventFlag::NONBLOCK | EventFlag::CLOEXEC)?;
    // SAFETY: each actual event endpoint transfers its owned descriptor once.
    Ok(unsafe {
        (
            File::from_raw_fd(consumer.into_raw_fd()),
            File::from_raw_fd(notifier.into_raw_fd()),
        )
    })
}

#[test]
fn vring_events_reject_aliases_and_retain_polled_counter_custody()
-> Result<(), Box<dyn std::error::Error>> {
    let work = cache_work(FINITE_CACHE_BUDGET, &crate::CancellationToken::new());
    let baseline = work.live_allocation_bytes();
    let (socket, _frontend) = UnixStream::pair()?;
    let device = VringControl::new(
        socket,
        NonZeroU16::new(256).ok_or("nonzero maximum queue size")?,
        &work,
    )?;
    let (kick, same_counter) = counter()?;
    device.set_kick(0, Some(kick))?;
    // Duplicated SCM_RIGHTS descriptors have different fd numbers, but refer
    // to the same kernel counter and cannot be independent kicks or calls.
    assert!(
        matches!(device.set_kick(1, Some(same_counter.try_clone()?)), Err(NativeMountError::Io(error)) if error.raw_os_error() == Some(libc::EINVAL))
    );
    assert!(
        matches!(device.set_call(1, Some(same_counter.try_clone()?)), Err(NativeMountError::Io(error)) if error.raw_os_error() == Some(libc::EINVAL))
    );
    assert!(
        matches!(device.set_error(1, Some(same_counter)), Err(NativeMountError::Io(error)) if error.raw_os_error() == Some(libc::EINVAL))
    );

    let (independent, _) = counter()?;
    device.set_kick(1, Some(independent))?;
    let (shared_call, duplicate_call) = counter()?;
    device.set_call(0, Some(shared_call))?;
    device.set_call(1, Some(duplicate_call))?;
    let (error, duplicate_error) = counter()?;
    device.set_error(0, Some(error))?;
    assert!(
        matches!(device.set_kick(1, Some(duplicate_error)), Err(NativeMountError::Io(error)) if error.raw_os_error() == Some(libc::EINVAL))
    );

    let polled = device
        .queue(0)?
        .kick
        .clone()
        .ok_or("original kick missing")?;
    // Reconcile retired fdinfo scratch custody before measuring the retained
    // counter. SET_KICK's admission performs the same canonical reconciliation.
    work.charge(crate::WorkCounters::UNCHARGED)?;
    let retained = work.live_allocation_bytes();
    device.set_kick(0, None)?;
    assert_eq!(work.live_allocation_bytes(), retained);
    assert!(!device.queue(0)?.accepts_kick(&polled));
    drop(polled);
    work.charge(crate::WorkCounters::UNCHARGED)?;
    assert!(work.live_allocation_bytes() < retained);
    drop(device);
    work.charge(crate::WorkCounters::UNCHARGED)?;
    assert_eq!(work.live_allocation_bytes(), baseline);
    Ok(())
}

#[test]
fn memory_replacement_keeps_original_mapping_until_admission()
-> Result<(), Box<dyn std::error::Error>> {
    let original = cache_work(FINITE_CACHE_BUDGET, &crate::CancellationToken::new());
    let baseline = original.live_allocation_bytes();
    {
        let (work, _cancellation) = original.invocation_view()?;
        let (socket, _frontend) = UnixStream::pair()?;
        let device = VringControl::new(
            socket,
            NonZeroU16::new(256).ok_or("nonzero maximum queue size")?,
            &work,
        )?;
        const USER: u64 = 0x100000;
        let offers = [VhostUserMemoryRegion {
            guest_phys_addr: 0,
            memory_size: 4096,
            user_addr: USER,
            mmap_offset: 0,
        }];
        let mapped_file = |byte| -> std::io::Result<File> {
            let file = tempfile::tempfile()?;
            file.set_len(4096)?;
            file.write_all_at(&[byte], 2048)?;
            Ok(file)
        };
        let unlocked = || {
            assert!(device.memory.try_write().is_ok());
            for queue in &device.queues {
                assert!(queue.state.try_lock().is_ok());
            }
        };
        let original = mapped_file(7)?;
        device.replace_memory(|| {
            unlocked();
            GuestRam::new(&offers, vec![original], &work)
        })?;
        let inspect = || -> Result<u8, Box<dyn std::error::Error>> {
            let memory = device.memory.read().map_err(|_| "memory lock poisoned")?;
            Ok(memory
                .as_ref()
                .ok_or("original mapping missing")?
                .memory()
                .read_obj(GuestAddress(2048))?)
        };
        assert_eq!(inspect()?, 7);
        let replacement = mapped_file(9)?;
        let late_configuration = device.replace_memory(|| {
            unlocked();
            let prepared = GuestRam::new(&offers, vec![replacement], &work)?;
            // A valid setup of the ORIGINAL ring wins while physical preparation
            // is outside the guards. The late candidate cannot erase its mapping.
            device.queue(0)?.set_size(8)?;
            device.set_addresses(0, USER, USER + 256, USER + 512)?;
            Ok(prepared)
        });
        assert!(
            matches!(late_configuration, Err(NativeMountError::Io(error)) if error.raw_os_error() == Some(libc::EBUSY))
        );
        assert_eq!(inspect()?, 7);
        let mut prepared_while_ready = false;
        let refused = device.replace_memory(|| {
            prepared_while_ready = true;
            GuestRam::new(&offers, vec![mapped_file(10)?], &work)
        });
        assert!(
            matches!(refused, Err(NativeMountError::Io(error)) if error.raw_os_error() == Some(libc::EBUSY))
        );
        assert!(!prepared_while_ready);
        device.queue(0)?.invalidate_addresses();
        let replacement = mapped_file(11)?;
        device.set_memory(&offers, vec![replacement])?;
        assert_eq!(inspect()?, 11);
        let replacement = mapped_file(13)?;
        let cancelled = device.replace_memory(|| {
            unlocked();
            let prepared = GuestRam::new(&offers, vec![replacement], &work)?;
            work.cancellation().cancel();
            Ok(prepared)
        });
        assert!(matches!(
            cancelled,
            Err(NativeMountError::Materialization(
                crate::NativeTreeMaterializationError::Cancelled
            ))
        ));
        assert_eq!(inspect()?, 11);
        unlocked();
    }
    original.charge(crate::WorkCounters::UNCHARGED)?;
    assert_eq!(original.live_allocation_bytes(), baseline);
    Ok(())
}
