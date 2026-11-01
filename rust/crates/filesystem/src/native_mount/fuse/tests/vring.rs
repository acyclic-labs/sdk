//! Actual Linux counter ownership, without manufactured provider/ring traffic.

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
                assert!(queue.try_lock().is_ok());
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
