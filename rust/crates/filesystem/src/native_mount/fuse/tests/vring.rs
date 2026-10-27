//! Actual Linux counter ownership, without manufactured provider/ring traffic.

use super::super::vring_control::VringControl;
use super::*;
use std::fs::File;
use std::num::NonZeroU16;
use std::os::fd::{FromRawFd, IntoRawFd};
use std::os::unix::net::UnixStream;
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
