//! Exercise the maintained frontend over actual sockets, not wire fixtures.

use super::super::packet_router::PacketRouter;
use super::super::packet_session::PacketSession;
use super::super::vhost_session::VhostSession;
use super::super::vring_control::FEATURES;
use super::*;
use std::net::Shutdown;
use std::num::{NonZeroU16, NonZeroU32};
use std::os::unix::net::UnixStream;
use vhost::VhostBackend;
use vhost::vhost_user::{Frontend, VhostUserFrontend, message::VhostUserConfigFlags};

#[test]
fn standard_config_then_quiet_cancel_or_unexpected_eof_keeps_actual_outcome()
-> Result<(), Box<dyn std::error::Error>> {
    for cancel in [false, true] {
        let cancellation = crate::CancellationToken::new();
        let work = cache_work(FINITE_CACHE_BUDGET, &cancellation);
        let baseline = work.live_allocation_bytes();
        let (source, _) = shared_sources(posix_config())?;
        let core = projection_core(Arc::new(source), FINITE_HANDLER_POLICY, &cancellation)?;
        let protocol = PacketSession::new(
            PacketRouter::Single(super::super::FuseProjection { core }),
            64 * 1024,
            NonZeroU32::new(4096).ok_or(libc::EINVAL)?,
        )?;
        let (socket, peer) = UnixStream::pair()?;
        peer.set_read_timeout(Some(Duration::from_secs(2)))?;
        peer.set_write_timeout(Some(Duration::from_secs(2)))?;
        let disconnect = peer.try_clone()?;
        let session = VhostSession::new(
            protocol,
            socket,
            NonZeroU16::new(256).ok_or(libc::EINVAL)?,
            b"canonical-test",
            &work,
        )?;
        let (finished, completion) = sync_channel(1);
        let worker = std::thread::spawn(move || {
            let _ = finished.send(session.run());
        });
        let mut frontend = Frontend::from_stream(peer, 2);
        let negotiation = (|| -> Result<(), Box<dyn std::error::Error>> {
            assert_eq!(frontend.get_features()?, FEATURES);
            frontend.set_owner()?;
            let features = frontend.get_protocol_features()?;
            frontend.set_protocol_features(features)?;
            // Cloud's standard order inspects CONFIG before sharing RAM or
            // selecting virtio features. No source volume I/O is needed here.
            let (_, config) =
                frontend.get_config(0, 40, VhostUserConfigFlags::empty(), &[0; 40])?;
            assert_eq!(config.len(), 40);
            assert_eq!(&config[..14], b"canonical-test");
            assert!(config[14..36].iter().all(|byte| *byte == 0));
            assert_eq!(u32::from_le_bytes(config[36..40].try_into()?), 1);
            frontend.set_features(FEATURES)?;
            Ok(())
        })();
        let stopped = if cancel || negotiation.is_err() {
            cancellation.cancel();
            Ok(())
        } else {
            disconnect.shutdown(Shutdown::Both)
        };
        if stopped.is_err() {
            cancellation.cancel();
        }
        let result = completion.recv_timeout(Duration::from_secs(2));
        // A failed readiness/retirement assertion still positively stops and
        // joins its actual server; it cannot leak a quiet controller thread.
        cancellation.cancel();
        worker
            .join()
            .map_err(|_| std::io::Error::other("standard socket owner panicked"))?;
        negotiation?;
        stopped?;
        let retired = result?;
        assert!(retired.take_callback_failure().is_none());
        assert_eq!(
            retired.completed_callback_work(),
            crate::WorkCounters::UNCHARGED
        );
        if cancel {
            assert!(matches!(
                retired.transport,
                Err(NativeMountError::Materialization(
                    crate::NativeTreeMaterializationError::Cancelled
                ))
            ));
        } else {
            assert!(matches!(
                retired.transport,
                Err(NativeMountError::VirtioFsControl(
                    vhost::vhost_user::Error::Disconnected
                ))
            ));
        }
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
    Ok(())
}
