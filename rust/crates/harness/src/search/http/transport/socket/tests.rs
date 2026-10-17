use super::*;
use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
use std::cell::Cell;
use tokio::io::AsyncReadExt as _;

struct Peer<'a> {
    received: &'a Cell<usize>,
}
impl AsyncRead for Peer<'_> {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let count = buffer.remaining().min(16_384);
        buffer.put_slice(&vec![b'x'; count]);
        self.received.set(self.received.get() + count);
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn raw_transport_cannot_receive_a_chunk_beyond_its_allowance() -> Result<()> {
    for allowance in [0, 1, 8192, 16_384] {
        let work = NativeMaterializationWork::new(
            WorkBudget {
                source_bytes_read: allowance,
                ..acyclic_fs::performance::BOUNDARY_BUDGET
            },
            &CancellationToken::new(),
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let baseline = work.live_allocation_bytes();
        let received = Cell::new(0);
        let mut failure = None;
        let mut socket = Socket::new(
            Peer {
                received: &received,
            },
            Some(&work),
            &mut failure,
        )?;
        let mut buffer = [0; 16_384];
        let first = socket.read(&mut buffer).await;
        if allowance == 0 {
            assert!(first.is_err());
        } else {
            assert_eq!(first.unwrap() as u64, allowance.min(8192));
        }
        if allowance > 0 && allowance <= 8192 {
            assert!(socket.read(&mut buffer).await.is_err());
        }
        assert_eq!(received.get() as u64, allowance.min(8192));
        assert_eq!(work.receipt().source_bytes_read, received.get() as u64);
        assert!(work.live_allocation_bytes() >= baseline + buffer.len() as u64);
        drop(socket);
        if allowance <= 8192 {
            assert!(
                matches!(failure, Some(Error::NativeWork { .. })),
                "{failure:?}"
            );
        }
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
    Ok(())
}

#[tokio::test]
async fn cancellation_and_retained_memory_denial_precede_transport_reads() -> Result<()> {
    for cancelled in [false, true] {
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(
            WorkBudget {
                peak_allocation_bytes: if cancelled {
                    1_048_576
                } else {
                    NativeMaterializationWork::accounting_state_bytes()
                },
                ..acyclic_fs::performance::BOUNDARY_BUDGET
            },
            &cancellation,
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let baseline = work.live_allocation_bytes();
        let received = Cell::new(0);
        let mut failure = None;
        let mut socket = Socket::new(
            Peer {
                received: &received,
            },
            Some(&work),
            &mut failure,
        )?;
        if cancelled {
            cancellation.cancel();
        }
        assert!(socket.read(&mut [0; 16_384]).await.is_err());
        assert_eq!(received.get(), 0);
        assert_eq!(work.receipt().source_bytes_read, 0);
        drop(socket);
        assert!(failure.is_some());
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
    Ok(())
}
