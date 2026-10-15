use super::*;
use crate::Error;
use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
use std::{
    cell::Cell,
    pin::Pin,
    task::{Context, Poll},
};

struct Reader<'a> {
    work: &'a NativeMaterializationWork,
    bytes: &'a [u8],
    polls: &'a Cell<usize>,
}

impl AsyncRead for Reader<'_> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buffer: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let remaining = self.work.remaining().expect("read must remain admitted");
        let budget = self.work.budget();
        let spent = self.work.receipt();
        assert!(
            buffer.len() as u64
                <= budget.source_bytes_read - spent.source_bytes_read - remaining.source_bytes_read
        );
        assert!(
            buffer.len() as u64 * 2
                <= budget.bytes_copied - spent.bytes_copied - remaining.bytes_copied
        );
        assert!(budget.items_examined - spent.items_examined - remaining.items_examined >= 1);
        self.polls.set(self.polls.get() + 1);
        let count = buffer.len().min(self.bytes.len());
        buffer[..count].copy_from_slice(&self.bytes[..count]);
        self.bytes = &self.bytes[count..];
        Poll::Ready(Ok(count))
    }
}

#[tokio::test]
async fn body_reads_are_bounded_and_admitted_before_polling() -> Result<()> {
    let body = br#"{"results":[]}"#;
    for length in [None, Some(body.len() as u64)] {
        for allowance in [0, 1, body.len() as u64, body.len() as u64 + 1] {
            let work = NativeMaterializationWork::new(
                WorkBudget {
                    source_bytes_read: allowance,
                    ..acyclic_fs::performance::work_budget(16_384, 16 * 1_024 * 1_024)
                },
                &CancellationToken::new(),
            )
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let baseline = work.live_allocation_bytes();
            let polls = Cell::new(0);
            let reader = Reader {
                work: &work,
                bytes: body,
                polls: &polls,
            };
            let result = read(reader, length, Some(&work)).await;
            let succeeds = allowance > body.len() as u64
                || (length.is_some() && allowance == body.len() as u64);
            if succeeds {
                assert_eq!(result.as_ref().unwrap().0, body);
            } else {
                assert!(
                    matches!(result, Err(Error::NativeWork { .. })),
                    "{result:?}"
                );
            }
            assert_eq!(
                work.receipt().source_bytes_read,
                allowance.min(body.len() as u64)
            );
            assert_eq!(
                work.receipt().bytes_copied,
                allowance.min(body.len() as u64) * 2
            );
            assert_eq!(
                polls.get(),
                usize::from(allowance > 0) + usize::from(length.is_none() && succeeds)
            );
            drop(result);
            assert_eq!(work.live_allocation_bytes(), baseline);
        }
    }
    Ok(())
}

#[tokio::test]
async fn body_copy_exhaustion_and_cancellation_precede_polling() -> Result<()> {
    for cancelled in [false, true] {
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(
            WorkBudget {
                bytes_copied: if cancelled { 16384 } else { 1 },
                ..acyclic_fs::performance::work_budget(16_384, 16 * 1_024 * 1_024)
            },
            &cancellation,
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let baseline = work.live_allocation_bytes();
        if cancelled {
            cancellation.cancel();
        }
        let polls = Cell::new(0);
        let result = read(
            Reader {
                work: &work,
                bytes: b"body",
                polls: &polls,
            },
            None,
            Some(&work),
        )
        .await;
        assert!(result.is_err());
        assert_eq!(polls.get(), 0);
        assert_eq!(work.receipt().source_bytes_read, 0);
        drop(result);
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
    Ok(())
}
