//! Concurrent attachment and input pumping for split exec transports.
use crate::{ExecOutput, ExecStream, ProviderError};
use futures::{StreamExt, TryStreamExt, future::BoxFuture, stream};
use std::future::Future;

/// Output headers can depend on input. Poll both attachments and the input
/// pump together, then settle the exit only after all bytes have drained.
pub(super) fn duplex_exec<SO, SE>(
    stdout: impl Future<Output = Result<SO, ProviderError>> + Send + 'static,
    stderr: impl Future<Output = Result<SE, ProviderError>> + Send + 'static,
    input: impl Future<Output = Result<(), ProviderError>> + Send + 'static,
    exit: impl Future<Output = Result<i32, ProviderError>> + Send + 'static,
) -> ExecStream
where
    SO: futures::Stream<Item = Result<Vec<u8>, ProviderError>> + Send + 'static,
    SE: futures::Stream<Item = Result<Vec<u8>, ProviderError>> + Send + 'static,
{
    fn output<S>(
        attachment: impl Future<Output = Result<S, ProviderError>> + Send + 'static,
        channel: fn(Vec<u8>) -> ExecOutput,
    ) -> ExecStream
    where
        S: futures::Stream<Item = Result<Vec<u8>, ProviderError>> + Send + 'static,
    {
        stream::once(attachment)
            .try_flatten()
            .try_filter_map(
                move |bytes| async move { Ok((!bytes.is_empty()).then(|| channel(bytes))) },
            )
            .boxed()
    }
    let stdout = output(stdout, ExecOutput::Stdout);
    let stderr = output(stderr, ExecOutput::Stderr);
    let input = stream::once(async move { input.await.map(|()| None::<ExecOutput>) })
        .try_filter_map(|item| async move { Ok(item) });
    stream::select(stream::select(stdout, stderr), input)
        .chain(stream::once(
            async move { exit.await.map(ExecOutput::ExitCode) },
        ))
        .boxed()
}

pub(super) async fn pump_stdin(
    input: Option<Vec<u8>>,
    mut write: impl FnMut(u64, Vec<u8>, bool) -> BoxFuture<'static, Result<(), ProviderError>>,
) -> Result<(), ProviderError> {
    if let Some(data) = input {
        for (offset, bytes) in data.chunks(65_536).enumerate() {
            write(offset as u64 * 65_536, bytes.to_vec(), false).await?;
        }
        write(data.len() as u64, Vec::new(), true).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicBool, Ordering},
    };

    #[tokio::test]
    async fn input_unblocks_both_attachments_and_exit_follows_all_output()
    -> Result<(), Box<dyn std::error::Error>> {
        let (stdout_ready, stdout_gate) = tokio::sync::oneshot::channel();
        let (stderr_ready, stderr_gate) = tokio::sync::oneshot::channel();
        let drained = Arc::new(AtomicBool::new(false));
        let stdout_drained = Arc::clone(&drained);
        let stdout = async move {
            stdout_gate.await.map_err(|_| ProviderError::Unavailable)?;
            Ok(stream::iter([Ok(vec![0, 255])])
                .chain(stream::once(async move {
                    stdout_drained.store(true, Ordering::Release);
                    Ok(vec![1])
                }))
                .boxed())
        };
        let stderr = async move {
            stderr_gate.await.map_err(|_| ProviderError::Unavailable)?;
            Ok(stream::iter([Ok(vec![255, 0])]).boxed())
        };
        let writes = Arc::new(Mutex::new(Vec::new()));
        let actual_writes = Arc::clone(&writes);
        let input = async move {
            pump_stdin(Some(vec![9; 65_537]), move |offset, bytes, eof| {
                actual_writes
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push((offset, bytes.len(), eof));
                Box::pin(async { Ok(()) })
            })
            .await?;
            stdout_ready
                .send(())
                .map_err(|_| ProviderError::Unavailable)?;
            stderr_ready
                .send(())
                .map_err(|_| ProviderError::Unavailable)?;
            Ok(())
        };
        let exit = async move {
            assert!(
                drained.load(Ordering::Acquire),
                "wait must follow output drain"
            );
            Ok(7)
        };
        let result: Vec<_> = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            duplex_exec(stdout, stderr, input, exit).try_collect(),
        )
        .await??;
        assert!(matches!(result.last(), Some(ExecOutput::ExitCode(7))));
        assert_eq!(
            result
                .iter()
                .filter(|frame| matches!(frame, ExecOutput::Stdout(_)))
                .count(),
            2
        );
        assert_eq!(
            result
                .iter()
                .filter(|frame| matches!(frame, ExecOutput::Stderr(_)))
                .count(),
            1
        );
        assert_eq!(
            *writes.lock().unwrap_or_else(PoisonError::into_inner),
            [(0, 65_536, false), (65_536, 1, false), (65_537, 0, true)]
        );
        Ok(())
    }
}
