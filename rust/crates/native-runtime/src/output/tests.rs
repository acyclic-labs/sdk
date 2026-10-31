use super::*;
use bytes::Bytes;
use std::{
    future::{Future as _, poll_fn},
    io::Read as _,
    os::fd::{AsFd as _, AsRawFd},
    sync::Arc,
};
use tokio::{io::AsyncWriteExt as _, sync::Semaphore};

fn flags(file: &impl AsRawFd) -> io::Result<i32> {
    // SAFETY: F_GETFL reads flags on the still-borrowed descriptor.
    #[expect(
        unsafe_code,
        reason = "checks that the original shared flags are unchanged"
    )]
    let flags = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFL) };
    if flags < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(flags)
    }
}

struct Credited {
    bytes: Vec<u8>,
    _credit: tokio::sync::OwnedSemaphorePermit,
}

impl AsRef<[u8]> for Credited {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

#[test]
fn full_output_preserves_credit_flags_and_independent_progress() -> io::Result<()> {
    crate::coordination_runtime()?.block_on(async {
        for disposition in ["drain", "cancel", "close"] {
            let (mut reader, original) = std::io::pipe()?;
            let original_flags = flags(&original)?;
            assert_eq!(original_flags & libc::O_NONBLOCK, 0);
            let mut output =
                InheritedOutput::from_file(original.as_fd().try_clone_to_owned()?.into())?;
            assert_eq!(flags(&original)?, original_flags);
            assert_ne!(
                flags(output.descriptor.as_ref().unwrap().get_ref())? & libc::O_NONBLOCK,
                0,
            );

            // Await initial registration before exhausting real kernel credit.
            // Subsequent writes consume the cached readiness until the actual
            // full pipe clears it and registers its next drain/close event.
            let chunk = [b'x'; 8192];
            let mut filled = output.write(&chunk).await?;
            assert_ne!(filled, 0);
            loop {
                let written = poll_fn(|cx| {
                    Poll::Ready(match Pin::new(&mut output).poll_write(cx, &chunk) {
                        Poll::Ready(result) => Some(result),
                        Poll::Pending => None,
                    })
                })
                .await;
                let Some(written) = written else { break };
                let written = written?;
                assert_ne!(written, 0);
                filled += written;
            }
            assert_ne!(filled, 0);
            assert_eq!(output.write(&[]).await?, 0);

            let credit = Arc::new(Semaphore::new(1));
            let permit = Arc::clone(&credit)
                .acquire_owned()
                .await
                .map_err(io::Error::other)?;
            let bytes = Bytes::from_owner(Credited {
                bytes: b"original-credited-buffer".to_vec(),
                _credit: permit,
            });
            let pointer = bytes.as_ptr();
            {
                let writer = &mut output;
                let mut delivery = Box::pin(async move {
                    assert_eq!(bytes.as_ptr(), pointer);
                    writer.write_all(&bytes).await
                });
                poll_fn(|cx| {
                    assert!(delivery.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                assert_eq!(credit.available_permits(), 0);
                // The real pipe is full, yet the same runtime and its original
                // blocking slot remain available to unrelated producers.
                assert_eq!(
                    tokio::task::spawn_blocking(|| 17)
                        .await
                        .map_err(io::Error::other)?,
                    17,
                );
                assert_eq!(credit.available_permits(), 0);
                if disposition == "close" {
                    drop(reader);
                    assert_eq!(
                        delivery.await.unwrap_err().kind(),
                        io::ErrorKind::BrokenPipe
                    );
                } else {
                    if disposition == "cancel" {
                        drop(delivery);
                    } else {
                        drain(&mut reader, filled)?;
                        delivery.await?;
                        output.flush().await?;
                        let mut received = [0; b"original-credited-buffer".len()];
                        reader.read_exact(&mut received)?;
                        assert_eq!(&received, b"original-credited-buffer");
                        filled = 0;
                    }
                    assert_eq!(credit.available_permits(), 1);
                    output.shutdown().await?;
                    assert_eq!(
                        output.write(b"after close").await.unwrap_err().kind(),
                        io::ErrorKind::BrokenPipe,
                    );
                    // Closing the private descriptor cannot close the original.
                    assert_eq!(flags(&original)?, original_flags);
                    drop(original);
                    drain(&mut reader, filled)?;
                    assert_eq!(reader.read(&mut [0])?, 0);
                }
            }
            assert_eq!(credit.available_permits(), 1);
        }

        // O_NONBLOCK on a regular file does not promise nonblocking writes.
        // Admission refuses it without consuming the original cursor or bytes.
        let file = tempfile::tempfile()?;
        let original_flags = flags(&file)?;
        let refused = InheritedOutput::from_file(file.try_clone()?);
        assert!(matches!(refused, Err(error) if error.kind() == io::ErrorKind::Unsupported));
        assert_eq!(flags(&file)?, original_flags);
        assert_eq!(file.metadata()?.len(), 0);
        Ok(())
    })
}

fn drain(reader: &mut impl io::Read, mut bytes: usize) -> io::Result<()> {
    let mut buffer = [0; 8192];
    while bytes != 0 {
        let count = bytes.min(buffer.len());
        reader.read_exact(&mut buffer[..count])?;
        assert!(buffer[..count].iter().all(|byte| *byte == b'x'));
        bytes -= count;
    }
    Ok(())
}
