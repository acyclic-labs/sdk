use super::*;
use bytes::Bytes;
use std::{
    future::{Future as _, poll_fn},
    io::{Read as _, Seek as _},
    os::fd::{AsFd as _, AsRawFd, FromRawFd as _},
    sync::{Arc, Mutex},
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
            let Backend::Readiness(descriptor) = &output.backend else {
                panic!("Linux pipes must retain native readiness");
            };
            assert_ne!(flags(descriptor.get_ref())? & libc::O_NONBLOCK, 0,);

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

        // A regular file retains its original shared cursor and original
        // payload. Only the owned task may submit its synchronous write.
        let mut file = tempfile::tempfile()?;
        let original_flags = flags(&file)?;
        let mut output = InheritedOutput::from_file(file.try_clone()?)?;
        assert_eq!(
            output.write(b"borrowed").await.unwrap_err().kind(),
            io::ErrorKind::Unsupported
        );
        assert_eq!(flags(&file)?, original_flags);
        assert_eq!(file.metadata()?.len(), 0);
        let bytes = b"original redirected bytes".to_vec();
        let pointer = bytes.as_ptr();
        let (output, bytes) = output.write_owned(bytes).await?;
        assert_eq!(bytes.as_ptr(), pointer);
        assert_eq!(file.stream_position()?, bytes.len() as u64);
        let (output, _) = output.write_owned(b"\n").await?;
        drop(output);
        file.rewind()?;
        let mut actual = Vec::new();
        file.read_to_end(&mut actual)?;
        assert_eq!(actual, b"original redirected bytes\n");
        assert_eq!(flags(&file)?, original_flags);

        // Blocking terminals keep the original endpoint rather than reopening
        // a PTY master (which would create another endpoint).
        // SAFETY: posix_openpt returns an unaliased descriptor or -1.
        #[expect(unsafe_code, reason = "opens a real terminal admission control")]
        let master = unsafe { libc::posix_openpt(libc::O_RDWR | libc::O_NOCTTY | libc::O_CLOEXEC) };
        if master < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the successful descriptor is transferred exactly once.
        #[expect(unsafe_code, reason = "owns the original terminal control descriptor")]
        let terminal = unsafe { File::from_raw_fd(master) };
        let original_flags = flags(&terminal)?;
        let output = InheritedOutput::from_file(terminal.try_clone()?)?;
        assert!(matches!(output.backend, Backend::Synchronous(_)));
        assert_eq!(flags(&terminal)?, original_flags);
        synchronous_output_retains_owned_credit_through_observer_loss().await?;
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

struct OwnedCredit {
    bytes: Vec<u8>,
    pointer: usize,
    credit: Option<tokio::sync::OwnedSemaphorePermit>,
    caller: std::thread::ThreadId,
    entered: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    retired: Option<tokio::sync::oneshot::Sender<()>>,
}

impl AsRef<[u8]> for OwnedCredit {
    fn as_ref(&self) -> &[u8] {
        assert_eq!(self.bytes.as_ptr() as usize, self.pointer);
        if std::thread::current().id() != self.caller
            && let Some(entered) = self.entered.lock().unwrap().take()
        {
            let _ = entered.send(());
        }
        &self.bytes
    }
}

impl Drop for OwnedCredit {
    fn drop(&mut self) {
        drop(self.credit.take());
        if let Some(retired) = self.retired.take() {
            let _ = retired.send(());
        }
    }
}

async fn synchronous_output_retains_owned_credit_through_observer_loss() -> io::Result<()> {
    for disposition in ["drain", "cancel", "close"] {
        let (mut reader, original) = std::os::unix::net::UnixStream::pair()?;
        let original: File = std::os::fd::OwnedFd::from(original).into();
        let original_flags = flags(&original)?;
        let filled = fill_socket(&original)?;
        let output = InheritedOutput::from_file(original.try_clone()?)?;
        let credit = Arc::new(Semaphore::new(1));
        let (entered, started) = tokio::sync::oneshot::channel();
        let (retired, retirement) = tokio::sync::oneshot::channel();
        let bytes = b"original owned synchronous bytes".to_vec();
        let pointer = bytes.as_ptr() as usize;
        let buffer = OwnedCredit {
            bytes,
            pointer,
            credit: Some(
                Arc::clone(&credit)
                    .acquire_owned()
                    .await
                    .map_err(io::Error::other)?,
            ),
            caller: std::thread::current().id(),
            entered: Mutex::new(Some(entered)),
            retired: Some(retired),
        };
        let mut delivery = Some(Box::pin(output.write_owned(buffer)));
        {
            // Even a surrounding inline-admitted host callback cannot
            // execute a synchronous inherited write on this coordinator.
            let _inline = crate::InlineBlocking::enter();
            poll_fn(|cx| {
                assert!(delivery.as_mut().unwrap().as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
        }
        started.await.map_err(io::Error::other)?;
        assert_eq!(credit.available_permits(), 0);
        assert_eq!(flags(&original)?, original_flags);
        assert_eq!(crate::run_blocking_io(|| 17, None)?.await?, 17);
        if disposition == "close" {
            drop(reader);
            let error = match delivery.take().unwrap().await {
                Ok(_) => panic!("a withdrawn reader cannot accept a full-socket write"),
                Err(error) => error,
            };
            assert!(matches!(
                error.kind(),
                io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset
            ));
            assert!(error.raw_os_error().is_some());
        } else {
            if disposition == "cancel" {
                drop(delivery.take());
                // The accepted worker still owns the original bytes and
                // credit; observer destruction is not write completion.
                assert_eq!(credit.available_permits(), 0);
            }
            drop(original);
            let mut read = Box::pin(crate::run_blocking_io(
                move || {
                    drain(&mut reader, filled)?;
                    let mut bytes = Vec::new();
                    reader.read_to_end(&mut bytes)?;
                    Ok::<_, io::Error>(bytes)
                },
                None,
            )?);
            let completed = poll_fn(|cx| {
                Poll::Ready(match read.as_mut().poll(cx) {
                    Poll::Ready(result) => Some(result),
                    Poll::Pending => None,
                })
            })
            .await;
            if disposition == "drain" {
                let (output, buffer) = delivery.take().unwrap().await?;
                assert_eq!(buffer.bytes.as_ptr() as usize, pointer);
                assert_eq!(credit.available_permits(), 0);
                drop(output);
                drop(buffer);
            }
            // EOF comes from the original descriptor closing, after the
            // same worker has returned from its physical write.
            let actual = match completed {
                Some(result) => result??,
                None => read.await??,
            };
            assert_eq!(actual, b"original owned synchronous bytes");
        }
        retirement.await.map_err(io::Error::other)?;
        assert_eq!(credit.available_permits(), 1);
    }
    Ok(())
}

fn fill_socket(file: &File) -> io::Result<usize> {
    let bytes = [b'x'; 8192];
    let mut filled = 0;
    loop {
        // MSG_DONTWAIT exhausts actual kernel credit without changing the
        // inherited socket's shared flags or relying on a guessed capacity.
        // SAFETY: send borrows a live descriptor and initialized stack slice;
        // this synchronous call retains no user buffer after returning.
        #[expect(
            unsafe_code,
            reason = "fills a real socket without changing shared flags"
        )]
        let written = unsafe {
            libc::send(
                file.as_raw_fd(),
                bytes.as_ptr().cast(),
                bytes.len(),
                libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL,
            )
        };
        if written > 0 {
            filled += written as usize;
            continue;
        }
        if written == 0 {
            return Err(io::ErrorKind::WriteZero.into());
        }
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::WouldBlock {
            assert_ne!(filled, 0);
            return Ok(filled);
        }
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}
