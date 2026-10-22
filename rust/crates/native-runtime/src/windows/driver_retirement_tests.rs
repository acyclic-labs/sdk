use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

struct Buffer(Arc<AtomicBool>);
impl AsRef<[u8]> for Buffer {
    fn as_ref(&self) -> &[u8] {
        b"abc"
    }
}
impl Drop for Buffer {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

fn bytes() -> (Bytes, Arc<AtomicBool>) {
    let dropped = Arc::new(AtomicBool::new(false));
    (Bytes::from_owner(Buffer(Arc::clone(&dropped))), dropped)
}

fn state(flags: Vec<Arc<AtomicBool>>, error: bool) -> (DriverState, DriverFile, Arc<AtomicBool>) {
    let (_, receiver) = mpsc::sync_channel(1);
    let mut state = DriverState::new(Proactor::new().unwrap(), receiver);
    let published = Arc::new(AtomicBool::new(false));
    let observed = Arc::clone(&published);
    state.batches.insert(
        1,
        Batch {
            remaining: flags.len(),
            first_error: None,
            result: BatchResult::Writes(Box::new(move |result| {
                assert!(
                    flags.iter().all(|flag| flag.load(Ordering::Acquire)),
                    "terminal callback retained a completed write buffer"
                );
                assert_eq!(result.is_err(), error);
                observed.store(true, Ordering::Release);
            })),
        },
    );
    let file = DriverFile::Original(Arc::new(NativeFilePayload {
        file: tempfile::tempfile().unwrap(),
        _allocation: None,
    }));
    (state, file, published)
}

#[test]
fn write_completion_retires_buffers_before_success_and_errors() {
    for count in [Ok(3), Ok(0), Ok(4), Err(io::Error::from_raw_os_error(1117))] {
        let error = !matches!(&count, Ok(3));
        let (buffer, dropped) = bytes();
        let (mut state, file, published) = state(vec![dropped], error);
        assert!(
            state
                .write_done(
                    1,
                    0,
                    compio_buf::BufResult(count, WriteAt::new(file, 0, buffer))
                )
                .is_none()
        );
        assert!(published.load(Ordering::Acquire));
    }
}

#[test]
fn write_completion_retains_partial_bytes_and_retires_every_batch_owner() {
    let (first, first_dropped) = bytes();
    let (second, second_dropped) = bytes();
    let (mut state, file, published) =
        state(vec![first_dropped.clone(), second_dropped.clone()], false);
    let (written, suffix) = state
        .write_done(
            1,
            0,
            compio_buf::BufResult(Ok(1), WriteAt::new(file.clone(), 0, first)),
        )
        .unwrap();
    assert_eq!(written, 1);
    assert_eq!(suffix.as_ref(), b"bc");
    assert!(!first_dropped.load(Ordering::Acquire));
    assert!(!published.load(Ordering::Acquire));
    assert!(
        state
            .write_done(
                1,
                0,
                compio_buf::BufResult(Ok(2), WriteAt::new(file.clone(), 1, suffix))
            )
            .is_none()
    );
    assert!(first_dropped.load(Ordering::Acquire));
    assert!(!published.load(Ordering::Acquire));
    assert!(
        state
            .write_done(
                1,
                1,
                compio_buf::BufResult(Ok(3), WriteAt::new(file, 3, second))
            )
            .is_none()
    );
    assert!(published.load(Ordering::Acquire));
}

#[test]
fn write_completion_retires_real_driver_buffers_before_its_callback() -> io::Result<()> {
    use std::os::windows::fs::OpenOptionsExt as _;
    use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OVERLAPPED;
    let directory = tempfile::tempdir()?;
    for overlapped in [false, true] {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .custom_flags(if overlapped { FILE_FLAG_OVERLAPPED } else { 0 })
            .open(directory.path().join(format!("write-{overlapped}")))?;
        let file = Arc::new(NativeFilePayload {
            file,
            _allocation: None,
        });
        let (buffer, dropped) = bytes();
        let (sender, receiver) = mpsc::sync_channel(1);
        submit_write(
            file,
            overlapped,
            vec![OwnedWrite {
                offset: 0,
                bytes: buffer,
            }],
            Box::new(move |result| {
                let _ = sender.send((result, dropped.load(Ordering::Acquire)));
            }),
        )?;
        let (result, retired) = receiver
            .recv_timeout(Duration::from_secs(5))
            .map_err(io::Error::other)?;
        result?;
        assert!(
            retired,
            "the real completion callback still owns the write buffer"
        );
    }
    Ok(())
}
