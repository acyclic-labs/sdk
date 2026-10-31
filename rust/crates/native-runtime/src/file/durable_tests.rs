//! Real held-file writes with a fault at the original Write boundary.
use super::*;
use std::io::{Read as _, Seek as _};

struct Writer {
    file: File,
    calls: usize,
    stop_at: usize,
    zero: bool,
    chunk: usize,
}

impl Writer {
    fn new(stop_at: usize, zero: bool) -> io::Result<Self> {
        Ok(Self {
            file: tempfile::tempfile()?,
            calls: 0,
            stop_at,
            zero,
            chunk: 3,
        })
    }

    fn bytes(&mut self) -> io::Result<Vec<u8>> {
        self.file.rewind()?;
        let mut bytes = Vec::new();
        self.file.read_to_end(&mut bytes)?;
        Ok(bytes)
    }
}

impl Write for Writer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.calls += 1;
        if self.calls == self.stop_at {
            return if self.zero {
                Ok(0)
            } else {
                Err(io::ErrorKind::Interrupted.into())
            };
        }
        self.file.write(
            bytes
                .get(..bytes.len().min(self.chunk))
                .ok_or(io::ErrorKind::InvalidData)?,
        )
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

#[test]
fn durable_writer_stops_on_the_first_failure_and_keeps_exact_partial_progress() -> io::Result<()> {
    for stop_at in [1, 2, 3] {
        for zero in [false, true] {
            let mut writer = Writer::new(stop_at, zero)?;
            let error = write_file_all(&mut writer, b"retained").unwrap_err();
            assert_eq!(
                error.kind(),
                if zero {
                    io::ErrorKind::WriteZero
                } else {
                    io::ErrorKind::Interrupted
                }
            );
            assert_eq!(writer.calls, stop_at);
            assert_eq!(writer.bytes()?, b"retained"[..(stop_at - 1) * 3]);
        }
    }
    let mut writer = Writer::new(1, false)?;
    assert_eq!(
        write_file_all(&mut writer, b"retained").unwrap_err().kind(),
        io::ErrorKind::Interrupted
    );
    write_file_all(&mut writer, b"retained")?;
    assert_eq!(writer.calls, 4);
    assert_eq!(writer.bytes()?, b"retained");
    // The former std::Write::write_all implementation consumes the very same
    // interruption internally and writes bytes after the first failed call.
    let mut old = Writer::new(1, false)?;
    old.write_all(b"retained")?;
    assert_eq!(old.calls, 4);
    assert_eq!(old.bytes()?, b"retained");
    Ok(())
}
