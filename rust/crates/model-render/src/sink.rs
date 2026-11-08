use std::io::{self, Write};

/// One sink for raw segments and JSON serialization. Unlike Write's default
/// write_all, an Interrupted error is returned to the request owner immediately.
/// A successful partial write strictly decreases the remaining byte count.
pub(crate) struct Sink<'a, W>(pub(crate) &'a mut W);

impl<W: Write> Write for Sink<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.write(bytes)
    }

    fn write_all(&mut self, mut bytes: &[u8]) -> io::Result<()> {
        while !bytes.is_empty() {
            let written = self.write(bytes)?;
            if written == 0 {
                return Err(io::ErrorKind::WriteZero.into());
            }
            bytes = bytes
                .get(written..)
                .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidData))?;
        }
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}
