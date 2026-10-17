//! JSON inside a JSON string, streamed through serde's string formatter.
use serde::{Serialize, Serializer};
use std::{fmt, io};

pub(super) struct JsonString<T>(pub T);
impl<T: Serialize> Serialize for JsonString<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}
impl<T: Serialize> fmt::Display for JsonString<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct Writer<'a, 'b>(&'a mut fmt::Formatter<'b>);
        impl io::Write for Writer<'_, '_> {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                let text = std::str::from_utf8(bytes).map_err(io::Error::other)?;
                self.0.write_str(text).map_err(io::Error::other)?;
                Ok(bytes.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        serde_json::to_writer(Writer(formatter), &self.0).map_err(|_| fmt::Error)
    }
}
