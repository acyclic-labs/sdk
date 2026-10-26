//! Line-framed readiness for subprocess fixtures sharing stdout with libtest.
use std::io::{self, BufRead, Write};

/// Publish a complete record even when libtest has left an unterminated prefix.
/// Hold the stdout lock while calling this function to keep the frame together.
pub fn signal(writer: &mut impl Write, marker: &str) -> io::Result<()> {
    writeln!(writer, "\n{marker}")?;
    writer.flush()
}

/// Consume fixture output through its complete readiness record.
/// The caller retains the reader, including any buffered output after the record.
pub fn wait(reader: &mut impl BufRead, marker: &str) -> io::Result<()> {
    let mut line = String::new();
    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "fixture exited before signaling readiness",
            ));
        }
        if line.ends_with('\n') && line.trim_end_matches(['\r', '\n']) == marker {
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests;
