//! Complete native logs enter the shared publisher in one pass.
use super::*;
use futures::io::AsyncRead;
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

/// Recovery uses the same complete-line policy as live ingestion, retaining only the last
/// valid resume identity. A malformed event never hides a later valid event.
struct Resume {
    lines: acyclic_native_runtime::lines::LineBuffer,
    id: Option<String>,
}

impl Resume {
    fn new() -> Self {
        Self {
            lines: acyclic_native_runtime::lines::LineBuffer::new(None),
            id: None,
        }
    }

    fn push(&mut self, bytes: &[u8]) {
        // Bound the framer's returned collection even when its caller supplies
        // a larger read buffer. No event history is retained.
        for page in bytes.chunks(8192) {
            for line in self.lines.push(page) {
                self.record(line);
            }
        }
    }

    fn finish(&mut self) {
        if let Some(line) = self.lines.finish() {
            self.record(line);
        }
    }

    fn record(&mut self, line: io::Result<String>) {
        if let Ok(line) = line
            && let Some(id) = sessions::logged_session_id(line.as_bytes())
        {
            self.id = Some(id);
        }
    }
}

/// The open file remains under the original process fence until publication.
/// Its observed length funds this finite source; EOF, including a truncated
/// source's typed refusal, is produced by that same file rather than a timer.
pub(super) struct NativeEvents {
    file: futures::io::AllowStdIo<std::fs::File>,
    resume: Resume,
    length: u64,
    remaining: u64,
}

impl NativeEvents {
    pub(super) fn new(file: std::fs::File) -> Result<Self> {
        let length = file.metadata().map_err(storage)?.len();
        Ok(Self {
            file: futures::io::AllowStdIo::new(file),
            resume: Resume::new(),
            length,
            remaining: length,
        })
    }

    pub(super) fn maximum_bytes(&self) -> u64 {
        // The publisher requires positive credit even for an empty artifact.
        self.length.max(1)
    }

    pub(super) fn session(self) -> Option<String> {
        self.resume.id
    }
}

impl AsyncRead for NativeEvents {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if bytes.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let count = match Pin::new(&mut this.file).poll_read(cx, bytes) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => {
                return Poll::Ready(Err(io::Error::new(
                    error.kind(),
                    format!(
                        "step=read native event log: {error}; recovery: settle the original native turn again"
                    ),
                )));
            }
            Poll::Ready(Ok(count)) => count,
        };
        if count as u64 > this.remaining || (count == 0 && this.remaining != 0) {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "step=read native event log: source length changed under its process fence; recovery: restore the original custodied log before settling the turn",
            )));
        }
        this.remaining -= count as u64;
        this.resume.push(&bytes[..count]);
        if count == 0 {
            this.resume.finish();
        }
        Poll::Ready(Ok(count))
    }
}

/// Fuzzes the production incremental selector against closed original lines.
/// The oracle uses the existing JSON selector, independently of read chunking.
#[cfg(any(test, feature = "test-support"))]
pub fn check_native_event_resume(bytes: &[u8]) {
    let expected = bytes
        .split(|byte| *byte == b'\n')
        .filter_map(sessions::logged_session_id)
        .last();
    for chunk in [1, 7, 8192] {
        let mut resume = Resume::new();
        for page in bytes.chunks(chunk) {
            resume.push(page);
        }
        resume.finish();
        assert_eq!(
            resume.id, expected,
            "native resume depends on event chunking"
        );
    }
}
