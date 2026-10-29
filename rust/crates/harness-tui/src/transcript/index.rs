//! Fixed-width temporary indexes, with only the last record retained in memory.

use super::{invalid, overflow, read_bytes};
use std::{
    fs::File,
    io::{self, BufWriter, Seek, SeekFrom, Write},
    ops::Range,
};

pub(super) struct Index<const WORDS: usize> {
    file: BufWriter<File>,
    len: usize,
    last: Option<[u64; WORDS]>,
    failed: bool,
}

impl<const WORDS: usize> Index<WORDS> {
    pub(super) fn new() -> io::Result<Self> {
        Ok(Self {
            file: BufWriter::new(tempfile::tempfile()?),
            len: 0,
            last: None,
            failed: false,
        })
    }

    pub(super) fn len(&self) -> usize {
        self.len
    }

    pub(super) fn last(&self) -> Option<[u64; WORDS]> {
        self.last
    }

    pub(super) fn push(&mut self, record: [u64; WORDS]) -> io::Result<()> {
        let len = self.len.checked_add(1).ok_or_else(overflow)?;
        if self.failed {
            self.truncate(self.len)?;
        }
        // A partial failed write is never a published record. A subsequent
        // append first retires its bytes at the original logical end.
        self.failed = true;
        for word in record {
            self.file.write_all(&word.to_le_bytes())?;
        }
        self.len = len;
        self.last = Some(record);
        self.failed = false;
        Ok(())
    }

    pub(super) fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }

    pub(super) fn truncate(&mut self, len: usize) -> io::Result<()> {
        if len > self.len {
            return Err(invalid("index truncation would extend storage"));
        }
        if len == self.len && !self.failed {
            return Ok(());
        }
        let last = if len == self.len {
            self.last
        } else if len == 0 {
            None
        } else {
            self.page(len - 1..len)?.pop()
        };
        let bytes = address::<WORDS>(len)?;
        self.file.flush()?;
        self.file.get_mut().set_len(bytes)?;
        self.file.seek(SeekFrom::Start(bytes))?;
        self.len = len;
        self.last = last;
        self.failed = false;
        Ok(())
    }

    pub(super) fn page(&mut self, range: Range<usize>) -> io::Result<Vec<[u64; WORDS]>> {
        if range.start > range.end || range.end > self.len {
            return Err(invalid("index page outside storage"));
        }
        self.file.flush()?;
        let start = address::<WORDS>(range.start)?;
        let end = address::<WORDS>(range.end)?;
        self.file.get_mut().seek(SeekFrom::Start(start))?;
        let bytes = read_bytes(self.file.get_mut(), end - start);
        // Reads must not move the next append away from the published end.
        let restore = self
            .file
            .get_mut()
            .seek(SeekFrom::Start(address::<WORDS>(self.len)?));
        let bytes = bytes?;
        restore?;
        let mut records = Vec::with_capacity(range.len());
        for row in bytes.chunks_exact(WORDS * size_of::<u64>()) {
            let mut record = [0; WORDS];
            for (word, bytes) in record.iter_mut().zip(row.chunks_exact(size_of::<u64>())) {
                *word = u64::from_le_bytes(bytes.try_into().map_err(invalid)?);
            }
            records.push(record);
        }
        Ok(records)
    }
}

fn address<const WORDS: usize>(row: usize) -> io::Result<u64> {
    u64::try_from(row)
        .ok()
        .and_then(|row| row.checked_mul(WORDS as u64))
        .and_then(|words| words.checked_mul(size_of::<u64>() as u64))
        .ok_or_else(overflow)
}
