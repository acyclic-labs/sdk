//! Bounded presentation over the existing process stream. Overflow goes to
//! the task's scratch mount and is captured by the command's ordinary commit.
use std::{
    collections::VecDeque,
    io::{self, Write},
    path::PathBuf,
};

pub(super) struct Tail {
    bytes: VecDeque<u8>,
    total: u64,
    maximum: usize,
    path: PathBuf,
    spill: Option<std::fs::File>,
}

impl Tail {
    pub(super) fn new(path: PathBuf, maximum: usize) -> Self {
        Self {
            bytes: VecDeque::new(),
            total: 0,
            maximum,
            path,
            spill: None,
        }
    }

    pub(super) fn observe(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.total = self.total.checked_add(bytes.len() as u64).ok_or_else(|| {
            io::Error::other(
                "shell output count overflow; recovery: reconcile the original command",
            )
        })?;
        if self.spill.is_none() && self.total > self.maximum as u64 {
            let mut file = std::fs::OpenOptions::new().write(true).create_new(true).open(&self.path)
                .map_err(|error| io::Error::new(error.kind(), format!("shell spill creation failed: {error}; recovery: inspect {} and reconcile the original command", self.path.display())))?;
            let (first, second) = self.bytes.as_slices();
            file.write_all(first).and_then(|()| file.write_all(second)).map_err(|error| {
                io::Error::new(error.kind(), format!("shell spill prefix write failed: {error}; recovery: inspect {} and reconcile the original command", self.path.display()))
            })?;
            self.spill = Some(file);
        }
        if let Some(file) = &mut self.spill {
            file.write_all(bytes).map_err(|error| io::Error::new(error.kind(), format!("shell spill write failed: {error}; recovery: inspect {} and reconcile the original command", self.path.display())))?;
        }
        let keep = bytes.len().min(self.maximum);
        let remove = self
            .bytes
            .len()
            .saturating_add(keep)
            .saturating_sub(self.maximum);
        self.bytes.drain(..remove);
        self.bytes.extend(&bytes[bytes.len() - keep..]);
        Ok(())
    }

    pub(super) fn finish(mut self) -> (String, u64, bool) {
        // A byte boundary may cut the first scalar. Omit that partial scalar
        // rather than presenting an invented replacement as process output.
        let raw = self.bytes.make_contiguous();
        let skip = if self.total > raw.len() as u64 {
            raw.iter().take_while(|byte| **byte & 0xc0 == 0x80).count()
        } else {
            0
        };
        let text = String::from_utf8_lossy(&raw[skip..]).into_owned();
        (
            text,
            self.total - (raw.len() - skip) as u64,
            self.spill.is_some(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        // Arbitrary chunk boundaries cannot change the retained suffix or
        // exact spill bytes, and storage stays independent of total output.
        #[test]
        fn chunks_preserve_tail_and_complete_spill(bytes in prop::collection::vec(any::<u8>(), 0..4096), maximum in 0usize..128, chunk in 1usize..256) {
            let directory = tempfile::tempdir()?;
            let path = directory.path().join("output");
            let mut tail = Tail::new(path.clone(), maximum);
            for bytes in bytes.chunks(chunk) {
                tail.observe(bytes)?;
                prop_assert!(tail.bytes.len() <= maximum);
            }
            let raw = bytes.len().saturating_sub(maximum);
            prop_assert!(tail.bytes.iter().copied().eq(bytes[raw..].iter().copied()));
            let (_, _, spilled) = tail.finish();
            prop_assert_eq!(spilled, bytes.len() > maximum);
            if spilled {
                prop_assert_eq!(std::fs::read(path)?, bytes);
            } else {
                prop_assert!(!path.exists());
            }
        }
    }
}
