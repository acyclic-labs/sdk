//! Bounded presentation over the existing process stream. Overflow goes to
//! the task's scratch mount and is captured by the command's ordinary commit.
use std::{
    collections::VecDeque,
    io::{self, Write},
    path::PathBuf,
};

pub(super) struct Tail {
    bytes: VecDeque<u8>,
    channels: VecDeque<(bool, usize)>,
    total: u64,
    maximum: usize,
    path: PathBuf,
    spill: Option<std::fs::File>,
}

impl Tail {
    pub(super) fn new(path: PathBuf, maximum: usize) -> Self {
        Self {
            bytes: VecDeque::new(),
            channels: VecDeque::new(),
            total: 0,
            maximum,
            path,
            spill: None,
        }
    }

    pub(super) fn observe(&mut self, bytes: &[u8], stderr: bool) -> io::Result<()> {
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
        let mut evicted = remove;
        while evicted > 0 {
            let Some((_, length)) = self.channels.front_mut() else {
                break;
            };
            let cut = evicted.min(*length);
            *length -= cut;
            evicted -= cut;
            if *length == 0 {
                self.channels.pop_front();
            }
        }
        self.bytes.extend(&bytes[bytes.len() - keep..]);
        if keep > 0 {
            if let Some((_, length)) = self
                .channels
                .back_mut()
                .filter(|(channel, _)| *channel == stderr)
            {
                *length += keep;
            } else {
                self.channels.push_back((stderr, keep));
            }
        }
        Ok(())
    }

    pub(super) fn finish(mut self) -> (String, String, u64, bool) {
        let raw = self.bytes.make_contiguous();
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let mut offset = 0;
        for (error, length) in self.channels {
            let output = if error { &mut stderr } else { &mut stdout };
            output.extend_from_slice(&raw[offset..offset + length]);
            offset += length;
        }
        let mut omitted = self.total - raw.len() as u64;
        // A retained suffix can start inside a scalar on either channel.
        // Omit the partial scalar rather than displaying an invented byte.
        let mut text = |bytes: &[u8]| {
            let skip = if self.total > raw.len() as u64 {
                bytes
                    .iter()
                    .take_while(|byte| **byte & 0xc0 == 0x80)
                    .count()
            } else {
                0
            };
            omitted += skip as u64;
            String::from_utf8_lossy(&bytes[skip..]).into_owned()
        };
        let (stdout, stderr) = (text(&stdout), text(&stderr));
        (stdout, stderr, omitted, self.spill.is_some())
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
            let mut channels = Vec::new();
            for (index, bytes) in bytes.chunks(chunk).enumerate() {
                let stderr = index % 2 == 1;
                channels.extend(std::iter::repeat_n(stderr, bytes.len()));
                tail.observe(bytes, stderr)?;
                prop_assert!(tail.bytes.len() <= maximum);
            }
            let raw = bytes.len().saturating_sub(maximum);
            prop_assert!(tail.bytes.iter().copied().eq(bytes[raw..].iter().copied()));
            let retained_channels = tail.channels.iter().flat_map(|(channel, length)| std::iter::repeat_n(*channel, *length)).collect::<Vec<_>>();
            prop_assert_eq!(retained_channels.as_slice(), &channels[raw..]);
            let (_, _, _, spilled) = tail.finish();
            prop_assert_eq!(spilled, bytes.len() > maximum);
            if spilled {
                prop_assert_eq!(std::fs::read(path)?, bytes);
            } else {
                prop_assert!(!path.exists());
            }
        }
    }
}
