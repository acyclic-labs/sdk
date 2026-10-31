//! Bounded presentation shared by local and streamed shell capture.
use std::{collections::VecDeque, io};

/// A byte-bounded suffix with its original stdout/stderr channel boundaries.
/// This owns no output file or producer: the caller retains full bytes through
/// its original publication path before an eviction callback returns.
pub struct OutputTail {
    bytes: VecDeque<u8>,
    channels: VecDeque<(bool, usize)>,
    total: u64,
    maximum: usize,
}

impl OutputTail {
    /// Bounds presentation storage independently of physical output admission.
    pub fn new(maximum: usize) -> Self {
        Self {
            bytes: VecDeque::new(),
            channels: VecDeque::new(),
            total: 0,
            maximum,
        }
    }

    /// Observes bytes already retained by the caller's original output sink.
    pub fn observe(&mut self, bytes: &[u8], stderr: bool) -> io::Result<()> {
        self.observe_with(bytes, stderr, |_, _| Ok(()))
    }

    /// Before presentation evicts bytes, gives the sink the previous retained
    /// prefix. The first such prefix is complete; later prefixes are bounded
    /// suffixes. A sink failure leaves the presentation unchanged and fails the
    /// original producer rather than silently losing accepted output.
    pub fn observe_with(
        &mut self,
        bytes: &[u8],
        stderr: bool,
        before_eviction: impl FnOnce(&[u8], &[u8]) -> io::Result<()>,
    ) -> io::Result<()> {
        let total = self.total.checked_add(bytes.len() as u64).ok_or_else(|| {
            io::Error::other(
                "shell output count overflow; recovery: reconcile the original command",
            )
        })?;
        if total > self.maximum as u64 {
            let (first, second) = self.bytes.as_slices();
            before_eviction(first, second)?;
        }
        self.total = total;
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

    /// The retained prefix before the first omission, or bounded suffix after.
    pub fn as_slices(&self) -> (&[u8], &[u8]) {
        self.bytes.as_slices()
    }

    /// Channel text and the exact count of omitted original bytes. A suffix
    /// starting inside a UTF-8 scalar omits its partial prefix instead of
    /// inventing a replacement scalar at the presentation boundary.
    pub fn text(&mut self) -> (String, String, u64) {
        let raw = self.bytes.make_contiguous();
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let mut offset = 0;
        for &(error, length) in &self.channels {
            let output = if error { &mut stderr } else { &mut stdout };
            output.extend_from_slice(&raw[offset..offset + length]);
            offset += length;
        }
        let mut omitted = self.total - raw.len() as u64;
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
        (stdout, stderr, omitted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn retained_bytes_and_channels_are_the_original_suffix(
            bytes in prop::collection::vec(any::<u8>(), 0..4096),
            maximum in 0usize..128,
            chunk in 1usize..256,
        ) {
            let mut tail = OutputTail::new(maximum);
            let mut channels = Vec::new();
            for (index, bytes) in bytes.chunks(chunk).enumerate() {
                let stderr = index % 2 == 1;
                channels.extend(std::iter::repeat_n(stderr, bytes.len()));
                tail.observe(bytes, stderr)?;
                prop_assert!(tail.bytes.len() <= maximum);
            }
            let start = bytes.len().saturating_sub(maximum);
            prop_assert!(tail.bytes.iter().copied().eq(bytes[start..].iter().copied()));
            let retained_channels = tail.channels.iter()
                .flat_map(|(channel, length)| std::iter::repeat_n(*channel, *length))
                .collect::<Vec<_>>();
            prop_assert_eq!(retained_channels.as_slice(), &channels[start..]);
            let original = tail.bytes.clone();
            let total = tail.total;
            let rejected = tail.observe_with(&[0; 128], false, |_, _| {
                Err(io::Error::other("retention failure negative control"))
            });
            if total + 128 > maximum as u64 {
                prop_assert!(rejected.is_err());
                prop_assert_eq!(tail.bytes, original);
                prop_assert_eq!(tail.total, total);
            }
        }
    }
}
