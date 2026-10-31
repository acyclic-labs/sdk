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
            self.ensure_spill()?;
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

    /// Before eviction this buffer is the complete output, so a presentation
    /// cut discovered at settlement can still retain every original byte.
    pub(super) fn ensure_spill(&mut self) -> io::Result<()> {
        if self.spill.is_none() {
            let mut file = std::fs::OpenOptions::new().write(true).create_new(true).open(&self.path)
                .map_err(|error| io::Error::new(error.kind(), format!("shell spill creation failed: {error}; recovery: inspect {} and reconcile the original command", self.path.display())))?;
            let (first, second) = self.bytes.as_slices();
            file.write_all(first).and_then(|()| file.write_all(second)).map_err(|error| {
                io::Error::new(error.kind(), format!("shell spill prefix write failed: {error}; recovery: inspect {} and reconcile the original command", self.path.display()))
            })?;
            self.spill = Some(file);
        }
        Ok(())
    }

    pub(super) fn settle(
        mut self,
        mut result: crate::tool::ToolResult,
        maximum: u64,
    ) -> crate::Result<crate::tool::ToolResult> {
        let (stdout, stderr, omitted) = self.text();
        let fields = result.value.as_object_mut().ok_or_else(|| crate::Error::Invalid(
            "shell settlement requires terminal fields; recovery: bind the shared shell executor".into(),
        ))?;
        fields.insert("stdout".into(), stdout.into());
        fields.insert("stderr".into(), stderr.into());
        fields.insert("omitted_bytes".into(), omitted.into());
        let mut result = crate::tools::shell::bounded_result(result, maximum)?;
        if result.value["omitted_bytes"].as_u64().unwrap_or_default() > 0 {
            self.ensure_spill().map_err(crate::Error::process_io)?;
        } else {
            result.value["spill"] = serde_json::Value::Null;
        }
        Ok(result)
    }

    fn text(&mut self) -> (String, String, u64) {
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
        (stdout, stderr, omitted)
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
        fn chunks_preserve_tail_and_complete_spill(bytes in prop::collection::vec(any::<u8>(), 0..4096), maximum in 0usize..128, chunk in 1usize..256, render in 256u64..1024) {
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
            prop_assert_eq!(tail.spill.is_some(), bytes.len() > maximum);
            let result = tail.settle(crate::tool::ToolResult {
                value: serde_json::json!({"exit_code": 0, "spill": ".volumes/scratch/output"}),
            }, render)?;
            let projected = serde_json::json!({"kind":"parts", "parts":[{"kind":"text", "text":result.value["text"]}]}).to_string();
            prop_assert!(projected.len() as u64 <= render);
            let omitted = result.value["omitted_bytes"].as_u64().unwrap();
            if omitted > 0 {
                prop_assert_eq!(std::fs::read(path)?, bytes);
            } else {
                prop_assert!(!path.exists());
            }
        }
    }
}
