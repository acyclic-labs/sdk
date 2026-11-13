//! One bounded head/tail window of an interleaved output stream.
use crate::{Error, Result, tool::ToolResult};
use std::{collections::VecDeque, ops::Range};

pub(crate) struct Omission {
    omitted: u64,
    start: u64,
    end: u64,
}

pub(crate) fn notice(omitted: u64, start: u64, end: u64) -> Omission {
    Omission {
        omitted,
        start,
        end,
    }
}

impl std::fmt::Display for Omission {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.omitted == 0 {
            Ok(())
        } else {
            write!(
                formatter,
                "\n[{} bytes omitted in [{}, {})]\n",
                self.omitted, self.start, self.end
            )
        }
    }
}

impl serde::Serialize for Omission {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

/// A display window; complete output belongs to its original retained source.
/// Storage depends on the display quantum, never on the stream's length.
pub struct OutputWindow {
    head: Vec<(u8, bool)>,
    tail: VecDeque<(u8, bool)>,
    maximum: usize,
    total: u64,
}

impl OutputWindow {
    /// Retain at most `maximum` source bytes, divided between head and tail.
    pub fn new(maximum: usize) -> Self {
        Self {
            head: Vec::new(),
            tail: VecDeque::new(),
            maximum,
            total: 0,
        }
    }

    /// Original bytes observed, including the omitted middle.
    pub fn total(&self) -> u64 {
        self.total
    }

    /// Original retained bytes and their pipe, in observed order.
    pub fn retained(&self) -> impl Iterator<Item = (u8, bool)> + '_ {
        self.head.iter().chain(&self.tail).copied()
    }

    /// Retain this chunk without moving the stream's fixed head.
    pub fn observe(&mut self, bytes: &[u8], stderr: bool) -> std::io::Result<()> {
        self.total = self
            .total
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| std::io::Error::other("tool output byte count overflow"))?;
        let prefix = bytes
            .len()
            .min(self.maximum.div_ceil(2).saturating_sub(self.head.len()));
        self.head
            .extend(bytes[..prefix].iter().map(|&byte| (byte, stderr)));
        let bytes = &bytes[prefix..];
        let capacity = self.maximum / 2;
        let keep = bytes.len().min(capacity);
        let remove = self
            .tail
            .len()
            .saturating_add(keep)
            .saturating_sub(capacity);
        self.tail.drain(..remove);
        self.tail.extend(
            bytes[bytes.len() - keep..]
                .iter()
                .map(|&byte| (byte, stderr)),
        );
        Ok(())
    }

    /// Render this window using the original source's byte coordinates.
    pub fn render(&self, mut result: ToolResult, maximum: u64) -> Result<ToolResult> {
        let bytes = self.retained().map(|(byte, _)| byte).collect::<Vec<_>>();
        // Separate pipes can split an otherwise valid UTF-8 scalar. Try the
        // exact stream budget first; only if independent lossy channel decoding
        // expands it, select with JSON's maximum six bytes per source byte.
        // Two bounded passes, independent of the complete stream's length.
        for cost_per_byte in [1, 6] {
            let (selected, omitted) = crate::tools::bounds::shell_window(
                result,
                &bytes,
                self.head.len(),
                self.total,
                maximum,
                cost_per_byte,
            )?;
            result = selected;
            let fields = result
                .value
                .as_object_mut()
                .ok_or_else(|| Error::Invalid("shell result is not an object".into()))?;
            let mut head_lengths = serde_json::Map::new();
            for (channel, name) in [(false, "stdout"), (true, "stderr")] {
                let head = self
                    .retained()
                    .take(omitted.start)
                    .filter_map(|(byte, stderr)| (stderr == channel).then_some(byte))
                    .collect::<Vec<_>>();
                let tail = self
                    .retained()
                    .skip(omitted.end)
                    .filter_map(|(byte, stderr)| (stderr == channel).then_some(byte))
                    .collect::<Vec<_>>();
                let mut text = String::from_utf8_lossy(&head).into_owned();
                let head_length = text.len();
                text.push_str(&String::from_utf8_lossy(&tail));
                fields.insert(name.into(), text.into());
                head_lengths.insert(name.into(), head_length.into());
            }
            fields.insert(
                "retained_head".into(),
                serde_json::Value::Object(head_lengths),
            );
            if crate::tools::bounds::shell_projection_len(fields)? as u64
                <= super::view_bytes(Some(maximum))
            {
                return Ok(result);
            }
        }
        Err(Error::Invalid(
            "shell output framing cannot fit its view".into(),
        ))
    }
}

fn units(bytes: &[u8]) -> impl Iterator<Item = (usize, char)> + '_ {
    bytes.utf8_chunks().flat_map(|chunk| {
        chunk
            .valid()
            .chars()
            .map(|character| (character.len_utf8(), character))
            .chain(
                (!chunk.invalid().is_empty())
                    .then_some((chunk.invalid().len(), char::REPLACEMENT_CHARACTER)),
            )
    })
}

fn prefix(bytes: &[u8], budget: usize) -> (usize, usize) {
    let (mut end, mut used) = (0, 0);
    for (length, character) in units(bytes) {
        let cost = crate::tools::bounds::escaped(character);
        if used + cost > budget {
            break;
        }
        used += cost;
        end += length;
    }
    (end, used)
}

/// Select a byte prefix and suffix within encoded JSON-string credit. `prefix`
/// separates the retained ends when capture has already omitted a middle.
pub(crate) fn select(
    bytes: &[u8],
    prefix_length: usize,
    total: u64,
    budget: usize,
) -> Range<usize> {
    let missing = total > bytes.len() as u64;
    if !missing
        && units(bytes)
            .map(|(_, c)| crate::tools::bounds::escaped(c))
            .sum::<usize>()
            <= budget
    {
        return bytes.len()..bytes.len();
    }
    let head = if missing {
        &bytes[..prefix_length]
    } else {
        bytes
    };
    // A cut inside a scalar must enlarge the omitted range, not fabricate text.
    let mut complete = 0;
    for chunk in head.utf8_chunks() {
        complete += chunk.valid().len();
        if std::str::from_utf8(chunk.invalid()).is_err_and(|error| error.error_len().is_none()) {
            break;
        }
        complete += chunk.invalid().len();
    }
    let (start, used) = prefix(&head[..complete], budget.div_ceil(2));
    let mut end = if missing { prefix_length } else { start };
    if missing {
        end += bytes[end..]
            .iter()
            .take_while(|byte| **byte & 0xc0 == 0x80)
            .count();
    }
    let mut remaining = units(&bytes[end..])
        .map(|(_, c)| crate::tools::bounds::escaped(c))
        .sum::<usize>();
    for (length, character) in units(&bytes[end..]) {
        if remaining <= budget - used {
            break;
        }
        remaining -= crate::tools::bounds::escaped(character);
        end += length;
    }
    start..end
}
