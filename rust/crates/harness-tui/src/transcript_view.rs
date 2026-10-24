use crate::{RowPosition, Top, WrappedLines};
use serde::{Deserialize, Serialize};

/// Stable source sections, in their existing display order.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TranscriptPart {
    /// Optional reasoning preceding the answer.
    Reasoning,
    #[default]
    /// The retained and streaming answer.
    Answer,
    /// Tool output following the answer.
    Tools,
}

impl TranscriptPart {
    fn index(self) -> usize {
        match self {
            Self::Reasoning => 0,
            Self::Answer => 1,
            Self::Tools => 2,
        }
    }
    fn at(index: usize) -> Self {
        match index {
            0 => Self::Reasoning,
            1 => Self::Answer,
            2 => Self::Tools,
            _ => unreachable!(),
        }
    }
}

/// A logical row within one immutable followed source file.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TranscriptOrigin {
    /// Canonical followed file identity, independent of the retained window.
    pub file: [u8; 32],
    /// Logical display line created by that source file.
    pub line: usize,
}

/// Persisted top row. Absence means follow the live tail.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TranscriptAnchor {
    /// Source section.
    pub part: TranscriptPart,
    /// Logical source line and physical row within it.
    pub position: RowPosition,
    /// Followed source identity supplied by the host projection.
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "source_origin"
    )]
    pub origin: Option<TranscriptOrigin>,
}

// Internal wrapping algorithms use positions before the host supplies provenance.
// A persisted anchor must carry it; accepting a bare retained-window index would
// silently reintroduce the eviction jump on reopen.
fn source_origin<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<TranscriptOrigin>, D::Error> {
    TranscriptOrigin::deserialize(deserializer).map(Some)
}

fn top(parts: &[Option<&mut WrappedLines>; 3]) -> Option<TranscriptAnchor> {
    parts.iter().enumerate().find_map(|(index, part)| {
        let part = part.as_deref()?;
        (!part.shown().is_empty()).then(|| TranscriptAnchor {
            origin: None,
            part: TranscriptPart::at(index),
            position: part.window().top,
        })
    })
}

fn tail(parts: &mut [Option<&mut WrappedLines>; 3], height: usize, activity: usize) {
    let mut remaining = height.saturating_sub(activity);
    for part in parts.iter_mut().rev().flatten() {
        if remaining == 0 {
            part.hide();
        } else {
            part.view(Top::Tail, remaining);
            remaining -= part.shown().len();
        }
    }
}

/// Select a physical window without computing a total wrapped-row count.
/// `activity` is the derived activity row below the source sections.
pub fn view_transcript(
    mut parts: [Option<&mut WrappedLines>; 3],
    anchor: Option<TranscriptAnchor>,
    height: usize,
    activity: usize,
) -> Option<TranscriptAnchor> {
    if height == 0 {
        for part in parts.iter_mut().flatten() {
            part.hide();
        }
        return anchor;
    }
    let Some(anchor) = anchor else {
        tail(&mut parts, height, activity);
        return None;
    };
    for part in parts.iter_mut().flatten() {
        part.hide();
    }
    let mut remaining = height;
    let mut end = true;
    for (index, part) in parts.iter_mut().enumerate().skip(anchor.part.index()) {
        let Some(part) = part else {
            continue;
        };
        if part.source_len() == 0 {
            continue;
        }
        if remaining == 0 {
            end = false;
            break;
        }
        let position = if index == anchor.part.index() {
            anchor.position
        } else {
            RowPosition::default()
        };
        let position = RowPosition {
            line: position.line.min(part.source_len() - 1),
            row: position.row,
        };
        part.view_from(position, remaining);
        remaining -= part.shown().len();
        end &= part.at_tail();
    }
    if end && remaining >= activity {
        tail(&mut parts, height, activity);
        None
    } else {
        top(&parts)
    }
}

/// Apply one ordered movement, clamped against the composed source plus
/// activity row. Call repeatedly for a burst, then publish its final anchor.
pub fn move_transcript(
    mut parts: [Option<&mut WrappedLines>; 3],
    from: Option<TranscriptAnchor>,
    mut rows: isize,
    height: usize,
    activity: usize,
) -> Option<TranscriptAnchor> {
    if height == 0 {
        return from;
    }
    if rows == isize::MAX {
        return view_transcript(parts, None, height, activity);
    }
    if rows <= -isize::MAX {
        let first = parts.iter().enumerate().find_map(|(index, part)| {
            part.as_deref()
                .filter(|part| part.source_len() != 0)
                .map(|_| TranscriptAnchor {
                    origin: None,
                    part: TranscriptPart::at(index),
                    position: RowPosition::default(),
                })
        });
        return view_transcript(parts, first, height, activity);
    }
    let from = view_transcript(
        parts.each_mut().map(|part| part.as_deref_mut()),
        from,
        height,
        activity,
    );
    let mut anchor = from.or_else(|| top(&parts));
    if anchor.is_none() && rows < 0 {
        anchor = parts
            .iter_mut()
            .enumerate()
            .rev()
            .find_map(|(index, part)| {
                part.as_deref_mut()?
                    .last_position()
                    .map(|position| TranscriptAnchor {
                        origin: None,
                        part: TranscriptPart::at(index),
                        position,
                    })
            });
        rows = rows.saturating_add(1);
    }
    let mut anchor = anchor?;
    loop {
        let index = anchor.part.index();
        if let Some((position, remaining)) = parts
            .get_mut(index)?
            .as_deref_mut()?
            .step(anchor.position, rows)
        {
            anchor.position = position;
            rows = remaining;
        }
        if rows == 0 {
            break;
        }
        let next = if rows < 0 {
            (0..index).rev().find(|&at| {
                parts
                    .get(at)
                    .and_then(|part| part.as_deref())
                    .is_some_and(|part| part.source_len() != 0)
            })
        } else {
            (index + 1..parts.len()).find(|&at| {
                parts
                    .get(at)
                    .and_then(|part| part.as_deref())
                    .is_some_and(|part| part.source_len() != 0)
            })
        };
        let Some(next) = next else {
            if rows > 0 {
                // The tail can consist solely of the activity row. Crossing
                // the last source row must enter that frame, not stick on it.
                return view_transcript(parts, None, height, activity);
            }
            break;
        };
        anchor = TranscriptAnchor {
            origin: None,
            part: TranscriptPart::at(next),
            position: if rows < 0 {
                parts.get_mut(next)?.as_deref_mut()?.last_position()?
            } else {
                RowPosition::default()
            },
        };
        rows -= rows.signum();
    }
    view_transcript(parts, Some(anchor), height, activity)
}

#[cfg(test)]
mod tests;
