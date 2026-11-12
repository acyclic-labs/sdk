//! Versioned continuation data. Authority is checked by the original provider
//! on every use; the serialized cursor is never a bearer capability.
use super::{Cursor, Frame};
use crate::{Revision, RevisionScope, StreamPath};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireCursor {
    version: u32,
    pattern: Vec<u8>,
    cut: Option<(Vec<u8>, u128, u64)>,
    frontier: Vec<WireFrame>,
    matcher: Option<[u8; 32]>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireFrame {
    path: StreamPath,
    sequence: u64,
    tail: Option<u64>,
    after: Option<StreamPath>,
    content: Option<Vec<u8>>,
}

impl Serialize for Cursor {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        WireCursor {
            version: 1,
            pattern: self.pattern.clone(),
            matcher: self.matcher,
            cut: self
                .at
                .as_ref()
                .map(|at| (at.scope.as_bytes().to_vec(), at.incarnation, at.index)),
            frontier: self
                .frontier
                .iter()
                .map(|frame| WireFrame {
                    path: frame.path.clone(),
                    sequence: frame.sequence,
                    tail: frame.tail,
                    after: frame.after.clone(),
                    content: frame.content.clone(),
                })
                .collect(),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Cursor {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = WireCursor::deserialize(deserializer)?;
        let invalid = || serde::de::Error::custom("invalid search continuation");
        if wire.version != 1
            || wire.frontier.is_empty()
            || (wire.cut.is_none()
                && (wire.frontier.len() != 1
                    || wire.frontier.iter().any(|frame| frame.after.is_some())))
            || wire.frontier.iter().any(|frame| {
                frame.tail.is_some_and(|tail| frame.sequence > tail)
                    || (frame.content.is_some()
                        && frame.tail.is_none_or(|tail| frame.sequence >= tail))
                    || (frame.tail.is_none() && frame.sequence != 0)
                    || frame.after.as_ref().is_some_and(|after| {
                        after.as_str().rsplit_once('/').map(|(parent, _)| parent)
                            != Some(frame.path.as_str())
                    })
            })
            || wire.frontier.windows(2).any(|frames| {
                let [parent, child] = frames else {
                    return true;
                };
                child.path.as_str().rsplit_once('/').map(|(path, _)| path)
                    != Some(parent.path.as_str())
                    || parent.after.as_ref() != Some(&child.path)
            })
        {
            return Err(invalid());
        }
        let at = wire.cut.map(|(scope, incarnation, index)| Revision {
            scope: RevisionScope::new(scope),
            incarnation,
            index,
        });
        if at.is_none() && wire.frontier.iter().any(|frame| frame.tail.is_some()) {
            return Err(invalid());
        }
        Ok(Self {
            pattern: wire.pattern,
            at,
            matcher: wire.matcher,
            frontier: wire
                .frontier
                .into_iter()
                .map(|frame| Frame {
                    path: frame.path,
                    sequence: frame.sequence,
                    tail: frame.tail,
                    after: frame.after,
                    content: frame.content,
                })
                .collect(),
        })
    }
}
