//! Versioned continuation data. Authority is checked by the original provider
//! on every use; the serialized cursor is never a bearer capability.
use super::{Cursor, Frame};
use crate::{Revision, RevisionScope, StreamPath};
use serde::{Deserialize, Serialize};

/// Versioned search continuation data, decoded by the caller's funded serde
/// visitor before conversion to native-owned paths. This is data only: every
/// resumed page still checks the original provider's current authority.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CursorState<P = String> {
    version: u32,
    pattern: Vec<u8>,
    cut: Option<(Vec<u8>, u128, u64)>,
    frontier: Vec<Frame<P>>,
    matcher: Option<[u8; 32]>,
}

impl Serialize for Cursor {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Borrowed<'a> {
            version: u32,
            pattern: &'a [u8],
            cut: Option<(&'a [u8], u128, u64)>,
            frontier: &'a [Frame],
            matcher: Option<[u8; 32]>,
        }
        Borrowed {
            version: 1,
            pattern: &self.pattern,
            cut: self
                .at
                .as_ref()
                .map(|at| (at.scope.as_bytes(), at.incarnation, at.index)),
            frontier: &self.frontier,
            matcher: self.matcher,
        }
        .serialize(serializer)
    }
}

impl<P> CursorState<P> {
    fn valid(&self, path: fn(&P) -> &str) -> bool {
        self.version == 1
            && !self.frontier.is_empty()
            && (self.cut.is_some()
                || (self.frontier.len() == 1
                    && self
                        .frontier
                        .iter()
                        .all(|frame| frame.after.is_none() && frame.tail.is_none())))
            && self.frontier.iter().all(|frame| {
                StreamPath::validate(path(&frame.path)).is_ok()
                    && frame.tail.is_none_or(|tail| frame.sequence <= tail)
                    && (frame.content.is_none()
                        || frame.tail.is_some_and(|tail| frame.sequence < tail))
                    && (frame.tail.is_some() || frame.sequence == 0)
                    && frame.after.as_ref().is_none_or(|after| {
                        StreamPath::validate(path(after)).is_ok()
                            && path(after).rsplit_once('/').map(|(parent, _)| parent)
                                == Some(path(&frame.path))
                    })
            })
            && self.frontier.windows(2).all(|frames| {
                let [parent, child] = frames else {
                    return false;
                };
                path(&child.path).rsplit_once('/').map(|(parent, _)| parent)
                    == Some(path(&parent.path))
                    && parent.after.as_ref().map(path) == Some(path(&child.path))
            })
    }
}

impl<'de> Deserialize<'de> for Cursor {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = CursorState::<StreamPath>::deserialize(deserializer)?;
        if !wire.valid(StreamPath::as_str) {
            return Err(serde::de::Error::custom("invalid search continuation"));
        }
        Ok(Self {
            pattern: wire.pattern,
            at: wire.cut.map(|(scope, incarnation, index)| Revision {
                scope: RevisionScope::new(scope),
                incarnation,
                index,
            }),
            matcher: wire.matcher,
            frontier: wire.frontier,
        })
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl CursorState {
    /// Copies decoded continuation data directly onto the original native
    /// ledger. The caller retains its decoded input guard until this returns;
    /// the output then owns its paths, cut, pattern and frontier independently.
    pub fn to_native(
        &self,
        meter: &std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> std::io::Result<Result<crate::NativeStreamOutput<Cursor>, crate::StreamError>> {
        use acyclic_native_runtime::ReadAllocation;
        meter.check()?;
        if !self.valid(String::as_str) {
            return Ok(Err(crate::StreamError::InvalidArgument));
        }
        fn copy(
            bytes: &[u8],
            meter: &std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
            allocation: &mut ReadAllocation,
        ) -> std::io::Result<Vec<u8>> {
            let mut copied = crate::native::vector(bytes.len(), meter, allocation)?;
            meter.admit_codec(0, bytes.len() as u64, 0)?;
            copied.extend_from_slice(bytes);
            meter.record_codec(0, bytes.len() as u64, 0)?;
            Ok(copied)
        }
        let mut allocation = ReadAllocation::empty(meter);
        let pattern = copy(&self.pattern, meter, &mut allocation)?;
        let at = if let Some((scope, incarnation, index)) = &self.cut {
            let mut owner = ReadAllocation::empty(meter);
            let bytes = copy(scope, meter, &mut owner)?;
            let bytes =
                crate::OwnedBytes::from_vec_with_guard(bytes, owner, std::sync::Arc::clone(meter))
                    .map_err(|failure| match failure.into_error() {
                        crate::PreparedOwnedBytesError::Admission(error) => error,
                        crate::PreparedOwnedBytesError::NotFullyInitialized => {
                            std::io::ErrorKind::InvalidData.into()
                        }
                    })?;
            Some(Revision {
                scope: RevisionScope::new(bytes),
                incarnation: *incarnation,
                index: *index,
            })
        } else {
            None
        };
        let mut frontier = crate::native::vector(self.frontier.len(), meter, &mut allocation)?;
        for frame in &self.frontier {
            let path = match StreamPath::new_native(&frame.path, meter)? {
                Ok(path) => path,
                Err(error) => return Ok(Err(error)),
            };
            let after = match &frame.after {
                Some(after) => match StreamPath::new_native(after, meter)? {
                    Ok(path) => Some(path),
                    Err(error) => return Ok(Err(error)),
                },
                None => None,
            };
            let content = frame
                .content
                .as_ref()
                .map(|bytes| copy(bytes, meter, &mut allocation))
                .transpose()?;
            meter.admit_codec(0, std::mem::size_of::<Frame>() as u64, 0)?;
            frontier.push(Frame {
                path,
                after,
                content,
                sequence: frame.sequence,
                tail: frame.tail,
            });
            meter.record_codec(0, std::mem::size_of::<Frame>() as u64, 0)?;
        }
        meter.check()?;
        Ok(Ok(crate::NativeStreamOutput::new(
            Cursor {
                pattern,
                at,
                frontier,
                matcher: self.matcher,
            },
            allocation,
        )))
    }
}
