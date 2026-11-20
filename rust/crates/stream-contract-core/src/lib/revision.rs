//! One applied-revision token shared by Streams, Objects and Actors.
//!
//! A [`Revision`] names a committed publication of one authority (an account or
//! storage scope) in one physical incarnation. An index is never compared across
//! scopes or incarnations, and a stream offset is never a revision.
use crate::OwnedBytes as Bytes;
use crate::StreamCategory;
use crate::StreamError;
use crate::StreamProviderExt as _;
#[cfg(feature = "serde")]
#[path = "revision/serialization.rs"]
mod serialization;

/// Opaque identity of the authority that orders revisions.
#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RevisionScope(Bytes);

impl RevisionScope {
    /// Binds caller-owned authority bytes, such as an account or storage id.
    #[must_use]
    pub fn new(bytes: impl Into<Bytes>) -> Self {
        // Promote uniquely owned transport bytes at scope admission. A later
        // snapshot clone must not allocate a hidden shared header on its reader.
        let bytes = bytes.into();
        let shared = bytes.clone();
        drop(bytes);
        Self(shared)
    }

    /// The authority bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// One committed publication of an authority in one physical incarnation.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(deny_unknown_fields))]
pub struct Revision {
    /// The ordering authority.
    pub scope: RevisionScope,
    /// The authority's physical incarnation; a reset never reuses one.
    #[cfg_attr(feature = "serde", serde(with = "serialization::incarnation"))]
    pub incarnation: u128,
    /// Monotonic publication index within that scope and incarnation.
    #[cfg_attr(feature = "serde", serde(with = "acyclic_decimal_u64"))]
    pub index: u64,
}

impl Revision {
    /// Whether both revisions belong to the same scope and incarnation.
    #[must_use]
    pub fn same_authority(&self, other: &Self) -> bool {
        self.incarnation == other.incarnation && self.scope == other.scope
    }
}

/// A minimum cut: at most one revision per scope, ordered by scope. Multiple
/// authorities keep their own participant cuts; they are never folded into a max.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RevisionCut(Vec<Revision>);

impl RevisionCut {
    /// Copies the participant vector only after original native admission.
    /// Shared scope backing keeps its existing custody; the returned owner
    /// retains the copied vector until the consuming request has retired.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn native_clone(
        &self,
        meter: &std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> std::io::Result<crate::NativeStreamOutput<Self>> {
        meter.check()?;
        let mut allocation = acyclic_native_runtime::ReadAllocation::empty(meter);
        let revisions = crate::alloc::copy_vector(&self.0, meter, &mut allocation)?;
        meter.check()?;
        Ok(crate::NativeStreamOutput::new(Self(revisions), allocation))
    }

    /// A cut with one participant.
    #[must_use]
    pub fn of(revision: Revision) -> Self {
        Self(vec![revision])
    }

    /// Canonicalizes participants in place, retaining the greatest index per
    /// authority. Conflicting incarnations refuse with `RevisionForeign`:
    /// neither incarnation proves the other's committed publications.
    pub fn new(revisions: impl IntoIterator<Item = Revision>) -> Result<Self, StreamError> {
        let mut revisions: Vec<_> = revisions.into_iter().collect();
        revisions.sort_unstable_by(|left, right| left.scope.cmp(&right.scope));
        let mut foreign = false;
        revisions.dedup_by(|next, kept| {
            if next.scope != kept.scope {
                return false;
            }
            if next.same_authority(kept) {
                kept.index = kept.index.max(next.index);
            } else {
                foreign = true;
            }
            true
        });
        if foreign {
            Err(StreamError::RevisionForeign)
        } else {
            revisions.shrink_to_fit();
            Ok(Self(revisions))
        }
    }

    /// Raises this cut with one observed revision of its own authority.
    /// A different incarnation refuses with `RevisionForeign` and leaves the
    /// cut unchanged. A reset cannot silently retire the original requirement.
    /// Returns the retained participant without another lookup or clone.
    pub fn observe(&mut self, revision: Revision) -> Result<&Revision, StreamError> {
        let at = match self
            .0
            .binary_search_by(|entry| entry.scope.cmp(&revision.scope))
        {
            Ok(at) => {
                let entry = self.0.get_mut(at).ok_or(StreamError::InvalidResponse)?;
                if entry.incarnation != revision.incarnation {
                    return Err(StreamError::RevisionForeign);
                } else if entry.index < revision.index {
                    entry.index = revision.index;
                }
                at
            }
            Err(at) => {
                self.0.insert(at, revision);
                at
            }
        };
        self.0.get(at).ok_or(StreamError::InvalidResponse)
    }

    /// Whether this cut holds `revision`'s authority at or above its index.
    #[must_use]
    pub fn covers(&self, revision: &Revision) -> bool {
        self.get(&revision.scope)
            .is_some_and(|held| held.same_authority(revision) && held.index >= revision.index)
    }

    /// Whether `applied` proves every participant of this minimum. An empty
    /// minimum is always proven; a missing revision proves no minimum.
    #[must_use]
    pub fn proven_by(&self, applied: Option<&Revision>) -> bool {
        self.iter().all(|minimum| {
            applied.is_some_and(|applied| {
                applied.same_authority(minimum) && applied.index >= minimum.index
            })
        })
    }

    /// Explicitly removes one participant. The caller must already possess
    /// coverage evidence; a refusal alone proves no retirement.
    pub fn forget(&mut self, scope: &RevisionScope) {
        self.0.retain(|entry| &entry.scope != scope);
    }

    /// The participant for one scope.
    #[must_use]
    pub fn get(&self, scope: &RevisionScope) -> Option<&Revision> {
        self.0
            .binary_search_by(|entry| entry.scope.cmp(scope))
            .ok()
            .and_then(|at| self.0.get(at))
    }

    /// Whether the cut has no participant.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Participants in scope order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &Revision> {
        self.0.iter()
    }
}

/// The revision a server reports for one read: `provider`'s applied revision
/// once it covers `at_least`. A provider that issues no revisions reports none
/// and still enforces the cut it forwards.
pub async fn served<P: crate::StreamProvider + ?Sized>(
    provider: &P,
    at_least: &RevisionCut,
) -> Result<Option<Revision>, StreamError> {
    match provider.wait_applied(at_least).await {
        Ok(revision) => Ok(Some(revision)),
        Err(error) if error.category() == StreamCategory::Unsupported => Ok(None),
        Err(error) => Err(error),
    }
}

/// Encodes one revision.
#[must_use]
pub fn revision_to_wire(value: &Revision) -> acyclic_stream_wire::wire::Revision {
    acyclic_stream_wire::wire::Revision {
        scope: bytes::Bytes::copy_from_slice(value.scope.as_bytes()),
        incarnation: bytes::Bytes::copy_from_slice(&value.incarnation.to_be_bytes()),
        index: value.index,
    }
}

/// Decodes one revision; an incarnation other than 16 bytes is invalid.
pub fn revision_from_wire(
    value: &acyclic_stream_wire::wire::Revision,
) -> Result<Revision, StreamError> {
    revision_from(value)
}

/// Decodes a borrowed wire revision, copying only the scope it owns.
pub fn revision_from(value: &acyclic_stream_wire::wire::Revision) -> Result<Revision, StreamError> {
    let incarnation = <[u8; 16]>::try_from(value.incarnation.as_ref())
        .map_err(|_| StreamError::InvalidArgument)?;
    Ok(Revision {
        scope: RevisionScope::new(value.scope.to_vec()),
        incarnation: u128::from_be_bytes(incarnation),
        index: value.index,
    })
}

/// Encodes a minimum cut.
#[must_use]
pub fn cut_to_wire(value: &RevisionCut) -> Vec<acyclic_stream_wire::wire::Revision> {
    value.iter().map(revision_to_wire).collect()
}

/// Decodes a minimum cut, refusing incompatible incarnations of one scope.
pub fn cut_from_wire(
    value: Vec<acyclic_stream_wire::wire::Revision>,
) -> Result<RevisionCut, StreamError> {
    RevisionCut::new(
        value
            .into_iter()
            .map(|value| revision_from_wire(&value))
            .collect::<Result<Vec<_>, _>>()?,
    )
}
