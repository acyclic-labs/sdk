use super::*;
/// Self-contained source cut. Composite cuts retain both components, including
/// their original accounting custody, without a mutable lookup registry.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SourceReference {
    /// Provider-scoped opaque identity; no local path is encoded here.
    pub identity: [u8; 16],
    /// Provider leaf frontier; pairs retain the separate child frontiers.
    pub epoch: u64,
    /// Empty for a scalar cut; otherwise a tagged opaque payload (0) or pair
    /// (1). Pair children have a 32-byte header followed by their tagged body.
    pub components: acyclic_stream::OwnedBytes,
}

impl SourceReference {
    /// A scalar provider reference; construction allocates nothing.
    pub const fn new(identity: [u8; 16], epoch: u64) -> Self {
        Self {
            identity,
            epoch,
            components: acyclic_stream::OwnedBytes::new(),
        }
    }
    /// Captures a provider's complete immutable cut on the original ledger.
    /// The provider interprets the payload; routing only retains its bytes.
    pub fn opaque(
        identity: [u8; 16],
        epoch: u64,
        payload: &[u8],
        ctx: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<Self, DemandError> {
        if payload.is_empty() {
            ctx.check()?;
            return Ok(Self::new(identity, epoch));
        }
        let length = payload
            .len()
            .checked_add(1)
            .ok_or(crate::WorkError::Overflow)?;
        let mut bytes = crate::kernel::Retained::allocate(length, ctx)?;
        ctx.charge(WorkCounters {
            bytes_copied: payload.len() as u64,
            ..WorkCounters::UNCHARGED
        })?;
        bytes.value.push(0);
        bytes.value.extend_from_slice(payload);
        Ok(Self {
            identity,
            epoch,
            components: bytes.into_owned_bytes((), ctx)?,
        })
    }
    /// Borrow a provider payload, refusing a composed reference. Scalar cuts
    /// have an empty payload. The returned bytes share this cut's custody.
    pub fn payload(&self) -> Result<&[u8], DemandError> {
        match self.components.as_slice() {
            [] => Ok(&[]),
            [0, payload @ ..] if !payload.is_empty() => Ok(payload),
            _ => Err(DemandError::StaleSource),
        }
    }
    /// Canonical width, including all statically composed sources.
    pub fn encoded_len(&self) -> usize {
        32 + self.components.len()
    }
    /// Encode directly into a caller-owned, already admitted buffer.
    pub(crate) fn encode(&self, mut write: impl FnMut(&[u8])) {
        write(&self.identity);
        write(&self.epoch.to_le_bytes());
        write(&(self.components.len() as u64).to_le_bytes());
        write(self.components.as_slice());
    }
    pub(crate) fn decode(
        decoder: &mut crate::kernel::Decoder<'_>,
        observe: &mut impl FnMut(crate::kernel::ReadAllocationStep) -> Result<(), crate::FsError>,
    ) -> Result<Self, crate::FsError> {
        let identity = decoder.fixed(observe)?;
        let epoch = decoder.u64(observe)?;
        let length = usize::try_from(decoder.u64(observe)?)
            .map_err(|_| crate::kernel::CanonicalDecodeError::LengthOverflow)?;
        let bytes = decoder.take_exact(length)?;
        let components =
            crate::kernel::codec::copy_bytes(bytes, u64::from(length != 0), observe)?.into();
        let reference = Self {
            identity,
            epoch,
            components,
        };
        if !reference.valid() {
            return Err(crate::kernel::CanonicalDecodeError::Invariant(
                crate::kernel::CanonicalInvariant::Static("invalid source cut"),
            )
            .into());
        }
        Ok(reference)
    }
    /// Transfer freshly decoded backing out of its enclosing read allocation
    /// before a reference can be cloned or retained independently of that read.
    pub(crate) fn retain_decoded(
        &mut self,
        allocation: &mut crate::kernel::KernelOwnedAllocation,
        ctx: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        crate::kernel::retain_decoded_bytes(&mut self.components, allocation, ctx)
    }
    fn valid(&self) -> bool {
        let body = self.components.as_slice();
        fn pair(body: &[u8], from: usize, end: usize) -> bool {
            let Some((_, _, _, second)) = reference_node(body, from) else {
                return false;
            };
            reference_node(body, second).is_some_and(|(_, _, _, last)| last == end)
        }
        match body {
            [] => return true,
            [0, payload @ ..] => return !payload.is_empty(),
            [1, ..] if self.epoch == 0 && pair(body, 1, body.len()) => {}
            _ => return false,
        }
        let mut from = 1;
        while from < body.len() {
            let Some((_, epoch, components, end)) = reference_node(body, from) else {
                return false;
            };
            match components {
                [] => from = end,
                [0, payload @ ..] if !payload.is_empty() => from = end,
                [1, ..] if epoch == 0 && pair(body, from + 33, end) => from += 33,
                _ => return false,
            }
        }
        true
    }
    /// Whether this reference belongs to the same namespace and follows every
    /// retained leaf. Comparing preorder records needs no recursion or scratch.
    pub(crate) fn follows(&self, previous: &Self) -> bool {
        if !self.valid() || !previous.valid() {
            return false;
        }
        let mut now = self.nodes();
        let mut old = previous.nodes();
        loop {
            match (now.next(), old.next()) {
                (None, None) => return true,
                (Some((id, epoch, body)), Some((prior_id, prior_epoch, prior)))
                    if id == prior_id && epoch >= prior_epoch =>
                {
                    match (body.first(), prior.first()) {
                        (Some(1), Some(1)) => {}
                        (Some(1), _) | (_, Some(1)) => return false,
                        _ if epoch == prior_epoch && body != prior => return false,
                        _ => {}
                    }
                }
                _ => return false,
            }
        }
    }
    fn nodes(&self) -> impl Iterator<Item = ([u8; 16], u64, &[u8])> {
        let body = self.components.as_slice();
        let mut from = if body.first() == Some(&1) {
            1
        } else {
            body.len()
        };
        std::iter::once((self.identity, self.epoch, body)).chain(std::iter::from_fn(move || {
            if from == body.len() {
                return None;
            }
            let (identity, epoch, components, end) = reference_node(body, from)?;
            from = if components.first() == Some(&1) {
                from + 33
            } else {
                end
            };
            Some((identity, epoch, components))
        }))
    }
    pub(crate) fn pair(
        identity: [u8; 16],
        base: &Self,
        view: &Self,
        ctx: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<Self, DemandError> {
        let length = base
            .encoded_len()
            .checked_add(view.encoded_len())
            .and_then(|length| length.checked_add(1))
            .ok_or(crate::WorkError::Overflow)?;
        let mut bytes = crate::kernel::Retained::allocate(length, ctx)?;
        ctx.charge(WorkCounters {
            bytes_copied: (length - 1) as u64,
            ..WorkCounters::UNCHARGED
        })?;
        bytes.value.push(1);
        base.encode(|part| bytes.value.extend_from_slice(part));
        view.encode(|part| bytes.value.extend_from_slice(part));
        let components = bytes.into_owned_bytes((), ctx)?;
        Ok(Self {
            identity,
            epoch: 0,
            components,
        })
    }
    /// Split a held pair using slices of its original backing; no allocation.
    pub(crate) fn paired(&self) -> Result<(Self, Self), DemandError> {
        if !self.valid() || self.components.as_slice().first() != Some(&1) {
            return Err(DemandError::StaleSource);
        }
        fn node(
            bytes: &acyclic_stream::OwnedBytes,
            from: usize,
        ) -> Option<(SourceReference, usize)> {
            let (identity, epoch, body, end) = reference_node(bytes.as_slice(), from)?;
            let start = end - body.len();
            Some((
                SourceReference {
                    identity,
                    epoch,
                    components: if body.is_empty() {
                        acyclic_stream::OwnedBytes::new()
                    } else {
                        bytes.slice(start..end)
                    },
                },
                end,
            ))
        }
        let (base, next) = node(&self.components, 1).ok_or(DemandError::StaleSource)?;
        let (view, end) = node(&self.components, next).ok_or(DemandError::StaleSource)?;
        if end != self.components.len() || self.epoch != 0 {
            return Err(DemandError::StaleSource);
        }
        Ok((base, view))
    }
}

fn reference_node(body: &[u8], from: usize) -> Option<([u8; 16], u64, &[u8], usize)> {
    let head = body.get(from..from.checked_add(32)?)?;
    let length = usize::try_from(u64::from_le_bytes(head[24..32].try_into().ok()?)).ok()?;
    let start = from.checked_add(32)?;
    let end = start.checked_add(length)?;
    Some((
        head[..16].try_into().ok()?,
        u64::from_le_bytes(head[16..24].try_into().ok()?),
        body.get(start..end)?,
        end,
    ))
}

#[cfg(test)]
mod reference_tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn opaque_source_cuts_survive_nested_routing_and_canonical_round_trips(
            payload in prop::collection::vec(any::<u8>(), 0..256),
            changed in prop::collection::vec(any::<u8>(), 0..256),
            depth in 1u8..24,
        ) {
            let cancellation = CancellationToken::new();
            let mut ctx = crate::kernel::KernelAccounting::standalone(crate::WorkBudget::UNBOUNDED, &cancellation);
            let leaf = SourceReference::opaque([7; 16], 5, &payload, &mut ctx).unwrap();
            let mut old = leaf.clone();
            let mut now = SourceReference::opaque([7; 16], 6, &changed, &mut ctx).unwrap();
            for level in 0..depth {
                let side = SourceReference::new([100 + level; 16], 8);
                old = SourceReference::pair([30 + level; 16], &old, &side, &mut ctx).unwrap();
                now = SourceReference::pair([30 + level; 16], &now, &side, &mut ctx).unwrap();
            }
            prop_assert!(now.follows(&old));
            prop_assert!(!old.follows(&now));
            let mut encoder = crate::kernel::Encoder::new(b"source-cut-property", 1);
            old.encode(|bytes| encoder.fixed(bytes));
            let bytes = encoder.finish();
            let (mut decoded, mut allocation) = crate::kernel::decode_owned(&mut ctx, |mut observe| {
                let mut decoder = crate::kernel::Decoder::new(&bytes, b"source-cut-property", 1, bytes.len() as u64, &mut observe)?;
                let cut = SourceReference::decode(&mut decoder, &mut observe)?;
                decoder.finish()?;
                Ok(cut)
            }).unwrap();
            decoded.retain_decoded(&mut allocation, &mut ctx).unwrap();
            prop_assert_eq!(&decoded, &old);
            for _ in 0..depth {
                decoded = decoded.paired().unwrap().0;
            }
            prop_assert_eq!(decoded.payload().unwrap(), payload.as_slice());
            prop_assert_eq!(&decoded, &leaf);
            let truncated = SourceReference {
                components: old.components.slice(..old.components.len() - 1),
                ..old.clone()
            };
            prop_assert!(!truncated.valid());
            if payload != changed {
                let changed_at_same_epoch = SourceReference::opaque([7; 16], 5, &changed, &mut ctx).unwrap();
                prop_assert!(!changed_at_same_epoch.follows(&leaf));
            }
        }
    }
}

/// Evidence for the exact node observed by a source request.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct SourceVersion(pub [u8; 32]);

/// The kind of an observed source node.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum SourceNodeKind {
    /// Directory, whose children remain unresolved until requested.
    Directory,
    /// Regular file, whose content ranges remain unresolved until requested.
    RegularFile,
    /// Symbolic link, never followed by the source provider.
    SymbolicLink,
    /// POSIX named pipe.
    Fifo,
    /// POSIX local socket node.
    Socket,
    /// Native character device.
    CharacterDevice,
    /// Native block device.
    BlockDevice,
    /// Another native node kind which cannot be projected losslessly.
    Unsupported,
}

/// Scalar host metadata. `None` means the provider did not make that fact
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct SourceMetadata {
    /// POSIX permission and kind bits, when available.
    pub posix_mode: Option<u32>,
    /// POSIX numeric owner, when available.
    pub posix_uid: Option<u32>,
    /// POSIX numeric group, when available.
    pub posix_gid: Option<u32>,
    /// POSIX inode flags, when available.
    pub posix_flags: Option<u64>,
    /// Windows file attributes, when available.
    pub windows_attributes: Option<u32>,
    /// Creation/birth time in signed Unix nanoseconds.
    pub created_ns: Option<i128>,
    /// Last content modification time in signed Unix nanoseconds.
    pub modified_ns: Option<i128>,
    /// Last access time in signed Unix nanoseconds.
    pub accessed_ns: Option<i128>,
    /// Last metadata-change time in signed Unix nanoseconds.
    pub changed_ns: Option<i128>,
}

/// Metadata returned by an exact path lookup.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SourceNode {
    /// Source node kind.
    pub kind: SourceNodeKind,
    /// Stable path-independent source identity. Hard-link aliases share it.
    pub file_identity: [u8; 32],
    /// Exact number of source namespace bindings when the provider exposes it.
    pub link_count: Option<u64>,
    /// Native device identity for character and block devices.
    pub device: Option<(u32, u32)>,
    /// Logical length for a regular file; absent for other kinds.
    pub logical_bytes: Option<u64>,
    /// Version that must match a later read of this node.
    pub version: SourceVersion,
    /// Explicitly available scalar metadata.
    pub metadata: SourceMetadata,
}

/// One directory entry in native enumeration order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceDirectoryEntry {
    /// Lossless host component encoding.
    pub name: crate::kernel::LogicalName,
    /// The entry's node, exactly as a lookup of its path would report it.
    pub node: SourceNode,
}
