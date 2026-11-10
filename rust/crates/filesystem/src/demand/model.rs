use super::*;
/// Self-contained source cut. Composite cuts retain both components, including
/// their original accounting custody, without a mutable lookup registry.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SourceReference {
    /// Provider-scoped opaque identity; no local path is encoded here.
    pub identity: [u8; 16],
    /// Incremented when source knowledge is invalidated.
    pub epoch: u64,
    /// Canonical preorder encoding of the two routed components, or empty for
    /// a leaf. Each node has a 32-byte header followed by its children.
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
        if self.components.is_empty() {
            return Ok(());
        }
        let bytes = std::mem::take(&mut self.components)
            .try_into_ordinary_vec()
            .map_err(|bytes| {
                self.components = bytes;
                crate::FsError::from(crate::kernel::CanonicalDecodeError::Invariant(
                    crate::kernel::CanonicalInvariant::Static(
                        "decoded source cut backing is shared",
                    ),
                ))
            })?;
        let capacity = bytes.capacity() as u64;
        let guard = crate::kernel::split_allocation(allocation, capacity)?;
        self.components = crate::kernel::Retained::from_owned(
            crate::kernel::Owned::new(bytes, guard, capacity),
            ctx,
        )?
        .into_owned_bytes((), ctx)?;
        Ok(())
    }
    fn valid(&self) -> bool {
        if self.components.is_empty() {
            return true;
        }
        if self.epoch != 0 || self.components.len() % 32 != 0 {
            return false;
        }
        let body = self.components.as_slice();
        fn subtree(body: &[u8], start: usize) -> Option<usize> {
            let header = body.get(start..start.checked_add(32)?)?;
            let length = usize::try_from(u64::from_le_bytes(header[24..].try_into().ok()?)).ok()?;
            let end = start.checked_add(32)?.checked_add(length)?;
            (length % 32 == 0 && end <= body.len()).then_some(end)
        }
        let Some(second) = subtree(body, 0) else {
            return false;
        };
        if subtree(body, second) != Some(body.len()) {
            return false;
        }
        for start in (0..body.len()).step_by(32) {
            let Some(end) = subtree(body, start) else {
                return false;
            };
            if end == start + 32 {
                continue;
            }
            if body[start + 16..start + 24] != [0; 8] {
                return false;
            }
            let Some(second) = subtree(body, start + 32) else {
                return false;
            };
            if subtree(body, second) != Some(end) {
                return false;
            }
        }
        true
    }
    /// Whether this reference belongs to the same namespace and follows every
    /// retained leaf. Comparing preorder records needs no recursion or scratch.
    pub(crate) fn follows(&self, previous: &Self) -> bool {
        if !self.valid()
            || !previous.valid()
            || self.identity != previous.identity
            || self.epoch < previous.epoch
            || self.components.len() != previous.components.len()
            || self.components.len() % 32 != 0
        {
            return false;
        }
        self.components
            .as_slice()
            .chunks_exact(32)
            .zip(previous.components.as_slice().chunks_exact(32))
            .all(|(now, old)| {
                now[..16] == old[..16]
                    && now[24..] == old[24..]
                    && u64::from_le_bytes(now[16..24].try_into().unwrap())
                        >= u64::from_le_bytes(old[16..24].try_into().unwrap())
            })
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
            .ok_or(crate::WorkError::Overflow)?;
        let mut bytes = crate::kernel::Retained::allocate(length, ctx)?;
        ctx.charge(WorkCounters {
            bytes_copied: length as u64,
            ..WorkCounters::UNCHARGED
        })?;
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
        if !self.valid() {
            return Err(DemandError::StaleSource);
        }
        fn node(
            bytes: &acyclic_stream::OwnedBytes,
            from: usize,
        ) -> Option<(SourceReference, usize)> {
            let head = bytes.as_slice().get(from..from.checked_add(32)?)?;
            let length = usize::try_from(u64::from_le_bytes(head[24..32].try_into().ok()?)).ok()?;
            let start = from.checked_add(32)?;
            let end = start.checked_add(length)?;
            bytes.as_slice().get(start..end)?;
            Some((
                SourceReference {
                    identity: head[..16].try_into().ok()?,
                    epoch: u64::from_le_bytes(head[16..24].try_into().ok()?),
                    components: if length == 0 {
                        acyclic_stream::OwnedBytes::new()
                    } else {
                        bytes.slice(start..end)
                    },
                },
                end,
            ))
        }
        let (base, next) = node(&self.components, 0).ok_or(DemandError::StaleSource)?;
        let (view, end) = node(&self.components, next).ok_or(DemandError::StaleSource)?;
        if end != self.components.len() || self.epoch != 0 {
            return Err(DemandError::StaleSource);
        }
        Ok((base, view))
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
