use super::*;

pub(super) const LEAF_HEADER_BYTES: usize = 33;
const BRANCH_BUNDLE_V1: u8 = 0x13;
const LEAF_BUNDLE_V1: u8 = 0x14;
const BUNDLE_HEADER: usize = 3;
const BUNDLE_KEY: usize = 8;
const BUNDLE_ENTRY: usize = BUNDLE_KEY + 2;
const MAXIMUM_BRANCHES: usize = 256;
const BRANCH_FIXED_BYTES: usize = 2 + 2 * 32;
const MINIMUM_BRANCH_BYTES: usize = BRANCH_FIXED_BYTES + 2;
const MAXIMUM_BRANCH_BYTES: usize = BRANCH_FIXED_BYTES + 2 * 10;
const MAXIMUM_BUNDLE_BYTES: usize = if acyclic_stream::MAX_RECORD_BYTES < u16::MAX as usize {
    acyclic_stream::MAX_RECORD_BYTES
} else {
    u16::MAX as usize
};

pub(super) enum BundleLeaf<'a> {
    Separate(NodeRef),
    Inline {
        key: [u8; 32],
        value: &'a [u8],
        segmented: bool,
    },
}

pub(super) fn fits_bundle(branches: usize, value: &[u8]) -> bool {
    let overhead = BUNDLE_HEADER
        + (branches + 1) * BUNDLE_ENTRY
        + branches * MAXIMUM_BRANCH_BYTES
        + LEAF_HEADER_BYTES;
    branches != 0 && value.len() <= MAXIMUM_BUNDLE_BYTES.saturating_sub(overhead)
}

#[derive(Clone, Copy)]
pub(in crate::filesystem::execution_journal) enum Node<'a> {
    Leaf {
        key: [u8; 32],
        value: &'a [u8],
        segmented: bool,
    },
    Branch {
        bit: u16,
        children: [NodeRef; 2],
    },
}

impl<'a> Node<'a> {
    pub(in crate::filesystem::execution_journal) fn decode(
        bytes: &'a [u8],
        sequence: u64,
        io: Option<&PreparationIo>,
    ) -> Result<Self> {
        Self::decode_at(bytes, sequence, None, io)
    }

    pub(super) fn decode_at(
        bytes: &'a [u8],
        sequence: u64,
        previous_branch: Option<[u8; BUNDLE_KEY]>,
        io: Option<&PreparationIo>,
    ) -> Result<Self> {
        let malformed = || invalid(io, "execution index node is malformed");
        let version = [bytes.first().copied().ok_or_else(malformed)? >> 4];
        acyclic_auth::format::header(&version, b"", &[2], malformed, || {
            canonical_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "unsupported execution index node version",
            )
        })?;
        if bytes.len() > acyclic_stream::MAX_RECORD_BYTES {
            return Err(malformed());
        }
        // The high nibble is the page format version, the low nibble its kind.
        // SDK records already frame the value length; duplicating it here
        // costs bytes in every immutable copy of every changed key path.
        match bytes.first() {
            Some(tag @ (0x20 | 0x22)) => {
                let key = bytes
                    .get(1..LEAF_HEADER_BYTES)
                    .and_then(|key| key.try_into().ok())
                    .ok_or_else(malformed)?;
                let value = bytes.get(LEAF_HEADER_BYTES..).ok_or_else(malformed)?;
                Ok(Self::Leaf {
                    key,
                    value,
                    segmented: *tag == 0x22,
                })
            }
            Some(0x21) => {
                let bit = u16::from(*bytes.get(1).ok_or_else(malformed)?);
                let mut input = bytes.get(2..).ok_or_else(malformed)?;
                let mut bundled_link = false;
                let mut link = || -> Result<NodeRef> {
                    let child_sequence = read_number(&mut input, io)?;
                    let (digest, rest) = input.split_at_checked(32).ok_or_else(malformed)?;
                    let child = NodeRef {
                        sequence: child_sequence,
                        digest: digest.try_into().map_err(|_| malformed())?,
                    };
                    input = rest;
                    if child.sequence > sequence {
                        return Err(malformed());
                    }
                    if child.sequence == sequence {
                        if bundled_link
                            || previous_branch.as_ref() != child.digest.first_chunk::<BUNDLE_KEY>()
                        {
                            return Err(malformed());
                        }
                        bundled_link = true;
                    }
                    Ok(child)
                };
                let children = [link()?, link()?];
                if !input.is_empty() || bundled_link != previous_branch.is_some() {
                    return Err(malformed());
                }
                Ok(Self::Branch { bit, children })
            }
            _ => Err(malformed()),
        }
    }

    pub(in crate::filesystem::execution_journal) fn encode(
        &self,
        io: Option<&PreparationIo>,
    ) -> Result<(OwnedBytes, u64)> {
        let length = match self {
            Self::Leaf { value, .. } => value.len().checked_add(LEAF_HEADER_BYTES),
            Self::Branch { children, .. } => Some(branch_size(children)),
        }
        .filter(|length| *length <= acyclic_stream::MAX_RECORD_BYTES)
        .ok_or_else(|| invalid(io, "execution index node exceeds its record bound"))?;
        if let Some(io) = io {
            let charge = acyclic_fs_work::performance::WorkCounters {
                bytes_encoded: length as u64,
                bytes_copied: length as u64,
                items_examined: 1,
                ..Default::default()
            };
            io.preflight(charge)?;
            io.charge(charge)?;
        }
        crate::executor::with_history_io(io, |work| {
            let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
            let mut bytes = crate::core::reserve_copy_vector(length, &mut quota)?;
            match self {
                Self::Leaf {
                    key,
                    value,
                    segmented,
                } => {
                    write_leaf(key, value, *segmented, &mut bytes);
                }
                Self::Branch { bit, children } => {
                    write_branch(*bit, children, &mut bytes, io)?;
                }
            }
            quota.transfer();
            crate::store::operations::owned_bytes_from_retained_vec(bytes, quota.work())
        })
    }
}

fn write_leaf(key: &[u8; 32], value: &[u8], segmented: bool, bytes: &mut Vec<u8>) {
    bytes.push(if segmented { 0x22 } else { 0x20 });
    bytes.extend_from_slice(key);
    bytes.extend_from_slice(value);
}

fn branch_size(children: &[NodeRef; 2]) -> usize {
    BRANCH_FIXED_BYTES
        + children
            .iter()
            .map(|child| number_len(child.sequence))
            .sum::<usize>()
}

fn write_branch(
    bit: u16,
    children: &[NodeRef; 2],
    bytes: &mut Vec<u8>,
    io: Option<&PreparationIo>,
) -> Result<()> {
    bytes.push(0x21);
    bytes.push(u8::try_from(bit).map_err(|_| invalid(io, "execution index bit overflows"))?);
    for child in children {
        write_number(child.sequence, bytes);
        bytes.extend_from_slice(&child.digest);
    }
    Ok(())
}

pub(super) fn is_branch_bundle(bytes: &[u8]) -> bool {
    matches!(bytes.first(), Some(&(BRANCH_BUNDLE_V1 | LEAF_BUNDLE_V1)))
}

/// Each rewrite contributes one path, from leaf to root. The only link to
/// another member of this record points to the immediately preceding node.
/// Off-path links retain their original sequence and digest. The directory
/// offsets also delimit the bodies, so each body needs no extra length field.
pub(super) fn encode_branch_bundle(
    sequence: u64,
    leaf: &BundleLeaf<'_>,
    branches: impl Iterator<Item = (u16, usize, [NodeRef; 2])> + Clone,
    io: Option<&PreparationIo>,
) -> Result<(OwnedBytes, NodeRef)> {
    let malformed = || invalid(io, "execution index branch bundle is malformed");
    let branch_count = branches.clone().count();
    if !(1..=MAXIMUM_BRANCHES).contains(&branch_count) {
        return Err(malformed());
    }
    let (leaf_length, mut replacement_sequence) = match leaf {
        BundleLeaf::Separate(reference) => (0, reference.sequence),
        BundleLeaf::Inline { value, .. } => (
            value
                .len()
                .checked_add(LEAF_HEADER_BYTES)
                .ok_or_else(malformed)?,
            sequence,
        ),
    };
    let has_leaf = leaf_length != 0;
    let count = branch_count + usize::from(has_leaf);
    let header = BUNDLE_HEADER + count * BUNDLE_ENTRY;
    let mut length = header.checked_add(leaf_length).ok_or_else(malformed)?;
    for (_, side, mut children) in branches.clone() {
        children.get_mut(side).ok_or_else(malformed)?.sequence = replacement_sequence;
        length = length
            .checked_add(branch_size(&children))
            .ok_or_else(malformed)?;
        replacement_sequence = sequence;
    }
    if length > MAXIMUM_BUNDLE_BYTES {
        return Err(malformed());
    }
    if let Some(io) = io {
        let work = acyclic_fs_work::performance::WorkCounters {
            bytes_encoded: length as u64,
            bytes_copied: (length + count * BUNDLE_ENTRY) as u64,
            items_examined: 2 * count as u64,
            ..Default::default()
        };
        io.preflight(work)?;
        io.charge(work)?;
    }
    let count = u16::try_from(count).map_err(|_| malformed())?;
    crate::executor::with_history_io(io, |work| {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let mut bytes = crate::core::reserve_copy_vector(length, &mut quota)?;
        bytes.push(if has_leaf {
            LEAF_BUNDLE_V1
        } else {
            BRANCH_BUNDLE_V1
        });
        bytes.extend_from_slice(&count.to_le_bytes());
        bytes.resize(header, 0);
        let mut replacement = match leaf {
            BundleLeaf::Separate(reference) => *reference,
            BundleLeaf::Inline {
                key,
                value,
                segmented,
            } => {
                write_leaf(key, value, *segmented, &mut bytes);
                NodeRef {
                    sequence,
                    digest: bundle_entry(&mut bytes, 0, header, io)?,
                }
            }
        };
        for (index, (bit, side, mut children)) in branches.enumerate() {
            *children.get_mut(side).ok_or_else(malformed)? = replacement;
            let begin = bytes.len();
            write_branch(bit, &children, &mut bytes, io)?;
            let digest = bundle_entry(&mut bytes, index + usize::from(has_leaf), begin, io)?;
            replacement = NodeRef { sequence, digest };
        }
        quota.transfer();
        let (bytes, _) =
            crate::store::operations::owned_bytes_from_retained_vec(bytes, quota.work())?;
        Ok((bytes, replacement))
    })
}

fn bundle_entry(
    bytes: &mut [u8],
    index: usize,
    offset: usize,
    io: Option<&PreparationIo>,
) -> Result<[u8; 32]> {
    let malformed = || invalid(io, "execution index branch bundle is malformed");
    let digest = crate::model::hash_bytes(bytes.get(offset..).ok_or_else(malformed)?, io)?;
    let start = BUNDLE_HEADER + index * BUNDLE_ENTRY;
    let entry = bytes
        .get_mut(start..start + BUNDLE_ENTRY)
        .ok_or_else(malformed)?;
    let (key, position) = entry.split_at_mut(BUNDLE_KEY);
    key.copy_from_slice(digest.first_chunk::<BUNDLE_KEY>().ok_or_else(malformed)?);
    let offset = u16::try_from(offset).map_err(|_| malformed())?;
    position.copy_from_slice(&offset.to_le_bytes());
    Ok(digest)
}

pub(super) struct BundleSlot {
    index: usize,
    range: std::ops::Range<usize>,
    previous: Option<[u8; BUNDLE_KEY]>,
}

impl BundleSlot {
    fn directory<'a>(
        bytes: &'a [u8],
        io: Option<&PreparationIo>,
    ) -> Result<&'a [[u8; BUNDLE_ENTRY]]> {
        let malformed = || invalid(io, "execution index branch bundle is malformed");
        let header = bytes.first_chunk::<BUNDLE_HEADER>().ok_or_else(malformed)?;
        let [tag, low, high] = *header;
        let count = usize::from(u16::from_le_bytes([low, high]));
        let valid = match tag {
            BRANCH_BUNDLE_V1 => {
                (1..=MAXIMUM_BRANCHES).contains(&count)
                    && bytes.len() <= BUNDLE_HEADER + count * (BUNDLE_ENTRY + MAXIMUM_BRANCH_BYTES)
            }
            LEAF_BUNDLE_V1 => (2..=MAXIMUM_BRANCHES + 1).contains(&count),
            _ => false,
        };
        if !valid || bytes.len() > MAXIMUM_BUNDLE_BYTES {
            return Err(malformed());
        }
        let end = BUNDLE_HEADER + count * BUNDLE_ENTRY;
        let directory = bytes.get(BUNDLE_HEADER..end).ok_or_else(malformed)?;
        Ok(directory.as_chunks::<BUNDLE_ENTRY>().0)
    }

    fn at(
        bytes: &[u8],
        directory: &[[u8; BUNDLE_ENTRY]],
        index: usize,
        io: Option<&PreparationIo>,
    ) -> Result<Self> {
        let malformed = || invalid(io, "execution index branch bundle is malformed");
        let entry = directory.get(index).ok_or_else(malformed)?;
        let offset = |entry: &[u8; BUNDLE_ENTRY]| {
            let [.., low, high] = *entry;
            usize::from(u16::from_le_bytes([low, high]))
        };
        let start = offset(entry);
        if start < BUNDLE_HEADER + directory.len() * BUNDLE_ENTRY {
            return Err(malformed());
        }
        let end = directory.get(index + 1).map_or(bytes.len(), offset);
        let range = start..end;
        let body = bytes.get(range.clone()).ok_or_else(malformed)?;
        let valid = if index == 0 && bytes.first() == Some(&LEAF_BUNDLE_V1) {
            body.len() >= LEAF_HEADER_BYTES && matches!(body.first(), Some(0x20 | 0x22))
        } else {
            (MINIMUM_BRANCH_BYTES..=MAXIMUM_BRANCH_BYTES).contains(&body.len())
                && body.first() == Some(&0x21)
        };
        if !valid {
            return Err(malformed());
        }
        let previous = index
            .checked_sub(1)
            .map(|previous| {
                directory
                    .get(previous)
                    .and_then(|entry| entry.first_chunk::<BUNDLE_KEY>().copied())
                    .ok_or_else(malformed)
            })
            .transpose()?;
        Ok(Self {
            index,
            range,
            previous,
        })
    }

    pub(super) fn find(bytes: &[u8], digest: [u8; 32], io: Option<&PreparationIo>) -> Result<Self> {
        let directory = Self::directory(bytes, io)?;
        for (index, entry) in directory.iter().enumerate().rev() {
            crate::model::charge_items(io, 1)?;
            if entry.first_chunk::<BUNDLE_KEY>() == digest.first_chunk::<BUNDLE_KEY>() {
                let slot = Self::at(bytes, directory, index, io)?;
                // A short key only locates candidates. A collision can neither
                // replace a branch nor hide another branch with the same key.
                if crate::model::hash_bytes(slot.bytes(bytes, io)?, io)? == digest {
                    return Ok(slot);
                }
            }
        }
        Err(invalid(
            io,
            "execution index branch is missing from its bundle",
        ))
    }

    pub(super) fn preceding(
        &self,
        bytes: &[u8],
        digest: [u8; 32],
        io: Option<&PreparationIo>,
    ) -> Result<Self> {
        let index = self
            .index
            .checked_sub(1)
            .ok_or_else(|| invalid(io, "execution index branch cannot precede its bundle"))?;
        if self.previous.as_ref() != digest.first_chunk::<BUNDLE_KEY>() {
            return Err(invalid(io, "execution index previous branch differs"));
        }
        Self::at(bytes, Self::directory(bytes, io)?, index, io)
    }

    pub(super) fn bytes<'a>(
        &self,
        bytes: &'a [u8],
        io: Option<&PreparationIo>,
    ) -> Result<&'a [u8]> {
        bytes
            .get(self.range.clone())
            .ok_or_else(|| invalid(io, "execution index bundled branch is missing"))
    }

    pub(super) fn previous(&self) -> Option<[u8; BUNDLE_KEY]> {
        self.previous
    }
}

fn number_len(mut value: u64) -> usize {
    let mut length = 1;
    while value >= 128 {
        length += 1;
        value >>= 7;
    }
    length
}

fn write_number(mut value: u64, bytes: &mut Vec<u8>) {
    while value >= 128 {
        let [low, ..] = value.to_le_bytes();
        bytes.push((low & 127) | 128);
        value >>= 7;
    }
    let [low, ..] = value.to_le_bytes();
    bytes.push(low);
}

fn read_number(bytes: &mut &[u8], io: Option<&PreparationIo>) -> Result<u64> {
    let malformed = || invalid(io, "execution index sequence is malformed");
    let mut value = 0;
    for shift in (0..=63).step_by(7) {
        let (byte, rest) = bytes.split_first().ok_or_else(malformed)?;
        *bytes = rest;
        if shift == 63 && *byte > 1 {
            return Err(malformed());
        }
        value |= u64::from(byte & 127) << shift;
        if *byte < 128 {
            if shift != 0 && *byte == 0 {
                return Err(malformed());
            }
            return Ok(value);
        }
    }
    Err(malformed())
}

#[cfg(feature = "test-support")]
pub(in crate::filesystem::execution_journal) mod codec_fuzz {
    use super::*;

    pub(in crate::filesystem::execution_journal) fn check(bytes: &[u8]) {
        let result = check_node(bytes);
        assert!(
            result.is_ok(),
            "journal node codec check failed: {result:?}"
        );
    }

    fn check_node(bytes: &[u8]) -> Result<()> {
        if is_branch_bundle(bytes) {
            #[cfg(not(target_arch = "wasm32"))]
            {
                let ordinary = check_bundle(bytes, None).map_err(|error| error.code());
                crate::filesystem::execution_journal::codec_fuzz::check_decode_custody(|io| {
                    assert_eq!(
                        check_bundle(bytes, Some(io)).map_err(|error| error.code()),
                        ordinary
                    );
                    Ok(())
                })?;
            }
            #[cfg(target_arch = "wasm32")]
            let _ = check_bundle(bytes, None);
            return Ok(());
        }
        for sequence in [0, u64::MAX] {
            super::super::value::codec_fuzz::check(bytes, sequence);
            let ordinary = Node::decode(bytes, sequence, None);
            #[cfg(not(target_arch = "wasm32"))]
            crate::filesystem::execution_journal::codec_fuzz::check_decode_custody(|io| {
                let funded = Node::decode(bytes, sequence, Some(io));
                assert_eq!(
                    funded.as_ref().map(|_| ()).map_err(Error::code),
                    ordinary.as_ref().map(|_| ()).map_err(Error::code)
                );
                if let Ok(value) = funded {
                    let (encoded, held) = value.encode(Some(io))?;
                    let _hold = io.guard_owned(held)?;
                    assert_eq!(encoded.as_ref(), bytes);
                    assert!(Node::decode(&encoded, sequence, Some(io)).is_ok());
                }
                Ok(())
            })?;
            if let Ok(value) = ordinary {
                let (encoded, _) = value.encode(None)?;
                assert_eq!(encoded.as_ref(), bytes);
                let required = match value {
                    Node::Leaf { .. } => LEAF_HEADER_BYTES,
                    Node::Branch { .. } => encoded.len(),
                };
                let invalid_encoding =
                    || invalid(None, "admitted journal node encoded without its header");
                let mut framed = encoded.get(..required).ok_or_else(invalid_encoding)?;
                while let Some((_, truncated)) = framed.split_last() {
                    assert!(Node::decode(truncated, sequence, None).is_err());
                    framed = truncated;
                }
                let kind = encoded.first().copied().ok_or_else(invalid_encoding)? & 15;
                for version in [0, 1, 15] {
                    let header = [kind | (version << 4)];
                    assert!(Node::decode(&header, sequence, None)
                        .is_err_and(|error| error.code() == crate::wire::ErrorCode::Unsupported));
                }
                assert!(
                    Node::decode(&[0x23], sequence, None)
                        .is_err_and(|error| error.code() == crate::wire::ErrorCode::Storage)
                );
                if matches!(value, Node::Branch { .. }) {
                    assert!(Node::decode(&encoded, 0, None).is_err());
                    let mut trailing = encoded.to_vec();
                    trailing.push(0);
                    assert!(Node::decode(&trailing, sequence, None).is_err());
                }
            }
        }
        Ok(())
    }

    // Fuzz inputs supply records without a signed root. Derive a candidate
    // from the last body, and follow the same-frame links it authenticates.
    fn check_bundle(bytes: &[u8], io: Option<&PreparationIo>) -> Result<()> {
        let directory = BundleSlot::directory(bytes, io)?;
        let mut slot = BundleSlot::at(bytes, directory, directory.len() - 1, io)?;
        let mut digest = crate::model::hash_bytes(slot.bytes(bytes, io)?, io)?;
        slot = BundleSlot::find(bytes, digest, io)?;
        loop {
            let body = slot.bytes(bytes, io)?;
            if crate::model::hash_bytes(body, io)? != digest {
                return Err(invalid(io, "execution index bundled branch differs"));
            }
            let Node::Branch { children, .. } =
                Node::decode_at(body, u64::MAX, slot.previous(), io)?
            else {
                return Ok(());
            };
            let Some(next) = children.iter().find(|child| child.sequence == u64::MAX) else {
                return Ok(());
            };
            digest = next.digest;
            slot = slot.preceding(bytes, digest, io)?;
        }
    }

    pub(in crate::filesystem::execution_journal) fn seeds() -> Result<Vec<(&'static str, Vec<u8>)>>
    {
        let mut seeds = Vec::new();
        seeds.push((
            "execution-index-segmented-value",
            super::super::value::codec_fuzz::seed()?,
        ));
        use crate::filesystem::execution_journal::checkpoint_maps::Entry;
        for (name, marker) in [
            ("execution-index-inline-exv1-position", *b"EXV1"),
            ("execution-index-inline-exv2-position", *b"EXV2"),
        ] {
            let position = (u64::from(u32::from_le_bytes(marker)), [9; 32]);
            let value = position.encode(None)?;
            let (bytes, _) = Node::Leaf {
                key: [6; 32],
                value: &value,
                segmented: false,
            }
            .encode(None)?;
            seeds.push((name, bytes.to_vec()));
        }
        for (name, value) in [
            ("execution-index-leaf-empty", b"".as_slice()),
            ("execution-index-leaf-value", b"retained summary".as_slice()),
        ] {
            let (bytes, _) = Node::Leaf {
                key: [6; 32],
                value,
                segmented: false,
            }
            .encode(None)?;
            Node::decode(&bytes, u64::MAX, None)?;
            seeds.push((name, bytes.to_vec()));
        }
        for (name, sequence) in [
            ("execution-index-branch-zero", 0),
            ("execution-index-branch-one", 1),
            ("execution-index-branch-seven-bits", 127),
            ("execution-index-branch-eight-bits", 128),
            ("execution-index-branch-fourteen-bits", 16_383),
            ("execution-index-branch-fifteen-bits", 16_384),
            ("execution-index-branch-sixty-three-bits", (1_u64 << 63) - 1),
            ("execution-index-branch-sixty-four-bits", 1_u64 << 63),
            ("execution-index-branch-maximum-parent", u64::MAX - 1),
        ] {
            let (bytes, _) = Node::Branch {
                bit: 255,
                children: [
                    NodeRef {
                        sequence: 0,
                        digest: [7; 32],
                    },
                    NodeRef {
                        sequence,
                        digest: [8; 32],
                    },
                ],
            }
            .encode(None)?;
            Node::decode(&bytes, u64::MAX, None)?;
            // The first child is zero. Its two-byte spelling must not alias
            // the original canonical varint or shift the following digest.
            let mut overlong = bytes.to_vec();
            drop(overlong.splice(2..3, [128, 0]));
            assert!(Node::decode(&overlong, u64::MAX, None).is_err());
            seeds.push((name, bytes.to_vec()));
        }
        Ok(seeds)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bundle_directory_cannot_redirect_an_authenticated_child() -> Result<()> {
        let leaf = NodeRef {
            sequence: 0,
            digest: [1; 32],
        };
        let (bundle, root) = encode_branch_bundle(
            u64::MAX,
            &BundleLeaf::Separate(leaf),
            [(4, 0, [leaf; 2]), (1, 0, [leaf; 2])].into_iter(),
            None,
        )?;
        let slot = BundleSlot::find(&bundle, root.digest, None)?;
        let Node::Branch {
            children: [child, _],
            ..
        } = Node::decode_at(
            slot.bytes(&bundle, None)?,
            root.sequence,
            slot.previous(),
            None,
        )?
        else {
            return Err(invalid(None, "bundle root is not a branch"));
        };
        let child_slot = slot.preceding(&bundle, child.digest, None)?;
        assert_eq!(
            crate::model::hash_bytes(child_slot.bytes(&bundle, None)?, None)?,
            child.digest
        );

        let mut forged = bundle.to_vec();
        let first_offset = BUNDLE_HEADER + BUNDLE_KEY;
        forged
            .get_mut(first_offset..first_offset + 2)
            .ok_or_else(|| invalid(None, "bundle offset is missing"))?
            .fill(0);
        let root = BundleSlot::find(&forged, root.digest, None)?;
        assert!(root.preceding(&forged, child.digest, None).is_err());
        #[cfg(feature = "test-support")]
        {
            codec_fuzz::check(&bundle);
            codec_fuzz::check(&forged);
        }
        Ok(())
    }

    #[test]
    fn oversized_bundle_member_is_refused_before_hashing() -> Result<()> {
        let leaf = NodeRef {
            sequence: 0,
            digest: [1; 32],
        };
        let (bundle, root) = encode_branch_bundle(
            1,
            &BundleLeaf::Separate(leaf),
            [(4, 0, [leaf; 2]), (1, 0, [leaf; 2])].into_iter(),
            None,
        )?;
        let slot = BundleSlot::find(&bundle, root.digest, None)?;
        let padding = MAXIMUM_BRANCH_BYTES + 1 - slot.bytes(&bundle, None)?.len();
        let mut oversized = bundle.to_vec();
        oversized.resize(oversized.len() + padding, 0);

        let cancellation = acyclic_fs::CancellationToken::new();
        let io = PreparationIo::for_unmeasured(&cancellation)?;
        BundleSlot::find(&bundle, root.digest, Some(&io))?;
        let before = io.receipt()?.bytes_hashed;
        assert!(before > 0);
        assert!(BundleSlot::find(&oversized, root.digest, Some(&io)).is_err());
        assert_eq!(io.receipt()?.bytes_hashed, before);
        Ok(())
    }

    #[test]
    fn page_sequences_are_canonical_at_every_varint_boundary() -> Result<()> {
        for header in [0x00, 0x01, 0x10, 0x11, 0x12, 0xf0, 0xf1] {
            assert_eq!(
                Node::decode(&[header], 0, None)
                    .err()
                    .map(|error| error.code()),
                Some(crate::wire::ErrorCode::Unsupported)
            );
        }
        assert_eq!(
            Node::decode(&[0x23], 0, None)
                .err()
                .map(|error| error.code()),
            Some(crate::wire::ErrorCode::Storage)
        );
        for value in [0, 1, 127, 128, 16_383, 16_384, u64::MAX] {
            let mut bytes = Vec::new();
            write_number(value, &mut bytes);
            assert_eq!(bytes.len(), number_len(value));
            let mut input = bytes.as_slice();
            assert_eq!(read_number(&mut input, None)?, value);
            assert!(input.is_empty());
            for length in 0..bytes.len() {
                assert!(read_number(&mut &bytes[..length], None).is_err());
            }
        }
        for bytes in [vec![128, 0], vec![129, 0], vec![255; 10], vec![128; 11]] {
            assert!(read_number(&mut bytes.as_slice(), None).is_err());
        }
        let children = [
            NodeRef {
                sequence: 0,
                digest: [1; 32],
            },
            NodeRef {
                sequence: 128,
                digest: [2; 32],
            },
        ];
        let (bytes, _) = Node::Branch { bit: 255, children }.encode(None)?;
        assert_eq!(bytes.len(), 69);
        assert!(
            bytes.len() < 87,
            "fixed-width negative control did not exceed the compact branch size"
        );
        assert!(
            matches!(Node::decode(&bytes,129,None)?, Node::Branch { bit: 255, children: decoded } if decoded==children)
        );
        assert!(Node::decode(&bytes, 128, None).is_err());
        let mut trailing = bytes.to_vec();
        trailing.push(0);
        assert!(Node::decode(&trailing, 129, None).is_err());
        Ok(())
    }
}
