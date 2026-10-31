use super::*;

pub(super) const LEAF_HEADER_BYTES: usize = 33;

#[derive(Clone, Copy)]
pub(in crate::filesystem::execution_journal) enum Node<'a> {
    Leaf { key: [u8; 32], value: &'a [u8] },
    Branch { bit: u16, children: [NodeRef; 2] },
}

impl<'a> Node<'a> {
    pub(in crate::filesystem::execution_journal) fn decode(
        bytes: &'a [u8],
        sequence: u64,
        io: Option<&PreparationIo>,
    ) -> Result<Self> {
        let malformed = || invalid(io, "execution index node is malformed");
        let version = [bytes.first().copied().ok_or_else(malformed)? >> 4];
        acyclic_auth::format::header(&version, b"", &[1], malformed, || {
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
            Some(0x10) => {
                let key = bytes
                    .get(1..LEAF_HEADER_BYTES)
                    .and_then(|key| key.try_into().ok())
                    .ok_or_else(malformed)?;
                let value = bytes.get(LEAF_HEADER_BYTES..).ok_or_else(malformed)?;
                Ok(Self::Leaf { key, value })
            }
            Some(0x11) => {
                let bit = u16::from(*bytes.get(1).ok_or_else(malformed)?);
                let mut input = bytes.get(2..).ok_or_else(malformed)?;
                let mut link = || -> Result<NodeRef> {
                    let child_sequence = read_number(&mut input, io)?;
                    let (digest, rest) = input.split_at_checked(32).ok_or_else(malformed)?;
                    let child = NodeRef {
                        sequence: child_sequence,
                        digest: digest.try_into().map_err(|_| malformed())?,
                    };
                    input = rest;
                    if child.sequence >= sequence {
                        return Err(malformed());
                    }
                    Ok(child)
                };
                let children = [link()?, link()?];
                if !input.is_empty() {
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
            Self::Branch { children, .. } => Some(
                66 + children
                    .iter()
                    .map(|child| number_len(child.sequence))
                    .sum::<usize>(),
            ),
        }
        .filter(|length| *length <= acyclic_stream::MAX_RECORD_BYTES)
        .ok_or_else(|| invalid(io, "execution index node exceeds its record bound"))?;
        if let Some(io) = io {
            let charge = acyclic_fs::WorkCounters {
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
                Self::Leaf { key, value } => {
                    bytes.push(0x10);
                    bytes.extend_from_slice(key);
                    bytes.extend_from_slice(value);
                }
                Self::Branch { bit, children } => {
                    bytes.push(0x11);
                    bytes.push(
                        u8::try_from(*bit)
                            .map_err(|_| invalid(io, "execution index bit overflows"))?,
                    );
                    for child in children {
                        write_number(child.sequence, &mut bytes);
                        bytes.extend_from_slice(&child.digest);
                    }
                }
            }
            quota.transfer();
            crate::store::operations::owned_bytes_from_retained_vec(bytes, quota.work())
        })
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
        for sequence in [0, u64::MAX] {
            let ordinary = Node::decode(bytes, sequence, None);
            #[cfg(not(target_arch = "wasm32"))]
            {
                let cancellation = acyclic_fs::CancellationToken::new();
                let work = acyclic_fs::NativeMaterializationWork::new(
                    acyclic_fs::WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .expect("unbounded decoder work");
                let baseline = work.live_allocation_bytes();
                {
                    let io =
                        PreparationIo::from_native_work(&work).expect("original work is admitted");
                    let funded = Node::decode(bytes, sequence, Some(&io));
                    assert_eq!(
                        funded.as_ref().map(|_| ()).map_err(Error::code),
                        ordinary.as_ref().map(|_| ()).map_err(Error::code)
                    );
                    if let Ok(value) = funded {
                        let (encoded, held) = value.encode(Some(&io)).unwrap();
                        let _hold = io.guard_owned(held).unwrap();
                        assert_eq!(encoded.as_ref(), bytes);
                        assert!(Node::decode(&encoded, sequence, Some(&io)).is_ok());
                    }
                }
                assert_eq!(work.live_allocation_bytes(), baseline);
            }
            if let Ok(value) = ordinary {
                let (encoded, _) = value.encode(None).expect("admitted node encodes");
                assert_eq!(encoded.as_ref(), bytes);
                let required = match value {
                    Node::Leaf { .. } => LEAF_HEADER_BYTES,
                    Node::Branch { .. } => encoded.len(),
                };
                for end in 0..required {
                    assert!(Node::decode(&encoded[..end], sequence, None).is_err());
                }
                for version in [0, 2, 15] {
                    let header = [(encoded[0] & 15) | (version << 4)];
                    assert_eq!(
                        Node::decode(&header, sequence, None).err().unwrap().code(),
                        crate::wire::ErrorCode::Unsupported
                    );
                }
                assert_eq!(
                    Node::decode(&[0x12], sequence, None).err().unwrap().code(),
                    crate::wire::ErrorCode::Storage
                );
                if matches!(value, Node::Branch { .. }) {
                    assert!(Node::decode(&encoded, 0, None).is_err());
                    let mut trailing = encoded.to_vec();
                    trailing.push(0);
                    assert!(Node::decode(&trailing, sequence, None).is_err());
                }
            }
        }
    }

    pub(in crate::filesystem::execution_journal) fn seeds() -> Result<Vec<(&'static str, Vec<u8>)>>
    {
        let mut seeds = Vec::new();
        for (name, value) in [
            ("execution-index-leaf-empty", b"".as_slice()),
            ("execution-index-leaf-value", b"retained summary".as_slice()),
        ] {
            let (bytes, _) = Node::Leaf {
                key: [6; 32],
                value,
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
    fn page_sequences_are_canonical_at_every_varint_boundary() -> Result<()> {
        for header in [0x00, 0x01, 0x20, 0x21, 0xf0, 0xf1] {
            assert_eq!(
                Node::decode(&[header], 0, None)
                    .err()
                    .map(|error| error.code()),
                Some(crate::wire::ErrorCode::Unsupported)
            );
        }
        assert_eq!(
            Node::decode(&[0x12], 0, None)
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
