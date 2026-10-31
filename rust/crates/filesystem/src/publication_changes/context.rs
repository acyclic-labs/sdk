use crate::{GenerationId, Sequence, VolumeId};
use bytes::BufMut;

const VERSION: u8 = 1;
const PREFIX_BYTES: usize = 1 + 16;
const MAXIMUM_BYTES: usize = PREFIX_BYTES + 10 + crate::ObjectId::ENCODED_LEN + 8;

/// Complete generation association carried by one filesystem change record.
///
/// This is publication metadata, not an authorization proof or a follow cursor.
/// The original record's origin, sequence and commit identity remain its cursor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PublicationContext {
    /// Volume whose original publication produced the changes.
    pub volume: VolumeId,
    /// Complete canonical reference, including any sealed storage association.
    pub generation: GenerationId,
    /// Original authority publication sequence, distinct from the change stream sequence.
    pub authority_sequence: Sequence,
}

impl PublicationContext {
    /// Decodes the sole versioned filesystem publication context.
    ///
    /// The caller must separately authorize the followed record and check its
    /// expected volume. No storage lookup or mutable head observation occurs.
    ///
    /// # Errors
    /// Returns `InvalidArgument` for an unknown version, a noncanonical or
    /// wrong-kind reference, malformed framing, truncation or trailing bytes.
    pub fn from_bytes(mut input: &[u8]) -> Result<Self, acyclic_stream::StreamError> {
        let invalid = || acyclic_stream::StreamError::InvalidArgument;
        let (prefix, remaining) = input.split_at_checked(PREFIX_BYTES).ok_or_else(invalid)?;
        if prefix[0] != VERSION {
            return Err(invalid());
        }
        let volume = VolumeId::from_bytes(prefix[1..].try_into().map_err(|_| invalid())?);
        input = remaining;
        let before = input.len();
        let length = prost::encoding::decode_varint(&mut input).map_err(|_| invalid())?;
        if before - input.len() != prost::encoding::encoded_len_varint(length) {
            return Err(invalid());
        }
        let length = usize::try_from(length).map_err(|_| invalid())?;
        let (reference, sequence) = input.split_at_checked(length).ok_or_else(invalid)?;
        let generation = GenerationId::from_bytes(reference).ok_or_else(invalid)?;
        let authority_sequence = Sequence::new(u64::from_le_bytes(
            sequence.try_into().map_err(|_| invalid())?,
        ));
        Ok(Self {
            volume,
            generation,
            authority_sequence,
        })
    }

    pub(crate) fn encode(self) -> EncodedContext {
        let mut bytes = [0; MAXIMUM_BYTES];
        bytes[0] = VERSION;
        bytes[1..PREFIX_BYTES].copy_from_slice(&self.volume.into_bytes());
        let reference = self.generation.to_bytes();
        let mut output = &mut bytes[PREFIX_BYTES..];
        prost::encoding::encode_varint(reference.len() as u64, &mut output);
        output.put_slice(reference.as_slice());
        output.put_u64_le(self.authority_sequence.get());
        let length = MAXIMUM_BYTES - output.len();
        EncodedContext { bytes, length }
    }
}

pub(crate) struct EncodedContext {
    bytes: [u8; MAXIMUM_BYTES],
    length: usize,
}

impl AsRef<[u8]> for EncodedContext {
    fn as_ref(&self) -> &[u8] {
        &self.bytes[..self.length]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Digest, FileId, ObjectId, ObjectKind, ObjectStorage};

    #[test]
    fn publication_context_preserves_sealed_association_and_refuses_aliases() {
        for storage in [
            None,
            Some(ObjectStorage {
                locator: Digest::from_bytes([2; 32]),
                root_file_id: FileId::from_bytes([3; 16]),
                origin_generation: Digest::from_bytes([4; 32]),
            }),
        ] {
            let context = PublicationContext {
                volume: VolumeId::from_bytes([5; 16]),
                generation: GenerationId::from_root(ObjectId {
                    kind: ObjectKind::GenerationRoot,
                    digest: Digest::from_bytes([1; 32]),
                    storage,
                }),
                authority_sequence: Sequence::new(7),
            };
            let encoded = context.encode();
            let bytes = encoded.as_ref();
            assert_eq!(PublicationContext::from_bytes(bytes), Ok(context));
            for end in 0..bytes.len() {
                assert!(PublicationContext::from_bytes(&bytes[..end]).is_err());
            }
            let mut trailing = bytes.to_vec();
            trailing.push(0);
            assert!(PublicationContext::from_bytes(&trailing).is_err());
            let mut version = bytes.to_vec();
            version[0] = VERSION + 1;
            assert!(PublicationContext::from_bytes(&version).is_err());
            let mut nonminimal = bytes.to_vec();
            let length_end = PREFIX_BYTES
                + prost::encoding::encoded_len_varint(
                    context.generation.object_id().encoded_len() as u64
                );
            nonminimal[length_end - 1] |= 128;
            nonminimal.insert(length_end, 0);
            assert!(PublicationContext::from_bytes(&nonminimal).is_err());
            let mut root = context.generation.object_id();
            root.kind = ObjectKind::BlobChunk;
            let mut bytes = encoded.as_ref().to_vec();
            let start =
                PREFIX_BYTES + prost::encoding::encoded_len_varint(root.encoded_len() as u64);
            bytes[start..start + root.encoded_len()].copy_from_slice(root.to_bytes().as_slice());
            assert!(PublicationContext::from_bytes(&bytes).is_err());
            if storage.is_some() {
                let plain = PublicationContext {
                    generation: GenerationId::new(context.generation.digest()),
                    ..context
                };
                assert_ne!(plain.encode().as_ref(), encoded.as_ref());
                assert_ne!(
                    PublicationContext::from_bytes(plain.encode().as_ref()),
                    Ok(context)
                );
            }
        }
        assert!(PublicationContext::from_bytes(&[0; 56]).is_err());
    }
}
