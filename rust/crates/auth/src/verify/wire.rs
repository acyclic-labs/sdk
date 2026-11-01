use super::VerifyError;
use crate::contract::SignedBlockProto;
use prost::{
    Message,
    encoding::{decode_varint, encode_varint, encoded_len_varint},
};
use zeroize::Zeroizing;

pub(crate) enum Proof<'a> {
    Secret(Zeroizing<Vec<u8>>),
    Sealed(&'a [u8; 64]),
}

impl Proof<'_> {
    fn parts(&self) -> (u64, &[u8]) {
        match self {
            Self::Secret(bytes) => (18, bytes),
            Self::Sealed(bytes) => (26, *bytes),
        }
    }
}

pub(crate) struct Envelope<'a> {
    pub blocks: Vec<SignedBlockProto>,
    pub proof: Proof<'a>,
}

fn varint(input: &mut &[u8]) -> Result<u64, VerifyError> {
    decode_varint(input).map_err(|_| VerifyError::Malformed)
}

fn bytes<'a>(input: &mut &'a [u8]) -> Result<&'a [u8], VerifyError> {
    let len = usize::try_from(varint(input)?).map_err(|_| VerifyError::Malformed)?;
    let (value, rest) = input.split_at_checked(len).ok_or(VerifyError::Malformed)?;
    *input = rest;
    Ok(value)
}

pub(crate) fn decode(mut input: &[u8]) -> Result<Envelope<'_>, VerifyError> {
    let original = input;
    input = input.strip_prefix(&[32]).ok_or(VerifyError::Version(0))?;
    let version_bytes = input.len();
    let version = u32::try_from(varint(&mut input)?).map_err(|_| VerifyError::Malformed)?;
    if version != crate::VERSION {
        return Err(VerifyError::Version(version));
    }
    if version_bytes - input.len() != encoded_len_varint(u64::from(version)) {
        return Err(VerifyError::Malformed);
    }
    let mut blocks = Vec::new();
    let mut proof = None;
    while !input.is_empty() {
        match varint(&mut input)? {
            10 => blocks.push(
                SignedBlockProto::decode(bytes(&mut input)?).map_err(|_| VerifyError::Malformed)?,
            ),
            tag @ (18 | 26) => {
                // Reject before decoding or replacing either oneof variant.
                if proof.is_some() {
                    return Err(VerifyError::Malformed);
                }
                let value = bytes(&mut input)?;
                proof = Some(if tag == 18 {
                    if value.len() != 32 {
                        return Err(VerifyError::Malformed);
                    }
                    // No prost-owned temporary or unguarded secret allocation.
                    let mut secret = Zeroizing::new(vec![0; 32]);
                    secret.copy_from_slice(value);
                    Proof::Secret(secret)
                } else {
                    Proof::Sealed(value.try_into().map_err(|_| VerifyError::Malformed)?)
                });
            }
            _ => return Err(VerifyError::Malformed),
        }
    }
    let proof = proof.ok_or(VerifyError::Malformed)?;
    let (tag, value) = proof.parts();
    if encode(&blocks, Some((tag, value)), version).as_slice() != original {
        return Err(VerifyError::Malformed);
    }
    Ok(Envelope { blocks, proof })
}

pub(crate) fn encoded_len(
    blocks: &[SignedBlockProto],
    proof_len: Option<usize>,
    version: u32,
) -> usize {
    blocks
        .iter()
        .map(|block| {
            let len = block.encoded_len();
            1 + encoded_len_varint(len as u64) + len
        })
        .sum::<usize>()
        + proof_len.map_or(0, |length| 1 + encoded_len_varint(length as u64) + length)
        + 1
        + encoded_len_varint(u64::from(version))
}

pub(crate) fn encode(
    blocks: &[SignedBlockProto],
    proof: Option<(u64, &[u8])>,
    version: u32,
) -> Zeroizing<Vec<u8>> {
    let len = encoded_len(blocks, proof.map(|(_, bytes)| bytes.len()), version);
    let mut output = Zeroizing::new(Vec::with_capacity(len));
    encode_varint(32, &mut *output);
    encode_varint(u64::from(version), &mut *output);
    for block in blocks {
        encode_varint(10, &mut *output);
        encode_varint(block.encoded_len() as u64, &mut *output);
        block.encode_raw(&mut *output);
    }
    if let Some((tag, bytes)) = proof {
        encode_varint(tag, &mut *output);
        encode_varint(bytes.len() as u64, &mut *output);
        output.extend_from_slice(bytes);
    }
    output
}
