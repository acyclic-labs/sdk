//! On-demand oracles call the production decoders; no alternate wire parser.
use prost::Message;

pub struct Decoder {
    pub name: &'static str,
    pub check: fn(&[u8]),
    pub seed: fn() -> Vec<u8>,
}

fn check<T: Message + Default>(bytes: &[u8]) {
    if let Ok(value) = T::decode(bytes) {
        let encoded = value.encode_to_vec();
        // Unknown fields may be discarded at a wire boundary; the projection
        // must reach a fixed point after that first decode.
        assert_eq!(
            T::decode(encoded.as_slice()).unwrap().encode_to_vec(),
            encoded
        );
    }
}

fn seed<T: Message + Default>() -> Vec<u8> {
    T::default().encode_to_vec()
}

include!(concat!(env!("OUT_DIR"), "/decoders.rs"));

pub fn protobuf(bytes: &[u8]) {
    let Some((selector, payload)) = bytes.split_first_chunk::<8>() else {
        return;
    };
    let index = (u64::from_le_bytes(*selector) % DECODERS.len() as u64) as usize;
    (DECODERS[index].check)(payload);
}

fn framed(
    bytes: &[u8],
    grpc: bool,
    chunk: usize,
) -> Result<Vec<acyclic_stream::wire::SessionFrame>, acyclic_stream::StreamError> {
    use acyclic_stream::wire_codec::session::FrameDecoder;
    let mut decoder = if grpc {
        FrameDecoder::grpc()
    } else {
        FrameDecoder::default()
    };
    let mut frames = Vec::new();
    for mut input in bytes.chunks(chunk) {
        while !input.is_empty() {
            let before = input.len();
            if let Some(frame) = decoder.read(&mut input)? {
                frames.push(frame);
            }
            assert!(input.len() < before, "a decoder step must consume input");
        }
    }
    decoder.finish()?;
    Ok(frames)
}

pub fn session_frames(bytes: &[u8]) {
    for grpc in [false, true] {
        let whole = framed(bytes, grpc, bytes.len().max(1));
        for chunk in [1, 2, 5, 10, 127] {
            assert_eq!(framed(bytes, grpc, chunk), whole);
        }
    }
}

pub fn ids(bytes: &[u8]) {
    if let Ok(id) = acyclic_fs::ObjectId::try_from(bytes) {
        assert_eq!(id.to_bytes().as_slice(), bytes);
        assert_eq!(
            acyclic_fs::ObjectId::try_from(id.to_bytes().as_slice()),
            Ok(id)
        );
    }
    if let Some(id) = acyclic_fs::GenerationId::from_bytes(bytes) {
        assert_eq!(id.to_bytes().as_slice(), bytes);
        assert_eq!(
            acyclic_fs::GenerationId::from_bytes(id.to_bytes().as_slice()),
            Some(id)
        );
    }
}

pub fn auth(bytes: &[u8]) {
    use acyclic_auth::{authority, contract::*};
    if let Ok(value) = authority::decode_block(bytes) {
        assert_eq!(BlockProto::from(value).encode_to_vec(), bytes);
    }
    if let Ok(value) = authority::decode_capability(bytes) {
        assert_eq!(
            authority::decode_capability(&CapabilityProto::from(value.clone()).encode_to_vec()),
            Ok(value)
        );
    }
    if let Ok(value) = authority::decode_caveat(bytes) {
        assert_eq!(CaveatProto::from(value).encode_to_vec(), bytes);
    }
    if let Ok(value) = GrantSetProto::decode(bytes) {
        if let Ok(grants) = GrantSet::try_from(value) {
            let encoded = GrantSetProto::from(grants.clone()).encode_to_vec();
            assert_eq!(
                GrantSet::try_from(GrantSetProto::decode(encoded.as_slice()).unwrap()),
                Ok(grants)
            );
        }
    }
    let trust = acyclic_auth::testing::Snapshot::default();
    if let Ok(value) = acyclic_auth::verify::verify(bytes, &trust, acyclic_auth::testing::NOW) {
        assert_eq!(value.encode().as_slice(), bytes);
    }
}
