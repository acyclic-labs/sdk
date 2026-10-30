//! The byte codec shared by physical ownership publication and recovery.
use crate::{NativeRootIdentity, NativeTreeMaterializationError, VolumeId};

pub(crate) const RECEIPT_BYTES: usize = 124;
pub(crate) const HASHED_BYTES: usize = 92;
const DOMAIN: &[u8] = b"ACYPFSROOT";
const VERSION: &[u8] = b"000003";

#[cfg(feature = "test-support")]
#[path = "native_ownership_fuzz.rs"]
pub mod codec_fuzz;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct NativeOwnershipReceipt {
    pub root: NativeRootIdentity,
    pub fence: NativeRootIdentity,
    pub parent: NativeRootIdentity,
    pub volume: VolumeId,
    pub pid: u32,
    pub started: u64,
}

impl NativeOwnershipReceipt {
    pub(crate) fn encode(self) -> [u8; RECEIPT_BYTES] {
        let mut bytes = [0; RECEIPT_BYTES];
        let (prefix, checksum) = bytes.split_at_mut(HASHED_BYTES);
        self.encode_prefix((&mut *prefix).try_into().expect("fixed ownership prefix"));
        checksum.copy_from_slice(blake3::hash(prefix).as_bytes());
        bytes
    }

    fn encode_prefix(self, bytes: &mut [u8; HASHED_BYTES]) {
        bytes[..10].copy_from_slice(DOMAIN);
        bytes[10..16].copy_from_slice(VERSION);
        bytes[16..32].copy_from_slice(&self.root.to_bytes());
        bytes[32..48].copy_from_slice(&self.fence.to_bytes());
        bytes[48..64].copy_from_slice(&self.parent.to_bytes());
        bytes[64..80].copy_from_slice(&self.volume.into_bytes());
        bytes[80..84].copy_from_slice(&self.pid.to_le_bytes());
        bytes[84..].copy_from_slice(&self.started.to_le_bytes());
    }

    /// Admission precedes payload length, hashing and field interpretation.
    /// The physical adapter charges its original ledger immediately before hash.
    pub(crate) fn decode(
        bytes: &[u8],
        before_hash: impl FnOnce() -> Result<(), NativeTreeMaterializationError>,
    ) -> Result<Self, NativeTreeMaterializationError> {
        let body = acyclic_auth::format::header(
            bytes,
            DOMAIN,
            VERSION,
            || NativeTreeMaterializationError::InvalidLayout,
            || NativeTreeMaterializationError::UnsupportedFormat,
        )?;
        let body: &[u8; 108] = body
            .try_into()
            .map_err(|_| NativeTreeMaterializationError::InvalidLayout)?;
        before_hash()?;
        if body[76..] != *blake3::hash(&bytes[..HASHED_BYTES]).as_bytes() {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        Ok(Self::decode_fields(
            body[..76].try_into().expect("fixed ownership fields"),
        ))
    }

    fn decode_fields(bytes: &[u8; 76]) -> Self {
        Self {
            root: NativeRootIdentity::from_bytes(bytes[..16].try_into().unwrap()),
            fence: NativeRootIdentity::from_bytes(bytes[16..32].try_into().unwrap()),
            parent: NativeRootIdentity::from_bytes(bytes[32..48].try_into().unwrap()),
            volume: VolumeId::from_bytes(bytes[48..64].try_into().unwrap()),
            pid: u32::from_le_bytes(bytes[64..68].try_into().unwrap()),
            started: u64::from_le_bytes(bytes[68..].try_into().unwrap()),
        }
    }

    /// A copied receipt cannot substitute another held lock or parent.
    pub(crate) fn binds(self, fence: NativeRootIdentity, parent: NativeRootIdentity) -> bool {
        self.fence == fence && self.parent == parent
    }
}

#[cfg(kani)]
mod proofs {
    use super::*;

    #[kani::proof]
    #[kani::unwind(125)]
    fn native_ownership_fields_preserve_all_bits() {
        let fields: [u8; 76] = kani::any();
        let value = NativeOwnershipReceipt::decode_fields(&fields);
        let mut prefix = [0; HASHED_BYTES];
        value.encode_prefix(&mut prefix);
        assert_eq!(&prefix[16..], fields.as_slice());
        assert!(value.binds(value.fence, value.parent));
        let mut other = value.fence.to_bytes();
        other[0] ^= 1;
        assert!(!value.binds(NativeRootIdentity::from_bytes(other), value.parent));
        let mut other = value.parent.to_bytes();
        other[0] ^= 1;
        assert!(!value.binds(value.fence, NativeRootIdentity::from_bytes(other)));
    }

    #[kani::proof]
    #[kani::unwind(17)]
    fn native_ownership_version_refuses_before_payload_and_hash() {
        let version: [u8; 6] = kani::any();
        kani::assume(version.as_slice() != VERSION);
        let mut bytes = [0; 16];
        bytes[..10].copy_from_slice(DOMAIN);
        bytes[10..].copy_from_slice(&version);
        assert!(matches!(
            NativeOwnershipReceipt::decode(&bytes, || panic!("unsupported version reached hash")),
            Err(NativeTreeMaterializationError::UnsupportedFormat)
        ));
    }
}
