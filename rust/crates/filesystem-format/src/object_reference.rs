//! Canonical filesystem object references.

use crate::identity::{Digest, FileId};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Immutable object class. Hashes are scoped by both kind and canonical bytes.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ObjectKind {
    /// Authenticated blob-index page.
    Blob,
    /// Fixed-size immutable content chunk addressed by its complete bytes.
    BlobChunk,
    /// Authenticated namespace page.
    TreePage,
    /// Authenticated file-extent page.
    ExtentPage,
    /// Authenticated file-table page.
    FileTablePage,
    /// Composite immutable generation root.
    GenerationRoot,
    /// Canonical metadata object.
    Metadata,
    /// Authenticated named extended-attribute/stream page.
    AttributePage,
    /// Authenticated direct parent/name bindings for one file identity.
    BindingPage,
}

impl ObjectKind {
    /// Returns the stable canonical object-class tag.
    #[must_use]
    pub const fn canonical_tag(self) -> u8 {
        match self {
            Self::Blob => 1,
            Self::BlobChunk => 2,
            Self::TreePage => 3,
            Self::ExtentPage => 4,
            Self::FileTablePage => 5,
            Self::GenerationRoot => 6,
            Self::Metadata => 7,
            Self::AttributePage => 8,
            Self::BindingPage => 9,
        }
    }

    /// Parses one stable canonical object-class tag.
    pub const fn from_canonical_tag(tag: u8) -> Result<Self, ObjectKindError> {
        match tag {
            1 => Ok(Self::Blob),
            2 => Ok(Self::BlobChunk),
            3 => Ok(Self::TreePage),
            4 => Ok(Self::ExtentPage),
            5 => Ok(Self::FileTablePage),
            6 => Ok(Self::GenerationRoot),
            7 => Ok(Self::Metadata),
            8 => Ok(Self::AttributePage),
            9 => Ok(Self::BindingPage),
            value => Err(ObjectKindError::Unknown(value)),
        }
    }
}

/// Canonical object-kind parsing failures.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ObjectKindError {
    /// A newer or corrupt object used an unknown kind tag.
    #[error("unknown object kind tag {0}")]
    Unknown(u8),
}

/// Typed object identity.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ObjectId {
    /// Object class.
    pub kind: ObjectKind,
    /// BLAKE3 of canonical plaintext bytes.
    pub digest: Digest,
    /// Authenticated physical association, carried by the referring object.
    pub storage: Option<ObjectStorage>,
}

impl Serialize for ObjectId {
    /// Preserve the complete canonical reference, including sealed association,
    /// through the shared stack-owned encoder; no separate export identity.
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(self.to_bytes().as_slice())
    }
}

impl<'de> Deserialize<'de> for ObjectId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Reference;
        impl<'de> serde::de::Visitor<'de> for Reference {
            type Value = ObjectId;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("one complete canonical object reference")
            }
            fn visit_bytes<E: serde::de::Error>(self, bytes: &[u8]) -> Result<ObjectId, E> {
                ObjectId::try_from(bytes).map_err(E::custom)
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut input: A,
            ) -> Result<ObjectId, A::Error> {
                use serde::de::Error as _;
                // The largest canonical reference is fixed by the format. No
                // untrusted length is used for allocation or iteration bounds.
                let mut bytes = [0; ObjectId::ENCODED_LEN];
                let mut length = 0;
                while let Some(byte) = input.next_element::<u8>()? {
                    let slot = bytes
                        .get_mut(length)
                        .ok_or_else(|| A::Error::custom(ObjectIdError::Length))?;
                    *slot = byte;
                    length += 1;
                }
                let encoded = bytes
                    .get(..length)
                    .ok_or_else(|| serde::de::Error::custom("object reference is too long"))?;
                self.visit_bytes(encoded)
            }
        }
        deserializer.deserialize_bytes(Reference)
    }
}

/// Physical location of lineage-sealed bytes. The referring object's hash binds
/// this association; the plaintext digest remains the filesystem identity.
#[derive(
    Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize,
)]
pub struct ObjectStorage {
    /// BLAKE3 of the exact sealed CAS payload.
    pub locator: Digest,
    /// Root directory identity selecting the sealing lineage.
    pub root_file_id: FileId,
    /// Original generation authenticated by the seal, preserved when reused.
    pub origin_generation: Digest,
}

impl ObjectId {
    /// Largest canonical object-reference encoding.
    pub const ENCODED_LEN: usize = 113;
    /// Smallest canonical object-reference encoding: a plain reference.
    pub const PLAIN_ENCODED_LEN: usize = 33;

    /// Canonical bytes occupied by this reference.
    #[must_use]
    pub const fn encoded_len(self) -> usize {
        if self.storage.is_some() {
            Self::ENCODED_LEN
        } else {
            Self::PLAIN_ENCODED_LEN
        }
    }

    /// Parses one complete canonical reference, leaving subsequent fields unread.
    ///
    /// The returned suffix borrows the original input. Parsing performs no
    /// allocation and preserves the complete sealed storage association.
    pub fn from_prefix(bytes: &[u8]) -> Result<(Self, &[u8]), ObjectIdError> {
        let mut reader = ObjectPrefixReader { bytes };
        let object = Self::read_from(&mut reader)?;
        Ok((object, reader.bytes))
    }

    /// Reads a canonical reference through a caller-accounted reader.
    pub fn read_from<R: ObjectIdRead>(reader: &mut R) -> Result<Self, R::Error> {
        let tag = reader.tag()?;
        let digest = Digest::from_bytes(reader.fixed::<32>()?);
        let kind = ObjectKind::from_canonical_tag(tag & 127).map_err(|_| R::unknown_kind(tag))?;
        let storage = if tag & 128 == 0 {
            None
        } else {
            Some(ObjectStorage {
                locator: Digest::from_bytes(reader.fixed::<32>()?),
                root_file_id: FileId::from_bytes(reader.fixed::<16>()?),
                origin_generation: Digest::from_bytes(reader.fixed::<32>()?),
            })
        };
        Ok(Self {
            kind,
            digest,
            storage,
        })
    }

    /// Returns the canonical typed reference, including its sealed association.
    #[must_use]
    pub fn to_bytes(self) -> ObjectIdBytes {
        let mut bytes = [0; Self::ENCODED_LEN];
        bytes[0] = self.kind.canonical_tag() | if self.storage.is_some() { 128 } else { 0 };
        bytes[1..33].copy_from_slice(self.digest.as_bytes());
        if let Some(storage) = self.storage {
            bytes[33..65].copy_from_slice(storage.locator.as_bytes());
            bytes[65..81].copy_from_slice(&storage.root_file_id.into_bytes());
            bytes[81..113].copy_from_slice(storage.origin_generation.as_bytes());
        }
        ObjectIdBytes {
            bytes,
            length: self.encoded_len(),
        }
    }

    /// Fixed-width index key from the canonical encoding, padded with zeros.
    pub fn index_key(self) -> [u8; Self::ENCODED_LEN] {
        self.to_bytes().bytes
    }
}

/// Stack-owned canonical object reference, with no allocation for serialization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectIdBytes {
    bytes: [u8; ObjectId::ENCODED_LEN],
    length: usize,
}

impl ObjectIdBytes {
    /// Borrows the initialized canonical encoding.
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        self.bytes
            .get(..self.length)
            .unwrap_or_else(|| unreachable!("canonical object reference exceeds its backing array"))
    }
}

impl std::ops::Deref for ObjectIdBytes {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl TryFrom<&[u8]> for ObjectId {
    type Error = ObjectIdError;

    /// Parses a canonical typed reference and its optional sealed association.
    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let (object, remaining) = Self::from_prefix(bytes)?;
        if !remaining.is_empty() {
            return Err(ObjectIdError::Length);
        }
        Ok(object)
    }
}

/// Fixed-field admission belongs to the reader; identity framing belongs here.
pub trait ObjectIdRead {
    /// Typed framing or reader error.
    type Error;

    /// Reads the kind and sealed-reference tag.
    fn tag(&mut self) -> Result<u8, Self::Error> {
        self.fixed::<1>().map(|[tag]| tag)
    }
    /// Reads an entire fixed-width field.
    fn fixed<const N: usize>(&mut self) -> Result<[u8; N], Self::Error>;
    /// Constructs the reader's error for an unknown tag.
    /// Reports an unrecognized kind from the raw tag byte, including its storage bit.
    fn unknown_kind(tag: u8) -> Self::Error;
}

struct ObjectPrefixReader<'a> {
    bytes: &'a [u8],
}

impl ObjectIdRead for ObjectPrefixReader<'_> {
    type Error = ObjectIdError;

    fn fixed<const N: usize>(&mut self) -> Result<[u8; N], Self::Error> {
        let (field, remaining) = self
            .bytes
            .split_at_checked(N)
            .ok_or(ObjectIdError::Length)?;
        let value = field.try_into().map_err(|_| ObjectIdError::Length)?;
        self.bytes = remaining;
        Ok(value)
    }

    fn unknown_kind(tag: u8) -> Self::Error {
        ObjectKindError::Unknown(tag & 127).into()
    }
}

/// Canonical object-identity byte parsing failures.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ObjectIdError {
    /// The encoding has an invalid object-reference length.
    #[error("invalid object reference length")]
    Length,
    /// The tag byte names no known object class.
    #[error(transparent)]
    Kind(#[from] ObjectKindError),
}

/// Content-addressed generation identity and its authenticated storage association.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct GenerationId {
    digest: Digest,
    storage: Option<ObjectStorage>,
}

impl Serialize for GenerationId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.to_bytes().as_slice().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for GenerationId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Reference;
        impl<'de> serde::de::Visitor<'de> for Reference {
            type Value = GenerationId;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a canonical generation-root reference")
            }

            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut sequence: A,
            ) -> Result<Self::Value, A::Error> {
                let mut bytes = [0; ObjectId::ENCODED_LEN];
                let mut length = 0;
                while let Some(byte) = sequence.next_element::<u8>()? {
                    let slot = bytes.get_mut(length).ok_or_else(|| {
                        serde::de::Error::custom("generation reference is too long")
                    })?;
                    *slot = byte;
                    length += 1;
                }
                let encoded = bytes
                    .get(..length)
                    .ok_or_else(|| serde::de::Error::custom("generation reference is too long"))?;
                GenerationId::from_bytes(encoded).ok_or_else(|| {
                    serde::de::Error::custom("invalid canonical generation-root reference")
                })
            }
        }
        deserializer.deserialize_seq(Reference)
    }
}

impl GenerationId {
    /// Constructs an unsealed generation identity.
    #[must_use]
    pub const fn new(digest: Digest) -> Self {
        Self {
            digest,
            storage: None,
        }
    }

    /// Preserves the complete authenticated generation-root reference.
    #[must_use]
    pub const fn from_root(root: ObjectId) -> Self {
        Self {
            digest: root.digest,
            storage: root.storage,
        }
    }

    /// Returns the complete reference needed to reopen this generation.
    #[must_use]
    pub const fn object_id(self) -> ObjectId {
        ObjectId {
            kind: ObjectKind::GenerationRoot,
            digest: self.digest,
            storage: self.storage,
        }
    }

    /// Encodes the complete canonical generation-root reference.
    #[must_use]
    pub fn to_bytes(self) -> ObjectIdBytes {
        self.object_id().to_bytes()
    }

    /// Decodes a canonical generation-root reference, rejecting other object kinds.
    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let root = ObjectId::try_from(bytes).ok()?;
        (root.kind == ObjectKind::GenerationRoot).then(|| Self::from_root(root))
    }

    /// Returns the authenticated plaintext root digest.
    #[must_use]
    pub const fn digest(self) -> Digest {
        self.digest
    }
}
