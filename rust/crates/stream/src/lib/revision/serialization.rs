//! Portable serialization of the original primitive revision tokens.
use super::{Revision, RevisionCut, RevisionScope};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};

impl Serialize for RevisionScope {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.as_bytes().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for RevisionScope {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Vec::<u8>::deserialize(deserializer).map(Self::new)
    }
}

impl Serialize for RevisionCut {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for RevisionCut {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::new(Vec::<Revision>::deserialize(deserializer)?).map_err(D::Error::custom)
    }
}

pub(super) mod incarnation {
    use super::*;

    pub fn serialize<S: Serializer>(value: &u128, serializer: S) -> Result<S::Ok, S::Error> {
        value.to_be_bytes().serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u128, D::Error> {
        <[u8; 16]>::deserialize(deserializer).map(u128::from_be_bytes)
    }
}
