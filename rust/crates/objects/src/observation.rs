//! An original read's key and provider version, retained until conditional use.
use crate::MediumError;
use acyclic_stream::OwnedBytes;

/// The identity of one opened medium. Keep it in the medium's shared owner;
/// clones share this source, and a reopened medium creates a new one.
#[derive(Debug, Default)]
pub struct ObservationSource {
    _identity: u8,
}

/// A provider's conditional version from the same read that returned the bytes.
#[derive(Debug)]
pub enum ObservedVersion {
    /// An in-process mutation sequence, preserved when records move.
    Sequence(u64),
    /// The provider's opaque conditional tag.
    Tag(String),
    /// Original bytes for a provider whose atomic conditional primitive compares values.
    Value(OwnedBytes),
}

/// A key-bound conditional fact issued by the original opened medium.
/// Its source stays borrowed until the fact is consumed; consumers cannot
/// substitute a key, provider version or reopened source.
#[derive(Debug)]
pub struct Observation<'a> {
    source: &'a ObservationSource,
    key: String,
    version: ObservedVersion,
}

/// Complete bytes and their conditional fact from one physical read.
#[derive(Debug)]
pub struct Observed<'a> {
    /// The original returned bytes.
    pub bytes: OwnedBytes,
    /// The original key, source and provider version.
    pub observation: Observation<'a>,
}

impl ObservationSource {
    /// Starts the source of one original shared medium owner.
    #[must_use]
    pub const fn new() -> Self {
        Self { _identity: 0 }
    }

    /// Issues a fact only after reading or atomically writing this version.
    #[must_use]
    pub fn issue(&self, key: String, version: ObservedVersion) -> Observation<'_> {
        Observation {
            source: self,
            key,
            version,
        }
    }

    /// Checks the originating owner before a backend interprets its version.
    ///
    /// # Errors
    /// Refuses a fact from another owner before any physical mutation.
    pub fn version<'a>(
        &self,
        observation: &'a Observation<'_>,
    ) -> Result<&'a ObservedVersion, MediumError> {
        if !std::ptr::eq(self, observation.source) {
            return Err(MediumError::Failed(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "conditional medium source differs; recovery: observe the key on this opened medium",
            )));
        }
        Ok(&observation.version)
    }
    /// Consumes the original key and version after checking this owner.
    ///
    /// # Errors
    /// Refuses a fact from another opened source before effect.
    pub fn consume(
        &self,
        observation: Observation<'_>,
    ) -> Result<(String, ObservedVersion), MediumError> {
        self.version(&observation)?;
        Ok((observation.key, observation.version))
    }
}

impl Observation<'_> {
    /// The key selected by the original read.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Transfers the original key to the accepted successor fact.
    #[must_use]
    pub fn into_key(self) -> String {
        self.key
    }

    pub(crate) fn version_bytes(&self) -> usize {
        match &self.version {
            ObservedVersion::Sequence(_) => 0,
            ObservedVersion::Tag(tag) => tag.capacity(),
            ObservedVersion::Value(_) => 0,
        }
    }
}
