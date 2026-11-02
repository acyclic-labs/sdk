use super::FileRef;
use acyclic_model_render::{Header, Provenance};
use serde::{Deserialize, Deserializer, Serialize};

/// An optional producer-owned representation of a summary. Its immutable file
/// is read only after the renderer selects the admitted producing identity.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeSummary {
    /// Nonsecret identity supplied by the artifact's admitted producer.
    pub provenance: Provenance,
    /// Exact native artifact, never reconstructed from portable text.
    pub artifact: FileRef,
}

/// Frozen summary representations, owned by one epoch. Portable plaintext is
/// mandatory; native artifacts are optional and have no count limit. A late
/// artifact needs a new summary reference and a later epoch, never a lookup
/// that changes the current epoch's bytes.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct SummaryRef(SummaryWire);

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SummaryWire {
    version: u32,
    portable: FileRef,
    #[serde(default)]
    native: Vec<NativeSummary>,
}

/// Refusal before selecting or retaining a summary artifact.
#[derive(Debug, thiserror::Error)]
pub enum SummaryError {
    /// The stored representation is not this format.
    #[error("unsupported summary reference version")]
    Version,
    /// Portable text must be available even to a foreign provider.
    #[error("summary requires a text/plain portable file")]
    Portable,
    /// A producer's nonsecret identity is incomplete.
    #[error(transparent)]
    Provenance(#[from] acyclic_model_render::RenderError),
    /// The manifest selected an artifact outside this frozen set.
    #[error("summary artifact index is outside the frozen epoch set")]
    ArtifactIndex,
}

impl SummaryRef {
    /// Admits the complete artifact set once, before publishing an epoch.
    /// Identity validation is proportional to the supplied metadata; selecting
    /// a representation later examines at most one entry, independent of its
    /// position, artifact count or conversation length.
    ///
    /// # Errors
    /// Refuses a non-plaintext portable file or incomplete native provenance.
    pub fn new(portable: FileRef, native: Vec<NativeSummary>) -> Result<Self, SummaryError> {
        Self::admit(SummaryWire {
            version: 1,
            portable,
            native,
        })
    }

    fn admit(wire: SummaryWire) -> Result<Self, SummaryError> {
        if wire.version != 1 {
            return Err(SummaryError::Version);
        }
        if wire.portable.descriptor().media_type() != "text/plain" {
            return Err(SummaryError::Portable);
        }
        for native in &wire.native {
            native.provenance.validate()?;
        }
        Ok(Self(wire))
    }

    /// Required immutable portable text.
    pub const fn portable(&self) -> &FileRef {
        &self.0.portable
    }

    /// Artifact metadata frozen at admission; no mutation or late insertion.
    pub fn native(&self) -> &[NativeSummary] {
        &self.0.native
    }

    /// Selects the frozen header's optional artifact by direct index. `None`
    /// is the portable-only alloy default. A foreign or unknown owner receives
    /// portable text through the renderer's original guarded-message decision.
    /// No file is read and no other artifact is inspected or searched.
    /// The selected index is pinned with the request's frozen epoch header;
    /// changing a provider's chosen representation requires another epoch.
    ///
    /// # Errors
    /// Refuses an index outside this epoch's admitted artifact set.
    pub fn select(&self, header: &Header) -> Result<&FileRef, SummaryError> {
        let native = header
            .summary_artifact()
            .map(|index| self.0.native.get(index).ok_or(SummaryError::ArtifactIndex))
            .transpose()?;
        Ok(acyclic_model_render::summary_artifact(
            header.target(),
            &self.0.portable,
            native.map(|native| (&native.provenance, &native.artifact)),
        ))
    }
}

impl<'de> Deserialize<'de> for SummaryRef {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::admit(SummaryWire::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}
