use super::*;
use crate::conversation::VolumeRef;

/// Original project location, independent of an instruction's retained storage.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct InstructionIdentity {
    #[cfg_attr(feature = "wasm", tsify(type = "WasmVolumeRefWire"))]
    pub(super) volume: VolumeRef,
    pub(super) path: String,
}

impl InstructionIdentity {
    /// Captures the canonical source before archiving its bytes.
    pub fn new(volume: VolumeRef, path: String) -> Result<Self> {
        let identity = Self { volume, path };
        identity.validate(&mut None)?;
        Ok(identity)
    }

    #[doc(hidden)]
    pub fn validate(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<()> {
        self.volume.validate(work)?;
        crate::conversation::validate_content_path(&self.path, work)
    }

    pub(crate) fn copy_fields(
        &self,
        quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
    ) -> Result<Self> {
        Ok(Self {
            volume: self.volume.clone_in_quota(quota)?,
            path: crate::core::copy_text(&self.path, quota)?,
        })
    }

    pub(crate) fn volume(&self) -> &VolumeRef {
        &self.volume
    }
    pub(crate) fn path(&self) -> &str {
        &self.path
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn payload_bytes(&self) -> Result<u64> {
        super::inventory::add(
            self.volume.logical_owned_bytes()?,
            self.path.capacity() as u64,
        )
    }
}

/// Immutable instruction bytes together with their original project identity.
/// The direct form reads legacy manifests without rewriting their file references.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum ProjectInstruction {
    /// Retained bytes whose original source differs from their storage location.
    Captured {
        /// Authenticated retained bytes.
        #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
        file: FileRef,
        /// Canonical original project location.
        source: InstructionIdentity,
    },
    /// An instruction already stored at its canonical source location.
    Direct(#[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))] FileRef),
}

impl From<FileRef> for ProjectInstruction {
    fn from(file: FileRef) -> Self {
        Self::Direct(file)
    }
}

impl std::ops::Deref for ProjectInstruction {
    type Target = FileRef;
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Captured { file, .. } | Self::Direct(file) => file,
        }
    }
}

impl ProjectInstruction {
    /// Borrows both the original identity and the authenticated retained bytes.
    pub fn source(&self) -> InstructionSource<'_> {
        match self {
            Self::Captured { file, source } => InstructionSource::with_identity(file, source),
            Self::Direct(file) => InstructionSource::new(file),
        }
    }

    pub(crate) fn validate_source(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<()> {
        self.validate(work)?;
        if let Self::Captured { source, .. } = self {
            source.validate(work)?;
        }
        Ok(())
    }
}
