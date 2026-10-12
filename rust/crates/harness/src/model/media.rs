use super::*;

/// File policy chosen before dispatch.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum FileProjectionPolicy {
    /// Metadata only, without reading bytes.
    Reference,
    /// Verified bounded text.
    BoundedFull,
    /// Explicit bounded native media intent.
    Native(Box<NativeMediaPolicy>),
}
impl FileProjectionPolicy {
    pub(super) fn validate_file(&self, file: &FileRef, limits: Limits, work: &mut Option<&mut dyn HistoryIo>) -> Result<()> {
        if let Self::Native(policy) = self { return policy.validate(file, limits, work); }
        limits.validate_file(file, work)?;
        history_step(work, 1)?;
        if matches!(self, Self::BoundedFull) && file.descriptor().byte_length() > limits.render_bytes {
            return Err(invalid(work, "model file exceeds render limit"));
        }
        Ok(())
    }
}

/// Configuration claim bound to original task admission and code.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct NativeConfigurationBinding {
    /// Immutable extension admission event.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmEventReferenceWire"))]
    pub source: crate::core::EventReference,
    /// Registered option schema and pinned JSON file.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmExtensionConfigurationWire"))]
    pub configuration: crate::core::ExtensionConfiguration,
    /// Owner-linked executable implementation.
    #[cfg_attr(feature = "wasm", tsify(type = "readonly number[]"))]
    pub implementation_digest: [u8; 32],
}
impl NativeConfigurationBinding {
    pub(crate) fn verify_original<'a>(&self, admission: Option<&crate::core::ExtensionAdmission>,
        schemas: Option<&'a crate::core::SchemaRegistry>, runtime: Option<&crate::extension::ExtensionRuntime>, io: Option<&PreparationIo>,
    ) -> Result<(&'a crate::core::SchemaRegistry, [u8; 32])> {
        #[cfg(feature = "filesystem")]
        let _hold;
        #[cfg(feature = "filesystem")]
        let _bytes;
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            _bytes = io.encode(self, io.remaining()?.bytes_encoded, false)?;
            _hold = io.guard_owned(_bytes.len() as u64)?;
        }
        self.configuration.validate()?;
        let admission = admission.ok_or_else(|| Error::Unsupported("original task extension admission is unavailable".into()))?;
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            let _hold;
            let bytes = io.encode(&(admission.source(), admission.selected(), admission.configurations()), io.remaining()?.bytes_encoded, false)?;
            _hold = io.guard_bytes(&bytes)?;
        }
        if admission.source() != &self.source || !admission.selected().contains(&self.configuration.extension)
            || !admission.configurations().contains(&self.configuration)
        { return Err(Error::Unauthorized("native options differ from original task admission".into())); }
        let schemas = schemas.ok_or_else(|| Error::Unsupported("original task extension registry is unavailable".into()))?;
        let runtime = runtime.ok_or_else(|| Error::Unsupported("original task linked implementation is unavailable".into()))?;
        let extension = &self.configuration.extension;
        let mut linked = None;
        for identity in runtime.selected() {
            charge_items(io, (identity.name.len() as u64).saturating_add(1))?;
            if identity.name == extension.name && identity.version == extension.version { linked = Some(identity); break; }
        }
        let linked = linked.ok_or_else(|| Error::Unsupported("original linked extension version is unavailable".into()))?;
        let expected = with_history_io(io, |work| schemas.implementation_digest(&extension.name, extension.version, work))?;
        if linked.digest != expected || self.implementation_digest != expected {
            return Err(Error::Conflict("native options implementation binding differs".into()));
        }
        Ok((schemas, expected))
    }
}

/// Common image quality intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum ImageDetail {
    /// Adapter-selected detail.
    Auto,
    /// Low detail.
    Low,
    /// High detail.
    High,
}
/// Requested modality, independent of the stored MIME type.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum NativeMediaIntent {
    /// Image with explicit detail.
    Image {
        /// Image detail.
        detail: ImageDetail,
    },
    /// Complete audio input.
    Audio {
        /// Positive finite source duration ceiling.
        #[cfg_attr(feature = "wasm", tsify(type = "number"))]
        maximum_duration_ms: u64,
    },
    /// Complete video input.
    Video {
        /// Positive finite source duration ceiling.
        #[cfg_attr(feature = "wasm", tsify(type = "number"))]
        maximum_duration_ms: u64,
        /// Positive finite frame ceiling.
        maximum_frames: u32,
    },
    /// Complete document input.
    Document {
        /// Positive finite page ceiling.
        maximum_pages: u32,
    },
}
/// Bounded native input contract.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct NativeMediaPolicy {
    /// Modality and processing intent.
    pub intent: NativeMediaIntent,
    /// Positive finite byte ceiling.
    #[cfg_attr(feature = "wasm", tsify(type = "number"))]
    pub maximum_bytes: u64,
    /// Positive finite provider work ceiling.
    #[cfg_attr(feature = "wasm", tsify(type = "number"))]
    pub maximum_work: u64,
    /// Registered immutable adapter options.
    #[cfg_attr(feature = "wasm", tsify(type = "NativeConfigurationBinding | null"))]
    pub configuration: Option<NativeConfigurationBinding>,
}
impl NativeMediaPolicy {
    /// Validates declarative bounds before media or options are read.
    pub fn validate(&self, file: &FileRef, limits: Limits, work: &mut Option<&mut dyn HistoryIo>) -> Result<()> {
        let finite = |value: u64, work: &mut Option<&mut dyn HistoryIo>| -> Result<()> {
            history_step(work, 1)?;
            if value == 0 || value > crate::conversation::MAX_EXACT_JS_INTEGER {
                return Err(invalid(work, "native media ceiling must be positive and exactly portable"));
            }
            Ok(())
        };
        limits.validate_file(file, work)?;
        finite(self.maximum_bytes, work)?;
        finite(self.maximum_work, work)?;
        history_step(work, 1)?;
        if file.descriptor().byte_length() > self.maximum_bytes { return Err(invalid(work, "native media exceeds byte ceiling")); }
        history_step(work, 1)?;
        match &self.intent {
            NativeMediaIntent::Image { .. } => {},
            NativeMediaIntent::Audio { maximum_duration_ms } => finite(*maximum_duration_ms, work)?,
            NativeMediaIntent::Video { maximum_duration_ms, maximum_frames } => {
                finite(*maximum_duration_ms, work)?;
                history_step(work, 1)?;
                if *maximum_frames == 0 || *maximum_frames == u32::MAX { return Err(invalid(work, "native video frame ceiling must be positive and finite")); }
            }
            NativeMediaIntent::Document { maximum_pages } => {
                history_step(work, 1)?;
                if *maximum_pages == 0 || *maximum_pages == u32::MAX { return Err(invalid(work, "native document page ceiling must be positive and finite")); }
            }
        }
        history_step(work, 1)?;
        if let Some(binding) = &self.configuration {
            limits.validate_file(&binding.configuration.content, work)?;
            history_step(work, 1)?;
            if binding.configuration.content.descriptor().media_type() != "application/json"
                || binding.configuration.content.descriptor().byte_length() > limits.render_bytes
            { return Err(invalid(work, "native options require bounded JSON")); }
        }
        Ok(())
    }
}
