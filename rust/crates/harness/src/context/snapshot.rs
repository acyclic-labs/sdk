//! Immutable whole-context imports through the ordinary stage pipeline.

use super::{Context, ContextInput, ContextPlacement, ContextStage};
use crate::{
    Error, Result,
    conversation::{ContentResidencyVerifier, FileRef, Limits},
};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use serde_json::Value;
use std::sync::Arc;

/// Authenticated, immutable messages and metadata from one exact context file.
pub struct PinnedContextStage {
    name: String,
    revision: String,
    reference: FileRef,
    context: Context,
    reader: Arc<dyn ContentResidencyVerifier>,
    placement: ContextPlacement,
    limits: Limits,
}

impl PinnedContextStage {
    /// Explicitly reads and authenticates one canonical context snapshot.
    pub async fn capture(
        name: impl Into<String>,
        revision: impl Into<String>,
        reference: FileRef,
        reader: Arc<dyn ContentResidencyVerifier>,
        placement: ContextPlacement,
        limits: Limits,
    ) -> Result<Self> {
        let name = name.into();
        let revision = revision.into();
        crate::contract::validate_component_label(&name, "context snapshot")?;
        crate::contract::validate_component_label(&revision, "context snapshot revision")?;
        validate_reference(&reference, limits, None)?;
        let bytes = reader.read(&reference, limits.render_bytes, None)?.await?;
        reference.descriptor().verify(&bytes, &mut None)?;
        let context: Context = crate::contract::json_from_slice(&bytes)
            .map_err(|error| Error::Invalid(format!("context snapshot is invalid: {error}")))?;
        super::validate_projected_context(&context, limits, None)?;
        if context.current_input_index.is_some() {
            return Err(Error::Invalid(
                "context snapshot still marks a historical input as active".into(),
            ));
        }
        if crate::contract::canonical_json_bytes(&context)?.as_slice() != bytes.as_ref() {
            return Err(Error::Invalid("context snapshot is not canonical".into()));
        }
        super::validate_context_refs(&context, reader.as_ref(), limits, None).await?;
        Ok(Self {
            name,
            revision,
            reference,
            context,
            reader,
            placement,
            limits,
        })
    }

}

fn validate_reference(reference: &FileRef, limits: Limits, io: Option<&crate::executor::PreparationIo>) -> Result<()> {
    crate::executor::with_history_io(io, |work| limits.validate_file(reference, work))?;
    if reference.descriptor().media_type() != "application/json"
        || reference.descriptor().byte_length() > limits.render_bytes
    {
        return Err(crate::executor::stock_boundary(io, crate::wire::ErrorCode::Invalid,
            "context snapshot file exceeds JSON/render bounds"));
    }
    Ok(())
}

impl ContextStage for PinnedContextStage {
    fn name(&self) -> &str {
        &self.name
    }

    fn validate(&self, io: Option<&crate::executor::PreparationIo>) -> Result<()> {
        if !crate::is_valid_component_label(&self.name) || !crate::is_valid_component_label(&self.revision) {
            return Err(crate::executor::stock_boundary(io, crate::wire::ErrorCode::Invalid, "context snapshot identity is invalid"));
        }
        validate_reference(&self.reference, self.limits, io)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if io.and_then(crate::executor::PreparationIo::original_native_work).is_some()
            && !self.reader.supports_read(self.reference.volume(), io)? {
            return Err(crate::executor::stock_boundary(io, crate::wire::ErrorCode::Unsupported, "context snapshot reader has no original native capability"));
        }
        super::validate_projected_context(&self.context, self.limits, io)
    }

    fn contract(&self, io: Option<&crate::executor::PreparationIo>) -> Result<Value> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
            self.validate(io)?;
            #[derive(serde::Serialize)]
            struct Contract<'a> { name: &'a str, revision: &'a str, reference: &'a FileRef,
                placement: ContextPlacement, limits: Limits }
            let (value, custody) = super::context_contract_native(&Contract { name: &self.name,
                revision: &self.revision, reference: &self.reference, placement: self.placement, limits: self.limits }, work)?;
            io.expect("native ledger").absorb_native_custody(custody)?;
            return Ok(value);
        }
        let _ = io;
        Ok(serde_json::json!({"name": self.name, "revision": self.revision, "reference": self.reference,
            "placement": self.placement, "limits": self.limits}))
    }

    fn apply<'a>(&'a self, input: &'a ContextInput, context: Context, limits: Limits,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Context>> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if io.and_then(crate::executor::PreparationIo::original_native_work).is_some() {
            self.validate(io)?;
        }
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let _ = input;
            let limits = super::restrict_context_limits(self.limits, limits, io)?;
            validate_reference(&self.reference, limits, io)?;
            let composed = super::selection::place_snapshot(context, &self.context, self.placement, limits, io)?;
            self.reader.verify(&self.reference, limits.render_bytes, io)?.await?;
            super::validate_context_refs(&self.context, self.reader.as_ref(), limits, io).await?;
            Ok(composed)
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native(&self) -> bool { true }






}
