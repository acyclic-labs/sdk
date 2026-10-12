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
        validate_reference(&reference, limits)?;
        let bytes = reader.read(&reference).await?;
        reference.descriptor().verify(&bytes, &mut None)?;
        let context: Context = crate::contract::json_from_slice(&bytes)
            .map_err(|error| Error::Invalid(format!("context snapshot is invalid: {error}")))?;
        super::validate_projected_context(&context, limits)?;
        if context.current_input_index.is_some() {
            return Err(Error::Invalid(
                "context snapshot still marks a historical input as active".into(),
            ));
        }
        if crate::contract::canonical_json_bytes(&context)?.as_slice() != bytes.as_ref() {
            return Err(Error::Invalid("context snapshot is not canonical".into()));
        }
        super::validate_context_refs(&context, reader.as_ref(), limits).await?;
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

fn validate_reference(reference: &FileRef, limits: Limits) -> Result<()> {
    limits.validate(&mut None)?;
    limits.validate_file(reference, &mut None)?;
    if reference.descriptor().media_type() != "application/json"
        || reference.descriptor().byte_length() > limits.render_bytes
    {
        return Err(Error::Invalid(
            "context snapshot file exceeds JSON/render bounds".into(),
        ));
    }
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn validate_reference_native(
    reference: &FileRef, limits: Limits, work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    limits.validate_with_native_work(work)?;
    limits.validate_file_with_native_work(reference, work)?;
    if reference.descriptor().media_type() != "application/json"
        || reference.descriptor().byte_length() > limits.render_bytes
    {
        return Err(super::native_context_boundary(crate::wire::ErrorCode::Invalid,
            "context snapshot file exceeds JSON/render bounds"));
    }
    Ok(())
}

impl ContextStage for PinnedContextStage {
    fn name(&self) -> &str {
        &self.name
    }

    fn validate(&self) -> Result<()> {
        crate::contract::validate_component_label(&self.name, "context snapshot")?;
        crate::contract::validate_component_label(&self.revision, "context snapshot revision")?;
        validate_reference(&self.reference, self.limits)?;
        super::validate_projected_context(&self.context, self.limits)
    }

    fn contract(&self) -> Value {
        serde_json::json!({
            "name": self.name,
            "revision": self.revision,
            "reference": self.reference,
            "placement": self.placement,
            "limits": self.limits,
        })
    }

    fn apply<'a>(
        &'a self,
        _: &'a ContextInput,
        context: Context,
        limits: Limits,
    ) -> BoxFuture<'a, Result<Context>> {
        Box::pin(async move {
            let limits = super::restrict_context_limits(self.limits, limits)?;
            validate_reference(&self.reference, limits)?;
            let composed =
                super::selection::place_snapshot(context, &self.context, self.placement, limits)?;
            // A new admission still requires the original bound reader's current authority and retained content.
            self.reader.verify(&self.reference).await?;
            super::validate_context_refs(&self.context, self.reader.as_ref(), limits).await?;
            Ok(composed)
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native(&self) -> bool { true }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn validate_native(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        if !crate::is_valid_component_label(&self.name) || !crate::is_valid_component_label(&self.revision) {
            return Err(super::native_context_boundary(crate::wire::ErrorCode::Invalid,
                "context snapshot identity is invalid"));
        }
        validate_reference_native(&self.reference, self.limits, work)?;
        if !self.reader.supports_native_read(self.reference.volume(), work)? {
            return Err(super::native_context_boundary(crate::wire::ErrorCode::Unsupported,
                "context snapshot reader has no original native capability"));
        }
        super::validate_projected_context_native(&self.context, self.limits, work)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn contract_with_native_work(
        &self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Value, acyclic_fs::NativeAllocation)> {
        self.validate_native(work)?;
        #[derive(serde::Serialize)]
        struct Contract<'a> {
            name: &'a str, revision: &'a str, reference: &'a FileRef,
            placement: ContextPlacement, limits: Limits,
        }
        super::context_contract_native(&Contract { name: &self.name, revision: &self.revision,
            reference: &self.reference, placement: self.placement, limits: self.limits }, work)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn apply_native<'a>(
        &'a self, _: &'a ContextInput, context: (Context, acyclic_fs::NativeAllocation),
        limits: Limits, work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Context, acyclic_fs::NativeAllocation)>> {
        self.validate_native(work)?;
        crate::NativeFutureBundle::new(work, || async move {
            let limits = super::restrict_context_limits_native(self.limits, limits, work)?;
            validate_reference_native(&self.reference, limits, work)?;
            let composed = super::selection::place_snapshot_native(
                context, &self.context, self.placement, limits, work)?;
            super::verify_context_file_native(&self.reference, self.reader.as_ref(), work).await?;
            super::validate_context_refs_native(&self.context, self.reader.as_ref(), limits, work).await?;
            Ok(composed)
        })
    }
}
