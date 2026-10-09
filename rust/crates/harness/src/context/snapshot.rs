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
/// Unlike a messages-only `ContextSource`, this stage preserves metadata.
/// Its contract pins the complete immutable file reference. A fork consumer
/// must additionally bind the original seed/cut and receiving task admission.
/// It must choose one inherited model projection, avoiding duplicate history.
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
    /// The reader must already be bound to the original receiving authority;
    /// a file reference does not grant access. No model operation is performed.
    /// Construction of an ordinary builder does not call this capture method.
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
        reference.descriptor().verify(&bytes)?;
        let context: Context = crate::contract::json_from_slice(&bytes)
            .map_err(|error| Error::Invalid(format!("context snapshot is invalid: {error}")))?;
        super::validate_projected_context(&context, limits)?;
        if context.current_input_index.is_some() {
            return Err(Error::Invalid(
                "context snapshot still marks a historical input as active".into(),
            ));
        }
        if crate::contract::canonical_json_bytes(&context)? != bytes {
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
    limits.validate()?;
    limits.validate_file(reference)?;
    if reference.descriptor().media_type() != "application/json"
        || reference.descriptor().byte_length() > limits.render_bytes
    {
        return Err(Error::Invalid(
            "context snapshot file exceeds JSON/render bounds".into(),
        ));
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
            // A new admission still requires the original bound reader's current
            // authority and retained content. Admitted model replay skips stages.
            self.reader.verify(&self.reference).await?;
            super::validate_context_refs(&self.context, self.reader.as_ref(), limits).await?;
            Ok(composed)
        })
    }
}
