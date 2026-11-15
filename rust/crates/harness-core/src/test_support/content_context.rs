//! A minimal agent turn context over a content reader, for access tests.
use crate::{
    Capabilities, Result,
    conversation::{ContentPublisher, VolumeClass, VolumeOwner, VolumeRef},
};
use std::sync::Arc;

/// A private writer the context's scope never grants.
struct Ungranted(VolumeRef);

impl ContentPublisher for Ungranted {
    fn volume(&self) -> &VolumeRef {
        &self.0
    }
}

/// An agent turn context over `reader` with exactly `capabilities`. Without
/// `writer`, its private writer targets a volume those capabilities never grant.
pub async fn content_context(
    reader: Arc<dyn crate::conversation::ContentResidencyVerifier>,
    writer: Option<Arc<dyn ContentPublisher>>,
    capabilities: Capabilities,
) -> Result<Arc<crate::runtime::TaskContext>> {
    use crate::runtime::{Bindings, ContentBindings, RuntimeScope};
    let writer = match writer {
        Some(writer) => writer,
        None => Arc::new(Ungranted(VolumeRef::new(
            crate::resources::ProviderRef::new(
                "ungranted".into(),
                "filesystem".into(),
                "2".into(),
                &mut None,
            )?,
            "private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(crate::AgentId::from_bytes([0xee; 16])),
            &mut None,
        )?)),
    };
    let content = ContentBindings {
        volume: writer.volume().clone(),
        reader,
        writer: Some(writer),
    };
    let harness = Bindings {
        scope: RuntimeScope::new(capabilities, crate::conversation::Limits::default())?,
        content: Some(content.clone()),
        ..Bindings::local()
    }
    .build()?;
    Ok(Arc::new(harness.agent_context(
        &Capabilities::new(Vec::<String>::new()),
        content,
    )?))
}
