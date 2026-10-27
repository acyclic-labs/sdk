//! Agent turn contexts for unit tests that need no real content storage.
use super::*;
use crate::conversation::{VolumeClass, VolumeOwner};

/// A publisher that only names its volume `0`.
pub(crate) struct PrivateVolume(pub(crate) VolumeRef);

impl ContentPublisher for PrivateVolume {
    fn volume(&self) -> &VolumeRef {
        &self.0
    }
}

/// A reader that verifies nothing.
pub(crate) struct NoReads;
impl ContentResidencyVerifier for NoReads {}

/// The private volume of the test agent with id bytes `[agent; 16]`.
pub(crate) fn private_volume(agent: u8) -> Result<VolumeRef> {
    VolumeRef::new(
        crate::resources::ProviderRef::new(
            "test".into(),
            "filesystem".into(),
            "2".into(),
            &mut None,
        )?,
        "private".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(crate::AgentId::from_bytes([agent; 16])),
        &mut None,
    )
}

/// An agent turn context under exactly `scope`, with `artifacts` as the
/// harness's artifact storage. Its private writer targets a volume `scope`
/// does not grant, so it has no usable private writer of its own.
pub fn agent_context(
    scope: RuntimeScope,
    artifacts: Option<ContentBindings>,
) -> Result<TaskContext> {
    Bindings {
        scope,
        artifacts,
        ..Bindings::local()
    }
    .build()?
    .agent_context(
        &Capabilities::new(Vec::<String>::new()),
        ContentBindings {
            reader: Arc::new(NoReads),
            writer: Some(Arc::new(PrivateVolume(private_volume(0xee)?))),
        },
    )
}
