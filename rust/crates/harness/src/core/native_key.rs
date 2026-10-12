//! Identities of live reducer allocations.
use super::*;
#[derive(Debug, PartialEq)]
pub(super) enum NativeComponentKey<'a> {
    Base,
    Authority,
    Event(u64),
    EventBuffer,
    TerminalEffectBuffer,
    EffectAction(EffectId),
    EffectStatus(EffectId),
    EffectAttempts(EffectId),
    ExtensionState(Cow<'a, str>),
    ExtensionConfiguration(Cow<'a, ExtensionDependency>),
    ActiveSelection,
    Fork(Cow<'a, Authority>),
    Merge(Cow<'a, str>, Cow<'a, [u8]>),
    ConversationMessage(u64),
    ContextCheckpoint,
    InteractionTicket(uuid::Uuid),
    InteractionResolution(uuid::Uuid),
}
impl NativeComponentKey<'_> {
    pub(in crate::core) fn copy(
        &self,
        quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
    ) -> Result<NativeComponentKey<'static>> {
        use NativeComponentKey as K;
        Ok(match self {
            Self::Base => K::Base,
            Self::Authority => K::Authority,
            Self::Event(v) => K::Event(*v),
            Self::EventBuffer => K::EventBuffer,
            Self::TerminalEffectBuffer => K::TerminalEffectBuffer,
            Self::EffectAction(v) => K::EffectAction(*v),
            Self::EffectStatus(v) => K::EffectStatus(*v),
            Self::EffectAttempts(v) => K::EffectAttempts(*v),
            Self::ExtensionState(v) => K::ExtensionState(Cow::Owned(copy_text(v, quota)?)),
            Self::ExtensionConfiguration(v) => {
                K::ExtensionConfiguration(Cow::Owned(copy_dependency(v, quota)?))
            }
            Self::ActiveSelection => K::ActiveSelection,
            Self::Fork(v) => K::Fork(Cow::Owned(copy_authority(v, quota)?)),
            Self::Merge(a, b) => K::Merge(
                Cow::Owned(copy_text(a, quota)?),
                Cow::Owned(copy_copy_vector(b, quota)?),
            ),
            Self::ConversationMessage(v) => K::ConversationMessage(*v),
            Self::ContextCheckpoint => K::ContextCheckpoint,
            Self::InteractionTicket(v) => K::InteractionTicket(*v),
            Self::InteractionResolution(v) => K::InteractionResolution(*v),
        })
    }
}
