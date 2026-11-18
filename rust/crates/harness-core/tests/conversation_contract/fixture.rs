use super::{
    AgentId, FileDescriptor, FileRef, ProviderRef, Result, VolumeClass, VolumeOwner, VolumeRef,
};

pub(crate) fn file(agent: AgentId, path: &str) -> Result<FileRef> {
    FileRef::new(
        VolumeRef::new(
            ProviderRef::new("acyclic".into(), "filesystem".into(), "2".into(), &mut None)?,
            "scratch".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
            &mut None,
        )?,
        (path).into(),
        ("version-1").into(),
        FileDescriptor::from_bytes(b"hello", "text/plain", &mut None)?,
        ("text.txt").into(),
        &mut None,
    )
}
