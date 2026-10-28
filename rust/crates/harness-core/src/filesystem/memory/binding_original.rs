use super::*;

pub(super) async fn content(
    key: String,
    agent: AgentId,
    maximum_file_bytes: u64,
    filesystem: Fs<MemoryAuthorityBackend, MemoryObjectBackend>,
) -> Result<(
    Arc<FilesystemHost<MemoryAuthorityBackend, MemoryObjectBackend>>,
    VolumeRef,
)> {
    if maximum_file_bytes == 0 {
        return Err(Error::Invalid(
            "memory Harness file limit must be positive".into(),
        ));
    }
    let provider = ProviderRef::new("local".into(), "filesystem".into(), "2".into(), &mut None)?;
    let host: Arc<FilesystemHost<MemoryAuthorityBackend, MemoryObjectBackend>> =
        FilesystemHost::new(filesystem, &provider, None)?;
    let volume = VolumeRef::new(
        provider,
        key,
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )?;
    host.create_volume(&volume).await?;
    Ok((host, volume))
}
