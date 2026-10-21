use super::*;

pub(super) async fn content(
    key: String,
    agent: AgentId,
    maximum_file_bytes: u64,
    filesystem: Fs<MemoryAuthorityBackend, MemoryObjectBackend>,
) -> Result<(
    Arc<MemoryHost>,
    VolumeRef,
    Arc<std::sync::Mutex<crate::memory_store::MemoryStore>>,
)> {
    if maximum_file_bytes == 0 {
        return Err(Error::Invalid(
            "memory Harness file limit must be positive".into(),
        ));
    }
    let provider = ProviderRef::new("local".into(), "filesystem".into(), "2".into(), &mut None)?;
    let host: Arc<MemoryHost> = FilesystemHost::new(filesystem, &provider, None)?;
    let volume = VolumeRef::new(
        provider,
        key,
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )?;
    let memory_store = Arc::new(std::sync::Mutex::new(
        crate::memory_store::MemoryStore::new(
            volume.clone(),
            maximum_file_bytes,
            crate::conversation::MAX_PATH_BYTES as u64,
            u64::MAX,
            u64::MAX,
        )?,
    ));
    host.create_volume(&volume).await?;
    Ok((host, volume, memory_store))
}
