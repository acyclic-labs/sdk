use super::*;

/// Shared workspace seam for native range writes and whole-file replacement.
/// Whole-file providers require exclusive file ownership across this read/modify/write;
/// they do not acquire native pwrite atomicity. Never retries a dispatched upload.
pub async fn write_file_at(
    provider: &dyn MachinesProvider,
    machine: MachineId,
    access: FileAccess,
    data: Vec<u8>,
    truncate: bool,
) -> Result<FileWrite, ProviderError> {
    access.validate_range(data.len())?;
    if truncate && access.offset() != 0 {
        return Err(ProviderError::Invalid(
            "truncation requires offset zero".into(),
        ));
    }
    if provider.capability(ProviderCapability::RangeWrite) == Support::Supported {
        return provider.write_file(machine, access, data, truncate).await;
    }
    if provider.capability(ProviderCapability::WholeFile) != Support::Supported {
        return unsupported("guest file writes");
    }
    let whole = FileAccess::new(access.path().into(), 0, access.deadline_unix_ms())?;
    let mut bytes = if truncate {
        Vec::new()
    } else {
        match provider.download_file(machine, whole.clone()).await {
            Ok(download) => match download.collect().await? {
                FileRead::Data(bytes) => bytes,
                FileRead::Errno(errno) => return Ok(FileWrite::Errno(errno)),
            },
            Err(ProviderError::NotFound(_)) => Vec::new(),
            Err(error) => return Err(error),
        }
    };
    let offset = usize::try_from(access.offset())
        .map_err(|_| ProviderError::Invalid("guest offset exceeds address space".into()))?;
    let end = offset
        .checked_add(data.len())
        .ok_or_else(|| ProviderError::Invalid("guest range overflows address space".into()))?;
    if !data.is_empty() {
        if end > bytes.len() {
            bytes.try_reserve(end - bytes.len()).map_err(|_| ProviderError::Rejected("whole-file write exceeds available memory; recovery: use a range-capable provider".into()))?;
            bytes.resize(end, 0);
        }
        let target = bytes
            .get_mut(offset..end)
            .ok_or_else(|| ProviderError::Invalid("invalid guest range".into()))?;
        target.copy_from_slice(&data);
    }
    let written = u32::try_from(data.len())
        .map_err(|_| ProviderError::Invalid("invalid guest chunk".into()))?;
    provider.upload_file(machine, whole, bytes).await?;
    Ok(FileWrite::Written(written))
}

/// Shared range-read seam. Whole-file streams discard the prefix with bounded
/// memory and cancel the unread suffix once credit is filled. They do not
/// claim a provider-native seek or native errno guarantee.
pub async fn read_file_at(
    provider: &dyn MachinesProvider,
    machine: MachineId,
    access: FileAccess,
    limit: u32,
) -> Result<FileRead, ProviderError> {
    if limit == 0 {
        return Err(ProviderError::Invalid("empty file read credit".into()));
    }
    access.validate_range(limit as usize)?;
    if provider.capability(ProviderCapability::RangeRead) == Support::Supported {
        return provider.read_file(machine, access, limit).await;
    }
    if provider.capability(ProviderCapability::WholeFile) != Support::Supported {
        return unsupported("guest file reads");
    }
    let mut offset = access.offset();
    let whole = FileAccess::new(access.path().into(), 0, access.deadline_unix_ms())?;
    match provider.download_file(machine, whole).await? {
        FileDownload::Data(mut stream) => {
            let mut bytes = Vec::with_capacity(limit as usize);
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                if offset >= chunk.len() as u64 {
                    offset -= chunk.len() as u64;
                    continue;
                }
                let start = offset as usize;
                offset = 0;
                let count = (limit as usize - bytes.len()).min(chunk.len() - start);
                bytes.extend_from_slice(&chunk[start..start + count]);
                if bytes.len() == limit as usize {
                    break;
                }
            }
            Ok(FileRead::Data(bytes))
        }
        FileDownload::Errno(errno) => Ok(FileRead::Errno(errno)),
    }
}
