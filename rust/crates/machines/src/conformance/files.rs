//! The same binary workspace cases run against range and whole-file providers.
use crate::*;

/// Verifies binary chunks, offset overwrite, sparse extension and truncation on
/// one exclusively owned guest path. The fixture retains cleanup ownership.
pub async fn verify_files(
    provider: &dyn MachinesProvider,
    machine: MachineId,
    path: &str,
    deadline_unix_ms: u64,
) -> Result<(), String> {
    let access = |offset| FileAccess::new(path.into(), offset, deadline_unix_ms);
    let range = provider.capability(ProviderCapability::RangeWrite);
    if range == Support::Unsupported {
        super::require_unsupported(
            provider
                .write_file(
                    machine,
                    access(0).map_err(|e| e.to_string())?,
                    vec![1],
                    true,
                )
                .await,
        )?;
        if provider.capability(ProviderCapability::WholeFile) != Support::Supported {
            return Err("file conformance requires range or whole-file transfer".into());
        }
    } else {
        super::require_unsupported(
            provider
                .upload_file(machine, access(0).map_err(|e| e.to_string())?, vec![1])
                .await,
        )?;
    }
    let mut expected = (0..65_553)
        .map(|value| (value % 256) as u8)
        .collect::<Vec<_>>();
    let mut offset = 0;
    while offset < expected.len() {
        let end = (offset + MAX_OUTPUT_READ_BYTES as usize).min(expected.len());
        let bytes = expected
            .get(offset..end)
            .ok_or("fixture chunk missing")?
            .to_vec();
        let written = write_file_at(
            provider,
            machine,
            access(offset as u64).map_err(|e| e.to_string())?,
            bytes,
            offset == 0,
        )
        .await
        .map_err(|e| e.to_string())?;
        let FileWrite::Written(count) = written else {
            return Err("binary write returned errno".into());
        };
        if count == 0 || count as usize > end - offset {
            return Err("write made invalid progress".into());
        }
        offset += count as usize;
    }
    write_exact(
        provider,
        machine,
        access(1).map_err(|e| e.to_string())?,
        &[255, 0, 128],
        false,
    )
    .await?;
    expected
        .get_mut(1..4)
        .ok_or("fixture range missing")?
        .copy_from_slice(&[255, 0, 128]);
    write_exact(
        provider,
        machine,
        access(65_560).map_err(|e| e.to_string())?,
        &[0, 254],
        false,
    )
    .await?;
    expected.resize(65_560, 0);
    expected.extend([0, 254]);
    check_bytes(provider, machine, path, deadline_unix_ms, &expected).await?;
    // A zero-byte write at a distant offset must not extend an existing file.
    write_exact(
        provider,
        machine,
        access(99_999).map_err(|e| e.to_string())?,
        &[],
        false,
    )
    .await?;
    check_bytes(provider, machine, path, deadline_unix_ms, &expected).await?;
    write_exact(
        provider,
        machine,
        access(0).map_err(|e| e.to_string())?,
        &[0, 255],
        true,
    )
    .await?;
    check_bytes(provider, machine, path, deadline_unix_ms, &[0, 255]).await?;
    if write_file_at(
        provider,
        machine,
        access(1).map_err(|e| e.to_string())?,
        vec![1],
        true,
    )
    .await
    .is_ok()
    {
        return Err("nonzero-offset truncation was admitted".into());
    }
    check_bytes(provider, machine, path, deadline_unix_ms, &[0, 255]).await
}

async fn write_exact(
    provider: &dyn MachinesProvider,
    machine: MachineId,
    access: FileAccess,
    bytes: &[u8],
    truncate: bool,
) -> Result<(), String> {
    let mut offset = 0;
    loop {
        let next = FileAccess::new(
            access.path().into(),
            access.offset() + offset as u64,
            access.deadline_unix_ms(),
        )
        .map_err(|e| e.to_string())?;
        let remaining = bytes.get(offset..).ok_or("fixture write offset missing")?;
        let result = write_file_at(
            provider,
            machine,
            next,
            remaining.to_vec(),
            truncate && offset == 0,
        )
        .await
        .map_err(|e| e.to_string())?;
        let FileWrite::Written(count) = result else {
            return Err("file write returned errno".into());
        };
        if count as usize > remaining.len() || (count == 0 && !remaining.is_empty()) {
            return Err("file write made invalid progress".into());
        }
        offset += count as usize;
        if offset == bytes.len() {
            return Ok(());
        }
    }
}

async fn check_bytes(
    provider: &dyn MachinesProvider,
    machine: MachineId,
    path: &str,
    deadline: u64,
    expected: &[u8],
) -> Result<(), String> {
    let mut actual = Vec::new();
    loop {
        let access = FileAccess::new(path.into(), actual.len() as u64, deadline)
            .map_err(|e| e.to_string())?;
        let FileRead::Data(chunk) = read_file_at(provider, machine, access, MAX_OUTPUT_READ_BYTES)
            .await
            .map_err(|e| e.to_string())?
        else {
            return Err("file read returned errno".into());
        };
        if chunk.is_empty() {
            break;
        }
        if chunk.len() > MAX_OUTPUT_READ_BYTES as usize {
            return Err("file read exceeded credit".into());
        }
        actual.extend(chunk);
        if actual.len() > expected.len() {
            return Err("file length differs".into());
        }
    }
    if actual != expected {
        return Err("binary workspace bytes differ".into());
    }
    Ok(())
}
