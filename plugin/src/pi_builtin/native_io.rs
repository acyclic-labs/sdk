use super::*;

pub(super) fn require_builtin_mapping_access(
    mapping: &acyclic_harness::filesystem::NativeVolumeMapping,
    io: &PiBuiltinIo,
) -> Result<()> {
    if !matches!(io, PiBuiltinIo::Read { .. }) && !mapping.writable {
        return Err(denied(
            "builtin native volume is not an original writable destination",
        ));
    }
    Ok(())
}

pub(super) async fn accepted_builtin_io(
    context: &acyclic_harness::runtime::TaskContext,
    owner: &Owner,
    view: &crate::pi_native_host::PiNativeLiveView,
    request: &PiBuiltinRequest,
    binding: &crate::PiTaskBinding,
    work: &NativeMaterializationWork,
) -> Result<(serde_json::Value, NativeAllocation)> {
    use acyclic_harness::conversation::VolumeOperation;
    authorize_owner(context, owner, request.caller, work).await?;
    let manifest = view.manifest();
    validate_declared_working_roles(binding, manifest)?;
    let (mapping, leaf) = select_native_path(&request.path, binding, manifest)?;
    require_builtin_mapping_access(mapping, &request.io)?;
    let variants = match &request.io {
        PiBuiltinIo::Read { variants, .. } => variants.as_slice(),
        _ => &[],
    };
    let leaves = scoped_variants(variants, &mapping.path)?;
    let volume_role = if mapping.volume == binding.scratch.volume {
        "SCRATCH"
    } else {
        "PROJECT"
    };
    let response = match &request.io {
        PiBuiltinIo::Read { maximum_bytes, .. } | PiBuiltinIo::EditRead { maximum_bytes } => {
            let mut last_absence = None;
            for (leaf, candidate) in std::iter::once(leaf)
                .chain(leaves.into_iter().flatten())
                .zip(std::iter::once(&request.path).chain(variants.iter()))
            {
                let operation = if matches!(request.io, PiBuiltinIo::EditRead { .. }) {
                    VolumeOperation::Write
                } else {
                    VolumeOperation::Read
                };
                let volume = view
                    .working_volume(owner, &mapping.volume, operation, work)
                    .await?;
                match volume
                    .file_io(leaf, false, false, work, |file, work| {
                        read_file(file, *maximum_bytes as usize, work)
                    })
                    .await
                {
                    Ok((bytes, _bytes_custody)) => {
                        view.working_volume(owner, &mapping.volume, VolumeOperation::Read, work)
                            .await?;
                        authorize_owner(context, owner, request.caller, work).await?;
                        let (value, custody) =
                            read_response(bytes.as_slice(), candidate, volume_role, work)?;
                        return Ok((value, custody));
                    }
                    Err(error) => {
                        let error = Error::from(error);
                        if is_absent(&error) {
                            last_absence = Some(error);
                        } else {
                            return Err(error);
                        }
                    }
                }
            }
            return Err(last_absence.expect("the requested candidate is always checked"));
        }
        PiBuiltinIo::Write { content } | PiBuiltinIo::EditWrite { content, .. } => {
            let expected = match &request.io {
                PiBuiltinIo::EditWrite {
                    expected_sha256, ..
                } => Some(expected_digest(expected_sha256)?),
                _ => None,
            };
            let volume = view
                .working_volume(owner, &mapping.volume, VolumeOperation::Write, work)
                .await?;
            volume
                .file_io(
                    leaf,
                    true,
                    matches!(request.io, PiBuiltinIo::Write { .. }),
                    work,
                    |file, work| write_file(file, content.as_bytes(), expected, work),
                )
                .await?;
            view.working_volume(owner, &mapping.volume, VolumeOperation::Write, work)
                .await?;
            authorize_owner(context, owner, request.caller, work).await?;
            #[derive(Serialize)]
            struct Response<'a> {
                written: bool,
                path: &'a str,
                volume_role: &'a str,
            }
            crate::pi_root_provider::guarded_pi_value(
                &Response {
                    written: true,
                    path: &request.path,
                    volume_role,
                },
                8192,
                work,
            )?
        }
    };
    Ok(response)
}

fn read_file(
    file: &mut std::fs::File,
    maximum: usize,
    work: &NativeMaterializationWork,
) -> std::result::Result<(Vec<u8>, NativeAllocation), acyclic_fs::NativeTreeMaterializationError> {
    use acyclic_fs::NativeTreeMaterializationError as E;
    use std::io::Read as _;
    work.charge(WorkCounters {
        backend_read_operations: 1,
        ..WorkCounters::UNCHARGED
    })?;
    let before = file.metadata()?;
    let length = usize::try_from(before.len()).map_err(|_| acyclic_fs::WorkError::Overflow)?;
    if !before.is_file() || length > maximum {
        return Err(E::PayloadTooLarge {
            maximum: maximum as u64,
            actual: before.len(),
        });
    }
    let mut custody = work.claim(
        length as u64,
        u64::from(length != 0),
        WorkCounters::UNCHARGED,
    )?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length)
        .map_err(E::AllocationFailed)?;
    custody.reconcile_capacity(bytes.capacity() as u64)?;
    work.charge(WorkCounters {
        bytes_copied: length as u64,
        ..WorkCounters::UNCHARGED
    })?;
    bytes.resize(length, 0);
    let mut offset = 0;
    while offset != length {
        let count = acyclic_fs::native_host::file_transfer(
            acyclic_native_runtime::NativeOperationKind::Read,
            length - offset,
            work,
            || file.read(&mut bytes[offset..]),
        )?;
        if count == 0 {
            return Err(E::ExternalMutation);
        }
        offset += count;
    }
    work.charge(WorkCounters {
        backend_read_operations: 1,
        ..WorkCounters::UNCHARGED
    })?;
    let after = file.metadata()?;
    if before.len() != after.len() || before.modified()? != after.modified()? {
        return Err(E::ExternalMutation);
    }
    Ok((bytes, custody))
}

fn write_file(
    file: &mut std::fs::File,
    content: &[u8],
    expected: Option<[u8; 32]>,
    work: &NativeMaterializationWork,
) -> std::result::Result<(), acyclic_fs::NativeTreeMaterializationError> {
    use sha2::{Digest as _, Sha256};
    use std::io::{Seek as _, Write as _};
    if let Some(expected) = expected {
        let (before, _custody) = read_file(file, MAXIMUM_BUILTIN_BYTES as usize, work)?;
        work.charge(WorkCounters {
            bytes_hashed: before.len() as u64,
            ..WorkCounters::UNCHARGED
        })?;
        if <[u8; 32]>::from(Sha256::digest(&before)) != expected {
            return Err(acyclic_fs::NativeTreeMaterializationError::PreimageConflict);
        }
        file.rewind()?;
    }
    let mut offset = 0;
    while offset != content.len() {
        let count = acyclic_fs::native_host::file_transfer(
            acyclic_native_runtime::NativeOperationKind::Write,
            content.len() - offset,
            work,
            || file.write(&content[offset..]),
        )?;
        if count == 0 {
            return Err(std::io::Error::from(std::io::ErrorKind::WriteZero).into());
        }
        offset += count;
    }
    work.charge(WorkCounters {
        backend_write_operations: 1,
        ..WorkCounters::UNCHARGED
    })?;
    file.set_len(content.len() as u64)?;
    Ok(())
}
