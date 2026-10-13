use super::*;

pub(super) fn capture_stat(
    root: &HostRoot,
    path: &CaptureHostPath,
    meter: &CaptureWorkScope,
) -> Result<HostStat, DemandError> {
    #[cfg(unix)]
    {
        root.stat(path, &mut meter.native.clone())
    }
    #[cfg(windows)]
    {
        root.stat_wide(path, &mut meter.native.clone())
    }
}

pub(super) fn collect_host_walk_measured(
    root: &HostRoot,
    profile: FilesystemProfile,
    limits: crate::model::VolumeLimits,
    host_root: CaptureHostPath,
    volume_root: NamespacePath,
    policy: &CapturePolicy,
    meter: &CaptureWorkScope,
    mut insert: impl FnMut(NamespacePath, HostStat, &CaptureWorkScope) -> Result<(), CaptureError>,
) -> Result<(), CaptureError> {
    let slot = std::mem::size_of::<(CaptureHostPath, NamespacePath)>() as u64;
    let mut pending = capture_vec(1, meter)?;
    pending.push((host_root, volume_root));
    while let Some((host_parent, volume_parent)) = pending.pop() {
        meter.native.clone().check().map_err(capture_demand_error)?;
        #[cfg(unix)]
        let (mut reader, reader_allocation) = root
            .read_dir_stats(&host_parent, &mut meter.native.clone())
            .map_err(capture_demand_error)?;
        #[cfg(windows)]
        let (mut reader, reader_allocation) = root
            .read_dir_stats_wide(&host_parent, &mut meter.native.clone())
            .map_err(capture_demand_error)?;
        loop {
            #[cfg(unix)]
            let entry = reader
                .next(&mut meter.native.clone())
                .map_err(capture_demand_error)?;
            #[cfg(windows)]
            let entry = reader
                .next(&mut meter.native.clone())
                .map_err(capture_demand_error)?;
            let Some((entry, name_allocation)) = entry else {
                break;
            };
            let name = (|| {
                #[cfg(unix)]
                let name = {
                    let raw = entry.name.as_encoded_bytes();
                    capture_charge(
                        meter,
                        WorkCounters {
                            items_examined: raw.len() as u64,
                            ..WorkCounters::UNCHARGED
                        },
                    )?;
                    let (encoding, bytes) = if profile == FilesystemProfile::Windows {
                        let text = std::str::from_utf8(raw)
                            .map_err(|_| CaptureError::UnrepresentablePath)?;
                        let length = text
                            .encode_utf16()
                            .count()
                            .checked_mul(2)
                            .ok_or(CaptureError::from(WorkError::Overflow))?;
                        let mut bytes = capture_vec(length, meter)?;
                        for unit in text.encode_utf16() {
                            capture_charge(
                                meter,
                                WorkCounters {
                                    bytes_copied: 2,
                                    ..WorkCounters::UNCHARGED
                                },
                            )?;
                            bytes.extend_from_slice(&unit.to_le_bytes());
                        }
                        (NameEncoding::WindowsUtf16Le, bytes)
                    } else {
                        let mut bytes = capture_vec(raw.len(), meter)?;
                        capture_charge(
                            meter,
                            WorkCounters {
                                bytes_copied: raw.len() as u64,
                                ..WorkCounters::UNCHARGED
                            },
                        )?;
                        bytes.extend_from_slice(raw);
                        (NameEncoding::PosixBytes, bytes)
                    };
                    LogicalName::new(encoding, bytes, limits.maximum_component_bytes)
                        .map_err(CaptureError::from)?
                };
                #[cfg(windows)]
                let name = {
                    let (encoding, bytes) = if profile == FilesystemProfile::Windows {
                        let mut bytes = capture_vec(
                            entry
                                .name
                                .len()
                                .checked_mul(2)
                                .ok_or(CaptureError::from(WorkError::Overflow))?,
                            meter,
                        )?;
                        for unit in &entry.name {
                            capture_charge(
                                meter,
                                WorkCounters {
                                    bytes_copied: 2,
                                    ..WorkCounters::UNCHARGED
                                },
                            )?;
                            bytes.extend_from_slice(&unit.to_le_bytes());
                        }
                        (NameEncoding::WindowsUtf16Le, bytes)
                    } else {
                        let mut length = 0usize;
                        for character in char::decode_utf16(entry.name.iter().copied()) {
                            meter.native.clone().check().map_err(capture_demand_error)?;
                            length = length
                                .checked_add(
                                    character
                                        .map_err(|_| CaptureError::UnrepresentablePath)?
                                        .len_utf8(),
                                )
                                .ok_or(CaptureError::from(WorkError::Overflow))?;
                        }
                        let mut bytes = capture_vec(length, meter)?;
                        for character in char::decode_utf16(entry.name.iter().copied()) {
                            let character =
                                character.map_err(|_| CaptureError::UnrepresentablePath)?;
                            capture_charge(
                                meter,
                                WorkCounters {
                                    bytes_copied: character.len_utf8() as u64,
                                    ..WorkCounters::UNCHARGED
                                },
                            )?;
                            let mut buffer = [0; 4];
                            bytes.extend_from_slice(character.encode_utf8(&mut buffer).as_bytes());
                        }
                        (NameEncoding::PosixBytes, bytes)
                    };
                    LogicalName::new(encoding, bytes, limits.maximum_component_bytes)
                        .map_err(CaptureError::from)?
                };
                Ok::<_, CaptureError>(name)
            })()?;
            drop(entry.name);
            meter
                .native
                .clone()
                .release(name_allocation)
                .map_err(capture_demand_error)?;
            let child = append_path_measured(&volume_parent, name, limits, meter)?;
            if capture_excludes(policy, &child, meter)? {
                capture_drop_path(child, meter)?;
                continue;
            }
            let host_child = namespace_to_host_path_measured(&child, meter)?;
            let stat = match entry.stat {
                Some(stat) => stat,
                None => capture_stat(root, &host_child, meter).map_err(capture_demand_error)?,
            };
            let recurse = stat.is_dir() && !stat.file_type().is_symlink();
            let observed_path = clone_capture_path(&child, meter)?;
            insert(observed_path, stat, meter)?;
            if recurse {
                capture_reserve(&mut pending, 1, meter)?;
                pending.push((host_child, child));
            } else {
                capture_drop_host_path(host_child, meter)?;
                capture_drop_path(child, meter)?;
            }
        }
        drop(reader);
        meter
            .native
            .clone()
            .release(reader_allocation)
            .map_err(capture_demand_error)?;
        capture_drop_host_path(host_parent, meter)?;
        capture_drop_path(volume_parent, meter)?;
    }
    let pending_bytes = pending.capacity() as u64 * slot;
    drop(pending);
    meter
        .native
        .clone()
        .release(pending_bytes)
        .map_err(capture_demand_error)?;
    Ok(())
}

pub(super) fn observe_unique_paths(
    paths: Vec<NamespacePath>,
    source_root: &HostRoot,
    cancellation: &CancellationToken,
    meter: &CaptureWorkScope,
) -> Result<(Vec<Option<HostStat>>, Vec<NamespacePath>), CaptureError> {
    let mut observations = capture_vec(paths.len(), meter)?;
    for path in &paths {
        capture_charge(
            meter,
            WorkCounters {
                source_entries_visited: 1,
                ..WorkCounters::UNCHARGED
            },
        )?;
        let host_path = namespace_to_host_path_measured(path, meter)?;
        observations.push(observe_host_path(source_root, &host_path, meter)?);
        capture_drop_host_path(host_path, meter)?;
    }
    cancellation.check()?;
    Ok((observations, paths))
}

pub(super) fn observe_host_path(
    source_root: &HostRoot,
    host_path: &CaptureHostPath,
    meter: &CaptureWorkScope,
) -> Result<Option<HostStat>, CaptureError> {
    classify_host_observation(capture_stat(source_root, host_path, meter))
}

pub(super) fn classify_host_observation(
    metadata: Result<HostStat, DemandError>,
) -> Result<Option<HostStat>, CaptureError> {
    match metadata {
        Ok(metadata) => Ok(Some(metadata)),
        Err(DemandError::Io(error)) if host_path_is_absent(&error) => Ok(None),
        Err(DemandError::Absent | DemandError::NotDirectory) => Ok(None),
        Err(error) => Err(capture_demand_error(error)),
    }
}

pub(super) fn insert_host_observation(
    observed: &mut CaptureMap<Option<HostStat>>,
    path: NamespacePath,
    metadata: HostStat,
    maximum: usize,
    meter: &CaptureWorkScope,
) -> Result<(), CaptureError> {
    if !observed.contains_key_measured(&path, meter)? && observed.len() >= maximum {
        return Err(CaptureError::InvalidOptions);
    }
    capture_charge(
        meter,
        WorkCounters {
            items_examined: 1,
            ..WorkCounters::UNCHARGED
        },
    )?;
    observed.insert_measured(path, Some(metadata), meter)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn collect_host_subtree_roots(
    source_root: &HostRoot,
    profile: FilesystemProfile,
    limits: crate::model::VolumeLimits,
    maximum: usize,
    roots: &[NamespacePath],
    policy: &CapturePolicy,
    observed: &mut CaptureMap<Option<HostStat>>,
    meter: &CaptureWorkScope,
) -> Result<(), CaptureError> {
    for root in roots {
        let host_root = namespace_to_host_path_measured(root, meter)?;
        if let Some(observation) = observe_host_path(source_root, &host_root, meter)? {
            let recurse = observation.is_dir() && !observation.file_type().is_symlink();
            let path = clone_capture_path(root, meter)?;
            insert_host_observation(observed, path, observation, maximum, meter)?;
            if recurse {
                let path = clone_capture_path(root, meter)?;
                collect_host_walk_measured(
                    source_root,
                    profile,
                    limits,
                    host_root,
                    path,
                    policy,
                    meter,
                    |path, stat, meter| {
                        insert_host_observation(observed, path, stat, maximum, meter)
                    },
                )?;
            } else {
                capture_drop_host_path(host_root, meter)?;
            }
        } else {
            capture_drop_host_path(host_root, meter)?;
        }
    }
    Ok(())
}
