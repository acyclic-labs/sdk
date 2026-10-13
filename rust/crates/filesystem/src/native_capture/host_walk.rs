use super::*;

pub(super) fn collect_host_walk_measured(
    root: &HostRoot,
    profile: FilesystemProfile,
    limits: crate::model::VolumeLimits,
    volume_root: NamespacePath,
    policy: &CapturePolicy,
    meter: &NativeMaterializationWork,
    mut insert: impl FnMut(
        NamespacePath,
        HostStat,
        &NativeMaterializationWork,
    ) -> Result<(), CaptureError>,
) -> Result<(), CaptureError> {
    let mut pending = capture_vec(1, meter)?;
    pending.push(volume_root);
    while let Some(parent) = pending.pop() {
        let host_path = namespace_to_host_path_measured(&parent, meter)?;
        let (mut reader, allocation) = root
            .read_dir_stats(&host_path, &mut meter.clone())
            .map_err(capture_demand_error)?;
        capture_drop_host_path(host_path, meter)?;
        while let Some((entry, name_allocation)) = reader
            .next(&mut meter.clone())
            .map_err(capture_demand_error)?
        {
            let (encoding, bytes) = meter
                .with_original_leaf(|work| {
                    #[cfg(unix)]
                    let name = crate::native_name::host_name_bytes_with_source_work(
                        &entry.name,
                        profile,
                        limits.maximum_component_bytes,
                        work,
                    );
                    #[cfg(windows)]
                    let name = crate::native_name::host_wide_name_bytes_with_source_work(
                        &entry.name,
                        profile,
                        limits.maximum_component_bytes,
                        work,
                    );
                    name.map(|(name, _allocation)| name)
                })
                .map_err(|error| match error {
                    DemandError::InvalidRequest => CaptureError::UnrepresentablePath,
                    error => capture_demand_error(error),
                })?;
            drop(entry.name);
            meter
                .clone()
                .release(name_allocation)
                .map_err(capture_demand_error)?;
            let name = LogicalName::new(encoding, bytes, limits.maximum_component_bytes)?;
            let child = append_path_measured(&parent, name, limits, meter)?;
            if capture_excludes(policy, &child, meter)? {
                capture_drop_path(child, meter)?;
                continue;
            }
            let stat = match entry.stat {
                Some(stat) => stat,
                None => {
                    let host_path = namespace_to_host_path_measured(&child, meter)?;
                    let stat = root
                        .stat(&host_path, &mut meter.clone())
                        .map_err(capture_demand_error)?;
                    capture_drop_host_path(host_path, meter)?;
                    stat
                }
            };
            let recurse = stat.file_type() == crate::demand::SourceNodeKind::Directory;
            insert(clone_capture_path(&child, meter)?, stat, meter)?;
            if recurse {
                capture_reserve(&mut pending, 1, meter)?;
                pending.push(child);
            } else {
                capture_drop_path(child, meter)?;
            }
        }
        drop(reader);
        meter
            .clone()
            .release(allocation)
            .map_err(capture_demand_error)?;
        capture_drop_path(parent, meter)?;
    }
    release_capture_paths(pending, meter)
}

pub(super) fn observe_unique_paths(
    paths: Vec<NamespacePath>,
    source_root: &HostRoot,
    meter: &NativeMaterializationWork,
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
    meter.clone().check().map_err(capture_demand_error)?;
    Ok((observations, paths))
}

pub(super) fn observe_host_path(
    source_root: &HostRoot,
    host_path: &CaptureHostPath,
    meter: &NativeMaterializationWork,
) -> Result<Option<HostStat>, CaptureError> {
    classify_host_observation(source_root.stat(host_path, &mut meter.clone()))
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
    observed: &mut Vec<(NamespacePath, Option<HostStat>)>,
    path: NamespacePath,
    metadata: HostStat,
    maximum: usize,
    meter: &NativeMaterializationWork,
) -> Result<(), CaptureError> {
    if capture_position(observed, &path, meter)?.is_err() && observed.len() >= maximum {
        return Err(CaptureError::InvalidOptions);
    }
    capture_charge(
        meter,
        WorkCounters {
            items_examined: 1,
            ..WorkCounters::UNCHARGED
        },
    )?;
    capture_replace(observed, path, Some(metadata), meter)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn collect_host_subtree_roots(
    source_root: &HostRoot,
    profile: FilesystemProfile,
    limits: crate::model::VolumeLimits,
    maximum: usize,
    roots: &[NamespacePath],
    policy: &CapturePolicy,
    observed: &mut Vec<(NamespacePath, Option<HostStat>)>,
    meter: &NativeMaterializationWork,
) -> Result<(), CaptureError> {
    for root in roots {
        let host_root = namespace_to_host_path_measured(root, meter)?;
        let observation = observe_host_path(source_root, &host_root, meter)?;
        capture_drop_host_path(host_root, meter)?;
        if let Some(observation) = observation {
            let recurse = observation.file_type() == crate::demand::SourceNodeKind::Directory;
            insert_host_observation(
                observed,
                clone_capture_path(root, meter)?,
                observation,
                maximum,
                meter,
            )?;
            if recurse {
                collect_host_walk_measured(
                    source_root,
                    profile,
                    limits,
                    clone_capture_path(root, meter)?,
                    policy,
                    meter,
                    |path, stat, meter| {
                        insert_host_observation(observed, path, stat, maximum, meter)
                    },
                )?;
            }
        }
    }
    Ok(())
}
