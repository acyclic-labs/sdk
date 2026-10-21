use super::*;

pub(super) fn clone_capture_name(
    name: &LogicalName,
    meter: &NativeMaterializationWork,
) -> Result<LogicalName, CaptureError> {
    capture_charge(
        meter,
        WorkCounters {
            source_path_components: 1,
            items_examined: 1,
            ..WorkCounters::UNCHARGED
        },
    )?;
    let copied = WorkCounters {
        source_bytes_read: name.as_bytes().len() as u64,
        bytes_copied: name.as_bytes().len() as u64,
        ..WorkCounters::UNCHARGED
    };
    crate::native_host::NativeHostWork::admit(&mut meter.clone(), copied)
        .map_err(capture_demand_error)?;
    let mut bytes = capture_vec(name.as_bytes().len(), meter)?;
    bytes.extend_from_slice(name.as_bytes());
    meter.clone().record(copied).map_err(capture_demand_error)?;
    LogicalName::new(name.encoding(), bytes, u32::MAX).map_err(CaptureError::from)
}

pub(super) fn clone_capture_prefix(
    path: &NamespacePath,
    depth: usize,
    limits: crate::model::VolumeLimits,
    meter: &NativeMaterializationWork,
) -> Result<NamespacePath, CaptureError> {
    let names = path.components().get(..depth).ok_or(CaptureError::from(
        crate::kernel::NamespacePathError::TooDeep,
    ))?;
    NamespacePath::validate_component_lengths(
        names.iter().map(|name| name.as_bytes().len()),
        limits,
    )
    .map_err(CaptureError::from)?;
    let mut components = capture_vec(depth, meter)?;
    for name in names {
        components.push(clone_capture_name(name, meter)?);
    }
    NamespacePath::new(components, limits).map_err(CaptureError::from)
}

pub(super) fn capture_excludes(
    policy: &CapturePolicy,
    path: &NamespacePath,
    meter: &NativeMaterializationWork,
) -> Result<bool, CaptureError> {
    for prefix in &policy.excluded_prefixes {
        capture_charge(
            meter,
            WorkCounters {
                items_examined: 1,
                source_path_components: prefix.depth().min(path.depth()) as u64,
                ..WorkCounters::UNCHARGED
            },
        )?;
        if path.is_within(prefix) {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(super) fn clone_capture_paths(
    paths: &[NamespacePath],
    policy: Option<&CapturePolicy>,
    meter: &NativeMaterializationWork,
) -> Result<Vec<NamespacePath>, CaptureError> {
    let mut result = capture_vec(paths.len(), meter)?;
    for path in paths {
        capture_charge(
            meter,
            crate::materializer::NativeResourceCredit::Items.delta(1),
        )?;
        if let Some(policy) = policy
            && capture_excludes(policy, path, meter)?
        {
            continue;
        }
        result.push(clone_capture_path(path, meter)?);
    }
    Ok(result)
}

pub(super) fn release_capture_paths(
    paths: Vec<NamespacePath>,
    meter: &NativeMaterializationWork,
) -> Result<(), CaptureError> {
    let bytes = paths.iter().try_fold(
        paths.capacity() as u64 * std::mem::size_of::<NamespacePath>() as u64,
        |bytes, path| {
            bytes
                .checked_add(path.retained_bytes())
                .ok_or(CaptureError::from(WorkError::Overflow))
        },
    )?;
    drop(paths);
    meter.clone().release(bytes).map_err(capture_demand_error)
}

pub(super) fn capture_has_duplicate_paths(
    paths: &[NamespacePath],
    meter: &NativeMaterializationWork,
) -> Result<bool, CaptureError> {
    for pair in paths.windows(2) {
        capture_charge(
            meter,
            crate::materializer::NativeResourceCredit::Items.delta(1),
        )?;
        if matches!(pair, [left, right] if left == right) {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(super) fn canonical_subtree_roots_measured(
    roots: &[NamespacePath],
    policy: &CapturePolicy,
    meter: &NativeMaterializationWork,
) -> Result<Vec<NamespacePath>, CaptureError> {
    let mut sorted = clone_capture_paths(roots, Some(policy), meter)?;
    capture_sort_by(&mut sorted, Ord::cmp, meter)?;
    for root in &sorted {
        capture_charge(
            meter,
            crate::materializer::NativeResourceCredit::Items.delta(1),
        )?;
        for _ in root.components() {
            capture_charge(
                meter,
                WorkCounters {
                    source_path_components: 1,
                    ..WorkCounters::UNCHARGED
                },
            )?;
        }
    }
    let empty = NamespacePath::new(Vec::new(), crate::model::VolumeLimits::default())?;
    let mut failure = None;
    sorted.dedup_by(|root, ancestor| {
        if failure.is_some() || !root.is_within(ancestor) {
            return false;
        }
        let removed = std::mem::replace(root, empty.clone());
        failure = capture_drop_path(removed, meter).err();
        true
    });
    if let Some(error) = failure {
        return Err(error);
    }
    Ok(sorted)
}

pub(super) fn clone_capture_path(
    path: &NamespacePath,
    meter: &NativeMaterializationWork,
) -> Result<NamespacePath, CaptureError> {
    meter.clone().check().map_err(capture_demand_error)?;
    // A full prefix preserves the already validated path's exact bounds.
    let limits = crate::model::VolumeLimits {
        maximum_path_bytes: path.encoded_bytes(),
        maximum_component_bytes: path.encoded_bytes(),
        maximum_path_depth: path.depth().try_into().map_err(|_| WorkError::Overflow)?,
        ..crate::model::VolumeLimits::default()
    };
    clone_capture_prefix(path, path.depth(), limits, meter)
}

pub(super) fn append_path_measured(
    parent: &NamespacePath,
    name: LogicalName,
    limits: crate::model::VolumeLimits,
    meter: &NativeMaterializationWork,
) -> Result<NamespacePath, CaptureError> {
    #[cfg(test)]
    #[cfg(feature = "memory")]
    if super::path_error_tests::restore_old_append() {
        return super::path_error_tests::old_append(parent, name, limits, meter);
    }
    meter.clone().check().map_err(capture_demand_error)?;
    NamespacePath::validate_component_lengths(
        parent
            .components()
            .iter()
            .chain(std::iter::once(&name))
            .map(|name| name.as_bytes().len()),
        limits,
    )
    .map_err(CaptureError::from)?;
    let depth = parent.depth().checked_add(1).ok_or(WorkError::Overflow)?;
    capture_charge(
        meter,
        WorkCounters {
            source_path_components: 1,
            items_examined: 1,
            ..WorkCounters::UNCHARGED
        },
    )?;
    let mut components = capture_vec(depth, meter)?;
    for component in parent.components() {
        components.push(clone_capture_name(component, meter)?);
    }
    components.push(name);
    NamespacePath::new(components, limits).map_err(CaptureError::from)
}

pub(super) fn capture_drop_path(
    path: NamespacePath,
    meter: &NativeMaterializationWork,
) -> Result<(), CaptureError> {
    let bytes = path.retained_bytes();
    drop(path);
    meter.clone().release(bytes).map_err(capture_demand_error)
}
