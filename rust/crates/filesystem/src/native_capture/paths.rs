use super::*;

pub(super) fn clone_capture_name(
    name: &LogicalName,
    meter: &CaptureWorkScope,
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
    crate::native_host::NativeHostWork::admit(&mut meter.native.clone(), copied)
        .map_err(capture_demand_error)?;
    let mut bytes = capture_vec(name.as_bytes().len(), meter)?;
    bytes.extend_from_slice(name.as_bytes());
    meter
        .native
        .clone()
        .record(copied)
        .map_err(capture_demand_error)?;
    LogicalName::new(name.encoding(), bytes, u32::MAX).map_err(CaptureError::from)
}

pub(super) fn clone_capture_prefix(
    path: &NamespacePath,
    depth: usize,
    limits: crate::model::VolumeLimits,
    meter: &CaptureWorkScope,
) -> Result<NamespacePath, CaptureError> {
    let names = path.components().get(..depth).ok_or(CaptureError::Kernel(
        crate::kernel::NamespacePathError::TooDeep.into(),
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
    meter: &CaptureWorkScope,
) -> Result<bool, CaptureError> {
    for prefix in &policy.excluded_prefixes {
        capture_charge(
            meter,
            WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            },
        )?;
        for _ in prefix.components().iter().zip(path.components()) {
            capture_charge(
                meter,
                WorkCounters {
                    source_path_components: 1,
                    ..WorkCounters::UNCHARGED
                },
            )?;
        }
        if path.is_within(prefix) {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(super) fn clone_capture_paths(
    paths: &[NamespacePath],
    policy: Option<&CapturePolicy>,
    meter: &CaptureWorkScope,
) -> Result<Vec<NamespacePath>, CaptureError> {
    let mut result = capture_vec(paths.len(), meter)?;
    for path in paths {
        capture_charge(
            meter,
            WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            },
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
    meter: &CaptureWorkScope,
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
    meter
        .native
        .clone()
        .release(bytes)
        .map_err(capture_demand_error)
}

pub(super) fn capture_has_duplicate_paths(
    paths: &[NamespacePath],
    meter: &CaptureWorkScope,
) -> Result<bool, CaptureError> {
    for pair in paths.windows(2) {
        capture_charge(
            meter,
            WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            },
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
    meter: &CaptureWorkScope,
) -> Result<Vec<NamespacePath>, CaptureError> {
    let mut sorted = clone_capture_paths(roots, Some(policy), meter)?;
    capture_sort_by(&mut sorted, Ord::cmp, meter)?;
    for root in &sorted {
        capture_charge(
            meter,
            WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            },
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
    let mut failure = None;
    sorted.dedup_by(|root, ancestor| {
        if failure.is_some() || !root.is_within(ancestor) {
            return false;
        }
        let removed = std::mem::replace(
            root,
            NamespacePath::new(Vec::new(), crate::model::VolumeLimits::default())
                .expect("an empty namespace path is valid"),
        );
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
    meter: &CaptureWorkScope,
) -> Result<NamespacePath, CaptureError> {
    use crate::kernel::{NamespacePathStep, ReadAllocationStep};
    let cancellation = meter
        .native
        .with_original_leaf(|work| work.cancellation().clone());
    path.clone_with_steps(&cancellation, &mut |step| -> Result<(), CaptureError> {
        match step {
            NamespacePathStep::Admit(delta) => {
                let mut delta = *delta;
                delta.source_bytes_read = delta.bytes_copied;
                crate::native_host::NativeHostWork::admit(&mut meter.native.clone(), delta)
                    .map_err(capture_demand_error)
            }
            NamespacePathStep::Performed(delta) => {
                let mut delta = *delta;
                delta.source_bytes_read = delta.bytes_copied;
                meter
                    .native
                    .clone()
                    .record(delta)
                    .map_err(capture_demand_error)
            }
            NamespacePathStep::Charge(delta) => capture_charge(meter, *delta),
            NamespacePathStep::Allocation(ReadAllocationStep::Allocate { bytes, operations }) => {
                meter
                    .native
                    .with_original_leaf(|work| {
                        work.claim_allocation(bytes, operations, crate::WorkCounters::UNCHARGED)
                    })
                    .map_err(capture_demand_error)
            }
            NamespacePathStep::Allocation(ReadAllocationStep::Capacity {
                admitted_bytes,
                actual_bytes,
            }) => {
                let mut allocation = admitted_bytes;
                meter
                    .native
                    .with_original_leaf(|work| {
                        work.reconcile_native_allocation(&mut allocation, actual_bytes)
                    })
                    .map_err(capture_demand_error)
            }
            NamespacePathStep::Allocation(ReadAllocationStep::ReserveFailed(error)) => {
                Err(CaptureError::from(error))
            }
            NamespacePathStep::Allocation(ReadAllocationStep::BeforeCopy(bytes)) => {
                crate::native_host::NativeHostWork::admit(
                    &mut meter.native.clone(),
                    WorkCounters {
                        bytes_copied: bytes,
                        source_bytes_read: bytes,
                        ..WorkCounters::UNCHARGED
                    },
                )
                .map_err(capture_demand_error)
            }
            NamespacePathStep::Allocation(ReadAllocationStep::Copied(bytes)) => meter
                .native
                .clone()
                .record(WorkCounters {
                    bytes_copied: bytes,
                    source_bytes_read: bytes,
                    ..WorkCounters::UNCHARGED
                })
                .map_err(capture_demand_error),
            NamespacePathStep::Allocation(ReadAllocationStep::BeforeHash(bytes)) => {
                meter.native.clone().check().map_err(capture_demand_error)?;
                crate::native_host::NativeHostWork::admit(
                    &mut meter.native.clone(),
                    WorkCounters {
                        bytes_hashed: bytes,
                        ..WorkCounters::UNCHARGED
                    },
                )
                .map_err(capture_demand_error)
            }
            NamespacePathStep::Allocation(ReadAllocationStep::Hashed(bytes)) => {
                meter
                    .native
                    .clone()
                    .record(WorkCounters {
                        bytes_hashed: bytes,
                        ..WorkCounters::UNCHARGED
                    })
                    .map_err(capture_demand_error)?;
                meter.native.clone().check().map_err(capture_demand_error)
            }
            NamespacePathStep::Allocation(ReadAllocationStep::BeforeItem) => {
                crate::native_host::NativeHostWork::admit(
                    &mut meter.native.clone(),
                    WorkCounters {
                        items_examined: 1,
                        ..WorkCounters::UNCHARGED
                    },
                )
                .map_err(capture_demand_error)
            }
            NamespacePathStep::Allocation(ReadAllocationStep::Item) => capture_charge(
                meter,
                WorkCounters {
                    items_examined: 1,
                    ..WorkCounters::UNCHARGED
                },
            ),
        }
    })
}

pub(super) fn append_path_measured(
    parent: &NamespacePath,
    name: LogicalName,
    limits: crate::model::VolumeLimits,
    meter: &CaptureWorkScope,
) -> Result<NamespacePath, CaptureError> {
    meter.native.clone().check().map_err(capture_demand_error)?;
    NamespacePath::validate_component_lengths(
        parent
            .components()
            .iter()
            .chain(std::iter::once(&name))
            .map(|name| name.as_bytes().len()),
        limits,
    )
    .map_err(CaptureError::from)?;
    let depth = parent
        .depth()
        .checked_add(1)
        .ok_or(CaptureError::from(WorkError::Overflow))?;
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
    meter: &CaptureWorkScope,
) -> Result<(), CaptureError> {
    let bytes = path.retained_bytes();
    drop(path);
    meter
        .native
        .clone()
        .release(bytes)
        .map_err(capture_demand_error)
}
