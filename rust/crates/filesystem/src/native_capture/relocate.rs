//! Recognise relocations from the bounded observed path set, before births.
use super::*;

pub(super) async fn observed_paths<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    states: Vec<CapturePathState>,
    page_size: usize,
    configuration: &CaptureBatch<'_>,
) -> Result<(u64, Vec<CapturePathState>), CaptureError> {
    let Some(namespace) = configuration.source_namespace else {
        return Ok((0, states));
    };
    let root = configuration.source_root;
    let meter = configuration.meter;
    let mut removed = Vec::new();
    let mut linked = false;
    capture_charge(
        meter,
        WorkCounters {
            items_examined: states.len() as u64,
            ..WorkCounters::UNCHARGED
        },
    )?;
    for (observed, _, current) in &states {
        if let (None, Some(record)) = (observed, current) {
            capture_reserve(&mut removed, 1, meter)?;
            removed.push(record.file_id);
        } else if let (Some(metadata), None) = (observed, current) {
            linked |= !metadata.is_dir() && metadata.nlink() > 1;
        }
    }
    if removed.is_empty() && !linked {
        return Ok((0, states));
    }
    capture_sort_by(&mut removed, Ord::cmp, meter)?;
    let mut scope = capture_vec(states.len(), meter)?;
    for (observed, path, current) in &states {
        capture_charge(
            meter,
            WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            },
        )?;
        let directory_destination = match observed {
            Some(metadata) if metadata.is_dir() => match current {
                None => true,
                Some(record) => {
                    let host = namespace_to_host_path_measured(path, meter)?;
                    let id = source_capture_id(root, &host, metadata, namespace, meter)?;
                    capture_drop_host_path(host, meter)?;
                    id != record.file_id && removed_identity(&removed, id, meter)?
                }
            },
            _ => false,
        };
        if observed.is_none() && current.is_some() || directory_destination {
            scope.push(clone_capture_path(path, meter)?);
        }
    }
    // A moved directory can replace an existing destination. Include only
    // destinations proved by a removed source identity, alongside new parents
    // and selected removals; unrelated aliases remain outside this scope.
    let scope = canonicalize_capture_roots(scope, meter)?;
    let mut count = 0;
    let mut moved_directory = false;
    for (observed, destination, current) in &states {
        let Some(metadata) = observed else {
            continue;
        };
        // Prior relocations can replace this binding or create its ancestors.
        // Preconditions must refer to the current private candidate, not the
        // original lookup page, before deciding whether to create or remove.
        let current = if count == 0 {
            *current
        } else {
            let lookup = checkout
                .lookup_no_follow(destination, meter, meter.cancellation())
                .await?;
            lookup.record
        };
        let host = namespace_to_host_path_measured(destination, meter)?;
        let id = source_capture_id(root, &host, metadata, namespace, meter)?;
        capture_drop_host_path(host, meter)?;
        if current.is_some_and(|record| record.file_id == id) {
            continue;
        }
        let relocated = removed_identity(&removed, id, meter)?;
        if current.is_some() && !relocated && !moved_directory {
            continue;
        }
        if !relocated && !moved_directory && !metadata.is_dir() && metadata.nlink() <= 1 {
            continue;
        }
        let mut accounting = crate::kernel::KernelAccounting::native(meter, meter.cancellation());
        // The authenticated parent-edge index finds one alias, never a tree
        // scan. Observed removals, host hard links and the children of a moved
        // directory buy this lookup; ordinary births keep the bulk creation
        // preflight. A parent move updates every later child's lookup.
        let source = crate::workspace::NamespaceView {
            objects: checkout.objects(),
            config: checkout.volume_config(),
            root: checkout.root(),
        }
        .first_path(id, &mut accounting, meter.cancellation())
        .await?;
        let Some(source) = source else {
            if metadata.is_dir() && current.is_none() {
                // Depth order creates each genuinely new parent before any
                // descendant relocation. Reuse ordinary capture so its source
                // identity and metadata are selected at creation, atomically.
                let mut parent = CaptureBatch::new(
                    root,
                    configuration.baseline,
                    Some(namespace),
                    configuration.maximum_extent_spans,
                    meter,
                );
                parent
                    .prepare(
                        checkout,
                        (
                            Some(metadata.clone()),
                            clone_capture_path(destination, meter)?,
                            None,
                        ),
                        CaptureIntent::Complete,
                    )
                    .await?;
                apply_capture_transaction(checkout, parent.mutations, meter).await?;
                release_host_links(parent.host_links, meter)?;
                count = checked_increment(count)?;
            }
            continue;
        };
        if source.value == *destination {
            source.retire(&mut accounting)?;
            continue;
        }
        let mut path = clone_capture_path(&source.value, meter)?;
        source.retire(&mut accounting)?;
        let host = namespace_to_host_path_measured(&path, meter)?;
        let same_node = match observe_host_path(root, &host, meter)? {
            Some(observed) => source_capture_id(root, &host, &observed, namespace, meter)? == id,
            None => false,
        };
        capture_drop_host_path(host, meter)?;
        let mut mutations = capture_vec(1, meter)?;
        let destination = clone_capture_path(destination, meter)?;
        let rename = !same_node && capture_within_roots(&scope, &path, meter)?;
        if rename && current.is_some() && path.is_within(&destination) {
            // Removing the destination must not retire the very subtree being
            // promoted. Park it outside that subtree in the private candidate;
            // neither this binding nor an intermediate generation is published.
            let parked = vacant_sibling(checkout, &destination, &path, meter).await?;
            let mut park = capture_vec(1, meter)?;
            park.push(AuthoredMutation::Rename {
                source: path,
                destination: clone_capture_path(&parked, meter)?,
                replace: false,
            });
            apply_capture_transaction(checkout, park, meter).await?;
            path = parked;
        }
        // The host replaced this binding. Remove its former descendants in
        // the same private transaction before installing the retained object.
        if let Some(record) = current {
            if record.kind == FileKind::Directory {
                let mut receipt = CaptureReceipt::default();
                push_subtree_removals(checkout, &destination, &mut mutations, &mut receipt, meter)
                    .await?;
                count = count
                    .checked_add(receipt.changed_paths)
                    .ok_or(WorkError::Overflow)?;
            }
            capture_reserve(&mut mutations, 1, meter)?;
            mutations.push(AuthoredMutation::Remove {
                path: clone_capture_path(&destination, meter)?,
                expected_file_id: Some(record.file_id),
            });
        }
        let mutation = if !rename {
            // A canonical alias outside the observed set is not ours to remove,
            // even when that alias has disappeared on the host. Add the selected
            // binding and let ordinary capture remove only selected old aliases.
            if host_kind(metadata)? != FileKind::Regular || !checkout.volume_config().hard_links {
                return Err(crate::FsError::FileIdentityCollision.into());
            }
            AuthoredMutation::HardLink {
                source: path,
                destination,
            }
        } else {
            AuthoredMutation::Rename {
                source: path,
                destination,
                replace: true,
            }
        };
        capture_reserve(&mut mutations, 1, meter)?;
        mutations.push(mutation);
        // The outer capture owns a private candidate; later refusals discard
        // these bindings together with every byte and metadata change.
        apply_capture_transaction(checkout, mutations, meter).await?;
        moved_directory |= rename && metadata.is_dir();
        count = checked_increment(count)?;
    }
    release_capture_paths(scope, meter)?;
    let removed_bytes = vector_bytes(&removed);
    drop(removed);
    meter
        .clone()
        .release(removed_bytes)
        .map_err(capture_demand_error)?;
    if count == 0 {
        return Ok((0, states));
    }
    let mut observations = capture_vec(states.len(), meter)?;
    let mut paths = capture_vec(states.len(), meter)?;
    let state_bytes = vector_bytes(&states);
    for (observed, path, _) in states {
        observations.push(observed);
        paths.push(path);
    }
    meter
        .clone()
        .release(state_bytes)
        .map_err(capture_demand_error)?;
    Ok((
        count,
        lookup_capture_states(checkout, (observations, paths), page_size, meter).await?,
    ))
}

// Allocate a temporary sibling without reserving any user spelling or raising
// its path limits. Collisions spend the original work; the byte counter visits
// every name of this encoding before growing, without an attempt limit.
async fn vacant_sibling<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    destination: &NamespacePath,
    source: &NamespacePath,
    meter: &NativeMaterializationWork,
) -> Result<NamespacePath, CaptureError> {
    let limits = checkout.volume_config().limits;
    let (parents, _) = destination
        .split_last()
        .ok_or(CaptureError::InvalidOptions)?;
    let parent = clone_capture_prefix(destination, parents.len(), limits, meter)?;
    let encoding = source
        .components()
        .last()
        .ok_or(CaptureError::InvalidOptions)?
        .encoding();
    let width = if encoding == NameEncoding::WindowsUtf16Le {
        2
    } else {
        1
    };
    let mut counter = capture_vec(width, meter)?;
    counter.resize(width, 0_u8);
    loop {
        capture_charge(
            meter,
            WorkCounters {
                items_examined: counter.len() as u64,
                ..WorkCounters::UNCHARGED
            },
        )?;
        let mut carry = true;
        for byte in counter.iter_mut().rev() {
            *byte = byte.wrapping_add(1);
            if *byte != 0 {
                carry = false;
                break;
            }
        }
        if carry {
            let length = counter
                .len()
                .checked_add(width)
                .ok_or(WorkError::Overflow)?;
            capture_reserve(&mut counter, width, meter)?;
            counter.resize(length, 0);
        }
        let copied = WorkCounters {
            bytes_copied: counter.len() as u64,
            ..WorkCounters::UNCHARGED
        };
        crate::native_host::NativeHostWork::admit(&mut meter.clone(), copied)
            .map_err(capture_demand_error)?;
        let mut bytes = capture_vec(counter.len(), meter)?;
        bytes.extend_from_slice(&counter);
        crate::native_host::NativeHostWork::record(&mut meter.clone(), copied)
            .map_err(capture_demand_error)?;
        let allocated = bytes.capacity() as u64;
        let name = match LogicalName::new(encoding, bytes, limits.maximum_component_bytes) {
            Ok(name) => name,
            Err(crate::kernel::TreePageError::InvalidName) => {
                meter
                    .clone()
                    .release(allocated)
                    .map_err(capture_demand_error)?;
                continue;
            }
            Err(error) => return Err(error.into()),
        };
        let path = append_path_measured(&parent, name, limits, meter)?;
        let record = checkout
            .lookup_no_follow(&path, meter, meter.cancellation())
            .await?
            .record;
        if record.is_none() {
            capture_drop_path(parent, meter)?;
            let allocated = counter.capacity() as u64;
            drop(counter);
            meter
                .clone()
                .release(allocated)
                .map_err(capture_demand_error)?;
            return Ok(path);
        }
        capture_drop_path(path, meter)?;
    }
}

fn removed_identity(
    removed: &[crate::FileId],
    id: crate::FileId,
    meter: &NativeMaterializationWork,
) -> Result<bool, CaptureError> {
    let mut comparisons = 0;
    let found = removed
        .binary_search_by(|candidate| {
            comparisons += 1;
            candidate.cmp(&id)
        })
        .is_ok();
    capture_charge(
        meter,
        WorkCounters {
            items_examined: comparisons,
            ..WorkCounters::UNCHARGED
        },
    )?;
    Ok(found)
}
