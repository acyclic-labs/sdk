use super::*;

/// Returns the stable identity of a no-follow, capability-held capture root.
pub fn capture_root_identity(path: &Path) -> Result<NativeRootIdentity, CaptureError> {
    HostRoot::open(path, &mut crate::native_host::ordinary_work())
        .map(|root| root.identity())
        .map_err(|_| CaptureError::InvalidOptions)
}

/// Reads a bounded set of final host states and applies one atomic checkout transaction.
pub async fn capture_paths<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    paths: &[NamespacePath],
    options: &CaptureOptions,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<CaptureReceipt>, OperationFailure<CaptureError>> {
    let native = capture_work(budget, cancellation)?;
    let result = async {
        validate_path_count(paths, options).map_err(OperationFailure::before_work)?;
        let mut candidate = checkout
            .private_candidate(&native, cancellation)
            .map_err(|error| OperationFailure::new(error.into(), native.receipt()))?;
        let scope = CaptureWorkScope::new(&native)?;
        let result = async {
            let source_root = open_source_root(options, &scope)?;
            let mut unique = clone_capture_paths(paths, None, &scope)?;
            capture_sort_by(&mut unique, Ord::cmp, &scope)?;
            if capture_has_duplicate_paths(&unique, &scope)? {
                return Err(CaptureError::InvalidOptions);
            }
            let observed = observe_unique_paths(unique, &source_root, cancellation, &scope)?;
            capture_observed_paths_from_root(
                &mut candidate,
                observed,
                options.maximum_extent_spans,
                &source_root,
                cancellation,
                None,
                &scope,
            )
            .await
        }
        .await;
        let receipt = capture_scope_result(scope, result)?;
        cancellation
            .commit_if_live(|| *checkout = candidate)
            .map_err(|error| OperationFailure::new(error.into(), native.receipt()))?;
        Ok(receipt)
    }
    .await;
    capture_original_result(&native, result)
}

#[cfg(feature = "native-mount")]
pub(crate) async fn capture_paths_batched<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    paths: &[NamespacePath],
    options: &CaptureOptions,
    batch_size: usize,
    cancellation: &CancellationToken,
    baseline: Option<&NativeViewBaseline>,
    work: &crate::NativeMaterializationWork,
) -> Result<OperationReceipt<CaptureReceipt>, OperationFailure<CaptureError>> {
    let scope = CaptureWorkScope::new(work)?;
    let result = capture_paths_batched_in_scope(
        checkout,
        paths,
        options,
        batch_size,
        cancellation,
        baseline,
        &scope,
    )
    .await;
    capture_scope_result(scope, result)
}

#[cfg(feature = "native-mount")]
pub(super) async fn capture_paths_batched_in_scope<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    paths: &[NamespacePath],
    options: &CaptureOptions,
    batch_size: usize,
    cancellation: &CancellationToken,
    baseline: Option<&NativeViewBaseline>,
    meter: &CaptureWorkScope,
) -> Result<CaptureReceipt, CaptureError> {
    let cancellation = {
        validate_path_count(paths, options)?;
        if batch_size == 0 || paths.len() > MAX_NATIVE_EXACT_CAPTURE_PATHS {
            return Err(CaptureError::InvalidOptions);
        }
        cancellation.check()?;
        meter.native.clone().check().map_err(capture_demand_error)?;
        if paths.is_empty() {
            return Ok(CaptureReceipt::default());
        }
        meter
            .native
            .with_original_leaf(|work| work.cancellation().clone())
    };
    let source_root = open_source_root(options, meter)?;
    let (mut receipt, mut candidate, unique) = {
        let mut unique = clone_capture_paths(paths, None, meter)?;
        capture_sort_by(&mut unique, Ord::cmp, meter)?;
        if capture_has_duplicate_paths(&unique, meter)? {
            return Err(CaptureError::InvalidOptions);
        }
        let receipt = CaptureReceipt::default();
        let candidate = checkout.private_candidate(&meter.native, &cancellation)?;
        (receipt, candidate, unique)
    };
    let observed = observe_unique_paths(unique, &source_root, &cancellation, meter)?;
    let states = lookup_capture_states(&mut candidate, observed, batch_size, meter).await?;
    let mut host_links = Vec::new();
    let states_bytes = states.capacity() as u64 * std::mem::size_of::<CapturePathState>() as u64;
    let mut states = states.into_iter();
    loop {
        let batch = {
            let count = states.len().min(batch_size);
            let mut batch = capture_vec(count, meter)?;
            batch.extend(states.by_ref().take(count));
            if batch.is_empty() {
                break;
            }
            batch
        };
        let captured = capture_ranked_paths_from_root(
            &mut candidate,
            batch,
            &mut host_links,
            options.maximum_extent_spans,
            &source_root,
            &cancellation,
            baseline,
            meter,
        )
        .await?;
        receipt.examined_paths = receipt
            .examined_paths
            .checked_add(captured.examined_paths)
            .ok_or(CaptureError::from(WorkError::Overflow))?;
        receipt.changed_paths = receipt
            .changed_paths
            .checked_add(captured.changed_paths)
            .ok_or(CaptureError::from(WorkError::Overflow))?;
        receipt.staged_file_bytes = receipt
            .staged_file_bytes
            .checked_add(captured.staged_file_bytes)
            .ok_or(CaptureError::from(WorkError::Overflow))?;
    }
    drop(states);
    {
        {
            meter
                .native
                .clone()
                .release(states_bytes)
                .map_err(capture_demand_error)?;
            release_host_links(host_links, meter)
        }?;
        meter.native.clone().check().map_err(capture_demand_error)?;
    }
    cancellation.commit_if_live(|| {
        *checkout = candidate;
    })?;
    Ok(receipt)
}

/// Reconciles one host directory and every descendant as one authored transaction.
pub async fn capture_subtree<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    root: NamespacePath,
    options: &CaptureOptions,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<CaptureReceipt>, OperationFailure<CaptureError>> {
    let native = capture_work(budget, cancellation)?;
    let mut candidate = checkout
        .private_candidate(&native, cancellation)
        .map_err(|error| OperationFailure::new(error.into(), native.receipt()))?;
    let result = capture_subtrees_with_native_work(
        &mut candidate,
        &[root],
        options,
        &CapturePolicy::default(),
        cancellation,
        None,
        &native,
    )
    .await;
    let receipt = capture_original_result(&native, result)?;
    cancellation
        .commit_if_live(|| *checkout = candidate)
        .map_err(|error| OperationFailure::new(error.into(), native.receipt()))?;
    Ok(receipt)
}

pub(crate) async fn capture_subtrees_with_native_work<
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
>(
    checkout: &mut Checkout<A, O>,
    roots: &[NamespacePath],
    options: &CaptureOptions,
    policy: &CapturePolicy,
    cancellation: &CancellationToken,
    baseline: Option<&NativeViewBaseline>,
    work: &crate::NativeMaterializationWork,
) -> Result<OperationReceipt<CaptureReceipt>, OperationFailure<CaptureError>> {
    let scope = CaptureWorkScope::new(work)?;
    let result = capture_subtrees_in_scope(
        checkout,
        roots,
        options,
        policy,
        cancellation,
        baseline,
        &scope,
    )
    .await;
    capture_scope_result(scope, result)
}

pub(super) async fn capture_subtrees_in_scope<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    roots: &[NamespacePath],
    options: &CaptureOptions,
    policy: &CapturePolicy,
    cancellation: &CancellationToken,
    baseline: Option<&NativeViewBaseline>,
    meter: &CaptureWorkScope,
) -> Result<CaptureReceipt, CaptureError> {
    let cancellation = {
        if options.maximum_paths == 0 || options.maximum_extent_spans == 0 {
            return Err(CaptureError::InvalidOptions);
        }
        cancellation.check()?;
        meter.native.clone().check().map_err(capture_demand_error)?;
        if roots.is_empty() {
            return Ok(CaptureReceipt::default());
        }
        meter
            .native
            .with_original_leaf(|work| work.cancellation().clone())
    };
    let limits = checkout.volume_config().limits;
    let profile = checkout.volume_config().profile;
    let (canonical, mut paths, host_roots, host_policy, source_path, source_path_bytes, maximum) = {
        let canonical = canonical_subtree_roots_measured(roots, policy, meter)?;
        if canonical.is_empty() {
            return Ok(CaptureReceipt::default());
        }
        let maximum =
            usize::try_from(options.maximum_paths).map_err(|_| CaptureError::InvalidOptions)?;
        let mut paths = CaptureMap::new();
        for path in &canonical {
            let path = clone_capture_path(path, meter)?;
            append_scanned_path(&mut paths, path, maximum, meter)?;
        }
        let host_roots = clone_capture_paths(&canonical, None, meter)?;
        let host_policy = CapturePolicy {
            excluded_prefixes: clone_capture_paths(&policy.excluded_prefixes, None, meter)?,
        };
        let (source_path, source_path_bytes) =
            crate::native_host::clone_path(&options.source_root, &mut meter.native.clone())
                .map_err(capture_demand_error)?;
        (
            canonical,
            paths,
            host_roots,
            host_policy,
            source_path,
            source_path_bytes,
            maximum,
        )
    };
    let expected_identity = options.expected_root_identity;
    let worker = meter.native.clone();
    let capture = std::sync::Arc::clone(meter.custody.as_ref().expect("capture scope is live"));
    let task_meter = meter.native.runtime_meter();
    let task = acyclic_native_runtime::run_blocking_io(
        move || {
            let worker = CaptureWorkScope {
                native: worker,
                custody: None,
            };
            let result = (|| {
                let source_root = HostRoot::open(&source_path, &mut worker.native.clone())
                    .map_err(capture_demand_error)?;
                if source_root.identity() != expected_identity {
                    return Err(CaptureError::RootChanged);
                }
                let mut observed = CaptureMap::new();
                collect_host_subtree_roots(
                    &source_root,
                    profile,
                    limits,
                    maximum,
                    &host_roots,
                    &host_policy,
                    &mut observed,
                    &worker,
                )?;
                Ok((source_root, observed))
            })();
            let cleanup = (|| {
                release_capture_paths(host_roots, &worker)?;
                release_capture_paths(host_policy.excluded_prefixes, &worker)?;
                drop(source_path);
                worker
                    .native
                    .clone()
                    .release(source_path_bytes)
                    .map_err(capture_demand_error)
            })();
            let result = result.and_then(|value| cleanup.map(|()| value));
            // Completion storage drops the payload before its accounting token.
            (result, capture)
        },
        Some((task_meter, false)),
    );
    let result = match task {
        Ok(task) => task.await.map_err(native_demand_error),
        Err(error) => Err(native_demand_error(error)),
    };
    let (result, _capture) = result.map_err(capture_demand_error)?;
    let (source_root, observed) = result?;
    let lookup_cancellation = meter
        .native
        .with_original_leaf(|work| work.cancellation().clone());
    let current = checkout
        .lookup_batch_no_follow(&canonical, &meter.native, &lookup_cancellation)
        .await?;

    let canonical_bytes = canonical.capacity() as u64 * std::mem::size_of::<NamespacePath>() as u64;
    for (root, entry) in canonical.into_iter().zip(current.0.entries) {
        if entry
            .record
            .is_some_and(|record| record.kind == FileKind::Directory)
        {
            collect_checkout_subtree_paths(
                checkout,
                limits,
                root,
                policy,
                maximum,
                &mut paths,
                &cancellation,
                meter,
            )
            .await?;
        } else {
            capture_drop_path(root, meter)?;
        }
    }
    drop(current.1);
    let ordered = {
        meter
            .native
            .clone()
            .release(canonical_bytes)
            .map_err(capture_demand_error)?;
        order_host_checkout_union(observed, paths, maximum, meter)?
    };
    capture_observed_paths_from_root(
        checkout,
        ordered,
        options.maximum_extent_spans,
        &source_root,
        &cancellation,
        baseline,
        meter,
    )
    .await
}

/// Captures a complete eligible baseline into one atomic candidate.
pub async fn capture_baseline<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    options: &CaptureOptions,
    policy: &CapturePolicy,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<CaptureReceipt>, OperationFailure<CaptureError>> {
    let native = capture_work(budget, cancellation)?;
    let work = &native;
    let meter = &work.state;
    let completed = { meter.lock().work };
    if checkout.has_pending_mutations() {
        return Err(OperationFailure::new(
            CaptureError::DirtyCheckout,
            completed,
        ));
    }
    if options.maximum_paths == 0 || options.maximum_extent_spans == 0 {
        return Err(OperationFailure::new(
            CaptureError::InvalidOptions,
            completed,
        ));
    }
    cancellation
        .check()
        .map_err(|error| OperationFailure::new(error.into(), completed))?;
    let (checked, completed) = {
        let held = meter.lock();
        (held.check(), held.work)
    };
    checked.map_err(|error| OperationFailure::new(capture_demand_error(error), completed))?;
    let mut candidate = checkout
        .private_candidate(work, cancellation)
        .map_err(|error| OperationFailure::new(CaptureError::from(error), work.receipt()))?;
    let scope = CaptureWorkScope::new(work)?;
    let meter = &scope;
    let result = (async {
        let maximum =
            usize::try_from(options.maximum_paths).map_err(|_| CaptureError::InvalidOptions)?;
        let source_root = open_source_root(options, meter)?;
        let limits = checkout.volume_config().limits;
        let profile = checkout.volume_config().profile;
        let mut paths = CaptureMap::new();

        let mut observed = CaptureMap::new();
        let root = NamespacePath::new(Vec::new(), limits).map_err(CaptureError::from)?;
        collect_host_walk_measured(
            &source_root,
            profile,
            limits,
            CaptureHostPath::new(),
            root,
            policy,
            meter,
            |path, stat, meter| insert_host_observation(&mut observed, path, stat, maximum, meter),
        )?;
        let root = NamespacePath::new(Vec::new(), limits).map_err(CaptureError::from)?;
        collect_checkout_subtree_paths(
            &mut candidate,
            limits,
            root,
            policy,
            maximum,
            &mut paths,
            cancellation,
            meter,
        )
        .await?;
        let ordered = order_host_checkout_union(observed, paths, maximum, meter)?;
        capture_observed_paths_from_root(
            &mut candidate,
            ordered,
            options.maximum_extent_spans,
            &source_root,
            cancellation,
            None,
            meter,
        )
        .await
    })
    .await;
    let receipt = capture_scope_result(scope, result)?;
    cancellation
        .commit_if_live(|| *checkout = candidate)
        .map_err(|error| OperationFailure::new(error.into(), work.receipt()))?;
    Ok(receipt)
}

/// Replays one watcher batch with the caller's full original native resource.
pub async fn capture_watch_batch<
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    W: crate::facade::FsWork,
>(
    checkout: &mut Checkout<A, O>,
    batch: WatchBatch,
    options: &CaptureOptions,
    policy: &CapturePolicy,
    budget: W,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<WatchCaptureReceipt>, OperationFailure<CaptureError>> {
    let accounting = budget.accounting(cancellation);
    let native = match accounting.native_work() {
        Some(original) => original.clone(),
        None => capture_work(
            accounting
                .remaining()
                .map_err(|error| OperationFailure::new(error.into(), accounting.spent()))?,
            cancellation,
        )?,
    };
    let mut candidate = checkout
        .private_candidate(&native, cancellation)
        .map_err(|error| OperationFailure::new(error.into(), native.receipt()))?;
    let scope = CaptureWorkScope::new(&native)?;
    let result =
        capture_watch_batch_in_scope(&mut candidate, batch, options, policy, cancellation, &scope)
            .await;
    let receipt = capture_scope_result(scope, result)?;
    cancellation
        .commit_if_live(|| *checkout = candidate)
        .map_err(|error| OperationFailure::new(error.into(), native.receipt()))?;
    Ok(receipt)
}
