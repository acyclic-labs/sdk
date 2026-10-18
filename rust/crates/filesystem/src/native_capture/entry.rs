use super::*;

/// Returns the stable identity of a no-follow, capability-held capture root.
pub fn capture_root_identity(path: &Path) -> Result<NativeRootIdentity, CaptureError> {
    HostRoot::open(path, &mut crate::native_host::ordinary_work())
        .map(|root| root.identity())
        .map_err(|_| CaptureError::InvalidOptions)
}

/// A root held open from its capture through its binding, so the root a
/// caller validated is the root it binds, whatever replaces the path in
/// between.
pub struct HeldRoot {
    pub(crate) root: HostRoot,
    pub(crate) path: PathBuf,
}

impl HeldRoot {
    /// The held root's device and object id, read when it was opened.
    #[must_use]
    pub const fn identity(&self) -> NativeRootIdentity {
        self.root.identity()
    }

    /// The held root's identity and generation, from one generation query of
    /// the held root. A failed query keeps its cause; a host without stable
    /// generation data is `Unsupported`.
    pub fn incarnation(&self) -> Result<crate::NativeRootIncarnation, DemandError> {
        self.root
            .incarnation(&mut crate::native_host::ordinary_work())
    }

    /// The path the root was opened at.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl std::fmt::Debug for HeldRoot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HeldRoot")
            .field("path", &self.path)
            .field("identity", &self.identity())
            .finish_non_exhaustive()
    }
}

/// Opens a no-follow root once and holds it. Every query failure keeps its
/// cause.
pub fn hold_root(path: &Path) -> Result<HeldRoot, DemandError> {
    Ok(HeldRoot {
        root: HostRoot::open(path, &mut crate::native_host::ordinary_work())?,
        path: path.to_path_buf(),
    })
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
    validate_path_count(paths, options)
        .map_err(|error| OperationFailure::new(error, native.receipt()))?;
    let mut candidate = checkout
        .private_candidate(
            &mut crate::kernel::allocation::KernelAccounting::native(&native, cancellation),
            cancellation,
        )
        .map_err(|error| OperationFailure::new(error.into(), native.receipt()))?;
    let scope = CaptureWorkScope::new(&native)?;
    let result = async {
        let source_root = open_source_root(options, &scope.native)?;
        let mut unique = clone_capture_paths(paths, None, &scope.native)?;
        capture_sort_by(&mut unique, Ord::cmp, &scope.native)?;
        if capture_has_duplicate_paths(&unique, &scope.native)? {
            return Err(CaptureError::InvalidOptions);
        }
        let observed = observe_unique_paths(unique, &source_root, &scope.native)?;
        capture_observed_paths_from_root(
            &mut candidate,
            observed,
            usize::MAX,
            options.maximum_extent_spans,
            &source_root,
            None,
            &scope.native,
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
    let checked = (|| {
        validate_path_count(paths, options)?;
        if batch_size == 0 || paths.len() > MAX_NATIVE_EXACT_CAPTURE_PATHS {
            return Err(CaptureError::InvalidOptions);
        }
        cancellation.check()?;
        work.clone().check().map_err(capture_demand_error)
    })();
    checked.map_err(|error| OperationFailure::new(error, work.receipt()))?;
    if paths.is_empty() {
        return Ok(OperationReceipt {
            value: CaptureReceipt::default(),
            work: work.receipt(),
        });
    }
    let scope = CaptureWorkScope::new(work)?;
    let meter = &scope.native;
    let result = async {
        let cancellation = meter.cancellation();
        let source_root = open_source_root(options, meter)?;
        let mut unique = clone_capture_paths(paths, None, meter)?;
        capture_sort_by(&mut unique, Ord::cmp, meter)?;
        if capture_has_duplicate_paths(&unique, meter)? {
            return Err(CaptureError::InvalidOptions);
        }
        let mut candidate = checkout.private_candidate(
            &mut crate::kernel::allocation::KernelAccounting::native(meter, cancellation),
            cancellation,
        )?;
        let observed = observe_unique_paths(unique, &source_root, meter)?;
        let receipt = capture_observed_paths_from_root(
            &mut candidate,
            observed,
            batch_size,
            options.maximum_extent_spans,
            &source_root,
            baseline,
            meter,
        )
        .await?;
        Ok((candidate, receipt))
    }
    .await;
    let receipt = capture_scope_result(scope, result)?;
    let (candidate, value) = receipt.value;
    cancellation
        .commit_if_live(|| *checkout = candidate)
        .map_err(|error| OperationFailure::new(error.into(), work.receipt()))?;
    Ok(OperationReceipt {
        value,
        work: receipt.work,
    })
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
        .private_candidate(
            &mut crate::kernel::allocation::KernelAccounting::native(&native, cancellation),
            cancellation,
        )
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
    let receipt = result?;
    cancellation
        .commit_if_live(|| *checkout = candidate)
        .map_err(|error| OperationFailure::new(error.into(), native.receipt()))?;
    Ok(receipt)
}

#[allow(
    clippy::too_many_lines,
    reason = "The funded capture frame retains scan inputs through worker completion and candidate commit."
)]
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
    let meter = &scope.native;
    let result = async {
        {
            if options.maximum_paths == 0 || options.maximum_extent_spans == 0 {
                return Err(CaptureError::InvalidOptions);
            }
            cancellation.check()?;
            meter.clone().check().map_err(capture_demand_error)?;
            if roots.is_empty() {
                return Ok(CaptureReceipt::default());
            }
        }
        let limits = checkout.volume_config().limits;
        let profile = checkout.volume_config().profile;
        let (canonical, mut paths, host_policy, source_path, source_path_bytes, maximum) = {
            let canonical = canonical_subtree_roots_measured(roots, policy, meter)?;
            if canonical.is_empty() {
                return Ok(CaptureReceipt::default());
            }
            let maximum =
                usize::try_from(options.maximum_paths).map_err(|_| CaptureError::InvalidOptions)?;
            let mut paths = Vec::new();
            for path in &canonical {
                let path = clone_capture_path(path, meter)?;
                append_scanned_path(&mut paths, path, maximum, meter)?;
            }
            let host_policy = CapturePolicy {
                excluded_prefixes: clone_capture_paths(&policy.excluded_prefixes, None, meter)?,
            };
            let (source_path, source_path_bytes) =
                crate::native_host::clone_path(&options.source_root, &mut meter.clone())
                    .map_err(capture_demand_error)?;
            (
                canonical,
                paths,
                host_policy,
                source_path,
                source_path_bytes,
                maximum,
            )
        };
        let expected_identity = options.expected_root_identity;
        let worker = meter.clone();
        let capture = meter
            .capture_origin
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .ok_or(crate::NativeTreeMaterializationError::OriginalWorkPoisoned)?;
        let task_meter = meter.runtime_meter();
        let task = acyclic_native_runtime::run_blocking_io(
            move || {
                let result = (|| {
                    let mut observed = Vec::new();
                    let source_root = scan_host_subtree_roots(
                        (&source_path, expected_identity),
                        (profile, limits),
                        maximum,
                        &canonical,
                        &host_policy,
                        &mut observed,
                        &worker,
                    )?;
                    Ok((canonical, source_root, observed))
                })();
                let cleanup =
                    release_host_scan_inputs(host_policy, source_path, source_path_bytes, &worker);
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
        let (canonical, source_root, observed) = result?;
        collect_captured_checkout_roots(
            checkout, canonical, limits, policy, maximum, &mut paths, meter,
        )
        .await?;
        let ordered = order_host_checkout_union(observed, paths, maximum, meter)?;
        capture_observed_paths_from_root(
            checkout,
            ordered,
            usize::MAX,
            options.maximum_extent_spans,
            &source_root,
            baseline,
            meter,
        )
        .await
    }
    .await;
    capture_scope_result(scope, result)
}

fn scan_host_subtree_roots(
    source: (&Path, NativeRootIdentity),
    volume: (FilesystemProfile, crate::model::VolumeLimits),
    maximum: usize,
    canonical: &[NamespacePath],
    policy: &CapturePolicy,
    observed: &mut Vec<(NamespacePath, Option<HostStat>)>,
    meter: &NativeMaterializationWork,
) -> Result<HostRoot, CaptureError> {
    let source_root = HostRoot::open(source.0, &mut meter.clone()).map_err(capture_demand_error)?;
    if source_root.identity() != source.1 {
        return Err(CaptureError::RootChanged);
    }
    collect_host_subtree_roots(
        &source_root,
        volume,
        maximum,
        canonical,
        policy,
        observed,
        meter,
    )
    .inspect_err(|_| drop(std::mem::take(observed)))?;
    Ok(source_root)
}

fn release_host_scan_inputs(
    policy: CapturePolicy,
    source_path: PathBuf,
    source_path_bytes: u64,
    meter: &NativeMaterializationWork,
) -> Result<(), CaptureError> {
    release_capture_paths(policy.excluded_prefixes, meter)?;
    drop(source_path);
    meter
        .clone()
        .release(source_path_bytes)
        .map_err(capture_demand_error)
}

async fn collect_captured_checkout_roots<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    canonical: Vec<NamespacePath>,
    limits: crate::model::VolumeLimits,
    policy: &CapturePolicy,
    maximum: usize,
    paths: &mut Vec<(NamespacePath, ())>,
    meter: &NativeMaterializationWork,
) -> Result<(), CaptureError> {
    let lookup_cancellation = meter.with_original_leaf(|work| work.cancellation().clone());
    let current = checkout
        .lookup_batch_no_follow(&canonical, meter, &lookup_cancellation)
        .await?;

    let canonical_bytes = canonical.capacity() as u64 * std::mem::size_of::<NamespacePath>() as u64;
    for (root, entry) in canonical.into_iter().zip(current.0.entries) {
        if entry
            .record
            .is_some_and(|record| record.kind == FileKind::Directory)
        {
            collect_checkout_subtree_paths(checkout, limits, root, policy, maximum, paths, meter)
                .await?;
        } else {
            capture_drop_path(root, meter)?;
        }
    }
    drop(current.1);
    meter
        .clone()
        .release(canonical_bytes)
        .map_err(capture_demand_error)?;
    Ok(())
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
    if checkout.has_pending_mutations() {
        return Err(OperationFailure::new(
            CaptureError::DirtyCheckout,
            native.receipt(),
        ));
    }
    if options.maximum_paths == 0 || options.maximum_extent_spans == 0 {
        return Err(OperationFailure::new(
            CaptureError::InvalidOptions,
            native.receipt(),
        ));
    }
    cancellation
        .check()
        .map_err(|error| OperationFailure::new(error.into(), native.receipt()))?;
    native
        .with_original_leaf(|work| work.check())
        .map_err(|error| OperationFailure::new(capture_demand_error(error), native.receipt()))?;
    let mut candidate = checkout
        .private_candidate(
            &mut crate::kernel::allocation::KernelAccounting::native(&native, cancellation),
            cancellation,
        )
        .map_err(|error| OperationFailure::new(CaptureError::from(error), native.receipt()))?;
    let scope = CaptureWorkScope::new(&native)?;
    let meter = &scope.native;
    let result = (async {
        let maximum =
            usize::try_from(options.maximum_paths).map_err(|_| CaptureError::InvalidOptions)?;
        let source_root = open_source_root(options, meter)?;
        let limits = checkout.volume_config().limits;
        let profile = checkout.volume_config().profile;
        let mut paths = Vec::new();

        let mut observed = Vec::new();
        let root = NamespacePath::new(Vec::new(), limits).map_err(CaptureError::from)?;
        collect_host_walk_measured(
            &source_root,
            profile,
            limits,
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
            meter,
        )
        .await?;
        let ordered = order_host_checkout_union(observed, paths, maximum, meter)?;
        capture_observed_paths_from_root(
            &mut candidate,
            ordered,
            usize::MAX,
            options.maximum_extent_spans,
            &source_root,
            None,
            meter,
        )
        .await
    })
    .await;
    let receipt = capture_scope_result(scope, result)?;
    cancellation
        .commit_if_live(|| *checkout = candidate)
        .map_err(|error| OperationFailure::new(error.into(), native.receipt()))?;
    Ok(receipt)
}
