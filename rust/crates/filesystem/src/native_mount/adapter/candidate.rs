pub(super) struct CheckoutCandidate<A, O> {
    base: Checkout<A, O>,
    pub(super) checkout: Checkout<A, O>,
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> CheckoutCandidate<A, O> {
    /// The exact effect installing this candidate has: the generation diff
    /// from the checkout it began from. `scope` names what the candidate was
    /// prepared to change and locates the directories whose names changed.
    pub(super) async fn installed_changes(
        &self,
        scope: InstallScope<'_>,
        cancellation: &CancellationToken,
    ) -> ViewEffect {
        installed_changes(&self.base, &self.checkout, scope, cancellation).await
    }

    /// The checkout this transaction began from.
    pub(super) fn base(&self) -> &Checkout<A, O> {
        &self.base
    }
}

/// What a prepared candidate was built to change.
#[derive(Clone, Copy)]
pub(super) enum InstallScope<'a> {
    /// Exactly these paths.
    Paths(&'a [NamespacePath]),
    /// Anything beneath this path.
    Subtree(&'a NamespacePath),
}

impl InstallScope<'_> {
    fn paths(&self) -> &[NamespacePath] {
        match self {
            Self::Paths(paths) => paths,
            Self::Subtree(root) => std::slice::from_ref(*root),
        }
    }
}

/// The exact effect of replacing `base` with `candidate`, from their
/// generation diff. A changed name in a directory the scope's paths do not
/// pass through is covered by its subtree scope, or by everything.
async fn installed_changes<A, O>(
    base: &Checkout<A, O>,
    candidate: &Checkout<A, O>,
    scope: InstallScope<'_>,
    cancellation: &CancellationToken,
) -> ViewEffect
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
{
    in_heap(move || async move {
        let diff = match base
            .candidate_diff(
                candidate,
                MAXIMUM_EXACT_INSTALL_CHANGES,
                boundary_budget(),
                cancellation,
            )
            .await
        {
            Ok(receipt) if !receipt.value.truncated => receipt.value,
            _ => {
                return ViewEffect {
                    everything: true,
                    ..ViewEffect::default()
                };
            }
        };
        let mut directories = std::collections::HashMap::new();
        if !diff.bindings.is_empty() {
            for path in scope.paths() {
                for prefix in prefixes(path, base.volume_config().limits) {
                    for checkout in [base, candidate] {
                        if let Ok(receipt) = checkout
                            .inspector()
                            .lookup_no_follow(&prefix, boundary_budget(), cancellation)
                            .await
                            && let Some(record) = receipt.value.record
                        {
                            directories
                                .entry(record.file_id)
                                .or_insert_with(|| prefix.clone());
                        }
                    }
                }
            }
        }
        let mut installed = ViewEffect {
            nodes: diff
                .files
                .into_iter()
                .map(|change| change.file_id)
                .collect(),
            ..ViewEffect::default()
        };
        for binding in diff.bindings {
            match (directories.get(&binding.directory_id), scope) {
                (Some(directory), _) => installed.names.push((directory.clone(), binding.name)),
                (None, InstallScope::Subtree(root)) => {
                    if installed.subtrees.is_empty() {
                        installed.subtrees.push(root.clone());
                    }
                }
                (None, InstallScope::Paths(_)) => installed.everything = true,
            }
        }
        installed
    })
    .await
}

/// Every proper and improper prefix of `path`, root first.
fn prefixes(path: &NamespacePath, limits: VolumeLimits) -> Vec<NamespacePath> {
    (0..=path.components().len())
        .filter_map(|length| path.components().get(..length))
        .filter_map(|components| NamespacePath::new(components.to_vec(), limits).ok())
        .collect()
}

