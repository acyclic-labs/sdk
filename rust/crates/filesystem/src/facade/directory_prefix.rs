use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {
    // One walk captures every prefix record that separate no-follow lookups
    // would observe. A non-directory prefix is returned before descending.
    //
    // Separate lookups stop at the first non-directory or missing prefix, so
    // only those prefixes' observations are admitted, one prefix at a time:
    // a dependency-limit failure keeps every completed prefix. On folded
    // volumes the caller looks the first missing prefix up again, because its
    // case-folded retry observes the parent's listing.
    //
    // Also returns how many base prefixes the walk reached. A pending removal
    // can leave base directories under the first missing prefix, which
    // separate lookups observe one by one between the creations.
    pub(crate) async fn directory_prefix_with_accounting(
        &mut self,
        path: &NamespacePath,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(crate::kernel::PathLookup, usize), FsError> {
        accounting.check()?;
        let before = readonly_standalone_live(accounting);
        let result = async {
            self.synchronize_live_with_accounting(accounting, cancellation)
                .await?;
            let observing = self.tracks_observations();
            let candidate = if !observing || self.has_pending_mutations() {
                Some(
                    crate::kernel::resolve_path_with_accounting(
                        &self.volume.fs.inner.objects,
                        &self.root,
                        path,
                        self.volume.config,
                        accounting,
                        cancellation,
                        Some(&self.dependencies.data_custody),
                        false,
                        crate::kernel::CaptureRecords::Prefixes,
                    )
                    .await?
                    .0
                    .lookup,
                )
            } else {
                None
            };
            if !observing {
                return candidate
                    .map(|lookup| (lookup, 0))
                    .ok_or(FsError::InvalidDiff);
            }
            let (mut observed, mut allocation) = crate::kernel::resolve_path_with_accounting(
                &self.volume.fs.inner.objects,
                &self.base_root,
                path,
                self.volume.config,
                accounting,
                cancellation,
                Some(&self.dependencies.data_custody),
                true,
                crate::kernel::CaptureRecords::Prefixes,
            )
            .await?;
            let reached = prefixes_reached(&observed.lookup, path.depth());
            let lookup = candidate.unwrap_or(observed.lookup);
            let resolved = usize::from(lookup.resolved_components);
            let observed_prefixes = match lookup.record {
                Some(record) if record.kind == FileKind::Directory => path.depth(),
                Some(_) => resolved,
                None if self.volume.config.case_sensitivity == CaseSensitivity::ProfileFolded => {
                    resolved
                }
                None => resolved.saturating_add(1),
            };
            // Each prefix contributes its directory-name observation followed
            // by its record when the base binds it.
            let starts = |dependency: &Dependency| {
                matches!(dependency.region, DependencyRegion::DirectoryName { .. })
            };
            let mut prefixes = 0_usize;
            let end = observed
                .dependencies
                .iter()
                .position(|dependency| {
                    prefixes += usize::from(starts(dependency));
                    prefixes > observed_prefixes
                })
                .unwrap_or(observed.dependencies.len());
            observed.dependencies.truncate(end);
            let maximum = self.volume.config.limits.maximum_checkout_dependencies;
            let (admitted, failure) = self.dependencies.admissible_observation_groups(
                &observed.dependencies,
                starts,
                maximum,
            );
            observed.dependencies.truncate(admitted);
            self.dependencies.extend_observations(
                std::mem::take(&mut observed.dependencies),
                &mut allocation,
                maximum,
                accounting,
                cancellation,
            )?;
            if let Some(failure) = failure {
                return Err(failure.into());
            }
            Ok((lookup, reached))
        }
        .await;
        readonly_cleanup_standalone(accounting, before, 0)?;
        result.map(|(mut lookup, reached)| {
            lookup.work = accounting.spent();
            (lookup, reached)
        })
    }
}

// Number of leading prefixes whose names the base walk observed.
fn prefixes_reached(lookup: &crate::kernel::PathLookup, depth: usize) -> usize {
    let resolved = usize::from(lookup.resolved_components);
    match lookup.record {
        None => resolved.saturating_add(1),
        Some(record) if record.kind == FileKind::Directory => depth,
        Some(_) => resolved,
    }
}
