use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Transaction<A, O> {
    /// Creates missing directory prefixes without replacing existing objects.
    pub async fn create_dir_all<W: crate::facade::FsWork>(
        &mut self,
        path: &str,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = self.create_dirs(path, &mut accounting, cancellation).await;
        work.finish(result, &accounting)
    }

    pub(crate) async fn create_dirs(
        &mut self,
        path: &str,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), WorkspaceError> {
        accounting.check()?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            self.checkout.validate_native_work(original)?;
        }
        let depth = PortablePath::validate(path, self.checkout.volume_config().limits).map_err(
            |error| WorkspaceError::from(crate::kernel::NamespacePathError::Path(error)),
        )?;
        if depth == 0 {
            return Ok(());
        }
        let folded = self.checkout.volume_config().case_sensitivity
            == crate::model::CaseSensitivity::ProfileFolded;
        let mut parsed = Some(self.mutation_path(path, accounting, cancellation)?);
        let result = async {
            let (lookup, reached) = self
                .checkout
                .directory_prefix_with_accounting(
                    &parsed.as_ref().ok_or(FsError::InvalidDiff)?.value,
                    accounting,
                    cancellation,
                )
                .await?;
            if let Some(record) = lookup.record {
                return if record.kind == FileKind::Directory {
                    Ok(())
                } else {
                    Err(WorkspaceError::NotDirectory)
                };
            }
            let first = usize::from(lookup.resolved_components) + 1;
            let mut names = prefixes(path).skip(first - 1);
            for index in first..=depth {
                let name = names.next().ok_or(FsError::InvalidDiff)?;
                // Retain the full parsed path for the final mutation instead of
                // validating and allocating its names again after the lookup.
                let prefix = if index == depth {
                    parsed.take().ok_or(FsError::InvalidDiff)?
                } else {
                    self.mutation_path(name, accounting, cancellation)?
                };
                // The walk observed every prefix up to the first missing one.
                // Folded retries, and base directories below a pending removal,
                // keep the observations of a separate lookup per prefix.
                if folded || (index > first && index <= reached) {
                    match self
                        .checkout
                        .lookup_no_follow_with_accounting(
                            &prefix.value,
                            accounting,
                            cancellation,
                            true,
                        )
                        .await
                    {
                        Ok((lookup, _)) => match lookup.record {
                            Some(record) if record.kind == FileKind::Directory => {
                                prefix.retire(accounting)?;
                                continue;
                            }
                            Some(_) => {
                                prefix.retire(accounting)?;
                                return Err(WorkspaceError::NotDirectory);
                            }
                            None => {}
                        },
                        Err(error) => {
                            prefix.retire(accounting)?;
                            return Err(error.into());
                        }
                    }
                }
                self.create_directory_path(prefix, accounting, cancellation)
                    .await?;
            }
            Ok(())
        }
        .await;
        if let Some(parsed) = parsed {
            parsed.retire(accounting)?;
        }
        result
    }

    pub(crate) async fn create_directory_path(
        &mut self,
        path: crate::kernel::Owned<NamespacePath>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), WorkspaceError> {
        let mut operations = crate::kernel::Owned::allocate(1, accounting)?;
        Self::push_mutation(
            &mut operations,
            crate::kernel::Owned::new(
                AuthoredMutation::CreateDirectory {
                    path: path.value,
                    metadata: FileMetadata::default(),
                },
                path.allocation,
                path.bytes,
            ),
            accounting,
        )?;
        self.apply_authored(operations, accounting, cancellation)
            .await
    }
}

fn prefixes(path: &str) -> impl Iterator<Item = &str> {
    path.match_indices('/')
        .skip(1)
        .map(|(index, _)| path.get(..index).expect("separator is a UTF-8 boundary"))
        .chain(std::iter::once(path))
}
