//! Exact immutable endpoints from the existing authority-log query.
use super::*;
use crate::context::{ContextRoot, DiffPath, DiffPublication, PinnedContextPath};

impl<P: StreamProvider + 'static, O: AsyncObjectStore + 'static> FilesystemFileQuery<P, O> {
    /// Snapshot changes from an immutable base, then follow exact affected paths.
    /// No native watcher, timer, execution event or caller-supplied file content.
    pub async fn observe_diff(
        &self,
        base: acyclic_fs::GenerationId,
        maximum_changes: u32,
    ) -> crate::Result<acyclic_stream::BoxProviderStream<'static, crate::Result<DiffPublication>>>
    {
        if maximum_changes == 0 {
            return Err(crate::Error::Invalid(
                "diff query requires a finite path bound".into(),
            ));
        }
        let io = PreparationIo::from_budget(self.budget, &acyclic_fs::CancellationToken::new())?;
        let base = self.reader.host.generation_ref_id(base, Some(&io))?;
        self.authorize(&Query::File(task_volume::FileKey::Goal), &io)
            .await?;
        let query = *blake3::hash(
            &serde_json::to_vec(&base).map_err(|e| crate::Error::Invalid(e.to_string()))?,
        )
        .as_bytes();
        let FileCut {
            workspace,
            current,
            cursor,
            records,
            ..
        } = self.open_cut(query, None, &io).await?;
        let baseline = self
            .reader
            .host
            .generation(&workspace, &base, Some(&io))
            .await?;
        let snapshot = self
            .publication(&baseline, &baseline, &current, maximum_changes, &io)
            .await?;
        let source = self.clone();
        let changes = futures::stream::try_unfold(
            (source, baseline, current, cursor.position, records),
            move |(source, base, current, position, mut records)| async move {
                let record = records.next().await.ok_or_else(|| {
                    crate::Error::Conflict("diff follow ended; reopen snapshot".into())
                })??;
                if record.sequence != position {
                    return Err(crate::Error::Conflict(
                        "diff follow cursor gap; reopen snapshot".into(),
                    ));
                }
                let io = PreparationIo::from_budget(
                    source.budget,
                    &acyclic_fs::CancellationToken::new(),
                )?;
                source
                    .authorize(&Query::File(task_volume::FileKey::Goal), &io)
                    .await?;
                let next = generation(&current.workspace(), &record, &io).await?;
                let publication = source
                    .publication(&base, &current, &next, maximum_changes, &io)
                    .await?;
                let position = position
                    .checked_add(1)
                    .ok_or_else(|| crate::Error::Invalid("diff cursor overflow".into()))?;
                Ok(Some((publication, (source, base, next, position, records))))
            },
        );
        Ok(Box::pin(
            futures::stream::once(async { Ok(snapshot) }).chain(changes),
        ))
    }

    async fn publication(
        &self,
        base: &acyclic_fs::Generation<StreamAuthorityStore<P>, O>,
        previous: &acyclic_fs::Generation<StreamAuthorityStore<P>, O>,
        current: &acyclic_fs::Generation<StreamAuthorityStore<P>, O>,
        maximum: u32,
        io: &PreparationIo,
    ) -> crate::Result<DiffPublication> {
        let permissions = self.authorize_policy(&current.workspace(), io).await?;
        for generation in [base, previous, current] {
            self.require_snapshot(&permissions, generation, io).await?;
        }
        let (diff, _allocation) = io.sdk(
            previous
                .diff_to(current, maximum, io, io.cancellation())
                .await,
        )?;
        if diff.changes().truncated {
            return Err(crate::Error::Invalid(
                "diff query exceeds path bound".into(),
            ));
        }
        let (paths, _paths_allocation) =
            io.sdk(diff.changed_paths(maximum, io, io.cancellation()).await)?;
        let mut result = DiffPublication {
            previous: self
                .reader
                .host
                .generation_ref_id(previous.id(), Some(io))?,
            generation: self.reader.host.generation_ref_id(current.id(), Some(io))?,
            paths: Vec::new(),
        };
        for change in paths {
            let mut path = String::new();
            for component in change.path.components() {
                let name = component
                    .unicode_text(
                        io.remaining().map_err(crate::Error::from)?,
                        io.cancellation(),
                    )
                    .map_err(|e| crate::Error::Invalid(e.error.to_string()))?;
                io.charge(name.work).map_err(crate::Error::from)?;
                if !path.is_empty() {
                    path.push('/');
                }
                path.push_str(&name.value);
            }
            if path.is_empty() || crate::conversation::is_internal_path(&path) {
                continue;
            }
            let before = self.endpoint(base, &path, io).await?;
            let after = self.endpoint(current, &path, io).await?;
            let largest_file_bytes = before
                .as_ref()
                .map(|(_, bytes)| *bytes)
                .into_iter()
                .chain(after.as_ref().map(|(_, bytes)| *bytes))
                .max();
            result.paths.push(DiffPath {
                path,
                base: before.map(|(source, _)| source),
                current: after.map(|(source, _)| source),
                largest_file_bytes,
                matches_base: false,
            });
        }
        Self::compare_publication(base, current, &mut result, io).await?;
        super::super::access::require_current(&current.workspace(), &permissions, io).await?;
        Ok(result)
    }

    async fn compare_publication(
        base: &acyclic_fs::Generation<StreamAuthorityStore<P>, O>,
        current: &acyclic_fs::Generation<StreamAuthorityStore<P>, O>,
        result: &mut DiffPublication,
        io: &PreparationIo,
    ) -> crate::Result<()> {
        // Equality is an optional hint: paths and immutable endpoints are the
        // publication. Reserve half of the remaining allowance for this hint,
        // leaving the original ledger available for the final authority fence.
        // A denied comparison must not discard unrelated small-file changes.
        let mut remaining = io.remaining()?;
        remaining.peak_allocation_bytes = remaining
            .peak_allocation_bytes
            .saturating_sub(io.receipt().peak_allocation_bytes);
        let allowance = comparison_allowance(remaining);
        let mut spent = acyclic_fs::WorkCounters::UNCHARGED;
        for change in &mut result.paths {
            if change.base.is_none() || change.current.is_none() {
                continue;
            }
            let remaining = spent
                .remaining(allowance)
                .map_err(acyclic_fs::FsError::from)?;
            let (matched, work) = match current
                .matches_content(
                    base,
                    &format!("/{}", change.path),
                    remaining,
                    io.cancellation(),
                )
                .await
            {
                Ok(receipt) => (Ok(receipt.value), receipt.work),
                Err(failure) => (Err(failure.error), *failure.work),
            };
            // Failed comparisons still own their exact work receipt. No body or
            // allocation escapes this bool projection.
            io.charge(work)?;
            spent = spent.checked_add(work).map_err(acyclic_fs::FsError::from)?;
            match matched {
                Ok(equal) => change.matches_base = equal,
                Err(acyclic_fs::WorkspaceError::Work(acyclic_fs::WorkError::BudgetExceeded {
                    ..
                })) => break,
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }

    async fn endpoint(
        &self,
        generation: &acyclic_fs::Generation<StreamAuthorityStore<P>, O>,
        path: &str,
        io: &PreparationIo,
    ) -> crate::Result<Option<(PinnedContextPath, u64)>> {
        let (stat, recorded) = generation
            .stat(&format!("/{path}"), io, io.cancellation())
            .await;
        recorded.map_err(crate::Error::from)?;
        match stat {
            Ok(stat) if stat.kind == acyclic_fs::kernel::FileKind::Regular => {
                let bytes = stat.logical_bytes.ok_or_else(|| {
                    crate::Error::Storage("diff endpoint has no file length".into())
                })?;
                Ok(Some((
                    PinnedContextPath {
                        root: ContextRoot {
                            volume: self.volume.clone(),
                            directory: String::new(),
                        },
                        generation: self
                            .reader
                            .host
                            .generation_ref_id(generation.id(), Some(io))?,
                        path: path.to_owned(),
                    },
                    bytes,
                )))
            }
            Ok(_)
            | Err(
                acyclic_fs::WorkspaceError::NotFound | acyclic_fs::WorkspaceError::NotDirectory,
            ) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }
}

// Separate optional content work from the mandatory publication/authority work.
fn comparison_allowance(remaining: WorkBudget) -> WorkBudget {
    WorkBudget {
        authority_records_read: remaining.authority_records_read / 2,
        authority_records_appended: remaining.authority_records_appended / 2,
        authority_bytes_read: remaining.authority_bytes_read / 2,
        authority_bytes_written: remaining.authority_bytes_written / 2,
        object_probes: remaining.object_probes / 2,
        backend_read_operations: remaining.backend_read_operations / 2,
        backend_write_operations: remaining.backend_write_operations / 2,
        durability_operations: remaining.durability_operations / 2,
        page_reads: remaining.page_reads / 2,
        page_writes: remaining.page_writes / 2,
        object_bytes_read: remaining.object_bytes_read / 2,
        object_bytes_written: remaining.object_bytes_written / 2,
        bytes_hashed: remaining.bytes_hashed / 2,
        bytes_copied: remaining.bytes_copied / 2,
        bytes_encoded: remaining.bytes_encoded / 2,
        source_bytes_read: remaining.source_bytes_read / 2,
        source_path_components: remaining.source_path_components / 2,
        source_entries_visited: remaining.source_entries_visited / 2,
        output_bytes: remaining.output_bytes / 2,
        items_examined: remaining.items_examined / 2,
        items_returned: remaining.items_returned / 2,
        allocation_operations: remaining.allocation_operations / 2,
        peak_allocation_bytes: remaining.peak_allocation_bytes / 2,
        materializations: remaining.materializations / 2,
    }
}
