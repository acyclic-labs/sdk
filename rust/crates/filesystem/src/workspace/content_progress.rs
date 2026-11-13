//! Regular-file progress over the shared Merkle changed-path frontier.
use super::*;

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> Generation<A, O> {
    /// Whether the authored namespace has a regular-file content change.
    /// Creation and removal count; metadata, file identities and storage layouts
    /// do not. Equal Merkle subtrees are skipped and the first byte difference
    /// ends the query. Source overlays and identity shadows are not represented
    /// by an authored generation and require their original logical cuts.
    pub async fn content_changed_from<W: crate::facade::FsWork>(
        &self,
        original: &Self,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<bool, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            if self.workspace.id != original.workspace.id
                || !self
                    .workspace
                    .volume
                    .fs
                    .same_deployment(&original.workspace.volume.fs)
            {
                return Err(WorkspaceError::ForeignGeneration);
            }
            let before = original
                .workspace
                .open_checkout(
                    GenerationSelector::Exact(original.id),
                    CheckoutMode::read_only_pinned(),
                    &mut accounting,
                    cancellation,
                )
                .await?;
            let after = match self
                .workspace
                .open_checkout(
                    GenerationSelector::Exact(self.id),
                    CheckoutMode::read_only_pinned(),
                    &mut accounting,
                    cancellation,
                )
                .await
            {
                Ok(after) => after,
                Err(error) => {
                    before.retire(&mut accounting)?;
                    return Err(error);
                }
            };
            let result = async {
                let roots = [Some(before.value.root()), Some(after.value.root())];
                let mut cursor = crate::kernel::changed_paths::Cursor::new(
                    roots,
                    self.workspace.volume.config,
                    &mut accounting,
                )?;
                let result: Result<bool, FsError> = async {
                    loop {
                        let Some(row) = cursor
                            .next(
                                self.workspace.volume.fs.objects(),
                                roots,
                                &mut accounting,
                                cancellation,
                            )
                            .await?
                        else {
                            return Ok(false);
                        };
                        let regular = |record: Option<crate::kernel::FileRecord>| {
                            record.filter(|record| record.kind == FileKind::Regular)
                        };
                        let changed = match (regular(row.value.before), regular(row.value.after)) {
                            (Some(before), Some(after)) => crate::kernel::records_match_content(
                                self.workspace.volume.fs.objects(),
                                before,
                                after,
                                crate::kernel::DecodeLimits::for_volume(
                                    self.workspace.volume.config,
                                ),
                                &mut accounting,
                                cancellation,
                            )
                            .await
                            .map(|equal| !equal),
                            (None, None) => Ok(false),
                            _ => Ok(true),
                        };
                        row.retire(&mut accounting)?;
                        if changed? {
                            return Ok(true);
                        }
                    }
                }
                .await;
                cursor.retire(&mut accounting)?;
                result
            }
            .await
            .map_err(WorkspaceError::from);
            after.retire(&mut accounting)?;
            before.retire(&mut accounting)?;
            result
        }
        .await;
        work.finish(result, &accounting)
    }
}
