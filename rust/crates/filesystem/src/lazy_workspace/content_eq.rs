//! Logical file equality at exact source-backed cuts, without namespace scans.
use super::*;

impl<
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
> LazyWorkspace<A, O, D, S>
{
    /// Compares one optional regular file at two immutable logical cuts.
    /// Authored data, identity shadows and retained source before-images use
    /// the same lookup as snapshot reads. Missing evidence is refused rather
    /// than replaced by the live source or authored generation alone.
    ///
    /// Metadata, file identities and storage layouts do not establish byte
    /// inequality. Creation and removal differ; matching absence is equal.
    /// This is a point comparison, not a complete source-alias enumeration.
    pub async fn matches_optional_content_at_snapshots(
        &self,
        before: LazySnapshotRef,
        after: LazySnapshotRef,
        path: &str,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<bool, LazyWorkspaceError> {
        let (left, _, left_allocation) = self.lookup_at_snapshot(before, path, ctx).await?;
        let result = async {
            let (right, _, right_allocation) = self.lookup_at_snapshot(after, path, ctx).await?;
            let result = self
                .compare_optional_content(
                    (before, left.as_ref()),
                    (after, right.as_ref()),
                    path,
                    ctx,
                )
                .await;
            if let Some(right) = right {
                self.retire_lookup((right, right_allocation), ctx)?;
            } else {
                crate::kernel::retire_allocation(right_allocation, 0, ctx)?;
            }
            result
        }
        .await;
        if let Some(left) = left {
            self.retire_lookup((left, left_allocation), ctx)?;
        } else {
            crate::kernel::retire_allocation(left_allocation, 0, ctx)?;
        }
        result
    }

    async fn compare_optional_content(
        &self,
        left: (LazySnapshotRef, Option<&LazyLookup>),
        right: (LazySnapshotRef, Option<&LazyLookup>),
        path: &str,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<bool, LazyWorkspaceError> {
        for (cut, lookup) in [left, right] {
            if let Some(lookup) = lookup {
                if lookup.stat().kind != SourceNodeKind::RegularFile {
                    return Err(LazyWorkspaceError::NotRegularFile);
                }
                if matches!(lookup, LazyLookup::Source { .. })
                    && cut.source != self.source.reference()
                {
                    return Err(LazyWorkspaceError::StaleSource);
                }
            }
        }
        let (Some(left), Some(right)) = (left.1, right.1) else {
            return Ok(left.1.is_none() && right.1.is_none());
        };
        let record = |lookup: &LazyLookup| match lookup {
            LazyLookup::Authored { record, .. } | LazyLookup::Shadow { record, .. } => {
                Some(*record)
            }
            LazyLookup::Source { .. } => None,
        };
        if let (Some(left), Some(right)) = (record(left), record(right)) {
            let cancellation = cancellation(ctx);
            return crate::kernel::records_match_content(
                self.workspace.volume.fs.objects(),
                left,
                right,
                crate::kernel::DecodeLimits::for_volume(self.workspace.volume.config),
                ctx,
                cancellation,
            )
            .await
            .map_err(Into::into);
        }
        let length = left
            .stat()
            .logical_bytes
            .ok_or(LazyWorkspaceError::NotRegularFile)?;
        if right.stat().logical_bytes != Some(length) {
            return Ok(false);
        }
        let mut offset = 0;
        while offset < length {
            ctx.check()?;
            let range = crate::ByteRange {
                offset,
                length: (length - offset).min(crate::kernel::CONTENT_READ_CHUNK_BYTES),
            };
            let a = self
                .read_resolved(left, path, Some(range), Some(range.length), ctx)
                .await?
                .retain_bytes(ctx)?;
            let b = self
                .read_resolved(right, path, Some(range), Some(range.length), ctx)
                .await?
                .retain_bytes(ctx)?;
            if a.len() as u64 != range.length || b.len() as u64 != range.length {
                return Err(DemandError::StaleVersion.into());
            }
            ctx.charge(WorkCounters {
                items_examined: 1,
                source_bytes_read: range
                    .length
                    .checked_mul(2)
                    .ok_or(crate::WorkError::Overflow)?,
                ..WorkCounters::UNCHARGED
            })?;
            if a.as_ref() != b.as_ref() {
                return Ok(false);
            }
            offset += range.length;
        }
        Ok(true)
    }
}
