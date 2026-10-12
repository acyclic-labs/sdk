use super::*;
use crate::kernel::KernelAccounting;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore, D: DemandSource + 'static, S: LazyWorkspaceStore>
    LazyWorkspace<A, O, D, S>
{
    /// Finds sparse authored boundaries or the dense unresolved source boundary.
    pub async fn seek(
        &self,
        path: &str,
        offset: u64,
        target: LazySeekTarget,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<u64>, LazyWorkspaceError> {
        let lookup = self.lookup(path, accounting).await?;
        let result = async {
            let stat = lookup.0.stat();
            if stat.kind != SourceNodeKind::RegularFile {
                return Err(LazyWorkspaceError::NotRegularFile);
            }
            let length = stat
                .logical_bytes
                .ok_or(LazyWorkspaceError::NotRegularFile)?;
            if offset >= length {
                return Ok(None);
            }
            if matches!(lookup.0, LazyLookup::Source { .. }) {
                return Ok(Some(match target {
                    LazySeekTarget::Data => offset,
                    LazySeekTarget::Hole => length,
                }));
            }
            let record = match &lookup.0 {
                LazyLookup::Shadow { record, .. } => *record,
                LazyLookup::Authored { stat, .. } => {
                    let checkout = self
                        .checkout(crate::model::GenerationSelector::Head, accounting)
                        .await?;
                    let record = checkout
                        .value
                        .lookup_file_record_by_id_with_accounting(
                            stat.file_id,
                            accounting,
                            cancellation(accounting),
                        )
                        .await;
                    checkout.retire(accounting)?;
                    record?.ok_or(LazyWorkspaceError::NotFound)?
                }
                LazyLookup::Source { .. } => unreachable!("dense source returned above"),
            };
            let target = match target {
                LazySeekTarget::Data => crate::kernel::ExtentSeekTarget::Data,
                LazySeekTarget::Hole => crate::kernel::ExtentSeekTarget::Hole,
            };
            crate::facade::seek_record_extent_with_accounting(
                &self.workspace.volume,
                record,
                offset,
                target,
                accounting,
                cancellation(accounting),
            )
            .await
            .map_err(Into::into)
        }
        .await;
        self.retire_lookup(lookup, accounting)?;
        result
    }
}
