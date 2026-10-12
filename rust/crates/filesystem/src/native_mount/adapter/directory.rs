use super::*;

impl<A, O> CheckoutMountSource<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    pub(super) fn read_directory(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountDirectoryPage, MountSourceError> {
        let path = self.path(path)?;
        let cursor = cursor.map(|bytes| self.logical_name(bytes)).transpose()?;
        let owner = ViewGate::callback_owner();
        self.runtime.wait(|| async {
            let observation = self
                .checkout
                .observe(
                    owner,
                    &mut NativeDemandCarrier::Borrowed(&mut crate::demand::DemandWork::new(
                        boundary_budget(),
                        &self.cancellation,
                    )),
                    &self.cancellation,
                    None,
                )
                .await?;
            let mut checkout = observation
                .observer(boundary_budget(), &self.cancellation)
                .map_err(facade_error)?
                .value;
            let receipt = checkout
                .list_directory_records(
                    &path,
                    cursor.as_ref(),
                    maximum_entries,
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map_err(engine_error)?;
            let has_more = receipt.value.has_more;
            let entries = receipt
                .value
                .entries
                .into_iter()
                .map(|entry| {
                    Ok(MountDirectoryEntry {
                        name: native_mount_name(
                            &entry.name,
                            &mut NativeDemandCarrier::Borrowed(
                                &mut crate::demand::DemandWork::new(
                                    boundary_budget(),
                                    &self.cancellation,
                                ),
                            ),
                        )?
                        .0,
                        node: mount_node(entry.record),
                        metadata: entry.metadata,
                        // A checkout lookup issues no content pin.
                        pin: None,
                    })
                })
                .collect::<Result<Vec<_>, MountSourceError>>()?;
            let next_cursor = has_more
                .then(|| entries.last().map(|entry| entry.name.clone()))
                .flatten();
            Ok(MountDirectoryPage {
                entries,
                next_cursor,
            })
        })
    }
}
