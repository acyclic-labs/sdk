impl<A: AsyncAuthorityStore, O: AsyncObjectStore> SharedCheckout<A, O> {
    async fn change_file_content<F: crate::facade::ContentFile>(
        &self,
        file: F,
        change: ContentChange<F>,
        append_only: bool,
        work: &crate::NativeMaterializationWork,
        lifetime: &CancellationToken,
        guard: Option<Arc<dyn super::NativeMountCallbackGuard>>,
    ) -> Result<(), MountSourceError> {
        let cancellation = work.cancellation();
        let mut carrier = NativeDemandCarrier::Native(work);
        let file_id = match file.address() {
            crate::facade::ContentAddress::File(id) => id,
            crate::facade::ContentAddress::Path(path) => {
                let observation = self
                    .observe(
                        ViewGate::callback_owner()?,
                        &mut carrier,
                        cancellation,
                        Some(lifetime),
                    )
                    .await?;
                observation
                    .inspector(
                        &mut crate::kernel::KernelAccounting::native(work, cancellation),
                        cancellation,
                    )?
                    .lookup_no_follow(path, work, cancellation)
                    .await?
                    .record
                    .ok_or(MountSourceError::NotFound)?
                    .file_id
            }
        };
        let mut candidate = self
            .file_candidate(file_id, &mut carrier, cancellation, Some(lifetime))
            .await?;
        if let crate::facade::ContentAddress::Path(path) = file.address() {
            let current = candidate
                .checkout
                .inspector(
                    &mut crate::kernel::KernelAccounting::native(work, cancellation),
                    cancellation,
                )?
                .lookup_no_follow(path, work, cancellation)
                .await?
                .record;
            if current.is_none_or(|record| record.file_id != file_id) {
                return Err(MountSourceError::Stale);
            }
        }
        if append_only {
            let record = candidate
                .checkout
                .read_file_record_by_id(file_id, work, cancellation)
                .await?;
            if !matches!(&change, ContentChange::Write { offset, .. } if *offset == mount_node(record).logical_bytes)
            {
                return Err(MountSourceError::AccessDenied);
            }
        }
        candidate
            .checkout
            .change_content(
                file,
                change,
                ContentTimes::Stamp(content_change_time()?),
                work,
                cancellation,
            )
            .await?;
        self.install_change(candidate, ViewChange::Node(file_id), work, lifetime, guard)
            .await
    }
}
