impl<A: AsyncAuthorityStore, O: AsyncObjectStore> CheckoutMountSource<A, O> {
    async fn mutate(
        &self,
        mut candidate: CheckoutCandidate<A, O>,
        epoch: u64,
        stamp: ViewStamp,
        change: AuthoredMutation,
        effect: ViewChange<'_>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        apply_authored(&mut candidate.checkout, &change, work).await?;
        let publication = self
            .checkout
            .install_change(candidate, epoch, stamp, effect, work, &self.cancellation)
            .await?;
        if publication == MountPublication::PerMutation {
            self.checkout
                .publish(
                    crate::PublicationPermit::Unrestricted,
                    work,
                    work.cancellation(),
                    Some(&self.cancellation),
                    self.native_callback_guard.clone(),
                )
                .await?;
        }
        Ok(())
    }
}

async fn apply_authored<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    change: &AuthoredMutation,
    work: &crate::NativeMaterializationWork,
) -> Result<(), MountSourceError> {
    let allocation = work
        .claim(
            std::mem::size_of::<AuthoredMutation>() as u64,
            0,
            crate::WorkCounters {
                items_examined: 1,
                bytes_copied: std::mem::size_of::<AuthoredMutation>() as u64,
                ..crate::WorkCounters::UNCHARGED
            },
        )
        .map_err(MountSourceError::NativeWork)?;
    checkout
        .apply_borrowed_authored_transaction_with_native_work(
            std::slice::from_ref(change),
            &allocation,
            work,
            work.cancellation(),
        )
        .await
        .map_err(fs_error)?;
    Ok(())
}
