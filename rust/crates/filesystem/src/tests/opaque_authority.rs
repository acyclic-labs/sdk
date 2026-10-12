#[derive(Default)]
pub(crate) struct OpaqueAuthority {
    pub(crate) inner: crate::MemoryAuthorityStore,
    pub(crate) ordinary_calls: std::sync::atomic::AtomicUsize,
}

impl crate::AsyncAuthorityStore for OpaqueAuthority {
    async fn create_authority<G: crate::AuthorityGuard>(
        &self,
        authority: crate::AuthorityId,
        epoch: crate::Epoch,
        work: &mut crate::AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<crate::CreateAuthorityOutcome, crate::AuthorityError<G::Error>> {
        if work.original.is_some() {
            return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into());
        }
        crate::async_storage::seal(guard.as_deref_mut(), work).await?;
        work.cancellation
            .check()
            .map_err(|_| crate::AuthorityStoreError::Cancelled)?;
        self.ordinary_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let value = crate::AsyncAuthorityStore::create_authority(
            &self.inner,
            authority,
            epoch,
            work,
            None::<&mut ()>,
        )
        .await
        .map_err(|error| {
            crate::AuthorityError::Authority(error.into_error(std::convert::identity))
        })?;
        if matches!(value, crate::CreateAuthorityOutcome::Existing(_)) {
            crate::async_storage::seal(guard.as_deref_mut(), work).await?;
        }
        Ok(value)
    }
    async fn compare_and_append<G: crate::AuthorityGuard>(
        &self,
        request: crate::GuardedAppend,
        work: &mut crate::AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<(crate::AppendOutcome, crate::AuthorityAllocation), crate::AuthorityError<G::Error>>
    {
        if work.original.is_some() {
            return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into());
        }
        if request.permit != crate::PublicationPermit::Unrestricted {
            return Err(crate::AuthorityStoreError::Rejected(
                "authority backend cannot atomically evaluate operation leases".to_owned(),
            )
            .into());
        }
        crate::async_storage::seal(guard.as_deref_mut(), work).await?;
        work.cancellation
            .check()
            .map_err(|_| crate::AuthorityStoreError::Cancelled)?;
        self.ordinary_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        crate::AsyncAuthorityStore::compare_and_append(&self.inner, request, work, None::<&mut ()>)
            .await
            .map_err(|error| {
                crate::AuthorityError::Authority(error.into_error(std::convert::identity))
            })
    }
    async fn head(
        &self,
        authority: crate::AuthorityId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::Head, crate::AuthorityStoreError> {
        if work.original.is_some() {
            return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into());
        }
        work.cancellation
            .check()
            .map_err(|_| crate::AuthorityStoreError::Cancelled)?;
        self.ordinary_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        crate::AsyncAuthorityStore::head(&self.inner, authority, work).await
    }
    async fn replay(
        &self,
        authority: crate::AuthorityId,
        after: crate::Sequence,
        limit: crate::ReplayLimit,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<crate::DurableCommit>, crate::AuthorityAllocation), crate::AuthorityStoreError>
    {
        if work.original.is_some() {
            return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into());
        }
        work.cancellation
            .check()
            .map_err(|_| crate::AuthorityStoreError::Cancelled)?;
        self.ordinary_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        crate::AsyncAuthorityStore::replay(&self.inner, authority, after, limit, work).await
    }
    async fn fence(
        &self,
        authority: crate::AuthorityId,
        expected: crate::Head,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::FenceOutcome, crate::AuthorityStoreError> {
        if work.original.is_some() {
            return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into());
        }
        work.cancellation
            .check()
            .map_err(|_| crate::AuthorityStoreError::Cancelled)?;
        self.ordinary_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        crate::AsyncAuthorityStore::fence(&self.inner, authority, expected, work).await
    }
    async fn find_operation(
        &self,
        authority: crate::AuthorityId,
        operation: crate::OperationId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<
        (Option<crate::DurableCommit>, crate::AuthorityAllocation),
        crate::AuthorityStoreError,
    > {
        if work.original.is_some() {
            return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into());
        }
        work.cancellation
            .check()
            .map_err(|_| crate::AuthorityStoreError::Cancelled)?;
        self.ordinary_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        crate::AsyncAuthorityStore::find_operation(&self.inner, authority, operation, work).await
    }
}
