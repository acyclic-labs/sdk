use super::*;

/// Work shared by authority operations in one budget or native ledger.
pub struct AuthorityWork<'a> {
    /// Completed ordinary provider work.
    pub work: WorkCounters,
    /// The operation budget.
    pub budget: WorkBudget,
    /// Cancellation shared by the operation.
    pub cancellation: &'a CancellationToken,
    #[cfg(not(target_arch = "wasm32"))]
    /// The original native ledger, when present.
    pub original: Option<&'a crate::NativeMaterializationWork>,
    pub(crate) allocations: crate::kernel::allocation::AllocationLedger,
    pub(crate) opaque_live_upper_bytes: u64,
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) request_allocation: Option<crate::NativeAllocation>,
}

impl<'a> AuthorityWork<'a> {
    /// Starts authority work against a bounded budget.
    pub fn new(budget: WorkBudget, cancellation: &'a CancellationToken) -> Self {
        Self {
            work: WorkCounters::UNCHARGED,
            budget,
            cancellation,
            allocations: Default::default(),
            opaque_live_upper_bytes: 0,
            #[cfg(not(target_arch = "wasm32"))]
            request_allocation: None,
            #[cfg(not(target_arch = "wasm32"))]
            original: None,
        }
    }

    /// Uses the caller's existing native ledger.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn native(
        original: &'a crate::NativeMaterializationWork,
        cancellation: &'a CancellationToken,
    ) -> Self {
        Self {
            original: Some(original),
            ..Self::new(WorkBudget::UNBOUNDED, cancellation)
        }
    }

    /// Returns actual completed work.
    pub fn spent(&self) -> WorkCounters {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = self.original {
            return original.receipt();
        }
        self.work
    }

    /// Returns the remaining operation budget.
    pub fn remaining(&self) -> Result<WorkBudget, crate::AuthorityStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = self.original {
            return original.remaining().map_err(Into::into);
        }
        let mut remaining = self.work.remaining(self.budget)?;
        remaining.peak_allocation_bytes = self
            .budget
            .peak_allocation_bytes
            .checked_sub(self.allocations.live_bytes())
            .and_then(|bytes| bytes.checked_sub(self.opaque_live_upper_bytes))
            .ok_or(crate::WorkError::Overflow)?;
        Ok(remaining)
    }

    /// Attaches actual work to a completed operation.
    pub fn receipt<T, E>(
        &self,
        result: Result<T, E>,
    ) -> crate::performance::MeasuredResult<crate::performance::OperationReceipt<T>, E> {
        result
            .map(|value| crate::performance::OperationReceipt {
                value,
                work: self.spent(),
            })
            .map_err(|error| crate::performance::OperationFailure::new(error, self.spent()))
    }

    pub(crate) fn check(&self) -> Result<(), crate::AuthorityStoreError> {
        self.cancellation
            .check()
            .map_err(|_| crate::AuthorityStoreError::Cancelled)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = self.original {
            return original.admit(WorkCounters::UNCHARGED).map_err(Into::into);
        }
        self.work.verify(self.budget)?;
        let peak = self
            .allocations
            .live_bytes()
            .checked_add(self.opaque_live_upper_bytes)
            .ok_or(crate::WorkError::Overflow)?;
        self.work
            .admit(
                &WorkCounters {
                    peak_allocation_bytes: peak,
                    ..WorkCounters::UNCHARGED
                },
                &self.budget,
            )
            .map_err(Into::into)
    }

    pub(crate) fn record(&mut self, delta: WorkCounters) -> Result<(), crate::AuthorityStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if self.original.is_some() {
            return Ok(());
        }
        self.work = self.work.checked_add(delta)?;
        Ok(())
    }

    /// Records the completed provider prefix and returns its value.
    pub fn accept<T, E: From<crate::WorkError>>(
        &mut self,
        result: crate::MeasuredResult<crate::OperationReceipt<T>, E>,
    ) -> Result<T, E> {
        let delta = match &result {
            Ok(receipt) => receipt.work,
            Err(failure) => *failure.work,
        };
        #[cfg(not(target_arch = "wasm32"))]
        if self.original.is_some() {
            return result
                .map(|receipt| receipt.value)
                .map_err(|failure| failure.error);
        }
        self.work = self.work.checked_add(delta).map_err(E::from)?;
        result
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
    }
}

/// Publication failed in the authority provider or its owner guard.
#[derive(Debug)]
pub enum AuthorityError<E> {
    /// The authority provider rejected the operation.
    Authority(crate::AuthorityStoreError),
    /// The owner guard rejected the operation.
    Owner(E),
}

impl<E> AuthorityError<E> {
    /// Preserves owner errors while converting provider failures.
    pub fn into_error(self, convert: impl FnOnce(crate::AuthorityStoreError) -> E) -> E {
        match self {
            Self::Authority(error) => convert(error),
            Self::Owner(error) => error,
        }
    }
}

impl<E> From<crate::AuthorityStoreError> for AuthorityError<E> {
    fn from(error: crate::AuthorityStoreError) -> Self {
        Self::Authority(error)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl<E> From<acyclic_stream::StreamError> for AuthorityError<E> {
    fn from(error: acyclic_stream::StreamError) -> Self {
        Self::Authority(error.into())
    }
}

/// Seals authority publication after planning and during recovery.
pub trait AuthorityGuard: ProviderTask {
    /// The owner's rejection type.
    type Error: Send;
    /// Checks the current owner against the same operation work.
    fn seal(
        &mut self,
        work: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<(), AuthorityError<Self::Error>>> + ProviderTask;

    /// Seals the provider's actual native commit attempt.
    #[cfg(not(target_arch = "wasm32"))]
    fn native_commit<P: acyclic_stream::StreamProvider>(
        &mut self,
        provider: &P,
        request: acyclic_stream::CommitRequest,
        deadline: Option<u64>,
        work: &mut AuthorityWork<'_>,
    ) -> impl Future<
        Output = acyclic_stream::NativeStreamResult<
            acyclic_stream::CommitOutcome,
            AuthorityError<Self::Error>,
        >,
    > + Send {
        async move {
            if let Err(error) = self.seal(work).await {
                return Ok(Err(error));
            }
            let Some(original) = work.original else {
                return Ok(Err(
                    crate::AuthorityStoreError::NativeOriginalUnsupported.into()
                ));
            };
            let meter = original.native_read_meter().clone();
            let result = match deadline {
                Some(deadline) => {
                    provider
                        .native_commit_before(request, deadline, meter)
                        .await
                }
                None => provider.native_commit(request, meter).await,
            };
            result.map(|result| result.map_err(AuthorityError::from))
        }
    }
}

impl AuthorityGuard for () {
    type Error = crate::AuthorityStoreError;
    async fn seal(&mut self, _: &mut AuthorityWork<'_>) -> Result<(), AuthorityError<Self::Error>> {
        Ok(())
    }
}

impl<F, Fut> AuthorityGuard for &mut F
where
    F: FnMut(WorkBudget) -> Fut + ProviderTask,
    Fut: Future<Output = crate::FsResult<()>> + ProviderTask,
{
    type Error = crate::FsError;
    async fn seal(
        &mut self,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), AuthorityError<Self::Error>> {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into());
        }
        let result = self(work.remaining()?).await;
        match result {
            Ok(receipt) => {
                work.record(receipt.work)?;
                work.work
                    .verify(work.budget)
                    .map_err(crate::AuthorityStoreError::from)?;
                Ok(())
            }
            Err(failure) => match work.work.checked_add(*failure.work) {
                Ok(combined) => {
                    work.work = combined;
                    Err(AuthorityError::Owner(failure.error))
                }
                Err(error) => Err(AuthorityError::Owner(crate::FsError::from(error))),
            },
        }
    }
}

/// Owns a native guard for provider retries after the caller yields.
#[cfg(not(target_arch = "wasm32"))]
pub struct NativeAuthorityGuard<F>(#[doc = "The owner callback."] pub F);

#[cfg(not(target_arch = "wasm32"))]
impl<E, F, Fut> AuthorityGuard for NativeAuthorityGuard<F>
where
    E: Send + 'static,
    F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
{
    type Error = E;
    async fn seal(&mut self, work: &mut AuthorityWork<'_>) -> Result<(), AuthorityError<E>> {
        let Some(original) = work.original else {
            return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into());
        };
        (self.0)(original).await.map_err(AuthorityError::Owner)
    }

    async fn native_commit<P: acyclic_stream::StreamProvider>(
        &mut self,
        provider: &P,
        request: acyclic_stream::CommitRequest,
        deadline: Option<u64>,
        work: &mut AuthorityWork<'_>,
    ) -> acyclic_stream::NativeStreamResult<acyclic_stream::CommitOutcome, AuthorityError<E>> {
        let Some(original) = work.original else {
            return Ok(Err(
                crate::AuthorityStoreError::NativeOriginalUnsupported.into()
            ));
        };
        let meter = original.native_read_meter().clone();
        let original = original.clone();
        let mut callback = self.0.clone();
        let before = move || {
            let future = callback(&original);
            async move { future.await.map_err(AuthorityError::Owner) }
        };
        provider
            .native_commit_guarded(request, deadline, meter, before)
            .await
    }
}

/// Retains an authority result's actual newly allocated native storage.
#[cfg(not(target_arch = "wasm32"))]
pub type AuthorityAllocation = Option<crate::NativeAllocation>;
/// Browser authority results need no native allocation guard.
#[cfg(target_arch = "wasm32")]
pub type AuthorityAllocation = Option<()>;
