use super::*;
use crate::{
    AuthorityStoreError, FsError, FsResult, ObjectStoreError, OperationFailure, OperationReceipt,
    WorkError,
};
#[cfg(not(target_arch = "wasm32"))]
use acyclic_stream::{CommitOutcome, CommitRequest, NativeStreamResult, StreamProvider};

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
    pub(crate) read_observation: Option<(&'a crate::kernel::ProofDataCustody, u64)>,
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
            read_observation: None,
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

    pub(crate) fn is_native(&self) -> bool {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.original.is_some()
        }
        #[cfg(target_arch = "wasm32")]
        {
            false
        }
    }

    pub(super) fn object_fallback(&self) -> Result<(), ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if self.original.is_some() {
            return Err(ObjectStoreError::NativeUnavailable);
        }
        Ok(())
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
    pub fn remaining(&self) -> Result<WorkBudget, AuthorityStoreError> {
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
            .ok_or(WorkError::Overflow)?;
        Ok(remaining)
    }

    /// Attaches actual work to a completed operation.
    pub fn receipt<T, E>(
        &self,
        result: Result<T, E>,
    ) -> crate::MeasuredResult<OperationReceipt<T>, E> {
        result
            .map(|value| OperationReceipt {
                value,
                work: self.spent(),
            })
            .map_err(|error| OperationFailure::new(error, self.spent()))
    }

    /// Returns the read budget after accounting for resident cache data.
    pub fn read_budget(&mut self) -> Result<WorkBudget, WorkError> {
        let mut remaining = self.work.remaining(self.budget)?;
        if let Some((_, standing)) = self.read_observation {
            let Some(peak) = remaining.peak_allocation_bytes.checked_sub(standing) else {
                self.read_observation = None;
                return Err(WorkError::BudgetExceeded {
                    counter: "peak_allocation_bytes",
                    observed: standing,
                    maximum: remaining.peak_allocation_bytes,
                });
            };
            remaining.peak_allocation_bytes = peak;
        }
        Ok(remaining)
    }

    pub(crate) fn read_receipt(
        &self,
        result: Result<ObjectRead, ObjectStoreError>,
    ) -> ObjectResult<ObjectRead> {
        let mut work = self.spent();
        if let Some((observation, standing)) = self.read_observation {
            observation
                .record_completed_leaf_work(work)
                .map_err(|error| crate::ObjectFailure::new(error.into(), work))?;
            if standing != 0 {
                work = WorkCounters::UNCHARGED
                    .with_backend(work, standing)
                    .map_err(|error| crate::ObjectFailure::new(error.into(), work))?;
                if result.is_ok() {
                    work.verify(self.budget)
                        .map_err(|error| crate::ObjectFailure::new(error.into(), work))?;
                }
            }
        }
        result
            .map(|value| crate::OperationReceipt { value, work })
            .map_err(|error| crate::ObjectFailure::new(error, work))
    }

    pub(crate) fn check(&self) -> Result<(), AuthorityStoreError> {
        self.admit(WorkCounters::UNCHARGED)
    }

    pub(crate) fn admit(&self, mut delta: WorkCounters) -> Result<(), AuthorityStoreError> {
        self.cancellation
            .check()
            .map_err(|_| AuthorityStoreError::Cancelled)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = self.original {
            return original.admit(delta).map_err(Into::into);
        }
        self.work.verify(self.budget)?;
        delta.peak_allocation_bytes = delta
            .peak_allocation_bytes
            .max(self.allocations.live_bytes())
            .checked_add(self.opaque_live_upper_bytes)
            .ok_or(WorkError::Overflow)?;
        self.work.admit(&delta, &self.budget).map_err(Into::into)
    }

    pub(crate) fn operation(&self) -> Result<AuthorityWork<'a>, AuthorityStoreError> {
        self.check()?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = self.original {
            return Ok(Self::native(original, self.cancellation));
        }
        Ok(Self::new(self.remaining()?, self.cancellation))
    }

    pub(crate) fn charge(&mut self, delta: WorkCounters) -> Result<(), AuthorityStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = self.original {
            let result = original.record_admitted(delta);
            return result.map_err(AuthorityStoreError::from);
        }
        self.work.try_add_assign(&delta)?;
        self.check()
    }

    pub(crate) fn vec_owned<T>(
        &mut self,
        capacity: usize,
    ) -> Result<(Vec<T>, AuthorityAllocation), AuthorityStoreError> {
        self.check()?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = self.original {
            let buffer = crate::native_host::buffer::<T, _>(capacity, &mut &*original)?;
            self.check()?;
            return Ok((buffer.0, Some(buffer.1)));
        }
        let bytes = capacity
            .checked_mul(std::mem::size_of::<T>())
            .ok_or(crate::WorkError::Overflow)? as u64;
        self.admit(WorkCounters {
            allocation_operations: u64::from(bytes != 0),
            peak_allocation_bytes: self
                .allocations
                .live_bytes()
                .checked_add(bytes)
                .ok_or(crate::WorkError::Overflow)?,
            ..WorkCounters::UNCHARGED
        })?;
        let allocation_error = |error: crate::kernel::allocation::AllocationError| {
            error.into_work_or(AuthorityStoreError::RejectedStatic(
                "fork allocation failed",
            ))
        };
        self.allocations
            .claim_bytes(bytes, u64::from(bytes != 0), &mut self.work, self.budget)
            .map_err(allocation_error)?;
        let mut values = Vec::new();
        values
            .try_reserve_exact(capacity)
            .map_err(|_| AuthorityStoreError::RejectedStatic("fork allocation failed"))?;
        let actual = values
            .capacity()
            .checked_mul(std::mem::size_of::<T>())
            .ok_or(crate::WorkError::Overflow)? as u64;
        // Successful reserve guarantees capacity >= the requested element count.
        self.allocations
            .record_owned_bytes(actual - bytes, &mut self.work)
            .map_err(allocation_error)?;
        self.check()?;
        Ok((values, None))
    }

    pub(crate) fn record(&mut self, delta: WorkCounters) -> Result<(), AuthorityStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if self.original.is_some() {
            return Ok(());
        }
        self.work = self.work.checked_add(delta)?;
        Ok(())
    }

    /// Records the completed provider prefix and returns its value.
    pub fn accept<T, E: From<WorkError>>(
        &mut self,
        result: crate::MeasuredResult<crate::OperationReceipt<T>, E>,
    ) -> Result<T, E> {
        let delta = match &result {
            Ok(receipt) => receipt.work,
            Err(failure) => *failure.work,
        };
        if !self.is_native() {
            self.work = self.work.checked_add(delta).map_err(E::from)?;
        }
        result
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
    }
}

/// Publication failed in the authority provider or its owner guard.
#[derive(Debug, thiserror::Error)]
pub enum AuthorityError<E> {
    /// The authority provider rejected the operation.
    #[error(transparent)]
    Authority(AuthorityStoreError),
    /// The owner guard rejected the operation.
    #[error(transparent)]
    Owner(E),
}

impl<E> AuthorityError<E> {
    /// Preserves owner errors while converting provider failures.
    pub fn into_error(self, convert: impl FnOnce(AuthorityStoreError) -> E) -> E {
        match self {
            Self::Authority(error) => convert(error),
            Self::Owner(error) => error,
        }
    }
}

impl<E> From<AuthorityStoreError> for AuthorityError<E> {
    fn from(error: AuthorityStoreError) -> Self {
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
    fn native_commit<P: StreamProvider>(
        &mut self,
        provider: &P,
        request: CommitRequest,
        deadline: Option<u64>,
        work: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = NativeStreamResult<CommitOutcome, AuthorityError<Self::Error>>> + Send
    {
        async move {
            if let Err(error) = self.seal(work).await {
                return Ok(Err(error));
            }
            let Some(original) = work.original else {
                return Ok(Err(AuthorityStoreError::NativeOriginalUnsupported.into()));
            };
            let meter = original.state.clone();
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
    type Error = AuthorityStoreError;
    async fn seal(&mut self, _: &mut AuthorityWork<'_>) -> Result<(), AuthorityError<Self::Error>> {
        Ok(())
    }
}

impl<F, Fut> AuthorityGuard for &mut F
where
    F: FnMut(WorkBudget) -> Fut + ProviderTask,
    Fut: Future<Output = FsResult<()>> + ProviderTask,
{
    type Error = FsError;
    async fn seal(
        &mut self,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), AuthorityError<Self::Error>> {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            return Err(AuthorityStoreError::NativeOriginalUnsupported.into());
        }
        let result = self(work.remaining()?).await;
        work.accept(result).map_err(AuthorityError::Owner)?;
        work.work
            .verify(work.budget)
            .map_err(AuthorityStoreError::from)?;
        Ok(())
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
        let original = work
            .original
            .ok_or(AuthorityStoreError::NativeOriginalUnsupported)?;
        (self.0)(original).await.map_err(AuthorityError::Owner)
    }

    async fn native_commit<P: StreamProvider>(
        &mut self,
        provider: &P,
        request: CommitRequest,
        deadline: Option<u64>,
        work: &mut AuthorityWork<'_>,
    ) -> NativeStreamResult<CommitOutcome, AuthorityError<E>> {
        let Some(original) = work.original else {
            return Ok(Err(AuthorityStoreError::NativeOriginalUnsupported.into()));
        };
        let meter = original.state.clone();
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
