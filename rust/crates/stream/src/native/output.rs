use super::*;

/// Preserves original observer failure separately from the provider's error.
pub type NativeStreamResult<T, E = StreamError> = std::io::Result<Result<NativeStreamOutput<T>, E>>;

/// The selected opening cut and original cursor preserve late native failures.
pub type NativeRecordStream =
    crate::Opened<Result<NativeStreamOutput<Record>, StreamError>, std::io::Error>;

/// A failure known before any provider frame exists.
///
/// It holds no output, so a future awaiting a native operation is never as
/// large as that operation's output.
pub enum NativeRefusal<E = StreamError> {
    /// The original observer or ledger failed.
    Io(std::io::Error),
    /// The provider refused the operation.
    Provider(E),
}

impl<E> NativeRefusal<E> {
    fn into_result<T>(self) -> NativeStreamResult<T, E> {
        match self {
            Self::Io(error) => Err(error),
            Self::Provider(error) => Ok(Err(error)),
        }
    }
}

/// Object-safe provider future whose real heap frame is admitted before boxing.
///
/// Every output comes from a funded frame; only a refusal completes without one.
pub enum NativeProviderFuture<'a, T, E = StreamError> {
    /// No physical future frame was allocated.
    Refused(Option<NativeRefusal<E>>),
    /// The boxed frame drops before its original-ledger allocation guard.
    Funded {
        /// Canonical producer; no alternate request or work root.
        future: std::pin::Pin<Box<dyn Future<Output = NativeStreamResult<T, E>> + Send + 'a>>,
        /// Actual frame storage on the original ledger.
        allocation: acyclic_native_runtime::ReadAllocation,
    },
}

// A future awaiting a native operation never grows with that operation's output.
const _: () = assert!(
    std::mem::size_of::<NativeProviderFuture<'static, [u8; 4096]>>()
        == std::mem::size_of::<NativeProviderFuture<'static, ()>>()
);

impl<'a, T, E> NativeProviderFuture<'a, T, E> {
    /// Admits the exact concrete frame before its sole physical allocation.
    pub fn new<F>(meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>, future: F) -> Self
    where
        F: Future<Output = NativeStreamResult<T, E>> + Send + 'a,
    {
        match Self::try_new(meter, future) {
            Ok(future) => future,
            Err(error) => Self::failed(error),
        }
    }

    /// Builds the same funded frame, reporting admission failure before a prepared ingress can be issued to its caller.
    pub(crate) fn try_new<F>(
        meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
        future: F,
    ) -> std::io::Result<Self>
    where
        F: Future<Output = NativeStreamResult<T, E>> + Send + 'a,
    {
        Self::try_build(meter, || future)
    }

    /// Admits the concrete frame before constructing its completion owner.
    pub(crate) fn try_build<F>(
        meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
        build: impl FnOnce() -> F,
    ) -> std::io::Result<Self>
    where
        F: Future<Output = NativeStreamResult<T, E>> + Send + 'a,
    {
        let bytes = std::mem::size_of::<F>();
        let allocation = if bytes == 0 {
            acyclic_native_runtime::ReadAllocation::empty(meter)
        } else {
            acyclic_native_runtime::ReadAllocation::new(meter, bytes)?
        };
        Ok(Self::Funded {
            future: Box::pin(build()),
            allocation,
        })
    }

    pub(crate) fn observed<F, K>(
        meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
        span: acyclic_native_runtime::obs::OperationSpan,
        future: F,
        error: K,
    ) -> Self
    where
        T: 'a,
        E: 'a,
        F: Future<Output = NativeStreamResult<T, E>> + Send + 'a,
        K: FnOnce(&NativeStreamResult<T, E>) -> Option<&'static str> + Send + 'a,
    {
        match Self::try_observed(meter, span, future, error) {
            Ok(future) => future,
            Err(error) => Self::failed(error),
        }
    }

    pub(crate) fn try_observed<F, K>(
        meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
        span: acyclic_native_runtime::obs::OperationSpan,
        future: F,
        error: K,
    ) -> std::io::Result<Self>
    where
        T: 'a,
        E: 'a,
        F: Future<Output = NativeStreamResult<T, E>> + Send + 'a,
        K: FnOnce(&NativeStreamResult<T, E>) -> Option<&'static str> + Send + 'a,
    {
        Self::try_build(meter, || {
            span.scope(acyclic_observability::observe_output(&span, future, error))
        })
        .inspect_err(|error| span.record_error(Some(error)))
    }

    pub(crate) fn refused_observed(
        span: acyclic_native_runtime::obs::OperationSpan,
        error: E,
        kind: impl FnOnce(&E) -> &'static str,
    ) -> Self {
        Self::refused(span.in_scope(|| {
            acyclic_observability::Completion::new(&span).complete(error, |error| Some(kind(error)))
        }))
    }

    /// Refuses without allocating a provider future.
    pub fn refused(error: E) -> Self {
        Self::Refused(Some(NativeRefusal::Provider(error)))
    }

    /// Reports an observer or ledger failure without allocating a provider future.
    pub fn failed(error: std::io::Error) -> Self {
        Self::Refused(Some(NativeRefusal::Io(error)))
    }
}

impl<T, E> Unpin for NativeProviderFuture<'_, T, E> {}

impl<T, E> Future for NativeProviderFuture<'_, T, E> {
    type Output = NativeStreamResult<T, E>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match self.get_mut() {
            Self::Refused(refusal) => std::task::Poll::Ready(refusal.take().map_or_else(
                || Err(std::io::ErrorKind::InvalidInput.into()),
                NativeRefusal::into_result,
            )),
            Self::Funded { future, .. } => future.as_mut().poll(context),
        }
    }
}

/// A value retaining its original native allocation until storage retires.
pub struct NativeStreamOutput<T> {
    pub(crate) value: T,
    pub(crate) allocation: acyclic_native_runtime::ReadAllocation,
}
impl<T> NativeStreamOutput<T> {
    /// Transfers an admitted value and its allocation owner.
    pub fn new(value: T, allocation: acyclic_native_runtime::ReadAllocation) -> Self {
        Self { value, allocation }
    }
    /// Borrows the retained value.
    pub fn value(&self) -> &T {
        &self.value
    }
    /// Borrows the retained value mutably.
    pub fn value_mut(&mut self) -> &mut T {
        &mut self.value
    }
    /// Transfers the value and its original allocation owner.
    pub fn into_parts(self) -> (T, acyclic_native_runtime::ReadAllocation) {
        (self.value, self.allocation)
    }
    pub(crate) fn map<U>(self, convert: impl FnOnce(T) -> U) -> NativeStreamOutput<U> {
        NativeStreamOutput {
            value: convert(self.value),
            allocation: self.allocation,
        }
    }
}
