use super::*;

/// Preserves original observer failure separately from the provider's error.
pub type NativeStreamResult<T, E = StreamError> = std::io::Result<Result<NativeStreamOutput<T>, E>>;

/// A finite cursor preserves late original failures after its actual prefix.
pub type NativeRecordStream =
    BoxProviderStream<'static, std::io::Result<Result<NativeStreamOutput<Record>, StreamError>>>;

/// Object-safe provider future whose real heap frame is admitted before boxing.
pub enum NativeProviderFuture<'a, T, E = StreamError> {
    /// No physical future frame was allocated.
    Ready(Option<NativeStreamResult<T, E>>),
    /// The boxed frame drops before its original-ledger allocation guard.
    Funded {
        /// Canonical producer; no alternate request or work root.
        future: std::pin::Pin<Box<dyn Future<Output = NativeStreamResult<T, E>> + Send + 'a>>,
        /// Actual frame storage on the original ledger.
        allocation: acyclic_native_runtime::ReadAllocation,
    },
}

impl<'a, T, E> NativeProviderFuture<'a, T, E> {
    /// Admits the exact concrete frame before its sole physical allocation.
    pub fn new<F>(meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>, future: F) -> Self
    where
        F: Future<Output = NativeStreamResult<T, E>> + Send + 'a,
    {
        match Self::try_new(meter, future) {
            Ok(future) => future,
            Err(error) => Self::Ready(Some(Err(error))),
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
        let bytes = std::mem::size_of::<F>();
        let allocation = if bytes == 0 {
            acyclic_native_runtime::ReadAllocation::empty(meter)
        } else {
            acyclic_native_runtime::ReadAllocation::new(meter, bytes)?
        };
        Ok(Self::Funded {
            future: Box::pin(future),
            allocation,
        })
    }

    /// Returns a result without allocating a provider future.
    pub fn ready(result: NativeStreamResult<T, E>) -> Self {
        Self::Ready(Some(result))
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
            Self::Ready(result) => std::task::Poll::Ready(
                result
                    .take()
                    .expect("native provider future polled after completion"),
            ),
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
