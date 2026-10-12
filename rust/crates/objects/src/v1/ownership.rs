use super::*;

/// A decoded acknowledgement inseparable from its original metadata custody.
#[cfg(not(target_arch = "wasm32"))]
pub type ObjectsPutReceipt = Guarded<wire::ObjectInfo>;

#[cfg(not(target_arch = "wasm32"))]
impl ObjectsPutReceipt {
    /// Retains decoded metadata and its original allocation claim.
    pub fn new(
        value: wire::ObjectInfo,
        allocation: Option<acyclic_native_runtime::ReadAllocation>,
    ) -> Self {
        Self {
            value,
            _allocation: allocation,
        }
    }
    /// Borrows the acknowledgement and keeps its metadata claim attached.
    pub const fn info(&self) -> &wire::ObjectInfo {
        &self.value
    }
}

/// A genuine ordered batch owning its original result-vector custody.
#[cfg(not(target_arch = "wasm32"))]
pub type ObjectsBatch<T> = Guarded<std::vec::IntoIter<Result<T, NativeObjectsError>>>;

#[cfg(not(target_arch = "wasm32"))]
impl<T> ObjectsBatch<T> {
    /// Transfers the already prepared result vector and its exact original claim.
    pub fn new(
        results: Vec<Result<T, NativeObjectsError>>,
        allocation: acyclic_native_runtime::ReadAllocation,
    ) -> Result<Self, NativeObjectsError> {
        let bytes = results
            .capacity()
            .checked_mul(std::mem::size_of::<Result<T, NativeObjectsError>>());
        if bytes.and_then(|bytes| u64::try_from(bytes).ok()) != Some(allocation.bytes()) {
            drop(results);
            drop(allocation);
            return Err(NativeObjectsError::from(NativeObjectsCause::Admission(
                std::io::ErrorKind::InvalidData.into(),
            )));
        }
        Ok(Self {
            value: results.into_iter(),
            _allocation: Some(allocation),
        })
    }

    /// Borrows the actual ordered results without separating their original claim.
    pub fn results(&self) -> &[Result<T, NativeObjectsError>] {
        self.value.as_slice()
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl<T> Iterator for ObjectsBatch<T> {
    type Item = Result<T, NativeObjectsError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.value.next()
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.value.size_hint()
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl<T> ExactSizeIterator for ObjectsBatch<T> {}

#[cfg(not(target_arch = "wasm32"))]
type GuardAllocation = Option<acyclic_native_runtime::ReadAllocation>;
#[cfg(target_arch = "wasm32")]
type GuardAllocation = ();

pin_project_lite::pin_project! {
    /// Storage that retires before its allocation guard.
    #[doc(hidden)]
    #[derive(Debug)]
    pub struct Guarded<T> {
        #[pin]
        pub(crate) value: T,
        pub(crate) _allocation: GuardAllocation,
    }
}
impl<F: Future> Future for Guarded<F> {
    type Output = F::Output;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.project().value.poll(context)
    }
}

impl<S: futures::Stream> futures::Stream for Guarded<S> {
    type Item = S::Item;
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.project().value.poll_next(cx)
    }
}

impl<T: PartialEq> PartialEq for Guarded<T> {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl<T: AsRef<[u8]>> AsRef<[u8]> for Guarded<T> {
    fn as_ref(&self) -> &[u8] {
        self.value.as_ref()
    }
}

/// A preadmitted boxed native future.
#[cfg(not(target_arch = "wasm32"))]
pub type ObjectsNativeFuture<'a, T> =
    Guarded<std::pin::Pin<Box<dyn Future<Output = Result<T, NativeObjectsError>> + Send + 'a>>>;

/// Admits the actual concrete native future before its physical box allocation.
#[cfg(not(target_arch = "wasm32"))]
pub fn native_future<'a, T: 'a, F>(
    meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    future: F,
) -> Result<ObjectsNativeFuture<'a, T>, NativeObjectsError>
where
    F: Future<Output = Result<T, NativeObjectsError>> + Send + 'a,
{
    meter.check()?;
    let allocation = acyclic_native_runtime::ReadAllocation::new(&meter, std::mem::size_of::<F>())?;
    Ok(Guarded {
        value: Box::pin(future),
        _allocation: Some(allocation),
    })
}

/// Checks only existing input custody identity, never namespace authority.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn native_input_meter<T>(
    meter: &std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    input: &ObjectsNativeRequest<T>,
) -> Result<(), NativeObjectsError> {
    let owner = meter
        .allocation_owner()
        .unwrap_or_else(|| std::sync::Arc::clone(meter));
    if !std::sync::Arc::ptr_eq(input.allocation.meter(), &owner) {
        return Err(NativeObjectsError::from(NativeObjectsCause::Admission(
            std::io::ErrorKind::InvalidInput.into(),
        )));
    }
    meter.check()?;
    Ok(())
}

/// Actual caller request data and its original, unmodified ownership guard.
#[cfg(not(target_arch = "wasm32"))]
pub struct ObjectsNativeRequest<T> {
    pub(crate) data: T,
    guard: Box<dyn Send + Sync>,
    allocation: acyclic_native_runtime::ReadAllocation,
}

#[cfg(not(target_arch = "wasm32"))]
impl<T> ObjectsNativeRequest<T> {
    /// Boxes the same original guard only after admission of that actual box.
    pub fn new<G>(
        data: T,
        guard: G,
        meter: &std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<Self, NativeObjectsError>
    where
        G: Send + Sync + 'static,
    {
        let request = (data, guard);
        meter.check()?;
        let allocation =
            acyclic_native_runtime::ReadAllocation::new(meter, std::mem::size_of::<G>())?;
        let (data, guard) = request;
        Ok(Self {
            data,
            guard: Box::new(guard),
            allocation,
        })
    }

    /// Borrows request data without detaching its original ownership.
    pub const fn data(&self) -> &T {
        &self.data
    }

    pub(crate) fn map<U>(self, map: impl FnOnce(T) -> U) -> ObjectsNativeRequest<U> {
        let Self {
            data,
            guard,
            allocation,
        } = self;
        ObjectsNativeRequest {
            data: map(data),
            guard,
            allocation,
        }
    }
}
