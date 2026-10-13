use super::*;

/// The exact native failure category, independent of retained physical payloads.
#[derive(Debug, thiserror::Error)]
pub enum NativeObjectsCause {
    /// The unchanged logical service failure.
    #[error(transparent)]
    Service(Error),
    /// The original native admission, cancellation or I/O error.
    #[error(transparent)]
    Admission(std::io::Error),
    /// The custom provider does not implement the original-meter producer.
    #[error("provider has no native immutable read producer")]
    Unsupported,
    /// The unchanged actual failure shared by a grouped publication.
    #[error(transparent)]
    SharedFailure(std::sync::Arc<NativeObjectsSharedFailure>),
}

/// One preadmitted actual grouped failure, retaining its original cause to last alias.
pub type NativeObjectsSharedFailure = Guarded<Option<NativeObjectsError>>;

impl NativeObjectsSharedFailure {
    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    pub(crate) fn prepare(
        meter: &std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<std::sync::Arc<Self>, NativeObjectsError> {
        let allocation =
            acyclic_native_runtime::ReadAllocation::new(meter, std::mem::size_of::<Self>())?;
        Ok(std::sync::Arc::new(Self {
            value: None,
            _allocation: Some(allocation),
        }))
    }

    /// Borrows the same original typed cause; published aliases are always initialized.
    pub fn error(&self) -> &NativeObjectsError {
        self.value
            .as_ref()
            .expect("only an initialized original failure is published")
    }
}

impl std::fmt::Display for NativeObjectsSharedFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self.error(), formatter)
    }
}

impl std::error::Error for NativeObjectsSharedFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.error())
    }
}

/// An original native failure retaining any completed physical data and its custody.
#[derive(Debug)]
pub struct NativeObjectsError {
    /// The primary unflattened error category.
    pub cause: NativeObjectsCause,
    /// The actual decoded object, intrinsically retaining its header custody.
    pub object: Option<Object>,
    /// An additional actual decoded object retained by a nested transport failure.
    pub secondary_object: Option<Object>,
    /// An actual decoded acknowledgement retained by a failed publication.
    pub receipt: Option<ObjectsPutReceipt>,
    /// An opaque transport chunk retained on a failed bounded collection.
    pub transport_chunk: Option<bytes::Bytes>,
}

impl From<NativeObjectsCause> for NativeObjectsError {
    fn from(cause: NativeObjectsCause) -> Self {
        Self {
            cause,
            object: None,
            secondary_object: None,
            receipt: None,
            transport_chunk: None,
        }
    }
}
impl From<Error> for NativeObjectsError {
    fn from(error: Error) -> Self {
        NativeObjectsError::from(NativeObjectsCause::Service(error))
    }
}
impl From<std::io::Error> for NativeObjectsError {
    fn from(error: std::io::Error) -> Self {
        NativeObjectsError::from(NativeObjectsCause::Admission(error))
    }
}
impl std::fmt::Display for NativeObjectsError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.cause, formatter)
    }
}
impl std::error::Error for NativeObjectsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        if self.has_payload() {
            None
        } else {
            std::error::Error::source(&self.cause)
        }
    }
}
impl NativeObjectsError {
    /// Destroys retained physical data before returning its original typed cause.
    pub fn into_cause(self) -> NativeObjectsCause {
        self.cause
    }

    /// Reports whether this failure retains physical data or metadata custody.
    pub fn has_payload(&self) -> bool {
        self.object.is_some()
            || self.secondary_object.is_some()
            || self.receipt.is_some()
            || self.transport_chunk.is_some()
    }
}

pub(crate) fn native_preparation_error(
    error: acyclic_stream::PreparedOwnedBytesError,
) -> NativeObjectsError {
    match error {
        acyclic_stream::PreparedOwnedBytesError::Admission(error) => error.into(),
        acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized => NativeObjectsError::from(
            NativeObjectsCause::Admission(std::io::ErrorKind::InvalidData.into()),
        ),
    }
}
