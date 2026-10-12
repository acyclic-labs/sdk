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
pub type NativeObjectsSharedFailure = Guarded<std::sync::OnceLock<NativeObjectsError>>;

impl NativeObjectsSharedFailure {
    pub(crate) fn prepare(
        meter: &std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<std::sync::Arc<Self>, NativeObjectsError> {
        let allocation =
            acyclic_native_runtime::ReadAllocation::new(meter, std::mem::size_of::<Self>())?;
        Ok(std::sync::Arc::new(Self {
            value: std::sync::OnceLock::new(),
            _allocation: Some(allocation),
        }))
    }

    /// Borrows the same original typed cause; published aliases are always initialized.
    pub fn error(&self) -> &NativeObjectsError {
        self.value
            .get()
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
    // An actual malformed SDK wire buffer, inseparable from its original claim.
    wire: Option<(Vec<u8>, acyclic_native_runtime::ReadAllocation)>,
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
            wire: None,
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
            || self.wire.is_some()
            || self.transport_chunk.is_some()
    }

    pub(super) fn with_wire(
        mut self,
        bytes: Vec<u8>,
        allocation: acyclic_native_runtime::ReadAllocation,
    ) -> Self {
        self.wire = Some((bytes, allocation));
        self
    }

    pub(super) fn with_header(
        mut self,
        header: wire::GetObjectHeader,
        allocation: acyclic_native_runtime::ReadAllocation,
    ) -> Self {
        self.secondary_object = self.object.take();
        self.object = Some(Object::new_native(
            header,
            acyclic_stream::OwnedBytes::new(),
            Some(allocation),
        ));
        self
    }
}

/// A preallocated error slot bridging existing semantic transport body streams.
pub(crate) struct NativeTransportErrors {
    pub(crate) meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    state: std::sync::Mutex<(
        Option<NativeObjectsError>,
        acyclic_native_runtime::ReadAllocation,
    )>,
    #[cfg(feature = "grpc")]
    codec_status: std::sync::Mutex<Option<tonic::Status>>,
    _allocation: acyclic_native_runtime::ReadAllocation,
}

impl NativeTransportErrors {
    pub(super) fn new(
        meter: &std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<std::sync::Arc<Self>, NativeObjectsError> {
        meter.check()?;
        let allocation =
            acyclic_native_runtime::ReadAllocation::new(meter, std::mem::size_of::<Self>())?;
        Ok(std::sync::Arc::new(Self {
            meter: std::sync::Arc::clone(meter),
            state: std::sync::Mutex::new((
                None,
                acyclic_native_runtime::ReadAllocation::empty(meter),
            )),
            #[cfg(feature = "grpc")]
            codec_status: std::sync::Mutex::new(None),
            _allocation: allocation,
        }))
    }

    pub(crate) fn record(&self, error: NativeObjectsError) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.0.is_none() {
            state.0 = Some(error);
        }
    }
    pub(crate) fn take(&self) -> Option<NativeObjectsError> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .0
            .take()
    }

    pub(crate) fn retain_wire(
        &self,
        mut allocation: acyclic_native_runtime::ReadAllocation,
    ) -> Result<(), NativeObjectsError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.1.absorb(&mut allocation)?;
        Ok(())
    }

    #[cfg(feature = "grpc")]
    pub(crate) fn take_wire(
        &self,
        bytes: usize,
    ) -> Result<acyclic_native_runtime::ReadAllocation, NativeObjectsError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.1.split(bytes).map_err(NativeObjectsError::from)
    }

    #[cfg(feature = "grpc")]
    pub(crate) fn prepare_codec_failure(&self) -> Result<(), NativeObjectsError> {
        let meter = &self.meter;
        meter.check()?;
        let owner = meter
            .allocation_owner()
            .unwrap_or_else(|| std::sync::Arc::clone(meter));
        if !std::sync::Arc::ptr_eq(self._allocation.meter(), &owner) {
            return Err(NativeObjectsError::from(NativeObjectsCause::Admission(
                std::io::ErrorKind::InvalidInput.into(),
            )));
        }
        let mut slot = self
            .codec_status
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if slot.is_none() {
            // Admit the opaque status control before codec I/O.
            meter.allocate_refcount_header()?;
            *slot = Some(tonic::Status::new(tonic::Code::Cancelled, ""));
        }
        Ok(())
    }

    #[cfg(feature = "grpc")]
    pub(crate) fn codec_failure(&self) -> Option<tonic::Status> {
        self.codec_status
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
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
