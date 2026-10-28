use crate::{Error, Object, ObjectsPutReceipt};

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
}

/// An original native failure retaining any completed physical data and its custody.
#[derive(Debug)]
pub struct NativeObjectsError {
    /// The primary unflattened error category.
    pub cause: NativeObjectsCause,
    /// The actual decoded object, intrinsically retaining its header custody.
    pub object: Option<Box<Object>>,
    /// An actual decoded acknowledgement retained by a failed publication.
    pub receipt: Option<Box<ObjectsPutReceipt>>,
}

impl From<NativeObjectsCause> for NativeObjectsError {
    fn from(cause: NativeObjectsCause) -> Self {
        Self {
            cause,
            object: None,
            receipt: None,
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
        self.object.is_some() || self.receipt.is_some()
    }
}
