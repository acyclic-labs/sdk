use super::*;

/// Publication failure retaining the source's observed completion.
#[derive(Debug, Error)]
#[error("{cause}")]
pub struct NativeMountPublicationFailure<E> {
    /// Original publication or owner-guard cause.
    #[source]
    pub cause: E,
    /// Distinct accounting failure after the original cause.
    pub accounting_failure: Option<Box<MountSourceError>>,
    /// Whether a real source result or DATA ACK was observed.
    pub source_acknowledged: bool,
    /// Qualified source metadata retained through finalization failure.
    pub publication: Option<Box<crate::NativePublicationSnapshot>>,
}

impl<E: From<MountLifecycleError>> From<crate::NativeTreeMaterializationError>
    for NativeMountPublicationFailure<E>
{
    fn from(cause: crate::NativeTreeMaterializationError) -> Self {
        Self {
            cause: E::from(MountLifecycleError::from(cause)),
            accounting_failure: None,
            source_acknowledged: false,
            publication: None,
        }
    }
}
