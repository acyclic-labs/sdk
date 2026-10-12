use super::*;

/// Publication failure retaining the source's observed completion.
#[derive(Debug, Error)]
#[error("{cause}")]
pub struct NativeMountPublicationFailure<E> {
    /// Original publication or owner-guard cause.
    #[source]
    pub cause: E,
    /// Distinct accounting failure after the original cause.
    pub accounting_failure: Option<MountSourceError>,
    /// Whether a real source result or DATA ACK was observed.
    pub source_acknowledged: bool,
    /// Qualified source metadata retained through finalization failure.
    pub publication: Option<crate::NativePublicationSnapshot>,
}
