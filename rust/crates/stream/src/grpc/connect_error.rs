/// Connection configuration failure.
#[derive(Debug, Error)]
pub enum ConnectError {
    /// Endpoint URI was invalid or the TLS connection failed.
    #[error("invalid or unavailable Stream endpoint: {0}")]
    Endpoint(#[from] tonic::transport::Error),
    /// Customer endpoints must use authenticated TLS.
    #[error("Stream endpoints must use https")]
    InsecureEndpoint,
    /// Bearer credential cannot be represented as HTTP metadata.
    #[error("invalid Stream bearer credential")]
    InvalidCredential,
    /// Caller-supplied private CA bundle is empty or exceeds its fixed bound.
    #[error("invalid Stream private CA certificate")]
    InvalidCaCertificate,
    /// At least one independently reachable endpoint is required.
    #[error("at least one Stream endpoint is required")]
    NoEndpoints,
    /// Endpoint count or URI bytes exceed the fixed client bound.
    #[error("Stream endpoint pool exceeds its fixed bound")]
    EndpointLimit,
}
