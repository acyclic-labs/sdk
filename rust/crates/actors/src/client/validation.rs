use super::{Error, wire};

macro_rules! validate_operation_request {
    (DeploymentRequest, $name:expr, $request:expr) => {
        $crate::client::validate_operation($name, $request)
    };
    ($input:ident, $name:expr, $request:expr) => {{
        let _ = $name;
        Ok::<(), $crate::client::Error>(())
    }};
}
pub(crate) use validate_operation_request;

/// Convert semantic deployment admission failures to the local transport error.
pub(crate) fn validate_operation(
    request_name: &str,
    deployment: &wire::DeploymentRequest,
) -> Result<(), Error> {
    crate::contract::validate_deployment_operation(request_name, deployment)
        .map_err(|message| Error::Configuration(message.into()))
}
