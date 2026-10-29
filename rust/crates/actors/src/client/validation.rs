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

/// Apply operation-specific constraints on shared deployment envelopes before I/O.
pub(crate) fn validate_operation(
    request_name: &str,
    deployment: &wire::DeploymentRequest,
) -> Result<(), Error> {
    // RPC descriptors use UpperCamelCase; HTTP_ROUTES exposes lowerCamelCase.
    // Both dispatchers select their operation before reaching this shared check.
    if request_name.eq_ignore_ascii_case("DeployWorker")
        || request_name.eq_ignore_ascii_case("DeployNamespace")
    {
        if request_name.eq_ignore_ascii_case("DeployNamespace") {
            if deployment.limits.is_none() || deployment.worker_limits.is_some() {
                return Err(Error::Configuration(
                    "namespace deploy requires actor limits".into(),
                ));
            }
            return Ok(());
        }
        crate::addressing::validate_dns_label(deployment.name.as_bytes())
            .map_err(|e| Error::Configuration(e.to_string()))?;
        if deployment.worker_limits.is_none()
            || deployment.limits.is_some()
            || !deployment.subscriptions.is_empty()
            || !deployment.jurisdiction.is_empty()
        {
            return Err(Error::Configuration(
                "Worker deploy cannot carry actor subscriptions or jurisdiction".into(),
            ));
        }
    }
    Ok(())
}
