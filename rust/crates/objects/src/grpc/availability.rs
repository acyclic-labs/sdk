use super::*;
use crate::availability::client::{Credential, observe};
use futures::StreamExt as _;

impl GrpcObjects {
    pub(super) async fn observe_availability(
        &self,
        digest: Digest,
    ) -> Result<crate::Availability, Error> {
        // Only opening is finite. An idle observation has no reconnect timer.
        tokio::time::timeout(REQUEST_TIMEOUT, async {
            let credential = Credential::resolve(Some(&self.credential)).await?;
            let message = wire::AvailabilityRequest {
                digest: Some(digest.into()),
            };
            let lease = credential
                .lease
                .as_ref()
                .ok_or(Error::from(wire::ErrorCode::AccessDenied))?;
            let request = acyclic_auth::channel::retained_request(message, lease)
                .map_err(|status| error_from_status(&status))?;
            let response = Self::client(acyclic_auth::channel::guard(self.channel.clone()))
                .availability(request)
                .await;
            if response.as_ref().is_err_and(|status| {
                status.code() == tonic::Code::Unauthenticated
                    && std::error::Error::source(status).is_none()
            }) && let Some(lease) = &credential.lease
            {
                lease
                    .reject()
                    .await
                    .map_err(|_| Error::from(wire::ErrorCode::AccessDenied))?;
            }
            let response = response.map_err(|status| error_from_status(&status))?;
            observe(
                response
                    .into_inner()
                    .map(|event| event.map_err(|status| error_from_status(&status))),
                credential,
            )
            .await
        })
        .await
        .map_err(|_| Error::from(wire::ErrorCode::DeadlineElapsed))?
    }
}
