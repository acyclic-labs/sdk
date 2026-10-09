//! Consumer signatures for every source-defined Workers remote operation.
use acyclic_workers::{CancellationToken, Client, Failure, WorkersCallOptions, domain};

type Requests = (
    domain::PublishVersionRequest,
    domain::SelectDeploymentRequest,
    domain::SubmitJobRequest,
    domain::InspectJobRequest,
    domain::CancelJobRequest,
    domain::InvokeVersionRequest,
    domain::InvokeDeploymentRequest,
);
type Responses = (
    domain::PublishVersionResponse,
    domain::SelectDeploymentResponse,
    domain::SubmitJobResponse,
    domain::InspectJobResponse,
    domain::CancelJobResponse,
    domain::InvokeResponse,
    domain::InvokeResponse,
);

#[allow(
    dead_code,
    reason = "compile-time coverage of all typed remote signatures"
)]
async fn all_operations(client: &Client, requests: Requests) -> Result<Responses, Failure> {
    Ok((
        client
            .publish_version(
                requests.0,
                WorkersCallOptions::default(),
                CancellationToken::new(),
            )
            .await?,
        client
            .select_deployment(
                requests.1,
                WorkersCallOptions::default(),
                CancellationToken::new(),
            )
            .await?,
        client
            .submit_job(
                requests.2,
                WorkersCallOptions::default(),
                CancellationToken::new(),
            )
            .await?,
        client
            .inspect_job(
                requests.3,
                WorkersCallOptions::default(),
                CancellationToken::new(),
            )
            .await?,
        client
            .cancel_job(
                requests.4,
                WorkersCallOptions::default(),
                CancellationToken::new(),
            )
            .await?,
        client
            .invoke_version(
                requests.5,
                WorkersCallOptions::default(),
                CancellationToken::new(),
            )
            .await?,
        client
            .invoke_deployment(
                requests.6,
                WorkersCallOptions::default(),
                CancellationToken::new(),
            )
            .await?,
    ))
}

fn main() {
    fn error<E: std::error::Error>() {}
    error::<Failure>();
}
