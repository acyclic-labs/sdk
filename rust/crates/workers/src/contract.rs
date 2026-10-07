//! Rust-owned Workers v1 contract.

use protify::*;

mod generated {
    #![allow(missing_docs)]

    use super::*;

    include!("contract_definitions.rs");
}

#[doc = "Workers protobuf package schema handle."]
pub use generated::WORKERS_PACKAGE;

/// Renders the canonical Workers protobuf input from the Rust declarations.
///
/// The generated `.proto` is an intermediate artifact for downstream tools;
/// these declarations are the contract authority.
pub fn render_proto_files(root: impl AsRef<std::path::Path>) -> std::io::Result<()> {
    let root = root.as_ref();
    std::fs::create_dir_all(root.join("workers/v1"))?;
    WORKERS_PACKAGE::get_package().render_files(root)
}

#[allow(unused_imports)]
pub use generated::{
    CancelJobRequest, CancelJobResponse, CodeVersion, Deployment, Error, ErrorCode, Header,
    InspectJobRequest, InspectJobResponse, InvokeDeploymentRequest, InvokeResponse,
    InvokeVersionRequest, JobLimits, JobObservation, JobResult, JobState, JobTarget, ObjectRef,
    Payload, PublishVersionRequest, PublishVersionResponse, RetryPolicy, SelectDeploymentRequest,
    SelectDeploymentResponse, SubmitJobRequest, SubmitJobResponse, WorkersService, job_target,
    payload,
};
