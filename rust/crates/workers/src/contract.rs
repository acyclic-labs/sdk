//! Workers declarations own the schema; maintained generators own wire and transport code.
use protify::*;
mod generated {
    use super::*;
    #[allow(
        dead_code,
        missing_docs,
        clippy::allow_attributes_without_reason,
        reason = "maintained semantic/schema derivation"
    )]
    pub mod domain {
        include!("domain.rs");
    }
    pub use domain::*;
    proto_package!(
        WORKERS_PACKAGE,
        name = "acyclic.workers.v1",
        files = [WORKERS_FILE]
    );
    define_proto_file!(
        WORKERS_FILE,
        name = "workers/v1/workers.proto",
        package = WORKERS_PACKAGE,
        options = [
            proto_option!("go_package" => "github.com/acyclic-labs/sdk/go/gen/workers/v1;workersv1")
        ],
        messages = [
            CodeVersionProto,
            DeploymentProto,
            PublishVersionRequestProto,
            PublishVersionResponseProto,
            SelectDeploymentRequestProto,
            SelectDeploymentResponseProto,
            ObjectRefProto,
            PayloadProto,
            JobResultProto,
            JobLimitsProto,
            RetryPolicyProto,
            JobTargetProto,
            SubmitJobRequestProto,
            SubmitJobResponseProto,
            JobObservationProto,
            InspectJobRequestProto,
            InspectJobResponseProto,
            CancelJobRequestProto,
            CancelJobResponseProto,
            HeaderProto,
            InvokeVersionRequestProto,
            InvokeDeploymentRequestProto,
            InvokeResponseProto,
            ErrorProto
        ],
        enums = [JobState, ErrorCode],
        services = [WorkersService]
    );
    /// Public Workers v1 operations over the Rust-owned contract.
    #[proto_service]
    pub enum WorkersService {
        /// Publish exact module bytes under their expected immutable digest.
        PublishVersion {
            request: PublishVersionRequestProto,
            response: PublishVersionResponseProto,
        },
        /// Select a deployment version with the supplied revision precondition.
        SelectDeployment {
            request: SelectDeploymentRequestProto,
            response: SelectDeploymentResponseProto,
        },
        /// Accept durable run-handler work against a resolved immutable version.
        SubmitJob {
            request: SubmitJobRequestProto,
            response: SubmitJobResponseProto,
        },
        /// Read the current durable job observation.
        InspectJob {
            request: InspectJobRequestProto,
            response: InspectJobResponseProto,
        },
        /// Request durable job cancellation with idempotent admission.
        CancelJob {
            request: CancelJobRequestProto,
            response: CancelJobResponseProto,
        },
        /// Invoke exact immutable code with ordinary HTTP request semantics.
        InvokeVersion {
            request: InvokeVersionRequestProto,
            response: InvokeResponseProto,
        },
        /// Resolve a deployment alias once and invoke its selected immutable code.
        InvokeDeployment {
            request: InvokeDeploymentRequestProto,
            response: InvokeResponseProto,
        },
    }
}
pub(crate) use generated::WORKERS_FILE;
/// Strong semantic types.
pub use generated::domain;
/// Public wire shadows.
pub use generated::{
    CancelJobRequestProto as CancelJobRequest, CancelJobResponseProto as CancelJobResponse,
    CodeVersionProto as CodeVersion, DeploymentProto as Deployment, ErrorCode, ErrorProto as Error,
    HeaderProto as Header, InspectJobRequestProto as InspectJobRequest,
    InspectJobResponseProto as InspectJobResponse,
    InvokeDeploymentRequestProto as InvokeDeploymentRequest, InvokeResponseProto as InvokeResponse,
    InvokeVersionRequestProto as InvokeVersionRequest, JobLimitsProto as JobLimits,
    JobObservationProto as JobObservation, JobResultProto as JobResult, JobState,
    JobTargetProto as JobTarget, ObjectRefProto as ObjectRef, PayloadProto as Payload,
    PublishVersionRequestProto as PublishVersionRequest,
    PublishVersionResponseProto as PublishVersionResponse, RetryPolicyProto as RetryPolicy,
    SelectDeploymentRequestProto as SelectDeploymentRequest,
    SelectDeploymentResponseProto as SelectDeploymentResponse,
    SubmitJobRequestProto as SubmitJobRequest, SubmitJobResponseProto as SubmitJobResponse,
};
/// Published payload source oneof.
pub mod payload {
    pub use super::generated::domain::payload::SourceProto as Source;
}
/// Published job target oneof.
pub mod job_target {
    pub use super::generated::domain::job_target::TargetProto as Target;
}
/// Render the canonical schema from these executable declarations.
pub fn render_proto_files(root: impl AsRef<std::path::Path>) -> std::io::Result<()> {
    std::fs::create_dir_all(root.as_ref().join("workers/v1"))?;
    generated::WORKERS_PACKAGE::get_package().render_files(root.as_ref())
}
