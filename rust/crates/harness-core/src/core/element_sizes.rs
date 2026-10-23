//! Inline storage bounds for records retained in checkpoints.
use super::*;
use std::mem::size_of;

macro_rules! ceiling {
    ($name:ident, $ty:ty, $maximum:expr) => {
        #[test]
        fn $name() {
            let bytes = size_of::<$ty>();
            println!("{}: {bytes}", stringify!($ty));
            assert!(
                bytes <= $maximum,
                "{} is {bytes} bytes, ceiling {}",
                stringify!($ty),
                $maximum
            );
        }
    };
}
ceiling!(reducer, Reducer, 1128);
ceiling!(snapshot, Snapshot, 1104);
ceiling!(projection, snapshot::Projection, 968);
ceiling!(event, Event, 376);
ceiling!(event_payload, EventPayload, 152);
ceiling!(effect_status, EffectStatus, 24);
ceiling!(effect_state, EffectState, 712);
ceiling!(extension_binding, ExtensionBinding, 192);
ceiling!(extension_admission, ExtensionAdmission, 88);
ceiling!(command, Command, 336);
ceiling!(action, Action, 88);
ceiling!(scope, Scope, 160);
ceiling!(recorded_scope, RecordedScope, 96);
ceiling!(capabilities, crate::Capabilities, 24);
ceiling!(extension_state_migration, ExtensionStateMigration, 408);
ceiling!(extension_record, ExtensionRecord, 368);
ceiling!(extension_configuration, ExtensionConfiguration, 336);
ceiling!(extension_dependency, ExtensionDependency, 32);
ceiling!(authority, Authority, 32);
ceiling!(event_reference, EventReference, 40);
ceiling!(fork_seed, ForkSeed, 2264);
ceiling!(fork_selection, crate::fork::ForkSelection, 80);
ceiling!(fork_omission, crate::fork::ForkOmission, 104);
ceiling!(captured_resource, crate::fork::CapturedResource, 144);
ceiling!(resource_revision, crate::fork::ResourceRevision, 72);
ceiling!(capture, crate::fork::Capture, 24);
ceiling!(fork_history_policy, crate::fork::ForkHistoryPolicy, 16);
ceiling!(shared_grant, crate::fork::SharedGrant, 176);
ceiling!(reference_grant, crate::fork::ReferenceGrant, 560);
ceiling!(summary_fork_capture, crate::fork::SummaryForkCapture, 672);
ceiling!(attested_boundary, crate::fork::AttestedBoundary, 96);
ceiling!(project_merge_receipt, ProjectMergeReceipt, 1504);
ceiling!(conversation_message, ConversationMessage, 648);
ceiling!(conversation_state, ConversationState, 424);
ceiling!(model_context_selection, ModelContextSelection, 304);
ceiling!(file_ref, FileRef, 272);
ceiling!(volume_ref, crate::conversation::VolumeRef, 136);
ceiling!(file_descriptor, crate::conversation::FileDescriptor, 64);
ceiling!(interaction_ticket, InteractionTicket, 360);
ceiling!(interaction_resolution, InteractionResolution, 320);
ceiling!(resource_ref, crate::resources::ResourceRef, 128);
ceiling!(provider_ref, crate::resources::ProviderRef, 72);
ceiling!(json_entry, (String, serde_json::Value), 56);
ceiling!(value, serde_json::Value, 32);
ceiling!(
    task_admission_record,
    crate::runtime::TaskAdmissionRecord,
    992
);
ceiling!(
    resource_preparation_request,
    crate::resource_preparation::ResourcePreparationRequest,
    2400
);
ceiling!(
    resource_creation_proof_ref,
    crate::resource_preparation::ResourceCreationProofRef,
    344
);
ceiling!(
    resource_creation_origin,
    crate::resource_preparation::ResourceCreationOrigin,
    216
);
ceiling!(workflow_command, crate::workflow::WorkflowCommand, 312);
ceiling!(
    project_source_authorization,
    crate::source_authorization::ProjectSourceAuthorization,
    648
);
