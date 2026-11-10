//! Inline storage bounds for records retained in checkpoints.
use super::*;
use crate::conversation::ConversationState;
use std::mem::size_of;

// NativeConversationCustody adds four allocation tickets (messages, digests,
// archive, index_buffer) and the indexes vector to ConversationState. Keep the
// record bounds independent of this feature's inline custody, and bound the
// allocation ticket separately so its growth cannot hide behind this formula.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
const CONVERSATION_CUSTODY_BYTES: usize = 4 * size_of::<Option<acyclic_fs::NativeAllocation>>()
    + size_of::<Vec<acyclic_fs::NativeAllocation>>();
#[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
const CONVERSATION_CUSTODY_BYTES: usize = 0;

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
ceiling!(extension_binding, registry::ExtensionBinding, 192);
ceiling!(extension_admission, ExtensionAdmission, 88);
ceiling!(scope, Scope, 160);
ceiling!(capabilities, crate::Capabilities, 24);
ceiling!(extension_state_migration, ExtensionStateMigration, 408);
ceiling!(extension_record, ExtensionRecord, 368);
ceiling!(extension_configuration, ExtensionConfiguration, 336);
ceiling!(extension_dependency, ExtensionDependency, 32);
ceiling!(authority, Authority, 32);
ceiling!(event_reference, EventReference, 40);
ceiling!(conversation_message, ConversationMessage, 648);
ceiling!(
    conversation_state,
    ConversationState,
    424 + CONVERSATION_CUSTODY_BYTES
);
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
ceiling!(
    native_conversation_allocation_ticket,
    Option<acyclic_fs::NativeAllocation>,
    64
);
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
    project_source_authorization,
    crate::source_authorization::ProjectSourceAuthorization,
    648
);
