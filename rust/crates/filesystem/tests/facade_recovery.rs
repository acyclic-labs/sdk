//! Failure atomicity and reconciliation through the linked filesystem library.

mod facade {
    mod tests {
        use acyclic_fs::facade::test_snapshot::checkout_state;
        use acyclic_fs::kernel::{
            AttributeName, ClosureError, ExtentSeekTarget, RebaseDecision, volume_authority_id,
        };
        use acyclic_fs::model::{CheckoutMode, GenerationSelector};
        use acyclic_fs::{
            AsyncAuthorityStore, AuthoredMutation, AuthorityStoreError, ByteRange,
            CancellationToken, ContentChange, ContentTimes, EmbeddedCapabilities, Fs, FsError,
            LiveMutationOutcome, LivePublicationObservation, MergePreparation,
            NamedAttributeWriteMode, ObjectStoreError, ReplayLimit, Sequence,
        };
        use acyclic_fs_format::identity::{OperationId, VolumeId};
        use acyclic_fs_format::metadata::{FileMetadata, MetadataField};
        use acyclic_fs_test_support::faults::{
            FaultAuthorityStore, FaultControl, FaultObjectStore, PostAppendAuthorityStore,
            PostPutObjectStore,
        };
        use acyclic_fs_test_support::{
            B, allow, config, path, path_parts, serialized_config, writable_live, writable_pinned,
        };
        use acyclic_stream::OwnedBytes;
        use std::sync::Arc;

        mod checkout_reconciliation {
            include!("facade_recovery/checkout_reconciliation.rs");
        }
        mod fault_creation {
            include!("facade_recovery/fault_creation.rs");
        }
        mod fault_mutations {
            include!("facade_recovery/fault_mutations.rs");
        }
        mod fault_publication {
            include!("facade_recovery/fault_publication.rs");
        }
    }
}
