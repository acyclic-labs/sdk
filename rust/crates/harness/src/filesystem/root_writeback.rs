//! Harness-owned authority adapter for native checkout writeback.

use super::FilesystemExecutionJournal;
use crate::{InteractionId, OperationId, tool::ToolApprovalVerifier};
use acyclic_fs::{
    AsyncAuthorityStore, AsyncObjectStore, RootWritebackApprovalContext,
    RootWritebackApprovalVerifier,
};
use acyclic_stream::StreamProvider;
use futures::future::BoxFuture;
use std::sync::Arc;

/// Binds one durable Harness interaction to one filesystem root-writeback
/// request. The filesystem receives only this narrow verifier capability;
/// it cannot mint or persist an approval itself.
pub struct HarnessRootWritebackApprovalVerifier {
    approvals: Arc<dyn ToolApprovalVerifier>,
    interaction_id: InteractionId,
}

impl HarnessRootWritebackApprovalVerifier {
    /// Creates a verifier for one owner-authenticated approval interaction.
    #[must_use]
    pub fn new(approvals: Arc<dyn ToolApprovalVerifier>, interaction_id: InteractionId) -> Self {
        Self {
            approvals,
            interaction_id,
        }
    }

    /// Binds the verifier directly to Harness's durable filesystem journal.
    /// The journal remains the only approval authority; this adapter only
    /// narrows its exact interaction check to filesystem root writeback.
    pub fn from_filesystem_journal<P, A, O>(
        journal: Arc<FilesystemExecutionJournal<P, A, O>>,
        interaction_id: InteractionId,
    ) -> Self
    where
        P: StreamProvider + Send + Sync + 'static,
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        Self::new(journal, interaction_id)
    }

    /// Returns the interaction identity retained by this capability.
    #[must_use]
    pub const fn interaction_id(&self) -> InteractionId {
        self.interaction_id
    }
}

impl RootWritebackApprovalVerifier for HarnessRootWritebackApprovalVerifier {
    fn verify<'a>(
        &'a self,
        context: RootWritebackApprovalContext,
    ) -> BoxFuture<'a, std::result::Result<(), String>> {
        Box::pin(async move {
            self.approvals
                .verify(
                    self.interaction_id,
                    OperationId::from_bytes(context.operation_id.into_bytes()),
                    *context.request_digest.as_bytes(),
                )
                .await
                .map_err(|error| error.to_string())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Error, InteractionId, Result};
    use acyclic_fs::Digest;

    struct ExactApproval {
        operation_id: OperationId,
        digest: [u8; 32],
    }

    impl ToolApprovalVerifier for ExactApproval {
        fn verify<'a>(
            &'a self,
            _interaction_id: InteractionId,
            operation_id: OperationId,
            definition_digest: [u8; 32],
        ) -> BoxFuture<'a, Result<()>> {
            Box::pin(async move {
                if operation_id == self.operation_id && definition_digest == self.digest {
                    Ok(())
                } else {
                    Err(Error::Unauthorized(
                        "root writeback approval identity mismatch".to_owned(),
                    ))
                }
            })
        }
    }

    #[tokio::test]
    async fn adapter_binds_exact_operation_and_digest() {
        let operation_id = acyclic_fs::OperationId::new();
        let digest = [7; 32];
        let verifier = HarnessRootWritebackApprovalVerifier::new(
            Arc::new(ExactApproval {
                operation_id: OperationId::from_bytes(operation_id.into_bytes()),
                digest,
            }),
            InteractionId::new(),
        );
        verifier
            .verify(RootWritebackApprovalContext {
                operation_id,
                request_digest: Digest::from_bytes(digest),
            })
            .await
            .expect("exact durable approval");
        let mismatch = verifier
            .verify(RootWritebackApprovalContext {
                operation_id,
                request_digest: Digest::from_bytes([8; 32]),
            })
            .await;
        assert!(mismatch.is_err(), "changed request must be denied");
    }
}
