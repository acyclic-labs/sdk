impl<A: 'static, O: 'static> SharedCheckoutState<A, O> {
    fn publishes_at_native_boundary(&self) -> bool {
        self.publication != MountPublication::Manual
    }

    /// Admits mutation or a new resolved-publication proof, not pinned data reads.
    /// Retained identity and evidence must survive cancellation or an unknown ACK.
    pub fn ensure_publication_resolved(&self) -> Result<(), MountSourceError> {
        if self.publication_operation.is_some() {
            return Err(MountSourceError::Stale);
        }
        Ok(())
    }

    /// Returns the exact operation retained until publication resolves.
    pub fn retained_operation_id(&mut self) -> OperationId {
        *self
            .publication_operation
            .get_or_insert_with(OperationId::new)
    }

    pub(super) fn has_retained_operation(&self) -> bool {
        self.publication_operation.is_some()
    }

    /// Retains a caller-selected operation identity, or rejects a conflicting
    pub fn retain_operation_id(
        &mut self,
        operation_id: OperationId,
    ) -> Result<(), MountSourceError> {
        match self.publication_operation {
            None => {
                self.publication_operation = Some(operation_id);
                Ok(())
            }
            Some(retained) if retained == operation_id => Ok(()),
            Some(_) => Err(MountSourceError::Stale),
        }
    }

    /// Reads the original context without minting or replacing operation custody.
    pub(in crate::native_mount) fn retained_publication_context(
        &self,
        operation: OperationId,
    ) -> Result<Option<crate::lazy_workspace::NativeSnapshotContext>, MountSourceError> {
        match self.publication_operation {
            None => Ok(None),
            Some(retained) if retained == operation => Ok(self
                .publication_evidence
                .as_ref()
                .map(|(context, _, _, _)| context.clone())),
            Some(_) => Err(MountSourceError::Stale),
        }
    }

    /// Clears only an operation with a known terminal publication outcome.
    pub fn clear_retained_operation(&mut self, operation_id: OperationId) {
        if self.publication_operation == Some(operation_id) {
            self.publication_operation = None;
            self.publication_evidence = None;
        }
    }
}
