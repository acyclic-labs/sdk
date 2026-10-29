//! Rebind verified publication receipts through authenticated content identities.
use super::*;

impl CheckoutPublisher {
    pub(super) async fn observed(
        &self,
        view: &workspace::View,
        operation: OperationId,
        file: FileRef,
    ) -> Result<FileRef> {
        self.observed_with_budget(view, operation, file, acyclic_fs::WorkBudget::UNBOUNDED)
            .await
    }

    pub(super) async fn observed_with_budget(
        &self,
        view: &workspace::View,
        operation: OperationId,
        file: FileRef,
        budget: acyclic_fs::WorkBudget,
    ) -> Result<FileRef> {
        let cancellation = acyclic_fs::CancellationToken::new();
        let key = acyclic_fs::IdempotencyKey::from_bytes(
            capture::capture_operation(write_capture(operation, file.path()), 1).into_bytes(),
        );
        let Some(capture) = view
            .workspace()
            .operation_generation(key, budget, &cancellation)
            .await
            .map_err(storage)?
            .value
        else {
            return Ok(file);
        };
        let parents = capture
            .parents(budget, &cancellation)
            .await
            .map_err(storage)?
            .value;
        if parents != [self.staged_generation(&file)?] {
            return Ok(file);
        }
        let unchanged_payload = self.known_payload(view, &capture, &file, budget).await?;
        if !unchanged_payload && !self.captured_bytes_match(&capture, &file, budget).await? {
            return Ok(file);
        }
        crate::filesystem::retain_published_generation(&capture).await?;
        let rebound = FileRef::new(
            file.volume().clone(),
            file.path().to_owned(),
            hex::encode(capture.id().digest().into_bytes()),
            file.descriptor().clone(),
            file.display_name().to_owned(),
            &mut None,
        )?;
        // Equal bytes can have different immutable extent layouts. That
        // bounded full-read fallback retains ordinary admission; only an
        // unchanged payload can transfer local verification evidence.
        if !unchanged_payload {
            return Ok(rebound);
        }
        self.writer_for(file.volume(), view.workspace())?
            .rebind_verified_publication(&file, rebound, None)
            .await
    }

    // All callers supply receipts produced or recovered by our authenticated
    // writer, which has already verified the staged bytes against the receipt.
    // Equal immutable extent roots and lengths prove the captured bytes are
    // identical; host metadata and file identities may legitimately differ.
    async fn known_payload(
        &self,
        view: &workspace::View,
        capture: &acyclic_fs::Generation<LocalAuthorityBackend, LocalObjectBackend>,
        file: &FileRef,
        budget: acyclic_fs::WorkBudget,
    ) -> Result<bool> {
        #[cfg(all(test, not(loom)))]
        if tests::REREAD_MATCHING_PAYLOAD.get() {
            return Ok(false);
        }
        use acyclic_fs::kernel::FilePayload;
        let cancellation = acyclic_fs::CancellationToken::new();
        let config = view.workspace().volume_config();
        let portable =
            acyclic_fs::path::PortablePath::parse(&format!("/{}", file.path()), config.limits)
                .map_err(storage)?;
        let path = acyclic_fs::kernel::NamespacePath::from_portable_in_profile(
            &portable,
            config.profile,
            config.limits,
        )
        .map_err(storage)?;
        let captured = capture
            .lookup_paths(std::slice::from_ref(&path), budget, &cancellation)
            .await
            .map_err(storage)?
            .value;
        let Some(payload) = captured
            .first()
            .and_then(Option::as_ref)
            .map(|record| record.payload)
        else {
            return Ok(false);
        };
        match payload {
            FilePayload::InlineRegular(bytes) => Ok(matches_descriptor(bytes.as_bytes(), file)),
            FilePayload::Regular { logical_bytes, .. }
                if logical_bytes == file.descriptor().byte_length() =>
            {
                let staged = view
                    .workspace()
                    .generation(self.staged_generation(file)?, budget, &cancellation)
                    .await
                    .map_err(storage)?
                    .value;
                let records = staged
                    .lookup_paths(std::slice::from_ref(&path), budget, &cancellation)
                    .await
                    .map_err(storage)?
                    .value;
                Ok(records
                    .first()
                    .and_then(Option::as_ref)
                    .is_some_and(|record| record.payload == payload))
            }
            _ => Ok(false),
        }
    }

    // Different extent layouts can still describe equal bytes. Keep the
    // original bounded content check for that case and for other entry kinds.
    async fn captured_bytes_match(
        &self,
        capture: &acyclic_fs::Generation<LocalAuthorityBackend, LocalObjectBackend>,
        file: &FileRef,
        budget: acyclic_fs::WorkBudget,
    ) -> Result<bool> {
        let cancellation = acyclic_fs::CancellationToken::new();
        match capture
            .read(
                &format!("/{}", file.path()),
                file.descriptor().byte_length().saturating_add(1),
                budget,
                &cancellation,
            )
            .await
        {
            Ok(read) => Ok(matches_descriptor(read.value.as_ref(), file)),
            Err(failure) => match failure.error {
                acyclic_fs::WorkspaceError::NotFound
                | acyclic_fs::WorkspaceError::NotRegularFile
                | acyclic_fs::WorkspaceError::IsDirectory
                | acyclic_fs::WorkspaceError::ReadLimitExceeded => Ok(false),
                error => Err(storage(error)),
            },
        }
    }
}

fn matches_descriptor(bytes: &[u8], file: &FileRef) -> bool {
    bytes.len() as u64 == file.descriptor().byte_length()
        && <sha2::Sha256 as sha2::Digest>::digest(bytes).as_slice() == file.descriptor().sha256()
}
