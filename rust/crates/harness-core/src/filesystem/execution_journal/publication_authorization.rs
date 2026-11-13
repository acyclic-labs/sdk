use super::*;

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: crate::store::JournalProvider + ?Sized,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn authenticate_publication_with_io(
        &self,
        operation: OperationId,
        file: &FileRef,
        io: Option<&PreparationIo>,
    ) -> Result<bool> {
        crate::executor::with_history_io(io, |work| file.validate(work))?;
        if !file.volume().is_agent_journal() || file.volume() != &self.volume {
            return Ok(false);
        }
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let result = async {
            crate::executor::with_history_io(io, |work| {
                ContentGrant::authorize_volume(
                    &self.verifier,
                    &self.scope,
                    &self.volume,
                    VolumeOperation::Read,
                    work,
                )
            })?;
            let key = super::super::content_publisher::once_key(operation, io)?;
            let key_hold = io
                .map(|io| io.guard_owned(key.as_str().len() as u64))
                .transpose()?;
            // Content lookup verifies the original receipt's descriptor. A
            // replacement at its mutable Head path cannot inherit that proof.
            let published = self
                .host
                .content_for_operation_record_io(
                    super::super::PublicationLookup {
                        volume: &self.volume,
                        key: &key,
                        expected_path: None,
                        maximum_bytes: Some(super::MAXIMUM_PAYLOAD_BYTES),
                        internal: false,
                        purpose: super::super::PublicationPurpose::Content,
                        scope: &self.scope,
                        ceiling: None,
                    },
                    io,
                )
                .await?;
            let allowed = published
                .as_ref()
                .is_some_and(|(original, bytes)| original == file && bytes.is_some());
            drop(published);
            drop(key);
            drop(key_hold);
            Ok(allowed)
        }
        .await;
        PreparationIo::release_since(io, Some(before))?;
        result
    }
}

#[cfg(all(test, not(loom), not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::executor::ExecutionJournal;

    #[tokio::test]
    async fn original_publication_rejects_replacement_and_unrelated_refs() -> Result<()> {
        let (private_journal, issuer) = super::super::test_support::journal().await?;
        let operation = OperationId::new();
        let private_content = private_journal
            .output_content(operation)?
            .expect("private content");
        let (private_file, _) = private_content
            .writer
            .as_deref()
            .expect("private writer")
            .stage_once(
                operation,
                &crate::context::ContextEpoch::path(operation),
                b"ordinary private publication",
                "text/plain",
                "manifest",
                crate::conversation::ContentAccess::default(),
            )?
            .await?;
        assert!(
            !private_journal
                .authenticate_publication(operation, &private_file, None)?
                .await?
        );
        let crate::conversation::VolumeOwner::Agent(owner) = private_journal.volume.owner() else {
            unreachable!("agent journal fixture");
        };
        let volume = crate::conversation::journal_volume(
            private_journal.volume.provider().clone(),
            &crate::agent::AgentId::from_bytes(owner.into_bytes()),
        )?;
        private_journal.host.create_volume(&volume).await?;
        let scope = issuer.bootstrap_scope(
            "host-journal-proof",
            &crate::Capabilities::new([
                crate::contract::capability::HARNESS_HOST.to_owned(),
                volume.capability(VolumeOperation::Read, &mut None)?,
                volume.capability(VolumeOperation::Write, &mut None)?,
            ]),
            Some(crate::AgentId::from_bytes(owner.into_bytes())),
            &mut None,
        )?;
        let journal = FilesystemExecutionJournal::new(
            Arc::new(acyclic_stream::MemoryStream::default()),
            private_journal.host.clone(),
            crate::filesystem::FilesystemJournalBinding {
                volume: &volume,
                verifier: &issuer.verifier(&mut None)?,
                scope: &scope,
                io: None,
            },
        )?;
        let content = journal.output_content(operation)?.expect("journal content");
        let writer = content.writer.as_deref().expect("journal writer");
        let path = crate::context::ContextEpoch::path(operation);
        let (original, _) = writer
            .stage_once(
                operation,
                &path,
                b"original",
                "text/plain",
                "manifest",
                crate::conversation::ContentAccess::default(),
            )?
            .await?;
        assert!(
            journal
                .authenticate_publication(operation, &original, None)?
                .await?
        );
        let (replacement, _) = writer
            .stage_at(OperationId::new(), &original, b"replacement", None, None)?
            .await?;
        assert!(
            !journal
                .authenticate_publication(operation, &replacement, None)?
                .await?
        );
        // The original receipt remains valid after Head changes.
        assert!(
            journal
                .authenticate_publication(operation, &original, None)?
                .await?
        );
        let unrelated_operation = OperationId::new();
        let (unrelated, _) = writer
            .stage_once(
                unrelated_operation,
                &crate::context::ContextEpoch::path(unrelated_operation),
                b"original",
                "text/plain",
                "manifest",
                crate::conversation::ContentAccess::default(),
            )?
            .await?;
        assert!(
            !journal
                .authenticate_publication(operation, &unrelated, None)?
                .await?
        );
        assert!(
            !journal
                .authenticate_publication(OperationId::new(), &original, None)?
                .await?
        );
        Ok(())
    }
}
