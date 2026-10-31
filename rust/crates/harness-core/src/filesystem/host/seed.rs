use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    /// Initializes absent files once using this host's workspace authority.
    ///
    /// Reserve `key` for this initialization and keep it stable across asset
    /// revisions. The immutable operation receipt
    /// is checked before `assets` is called, so later edits and deletions win.
    /// Existing bindings are preserved; any publication conflict is returned
    /// without retrying. This trusted setup API does not grant model authority.
    pub async fn seed_once<F>(
        &self,
        reference: &WorkspaceRef,
        key: &IdempotencyKey,
        assets: F,
    ) -> Result<GenerationRef>
    where
        F: FnOnce() -> Result<Vec<(String, acyclic_stream::OwnedBytes)>>,
    {
        let io = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
        let workspace = self.open(reference, Some(&io)).await?;
        let operation = filesystem_key(key);
        if let Some(prior) = io.sdk(
            workspace
                .operation_generation(operation, &io, io.cancellation())
                .await,
        )? {
            return self.generation_ref_id(prior.id(), Some(&io));
        }
        let mut transaction = io.sdk(
            workspace
                .begin_transaction(operation, &io, io.cancellation())
                .await,
        )?;
        for (path, bytes) in assets()? {
            // Create prefixes in the same private candidate as their files.
            // The primitive rejects non-directory and symlink prefixes.
            if let Some((parent, _)) = path.rsplit_once('/')
                && !parent.is_empty()
            {
                io.sdk(
                    transaction
                        .create_dir_all(parent, &io, io.cancellation())
                        .await,
                )?;
            }
            let (staged, recorded) = transaction
                .write_if(
                    &path,
                    bytes,
                    acyclic_fs::FileCondition::Absent,
                    &io,
                    io.cancellation(),
                )
                .await;
            let staged = match staged {
                // For Absent this means a binding already exists; the failed
                // condition stages nothing. Other failures must abort the batch.
                Err(WorkspaceError::StaleIdentity) => Ok(()),
                result => result,
            };
            io.sdk((staged, recorded))?;
        }
        // Stable evidence also records an operation when every file existed.
        // Its identity deliberately excludes the current embedded asset bytes.
        const DOMAIN: &[u8] = b"acyclic harness seed once v1";
        let generation = Self::commit_contextual_publication(
            transaction,
            &io,
            workspaces::FilePublication::Replace(None),
            Some((
                acyclic_fs::Digest::from_bytes(*blake3::hash(DOMAIN).as_bytes()),
                acyclic_stream::OwnedBytes::from_static(DOMAIN),
                acyclic_fs::PublicationPermit::Unrestricted,
            )),
        )
        .await?;
        self.generation_ref_id(generation.id(), Some(&io))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AgentId, conversation::VolumeOwner};

    #[tokio::test]
    async fn seed_receipt_preserves_edits_deletions_and_all_preexisting_files() -> Result<()> {
        let provider = ProviderRef::new("seed".into(), "filesystem".into(), "1".into(), &mut None)?;
        let filesystem = Fs::memory();
        let host = FilesystemHost::new(filesystem.clone(), &provider, None)?;
        let volume = VolumeRef::new(
            provider.clone(),
            "seed".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([17; 16])),
            &mut None,
        )?;
        let reference = host.create_volume(&volume).await?.workspace;
        host.apply(
            &reference,
            None,
            &[
                WorkspaceMutation::CreateDirectory {
                    path: "/skills/existing".into(),
                },
                WorkspaceMutation::PutFile {
                    path: "/skills/existing/SKILL.md".into(),
                    bytes: acyclic_stream::OwnedBytes::from_static(b"preexisting edit"),
                },
            ],
            &IdempotencyKey::new("existing")?,
        )
        .await?;
        let key = IdempotencyKey::new("built-in-skills-v1")?;
        let original = host
            .seed_once(&reference, &key, || {
                Ok(vec![
                    (
                        "/skills/existing/SKILL.md".into(),
                        acyclic_stream::OwnedBytes::from_static(b"default"),
                    ),
                    (
                        "/skills/edited/SKILL.md".into(),
                        acyclic_stream::OwnedBytes::from_static(b"original"),
                    ),
                    (
                        "/skills/deleted/SKILL.md".into(),
                        acyclic_stream::OwnedBytes::from_static(b"original"),
                    ),
                ])
            })
            .await?;
        assert_eq!(
            host.read(&reference, None, "/skills/existing/SKILL.md", 64, None)
                .await?
                .as_ref(),
            b"preexisting edit"
        );
        let changed = host
            .apply(
                &reference,
                None,
                &[
                    WorkspaceMutation::PutFile {
                        path: "/skills/edited/SKILL.md".into(),
                        bytes: acyclic_stream::OwnedBytes::from_static(b"user edit"),
                    },
                    WorkspaceMutation::Remove {
                        path: "/skills/deleted/SKILL.md".into(),
                    },
                ],
                &IdempotencyKey::new("user-edit-and-delete")?,
            )
            .await?;
        let reopened = FilesystemHost::new(filesystem, &provider, None)?;
        let reads = std::cell::Cell::new(0);
        let recovered = reopened
            .seed_once(&reference, &key, || {
                reads.set(reads.get() + 1);
                Ok(vec![
                    (
                        "/skills/edited/SKILL.md".into(),
                        acyclic_stream::OwnedBytes::from_static(b"changed default"),
                    ),
                    (
                        "/skills/deleted/SKILL.md".into(),
                        acyclic_stream::OwnedBytes::from_static(b"resurrected"),
                    ),
                ])
            })
            .await?;
        assert_eq!(reads.get(), 0);
        assert_eq!(recovered, original);
        assert_eq!(
            reopened.resolve(&reference, None).await?.generation,
            changed
        );
        assert_eq!(
            reopened
                .read(&reference, None, "/skills/edited/SKILL.md", 64, None)
                .await?
                .as_ref(),
            b"user edit"
        );
        assert!(matches!(
            reopened
                .stat(&reference, None, "/skills/deleted/SKILL.md")
                .await,
            Err(Error::NotFound(_))
        ));

        // Negative control: an all-preexisting candidate must still create a
        // durable receipt, rather than returning an unrecorded clean commit.
        let clean_key = IdempotencyKey::new("all-preexisting-seed-v1")?;
        let clean = reopened
            .seed_once(&reference, &clean_key, || {
                Ok(vec![(
                    "/skills/existing/SKILL.md".into(),
                    acyclic_stream::OwnedBytes::from_static(b"replacement"),
                )])
            })
            .await?;
        assert!(
            reopened
                .operation_generation(&reference, &clean_key, None)
                .await?
                .is_some()
        );
        assert_eq!(
            reopened
                .seed_once(&reference, &clean_key, || panic!(
                    "receipt must precede embedded bytes"
                ))
                .await?,
            clean
        );
        assert_eq!(
            reopened
                .read(&reference, None, "/skills/existing/SKILL.md", 64, None)
                .await?
                .as_ref(),
            b"preexisting edit"
        );
        Ok(())
    }
}
