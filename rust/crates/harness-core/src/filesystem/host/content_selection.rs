//! The root's original operation index pins content routing before its effect.
use super::*;

const SELECTION_BYTES: usize = 17;

fn selection_key(
    operation: acyclic_fs::IdempotencyKey,
    io: &PreparationIo,
) -> Result<FilesystemKey> {
    const DOMAIN: &str = "acyclic-harness-content-workspace-v1";
    let work = WorkCounters {
        bytes_hashed: DOMAIN.len() as u64 + 16,
        ..Default::default()
    };
    io.preflight(work)?;
    let digest = blake3::derive_key(DOMAIN, &operation.into_bytes());
    let mut key = [0; 16];
    key.copy_from_slice(&digest[..16]);
    if key == [0; 16] {
        key[0] = 1;
    }
    io.charge(work)?;
    Ok(FilesystemKey::from_bytes(key))
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    /// None means this operation has not selected a workspace yet.
    pub(in crate::filesystem) async fn content_operation_workspace(
        &self,
        root: &Workspace<A, O>,
        operation: acyclic_fs::IdempotencyKey,
        internal: bool,
        io: &PreparationIo,
    ) -> Result<Option<(Workspace<A, O>, Option<Generation<A, O>>)>> {
        let Some(family) = self
            .project_family
            .as_ref()
            .filter(|family| !internal && family.applies(root.id()) && family.selects_content())
        else {
            return Ok(Some((root.clone(), None)));
        };
        let operation = selection_key(operation, io)?;
        let Some(binding) = io.sdk(
            root.operation_publication(
                operation,
                Some(SELECTION_BYTES as u64),
                io,
                io.cancellation(),
            )
            .await,
        )?
        else {
            return Ok(None);
        };
        let bytes = super::publication::publication_evidence(binding.evidence(), Some(io))?;
        let Ok(&[1, owner @ ..]) = <&[u8; SELECTION_BYTES]>::try_from(bytes) else {
            return Err(stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Storage,
                "invalid content workspace binding",
            ));
        };
        let workspace = family
            .workspace(self, root, acyclic_fs::WorkspaceId::from_bytes(owner), io)?
            .await?;
        Ok(Some((workspace, Some(binding.into_generation()))))
    }

    pub(in crate::filesystem) fn bind_content_operation<'a>(
        &'a self,
        root: &'a Workspace<A, O>,
        selected: acyclic_fs::WorkspaceId,
        operation: acyclic_fs::IdempotencyKey,
        authorization: (acyclic_fs::access::PolicyFence, &'a ContentGrant),
        io: &'a PreparationIo,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(Some(io), || async move {
            let operation = selection_key(operation, io)?;
            let mut encoded = [1; SELECTION_BYTES];
            let [_, owner @ ..] = &mut encoded;
            *owner = selected.into_bytes();
            let evidence = encoded.as_slice().mutation_bytes(Some(io))?;
            let work = WorkCounters {
                bytes_hashed: SELECTION_BYTES as u64,
                ..Default::default()
            };
            io.preflight(work)?;
            let fingerprint =
                acyclic_fs_format::identity::Digest::from_bytes(*blake3::hash(&encoded).as_bytes());
            io.charge(work)?;
            let mut transaction = io.sdk(
                root.begin_transaction(operation, io, io.cancellation())
                    .await,
            )?;
            io.sdk(
                transaction
                    .observe_policy_fence(
                        authorization.0,
                        self.access_now()?,
                        io,
                        io.cancellation(),
                    )
                    .await,
            )?;
            // The ordinary publication receipt decides this binding. A conflict or
            // uncertain result never permits publication in a newly selected view.
            let binding = Self::commit_contextual_publication(
                transaction,
                io,
                super::workspaces::FilePublication::Authorized(
                    &authorization.0,
                    authorization.1,
                    None,
                ),
                Some((
                    fingerprint,
                    evidence,
                    acyclic_fs::PublicationPermit::Unrestricted,
                )),
                self.clock_handle(),
            )?
            .await?;
            bounded_io::retain(&binding, Some(io)).await?;
            Ok(())
        })
    }
}
