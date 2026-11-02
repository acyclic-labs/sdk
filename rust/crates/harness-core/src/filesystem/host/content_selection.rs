//! The root's original operation index pins content routing before its effect.
use super::*;

const SELECTION_PATH: &str = "/.system/harness-content-workspace";
const SELECTION_BYTES: u64 = 17;

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
            root.operation_generation(operation, io, io.cancellation())
                .await,
        )?
        else {
            return Ok(None);
        };
        let bytes = read_generation(&binding, SELECTION_PATH, SELECTION_BYTES, Some(io)).await?;
        let _hold = io.guard_bytes(&bytes)?;
        if bytes.len() != SELECTION_BYTES as usize || bytes[0] != 1 {
            return Err(stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Storage,
                "invalid content workspace binding",
            ));
        }
        let mut owner = [0; 16];
        owner.copy_from_slice(&bytes[1..]);
        let workspace = family
            .workspace(self, root, acyclic_fs::WorkspaceId::from_bytes(owner), io)?
            .await?;
        Ok(Some((workspace, Some(binding))))
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
            let mut encoded = [1; SELECTION_BYTES as usize];
            encoded[1..].copy_from_slice(&selected.into_bytes());
            let mutations = [
                WorkspaceMutation::CreateDirectory {
                    path: stock_string_copy("/.system", Some(io))?,
                },
                WorkspaceMutation::PutFile {
                    path: stock_string_copy(SELECTION_PATH, Some(io))?,
                    bytes: encoded.as_slice().mutation_bytes(Some(io))?,
                },
            ];
            // The ordinary publication receipt decides this binding. A conflict or
            // uncertain result never permits publication in a newly selected view.
            let binding = self
                .publish_selected_operation(
                    root,
                    &mutations,
                    operation,
                    io,
                    super::workspaces::FilePublication::Authorized(
                        authorization.0,
                        authorization.1,
                        None,
                    ),
                    None,
                )
                .await?;
            bounded_io::retain(&binding, Some(io)).await?;
            Ok(())
        })
    }
}
