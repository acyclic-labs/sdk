use super::*;
use crate::filesystem::access;
use acyclic_fs::{
    Workspace,
    access::{Action, PathTrie},
    model::{CheckoutMode, GenerationSelector},
};

pub(super) async fn compile<P: StreamProvider + 'static, O: AsyncObjectStore + 'static>(
    reader: &FilesystemContentVerifier<StreamAuthorityStore<P>, O>,
    workspace: &Workspace<StreamAuthorityStore<P>, O>,
    io: &PreparationIo,
) -> crate::Result<PathTrie> {
    let label = crate::core::filesystem_scope_label(
        reader.scope.capabilities(),
        &mut Some(&mut io.history_io()),
    )?;
    let (view, holds, _) = access::access_view(
        workspace,
        &reader.scope,
        None,
        access::capability_payload(label, crate::conversation::VolumeOperation::Read, io)?,
        None,
        io,
    )
    .await?;
    // This ordinary query owns its compiled storage across revision frames.
    // The shared AccessView supplies the same fenced publication as mounts.
    for hold in holds.into_iter().flatten() {
        hold.into_retained()?;
    }
    let bytes = (std::mem::size_of::<PathTrie>() + 2 * std::mem::size_of::<usize>()) as u64;
    io.retain_owned(bytes)?;
    io.allocation_attempt(bytes)?;
    Ok(view)
}

async fn revision<P: StreamProvider + 'static, O: AsyncObjectStore + 'static>(
    workspace: &Workspace<StreamAuthorityStore<P>, O>,
    io: &PreparationIo,
) -> crate::Result<(
    acyclic_fs::GenerationId,
    acyclic_fs::kernel::MetadataField<acyclic_fs::ObjectId>,
)> {
    let mut head = io.sdk(
        workspace
            .checkout(
                GenerationSelector::Head,
                CheckoutMode::read_only_pinned(),
                io,
                io.cancellation(),
            )
            .await,
    )?;
    let revision = io.sdk(head.access_policy_revision(io, io.cancellation()).await)?;
    Ok((head.generation_id(), revision))
}

impl<P: StreamProvider + 'static, O: AsyncObjectStore + 'static> FilesystemFileQuery<P, O> {
    pub(super) async fn authorize_policy(
        &self,
        workspace: &Workspace<StreamAuthorityStore<P>, O>,
        io: &PreparationIo,
    ) -> crate::Result<std::sync::Arc<PathTrie>> {
        let failure = |_| crate::Error::FilesystemAccessConfiguration("file query");
        let mut view = self.policy.current().map_err(failure)?;
        let mut current = revision(workspace, io).await?;
        if !view.matches_policy_revision(current.0, current.1)
            || view
                .valid_until()
                .is_some_and(|end| access::access_now() >= end)
        {
            self.policy
                .publish(compile(&self.reader, workspace, io).await?)
                .map_err(failure)?;
            current = revision(workspace, io).await?;
            view = self.policy.current().map_err(failure)?;
        }
        if !view.matches_policy_revision(current.0, current.1) || view.generation_sensitive() {
            return Err(crate::Error::FilesystemAccessConfiguration("file query"));
        }
        access::require_unrestricted(&view, &[Action::Read, Action::List], "file query")?;
        Ok(view)
    }

    pub(super) async fn require_snapshot(
        &self,
        view: &PathTrie,
        generation: &acyclic_fs::Generation<StreamAuthorityStore<P>, O>,
        io: &PreparationIo,
    ) -> crate::Result<()> {
        let mut snapshot = io.sdk(
            generation
                .workspace()
                .checkout(
                    GenerationSelector::Exact(generation.id()),
                    CheckoutMode::read_only_pinned(),
                    io,
                    io.cancellation(),
                )
                .await,
        )?;
        let revision = io.sdk(snapshot.access_policy_revision(io, io.cancellation()).await)?;
        if !view.matches_policy_revision(generation.id(), revision) {
            return Err(crate::Error::FilesystemAccessConfiguration("file query"));
        }
        Ok(())
    }
}
