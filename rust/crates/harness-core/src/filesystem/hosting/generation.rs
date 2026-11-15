//! One original volume publication wait shared by directory and file queries,
//! following the project selection and its selected producer.
use super::*;
use crate::{conversation::VolumeRef, resources::GenerationRef};

impl<P: AuthorityProvider + ?Sized + 'static, O: AsyncObjectStore + 'static> Bound<P, O> {
    pub(super) async fn volume_generation(
        &self,
        volume: &VolumeRef,
        initial: Option<GenerationRef>,
        after: Option<&GenerationRef>,
    ) -> Result<GenerationRef, Error> {
        let reference =
            super::super::bounded_io::content_workspace(&self.volumes.provider, volume, None)
                .map_err(turn)?;
        let Some(after) = after else {
            return match initial {
                Some(initial) => Ok(initial),
                None => Ok(self
                    .volumes
                    .resolve(&reference, None)
                    .await
                    .map_err(turn)?
                    .generation),
            };
        };
        let root = self.volumes.open(&reference, None).await.map_err(turn)?;
        // Authenticate the cut against the original project boundary, including
        // its authorized descendants, before disclosing any publication.
        self.volumes
            .generation(&root, after, None)
            .await
            .map_err(turn)?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let io = crate::filesystem::PreparationIo::for_unmeasured(&cancellation).map_err(turn)?;
        loop {
            let (workspace, cursors) = self.selected_publication(&root, &io).await?;
            let head = io
                .sdk(workspace.head(&io, io.cancellation()).await)
                .map_err(turn)?;
            let selected = self
                .volumes
                .generation_ref_id(head.id(), None)
                .map_err(turn)?;
            if after != &selected {
                return Ok(selected);
            }
            self.next_publication(cursors).await?;
        }
    }

    /// Selects the project's current producer with the cursors that observe
    /// any later change to it. Checkout can change the producer, so the
    /// selection cursor is captured before selecting it, and the producer's
    /// cursor before the caller samples its head.
    pub(super) async fn selected_publication(
        &self,
        root: &acyclic_fs::Workspace<StreamAuthorityStore<P>, O>,
        io: &crate::filesystem::PreparationIo,
    ) -> Result<
        (
            acyclic_fs::Workspace<StreamAuthorityStore<P>, O>,
            Vec<acyclic_stream::FollowRequest>,
        ),
        Error,
    > {
        let mut cursors = Vec::new();
        if let Some(path) = self
            .volumes
            .selection_changes_path(root.id())
            .map_err(turn)?
        {
            cursors.push(self.publication_cursor(path).await?);
        }
        let workspace = self
            .volumes
            .select_current_workspace(root, io)
            .await
            .map_err(turn)?;
        let path =
            acyclic_fs::session::changes_path(&self.volumes.filesystem, workspace.id().volume_id())
                .map_err(Error::Stream)?;
        cursors.push(self.publication_cursor(path).await?);
        Ok((workspace, cursors))
    }

    /// Waits for the first record after any of these cursors.
    pub(super) async fn next_publication(
        &self,
        cursors: Vec<acyclic_stream::FollowRequest>,
    ) -> Result<(), Error> {
        use futures::TryStreamExt as _;
        let mut updates = Vec::new();
        for cursor in cursors {
            updates.push(acyclic_stream::records_only(
                self.agents
                    .provider()
                    .follow(cursor)
                    .await
                    .map_err(Error::Stream)?,
            ));
        }
        futures::stream::select_all(updates)
            .try_next()
            .await
            .map_err(Error::Stream)?
            .ok_or(Error::Stream(acyclic_stream::StreamError::Unavailable))?;
        Ok(())
    }

    async fn publication_cursor(
        &self,
        path: acyclic_stream::StreamPath,
    ) -> Result<acyclic_stream::FollowRequest, Error> {
        let request = acyclic_stream::HeadsRequest {
            at: None,
            paths: [path.clone()].into_iter().collect(),
            retained: Default::default(),
            durable_through: None,
            projection: acyclic_stream::HeadProjection::Tail,
            children: None,
            limit: 1,
            at_least: Default::default(),
        };
        let heads = self
            .agents
            .provider()
            .heads(request.clone())
            .await
            .map_err(Error::Stream)?;
        heads.validate_for(&request).map_err(Error::Stream)?;
        Ok(acyclic_stream::FollowRequest {
            path,
            from: heads
                .heads
                .first()
                .and_then(Option::as_ref)
                .map_or(0, |head| head.tail),
            preceding: None,
            bootstrap_head: false,
            until: None,
            when_absent: acyclic_stream::WhenAbsent::Wait,
            filter: Default::default(),
            progress: false,
            at_least: Default::default(),
        })
    }
}
