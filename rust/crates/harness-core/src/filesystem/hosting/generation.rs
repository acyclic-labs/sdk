//! One original volume publication wait shared by directory and file queries.
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
        // Capture the original producer cursor before reading the head. A
        // publication between those operations is retained by this follow.
        let waiting = if let Some(after) = after {
            let workspace = self.volumes.open(&reference, None).await.map_err(turn)?;
            // A wait cut is a generation of this workspace, not just a value
            // unequal to its head. Use the original generation authenticator
            // shared with pinned reads before subscribing to this producer.
            self.volumes
                .generation(&workspace, after, None)
                .await
                .map_err(turn)?;
            let path = acyclic_fs::session::changes_path(
                &self.volumes.filesystem,
                workspace.id().volume_id(),
            )
            .map_err(Error::Stream)?;
            let request = acyclic_stream::HeadsRequest {
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
            Some(acyclic_stream::FollowRequest {
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
        } else {
            None
        };
        let mut selected = match initial {
            Some(selected) => selected,
            None => {
                self.volumes
                    .resolve(&reference, None)
                    .await
                    .map_err(turn)?
                    .generation
            }
        };
        if after == Some(&selected) {
            use futures::TryStreamExt as _;
            let updates = self
                .agents
                .provider()
                .follow(waiting.ok_or(Error::Record("volume wait has no producer"))?)
                .await
                .map_err(Error::Stream)?;
            let mut updates = acyclic_stream::records_only(updates);
            while after == Some(&selected) {
                updates
                    .try_next()
                    .await
                    .map_err(Error::Stream)?
                    .ok_or(Error::Stream(acyclic_stream::StreamError::Unavailable))?;
                selected = self
                    .volumes
                    .resolve(&reference, None)
                    .await
                    .map_err(turn)?
                    .generation;
            }
        }
        Ok(selected)
    }
}
