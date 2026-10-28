/// Projections of canonical provider operations, shared by every backend.
///
/// These methods introduce no provider dispatch or independent observation.
pub trait StreamProviderExt: StreamProvider {
    /// Observes one named head at a coherent authority snapshot.
    fn head(
        &self,
        path: StreamPath,
    ) -> impl std::future::Future<Output = Result<PathHead, StreamError>> + ProviderTask;

    /// Waits for the authority snapshot to cover the caller's minimum cut.
    fn wait_applied(
        &self,
        at_least: &RevisionCut,
    ) -> impl std::future::Future<Output = Result<Revision, StreamError>> + ProviderTask;
}

impl<P: StreamProvider + ?Sized> StreamProviderExt for P {
    fn head(
        &self,
        path: StreamPath,
    ) -> impl std::future::Future<Output = Result<PathHead, StreamError>> + ProviderTask {
        async move {
            let request = HeadsRequest {
                projection: Default::default(),
                paths: vec![path],
                children: None,
                limit: 1,
                at_least: RevisionCut::default(),
            };
            let mut view = self.heads(request).await?;
            if view.projection != HeadProjection::History
                || view.heads.len() != 1
                || view.children.is_some()
            {
                return Err(StreamError::InvalidResponse);
            }
            let head = view.heads.pop().flatten().ok_or(StreamError::NotFound)?;
            head.validate()?;
            Ok(head)
        }
    }

    fn wait_applied(
        &self,
        at_least: &RevisionCut,
    ) -> impl std::future::Future<Output = Result<Revision, StreamError>> + ProviderTask {
        async move {
            let view = self
                .heads(HeadsRequest {
                    projection: Default::default(),
                    paths: Vec::new(),
                    children: None,
                    limit: 1,
                    at_least: at_least.clone(),
                })
                .await?;
            if view.projection != HeadProjection::History
                || !view.heads.is_empty()
                || view.children.is_some()
                || !at_least.proven_by(Some(&view.snapshot))
            {
                return Err(StreamError::InvalidResponse);
            }
            Ok(view.snapshot)
        }
    }
}
