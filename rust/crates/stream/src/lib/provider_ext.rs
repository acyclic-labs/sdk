/// Projections of canonical provider operations, shared by every backend.
///
/// These methods introduce no provider dispatch or independent observation.
pub trait StreamProviderExt: StreamProvider {
    /// Observes one named head at a coherent authority snapshot.
    fn head(
        &self,
        path: StreamPath,
    ) -> impl std::future::Future<Output = Result<PathHead, StreamError>> + ProviderTask;
}

impl<P: StreamProvider + ?Sized> StreamProviderExt for P {
    fn head(
        &self,
        path: StreamPath,
    ) -> impl std::future::Future<Output = Result<PathHead, StreamError>> + ProviderTask {
        async move {
            let request = HeadsRequest {
                paths: vec![path],
                children: None,
                limit: 1,
                at_least: RevisionCut::default(),
            };
            let mut view = self.heads(request).await?;
            if view.heads.len() != 1 || view.children.is_some() {
                return Err(StreamError::InvalidResponse);
            }
            let head = view.heads.pop().flatten().ok_or(StreamError::NotFound)?;
            head.validate()?;
            Ok(head)
        }
    }
}
