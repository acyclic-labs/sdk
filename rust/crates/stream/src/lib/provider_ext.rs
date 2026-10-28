/// Projections of canonical provider operations, shared by every backend.
///
/// These methods introduce no provider dispatch or independent observation.
pub trait StreamProviderExt: StreamProvider {
    /// Observes one named head at a coherent authority snapshot.
    fn head(
        &self,
        path: StreamPath,
    ) -> impl std::future::Future<Output = Result<PathHead, StreamError>> + ProviderTask;

    /// Current accepted next sequence, without loading history or awaiting a flush.
    fn tail(
        &self,
        path: StreamPath,
    ) -> impl std::future::Future<Output = Result<u64, StreamError>> + ProviderTask;

    /// Accepted visibility and its confirmed durable prefix at one snapshot.
    fn watermarks(
        &self,
        path: StreamPath,
    ) -> impl std::future::Future<Output = Result<StreamWatermarks, StreamError>> + ProviderTask;

    /// Confirms an irreversible prefix at one captured view. A retained memory
    /// prefix is valid without asserting crash persistence.
    fn durable_receipt(
        &self,
        path: StreamPath,
        through: u64,
    ) -> impl std::future::Future<Output = Result<Option<DurableReceipt>, StreamError>> + ProviderTask;

    /// One bounded direct-child page at a coherent authority snapshot.
    fn children_page(
        &self,
        request: ChildrenPageRequest,
    ) -> impl std::future::Future<Output = Result<ChildrenPage, StreamError>> + ProviderTask;

    /// Waits for the authority snapshot to cover the caller's minimum cut.
    fn wait_applied(
        &self,
        at_least: &RevisionCut,
    ) -> impl std::future::Future<Output = Result<Revision, StreamError>> + ProviderTask;
}

impl<P: StreamProvider + ?Sized> StreamProviderExt for P {
    fn durable_receipt(
        &self,
        path: StreamPath,
        through: u64,
    ) -> impl std::future::Future<Output = Result<Option<DurableReceipt>, StreamError>> + ProviderTask
    {
        async move {
            let request = HeadsRequest {
                durable_through: Some(through),
                projection: HeadProjection::Watermarks,
                paths: [path.clone()].into(),
                children: None,
                limit: 1,
                at_least: Default::default(),
            };
            let mut view = self.heads(request.clone()).await?;
            view.validate_for(&request)?;
            let head = view.heads.pop().flatten().ok_or(StreamError::NotFound)?;
            Ok(head
                .retained_through
                .map(|through| DurableReceipt { path, through }))
        }
    }

    fn head(
        &self,
        path: StreamPath,
    ) -> impl std::future::Future<Output = Result<PathHead, StreamError>> + ProviderTask {
        named_head(self, path, HeadProjection::History)
    }

    fn tail(
        &self,
        path: StreamPath,
    ) -> impl std::future::Future<Output = Result<u64, StreamError>> + ProviderTask {
        async move { Ok(named_head(self, path, HeadProjection::Tail).await?.tail) }
    }

    fn watermarks(
        &self,
        path: StreamPath,
    ) -> impl std::future::Future<Output = Result<StreamWatermarks, StreamError>> + ProviderTask
    {
        async move {
            let head = named_head(self, path, HeadProjection::Watermarks).await?;
            Ok(StreamWatermarks {
                accepted: head.tail,
                durable: head.durable,
            })
        }
    }

    fn children_page(
        &self,
        request: ChildrenPageRequest,
    ) -> impl std::future::Future<Output = Result<ChildrenPage, StreamError>> + ProviderTask {
        async move {
            let limit = request.limit;
            let view = self
                .heads(HeadsRequest {
                    durable_through: None,
                    projection: HeadProjection::Watermarks,
                    paths: Default::default(),
                    children: Some(request),
                    limit,
                    at_least: Default::default(),
                })
                .await?;
            if view.projection != HeadProjection::Watermarks || !view.heads.is_empty() {
                return Err(StreamError::InvalidResponse);
            }
            let page = view.children.ok_or(StreamError::InvalidResponse)?;
            if page.children.len() > limit as usize {
                return Err(StreamError::InvalidResponse);
            }
            Ok(page)
        }
    }

    fn wait_applied(
        &self,
        at_least: &RevisionCut,
    ) -> impl std::future::Future<Output = Result<Revision, StreamError>> + ProviderTask {
        async move {
            let view = self
                .heads(HeadsRequest {
                    durable_through: None,
                    projection: Default::default(),
                    paths: Default::default(),
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

async fn named_head<P: StreamProvider + ?Sized>(
    provider: &P,
    path: StreamPath,
    projection: HeadProjection,
) -> Result<PathHead, StreamError> {
    let mut view = provider
        .heads(HeadsRequest {
            durable_through: None,
            projection,
            paths: [path].into(),
            children: None,
            limit: 1,
            at_least: Default::default(),
        })
        .await?;
    if view.projection != projection || view.heads.len() != 1 || view.children.is_some() {
        return Err(StreamError::InvalidResponse);
    }
    let head = view.heads.pop().flatten().ok_or(StreamError::NotFound)?;
    head.validate_projection(projection)?;
    Ok(head)
}
