//! Finite children pages on the session: the session form of
//! `StreamService.ChildrenPage`, answered as one fragmented one-shot reply.
use super::*;

/// The page channel's credential, read through its binding's authorization
/// when the reply is projected, so a replacement while the page is pending
/// applies to the reply. A child is returned only when that credential may
/// list it, and its tail record only when it could follow that stream: its
/// grant and its mandatory record predicate.
pub(super) struct Visibility {
    pub(super) authority: Arc<Authority>,
    pub(super) authorization: ResourceAuthorization,
    pub(super) channels: usize,
}
impl Visibility {
    fn permits(
        &self,
        capability: &dyn Admission,
        action: &'static str,
        path: &StreamPath,
    ) -> Result<bool, StreamError> {
        match self.authority.permits(
            capability,
            action,
            &authority::stream_resource(Some(path)),
            self.channels,
        ) {
            Ok(()) => Ok(true),
            Err(authority::Rejection::Forbidden) => Ok(false),
            Err(_) => Err(StreamError::AccessDenied),
        }
    }
    pub(super) fn page(
        &self,
        mut page: crate::ChildrenPage,
        tail_stream: Option<&crate::RelativePath>,
    ) -> Result<crate::ChildrenPage, StreamError> {
        let capability = self.authorization.current()?;
        let mut children = Vec::with_capacity(page.children.len());
        for mut child in page.children {
            if !self.permits(&*capability, "streams/list", &child.path)? {
                continue;
            }
            if let (Some(record), Some(relative)) = (child.tail_record.take(), tail_stream) {
                let target = StreamPath::new(format!("{}/{}", child.path, relative))?;
                if self.permits(&*capability, "streams/follow", &target)? {
                    child.tail_record = capability
                        .follow_filter(wire::SessionFilter::default())?
                        .project(record)?;
                }
            }
            children.push(child);
        }
        page.children = children;
        Ok(page)
    }
}

pub(super) fn source<P: StreamProvider + ?Sized>(
    provider: Arc<P>,
    request: crate::ChildrenPageRequest,
    visibility: Visibility,
    identity: wire::SessionChannel,
) -> Source {
    calls::reply(
        identity,
        Item::ChildrenPage,
        Box::pin(async move {
            let tail_stream = request.tail_stream.clone();
            let page = provider.children_page(request).await?;
            let page = visibility.page(page, tail_stream.as_ref())?;
            Ok(Bytes::from(
                wire_codec::children_page_to_wire(page).encode_to_vec(),
            ))
        }),
    )
}

/// Projects an already coherent provider view; authorization never causes a
/// second observation. A filtered-out named head refuses the whole view.
pub(super) fn heads<P: StreamProvider + ?Sized>(
    provider: Arc<P>,
    request: crate::HeadsRequest,
    authorization: ResourceAuthorization,
    visibility: Option<Visibility>,
    identity: wire::SessionChannel,
) -> Source {
    calls::reply(
        identity,
        Item::Heads,
        Box::pin(async move {
            let mut view = provider.heads(request.clone()).await?;
            view.validate_for(&request)?;
            let filter = authorization
                .current()?
                .follow_filter(wire::SessionFilter::default())?;
            for head in view.heads.iter().flatten() {
                if let Some(record) = &head.last {
                    if filter.project(record.clone())? != Some(record.clone()) {
                        return Err(StreamError::AccessDenied);
                    }
                }
            }
            for record in view
                .retained
                .iter()
                .filter_map(|selected| selected.record.as_ref())
            {
                if filter.project(record.clone())? != Some(record.clone()) {
                    return Err(StreamError::AccessDenied);
                }
            }
            if let (Some(page), Some(visibility)) = (view.children.take(), visibility) {
                view.children = Some(
                    visibility.page(
                        page,
                        request
                            .children
                            .as_ref()
                            .and_then(|page| page.tail_stream.as_ref()),
                    )?,
                );
            }
            Ok(Bytes::from(
                wire_codec::heads_to_wire(view)?.encode_to_vec(),
            ))
        }),
    )
}
