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
    fn page(
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
