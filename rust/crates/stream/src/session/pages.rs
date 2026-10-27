//! Finite children pages on the session: the session form of
//! `StreamService.ChildrenPage`, answered as one fragmented one-shot reply.
use super::*;
use acyclic_auth::verify::VerifiedCapability;

/// The admitted credential of one page. A child is returned only when the
/// credential may list it, and its tail record only when the same credential
/// could follow that stream: its grant and its mandatory record predicate.
pub(super) struct Visibility {
    pub(super) authority: Arc<Authority>,
    pub(super) capability: Arc<VerifiedCapability>,
    pub(super) channels: usize,
}
impl Visibility {
    fn permits(&self, action: &'static str, path: &StreamPath) -> Result<bool, StreamError> {
        match self.authority.permits(
            &self.capability,
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
        let mut children = Vec::with_capacity(page.children.len());
        for mut child in page.children {
            if !self.permits("streams/list", &child.path)? {
                continue;
            }
            if let (Some(record), Some(relative)) = (child.tail_record.take(), tail_stream) {
                let target = StreamPath::new(format!("{}/{}", child.path, relative))?;
                if self.permits("streams/follow", &target)? {
                    child.tail_record = crate::FollowFilter::authorized(
                        wire::SessionFilter::default(),
                        &self.capability,
                    )?
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
