use super::*;

pub(super) fn validate_resume(
    request: &crate::ChildrenRequest,
    resume: &wire::SessionCursor,
) -> Result<(), StreamError> {
    if resume.next == 0 {
        if !resume.opaque.is_empty() {
            return Err(StreamError::InvalidArgument);
        }
    } else {
        let path = StreamPath::new(
            std::str::from_utf8(&resume.opaque).map_err(|_| StreamError::InvalidArgument)?,
        )?;
        if path.parent() != request.parent {
            return Err(StreamError::InvalidArgument);
        }
    }
    Ok(())
}

pub(super) fn source<P: StreamProvider + ?Sized>(
    provider: Arc<P>,
    request: crate::ChildrenRequest,
    resume: wire::SessionCursor,
    identity: wire::SessionChannel,
) -> Source {
    let state = (provider, request, resume, None::<crate::ChildStream>);
    Box::pin(
        stream::try_unfold(
            state,
            |(provider, request, mut resume, mut children)| async move {
                if children.is_none() {
                    let mut opened = provider.children(request.clone()).await?;
                    if resume.next > 0 {
                        let anchor = StreamPath::new(
                            std::str::from_utf8(&resume.opaque)
                                .map_err(|_| StreamError::InvalidArgument)?,
                        )?;
                        // Validate existence before scanning: an invalid anchor must never
                        // park waiting for a future creation. Replayed paths are stable.
                        provider.watermarks(anchor.clone()).await?;
                        for index in 0..resume.next {
                            let child = opened.next().await.ok_or(StreamError::Unavailable)??;
                            if (child.path == anchor) != (index == resume.next - 1) {
                                return Err(StreamError::Unavailable);
                            }
                        }
                    }
                    children = Some(opened);
                    return Ok(Some((
                        Event::Opened(None),
                        (provider, request, resume, children),
                    )));
                }
                let child = children
                    .as_mut()
                    .ok_or(StreamError::Unavailable)?
                    .next()
                    .await
                    .ok_or(StreamError::Unavailable)??;
                resume.next = resume.next.checked_add(1).ok_or(StreamError::OutOfRange)?;
                resume.opaque = Bytes::copy_from_slice(child.path.as_str().as_bytes());
                Ok(Some((
                    Event::Child(child, resume.clone()),
                    (provider, request, resume, children),
                )))
            },
        )
        .map(move |event| (identity, event)),
    )
}
