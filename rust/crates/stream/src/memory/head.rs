impl Namespace {
    fn origin(&self, path: &StreamPath) -> Result<Option<crate::ForkOrigin>, StreamError> {
        let mut parts = path.as_str().split('/');
        let first = parts.next().ok_or(StreamError::InvalidPath)?;
        let mut node = lookup(&self.roots, first).ok_or(StreamError::NotFound)?;
        let mut end = first.len();
        let mut selected = node.origin.as_ref().map(|origin| (origin, end));
        for part in parts {
            node = lookup(&node.children, part).ok_or(StreamError::NotFound)?;
            end += 1 + part.len();
            if let Some(origin) = &node.origin
                && selected.is_none_or(|(previous, _)| origin.decision >= previous.decision)
            {
                selected = Some((origin, end));
            }
        }
        let Some((origin, end)) = selected else {
            return Ok(None);
        };
        let relative = path.as_str().get(end..).ok_or(StreamError::InvalidPath)?;
        let mut source = &origin.snapshot;
        if let Some(relative) = relative.strip_prefix('/') {
            for part in relative.split('/') {
                let Some(child) = lookup(&source.children, part) else {
                    // Created after the fork, including a formerly excluded path.
                    return Ok(None);
                };
                source = child;
            }
        }
        Ok(Some(crate::ForkOrigin {
            source: if relative.is_empty() {
                origin.source.clone()
            } else {
                StreamPath::new(format!("{}{relative}", origin.source.as_str()))?
            },
            forked_at: if relative.is_empty() {
                origin.forked_at
            } else {
                source.data.tail
            },
            commit_id: origin.commit_id,
        }))
    }
}
