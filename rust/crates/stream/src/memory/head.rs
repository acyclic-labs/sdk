struct PathObservation<'a> {
    node: &'a Arc<NamespaceNode>,
    origin: Option<(&'a ForkMarker, usize)>,
}
impl PathObservation<'_> {
    fn changed_at(&self) -> u64 {
        self.node
            .data
            .changed_at
            .max(self.origin.map_or(0, |(origin, _)| origin.decision))
    }
}
impl Namespace {
    fn observation_with(
        &self,
        path: &StreamPath,
        mut examine: impl FnMut() -> Result<(), StreamError>,
    ) -> Result<Option<PathObservation<'_>>, StreamError> {
        let mut node = None;
        let mut origin = None;
        let mut end = 0;
        let mut index = &self.roots;
        for part in path.as_str().split('/') {
            examine()?;
            let Some(current) = lookup(index, part) else {
                return Ok(None);
            };
            end += usize::from(node.is_some()) + part.len();
            if let Some(candidate) = &current.origin
                && origin.is_none_or(|(previous, _): (&ForkMarker, usize)| {
                    candidate.decision >= previous.decision
                })
            {
                origin = Some((candidate, end));
            }
            node = Some(current);
            index = &current.children;
        }
        Ok(node.map(|node| PathObservation { node, origin }))
    }
    fn origin(
        &self,
        path: &StreamPath,
    ) -> Result<Option<crate::Provenance<crate::StreamPath, crate::ForkCut>>, StreamError> {
        self.origin_with(
            path,
            || Ok(()),
            |source, relative| StreamPath::new(format!("{}{relative}", source.as_str())),
        )
    }

    fn origin_with(
        &self,
        path: &StreamPath,
        mut examine: impl FnMut() -> Result<(), StreamError>,
        extend: impl FnOnce(&StreamPath, &str) -> Result<StreamPath, StreamError>,
    ) -> Result<Option<crate::Provenance<crate::StreamPath, crate::ForkCut>>, StreamError> {
        let observation = self
            .observation_with(path, &mut examine)?
            .ok_or(StreamError::NotFound)?;
        let Some((origin, end)) = observation.origin else {
            return Ok(None);
        };
        let relative = path.as_str().get(end..).ok_or(StreamError::InvalidPath)?;
        let mut source = &origin.snapshot;
        if let Some(relative) = relative.strip_prefix('/') {
            for part in relative.split('/') {
                examine()?;
                let Some(child) = lookup(&source.children, part) else {
                    // Created after the fork, including a formerly excluded path.
                    return Ok(None);
                };
                source = child;
            }
        }
        Ok(Some(crate::Provenance {
            source: if relative.is_empty() {
                origin.source.clone()
            } else {
                extend(&origin.source, relative)?
            },
            inherited: crate::ForkCut {
                forked_at: if relative.is_empty() {
                    origin.forked_at
                } else {
                    source.data.tail
                },
                commit_id: origin.commit_id,
            },
        }))
    }
}
