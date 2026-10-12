use super::*;

impl<A, O, D, S> LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
/// Opens `path`, with the facts the open proved when it opened a source
    pub(super) fn open_proven(
        &self,
        path: &MountPath,
    ) -> Result<(Arc<dyn MountOpenFile>, Option<MountLookup>), MountSourceError> {
        let path_text = self.path(path)?;
        let owner = SourceViewGate::callback_owner();
        // A create returns an open handle before the operation barrier publishes the authored
        // generation. Consult the authored checkout first so that the just-created file can be
        // written through that handle instead of falling through to the still-unaware lazy view.
        let (lease, resolution, source_file) = self.wait(|| async {
            let lease = self.source_view.read_for_callback(owner, None).await?;
            let (resolution, source_file) = self
                .resolve_opened(path, &path_text, owner, SourceProof::Open)
                .await?;
            Ok((lease, resolution, source_file))
        })?;
        let (lookup, source) = match resolution {
            Resolution::Absent => return Err(MountSourceError::NotFound),
            Resolution::Authored(authored) => {
                if let Some(file) = self.detached_by_id(authored.node.file_id)? {
                    return self
                        .bind_open_file(file, Some(authored.node.file_id), lease.generation)
                        .map(|file| (file, None));
                }
                return self
                    .authored
                    .open_file(path)
                    .and_then(|file| {
                        self.bind_open_file(file, Some(authored.node.file_id), lease.generation)
                    })
                    .map(|file| (file, None));
            }
            Resolution::Unauthored(lookup, source) => (lookup, source),
        };
        let source_generation = lease.generation;
        match lookup {
            LazyLookup::Authored {
                path: authored_path,
                stat,
            } if authored_path == path_text => self
                .authored
                .open_file(path)
                .and_then(|file| self.bind_open_file(file, Some(stat.file_id), source_generation))
                .map(|file| (file, None)),
            LazyLookup::Shadow { record, .. } => {
                if let Some(file) = self.detached_by_id(record.file_id)? {
                    return self
                        .bind_open_file(file, Some(record.file_id), source_generation)
                        .map(|file| (file, None));
                }
                drop(lease);
                let _mutation = self.mutation_lease(Some(source_generation))?;
                self.promote_locked(path)?;
                self.authored
                    .open_file(path)
                    .and_then(|file| {
                        self.bind_open_file(file, Some(record.file_id), source_generation)
                    })
                    .map(|file| (file, None))
            }
            LazyLookup::Authored { stat, .. } => {
                drop(lease);
                let _mutation = self.mutation_lease(Some(source_generation))?;
                self.promote_locked(path)?;
                self.authored
                    .open_file(path)
                    .and_then(|file| {
                        self.bind_open_file(file, Some(stat.file_id), source_generation)
                    })
                    .map(|file| (file, None))
            }
            LazyLookup::Source(node) if node.kind == SourceNodeKind::RegularFile => {
                let source_file = source.and(source_file).ok_or(MountSourceError::Stale)?;
                let expected_source = self.lazy.source_file_id(&node);
                let file: Arc<dyn MountOpenFile> = Arc::new(LazyOpenFile {
                    lazy: Arc::clone(&self.lazy),
                    authored: Arc::clone(&self.authored),
                    runtime: Arc::clone(&self.runtime),
                    path: path_text,
                    mount_path: path.clone(),
                    expected_source,
                    source_file,
                    source_node: node,
                    source_generation,
                    source_view: Arc::clone(&self.source_view),
                    detached: Arc::clone(&self.detached),
                    open_sources: Arc::clone(&self.open_sources),
                    promoted: Mutex::new(None),
                    unstaged: Arc::clone(&self.unstaged),
                });
                self.open_sources
                    .lock()
                    .map_err(|_| MountSourceError::Stale)?
                    .entry(expected_source)
                    .or_default()
                    .push(Arc::downgrade(&file));
                // The open proved the source file is this node, version and all.
                let proven = mount_lookup(LazyLookup::Source(node), expected_source);
                Ok((file, Some(proven)))
            }
            LazyLookup::Source(_) => Err(MountSourceError::Invalid(
                "open requires a regular file".to_owned(),
            )),
        }
    }

pub(super) fn bind_open_file(
        &self,
        file: Arc<dyn MountOpenFile>,
        file_id: Option<FileId>,
        generation: u64,
    ) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        let bound: Arc<dyn MountOpenFile> = Arc::new(ViewBoundOpenFile {
            inner: file,
            runtime: Arc::clone(&self.runtime),
            source_view: Arc::clone(&self.source_view),
            detached: Arc::clone(&self.detached),
            open_sources: Arc::clone(&self.open_sources),
            file_id,
            generation,
        });
        if let Some(file_id) = file_id {
            self.open_sources
                .lock()
                .map_err(|_| MountSourceError::Stale)?
                .entry(file_id)
                .or_default()
                .push(Arc::downgrade(&bound));
        }
        Ok(bound)
    }

pub(super) fn view_lease(&self, expected: Option<u64>) -> Result<SourceViewLease, MountSourceError> {
        let owner = SourceViewGate::callback_owner();
        self.wait(|| {
            let source_view = Arc::clone(&self.source_view);
            async move { source_view.read_for_callback(owner, expected).await }
        })
    }

/// Reads one range of a path's current content. A pin additionally
    pub(super) fn read_content(
        &self,
        path: &MountPath,
        pin: Option<MountContentPin>,
        offset: u64,
        length: u64,
        piece: u32,
        sink: &mut ContentSink<'_>,
    ) -> Result<(), MountSourceError> {
        let _lease = self.view_lease(None)?;
        if let Some(file) = self.detached_for_path(path)? {
            return stream_pieces(offset, length, piece, sink, |offset, length| {
                file.read_range(offset, length)
            });
        }
        let text = self.path(path)?;
        let owner = SourceViewGate::callback_owner();
        // The lookup or listing that projected this name remembered its
        // resolution, which answers here while the view proves it current.
        // Pinned content is opened at its exact version, which proves the
        // source; unpinned content follows the source as a lookup does.
        let proof = if pin.is_some() {
            SourceProof::Open
        } else {
            SourceProof::Lookup
        };
        let (resolution, source_file) =
            self.wait(|| async { self.resolve_opened(path, &text, owner, proof).await })?;
        match (resolution, source_file) {
            (Resolution::Absent, _) => Err(MountSourceError::NotFound),
            (Resolution::Authored(_), _) => {
                stream_pieces(offset, length, piece, sink, |offset, length| {
                    self.authored.read_range(path, offset, length)
                })
            }
            (Resolution::Unauthored(LazyLookup::Source(node), Some(source)), Some(file)) => {
                if pin.is_some_and(|pin| pin != source_content_pin(source, &node)) {
                    return Err(MountSourceError::Stale);
                }
                // One held source file serves the whole range; each read
                // proves it still is the pinned version.
                stream_pieces(offset, length, piece, sink, |offset, length| {
                    file.read_range(offset, u64::from(length), &crate::CancellationToken::new())
                        .map(|receipt| receipt.value)
                        .map_err(|failure| lazy_error(failure.error.into()))
                })
            }
            (Resolution::Unauthored(..), _) => {
                stream_pieces(offset, length, piece, sink, |offset, length| {
                    self.wait(|| async {
                        self.lazy
                            .read_range(&text, offset, u64::from(length))
                            .await
                            .map_err(lazy_error)
                    })
                })
            }
        }
    }
}

/// Delivers `length` bytes from `offset` to `sink` in pieces of at most
pub(super) fn stream_pieces(
    offset: u64,
    length: u64,
    piece: u32,
    sink: &mut ContentSink<'_>,
    mut read: impl FnMut(u64, u32) -> Result<Bytes, MountSourceError>,
) -> Result<(), MountSourceError> {
    let end = offset
        .checked_add(length)
        .ok_or_else(|| MountSourceError::Invalid("content range overflows".to_owned()))?;
    let mut next = offset;
    while next < end {
        let wanted = u32::try_from(end - next)
            .unwrap_or(u32::MAX)
            .min(piece.max(1));
        let bytes = read(next, wanted)?;
        let read_length = u64::try_from(bytes.len())
            .map_err(|_| MountSourceError::Invalid("content piece overflows".to_owned()))?;
        if read_length > u64::from(wanted) {
            return Err(MountSourceError::Invalid(
                "source returned more than was asked".to_owned(),
            ));
        }
        if read_length == 0 {
            break;
        }
        sink(next, bytes)?;
        next += read_length;
        if read_length < u64::from(wanted) {
            break;
        }
    }
    Ok(())
}

/// Binds one source node's exact content version into an opaque pin.
pub(super) fn source_content_pin(source: SourceReference, node: &SourceNode) -> MountContentPin {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic-fs-lazy-content-pin-v1\0");
    hasher.update(&source.identity);
    hasher.update(&node.file_identity);
    hasher.update(&node.version.0);
    MountContentPin(*hasher.finalize().as_bytes())
}
