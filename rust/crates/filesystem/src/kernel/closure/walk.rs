use super::*;
type PendingPage<N> = (ObjectId, Option<(N, ProofGuard)>, Option<(N, ProofGuard)>);
mod incremental;

pub(crate) struct ProofContext<'a, S> {
    store: &'a S,
    limits: ClosureLimits,
    objects: ProofVec<'a, ObjectId>,
    // ObjectId includes its kind: complete blob and extent proofs can share
    // one invocation-owned length table without a second allocation domain.
    proven_lengths: ProofVec<'a, (ObjectId, u64)>,
    pub(super) meter: ProofMeter<'a>,
    object_bytes: u64,
}

impl<'a, S: crate::AsyncObjectStore> ProofContext<'a, S> {
    pub(crate) fn new(store: &'a S, limits: ClosureLimits, meter: ProofMeter<'a>) -> Self {
        Self {
            store,
            limits,
            objects: ProofVec::new(&meter),
            proven_lengths: ProofVec::new(&meter),
            object_bytes: 0,
            meter,
        }
    }

    pub(crate) async fn prove_record_closure(
        &mut self,
        records: &[FileRecord],
    ) -> Result<(Vec<ObjectId>, ProofGuard), crate::FsError> {
        for record in records {
            self.meter.charge(WorkCounters::UNCHARGED)?;
            self.prove_record(record).await?;
        }
        self.take_objects()
    }

    fn examine(&self, count: usize) -> Result<(), crate::FsError> {
        self.meter.charge(WorkCounters {
            items_examined: usize_to_u64(count),
            ..WorkCounters::default()
        })
    }

    pub(super) fn take_objects(&mut self) -> Result<(Vec<ObjectId>, ProofGuard), crate::FsError> {
        let objects = std::mem::replace(&mut self.objects, ProofVec::new(&self.meter));
        objects.into_parts()
    }

    pub(crate) async fn prove(
        &mut self,
        generation_root: ObjectId,
    ) -> Result<(GenerationProof, ProofGuard), crate::FsError> {
        self.prove_cached(generation_root, None)
            .await
            .map(|(proof, guard, _)| (proof, guard))
    }

    /// Proves the closure, reusing the parent's published proof from `cache` when it is current.
    pub(crate) async fn prove_cached(
        &mut self,
        generation_root: ObjectId,
        cache: Option<&Mutex<CachedProofs>>,
    ) -> Result<(GenerationProof, ProofGuard, Option<PendingProof>), crate::FsError> {
        let store = self.store;
        let epochs = store.collection().map(|collection| collection.epochs());
        // Before the first object read: a collection begun later makes this proof stale.
        let began = crate::collection::CollectionEpoch::read(epochs);
        Self::require_kind(generation_root, ObjectKind::GenerationRoot)?;
        let root_bytes = self.read_object(generation_root, false).await?;
        let root = self.meter.decode_steps(|observe| {
            crate::kernel::generation::decode_generation_root(
                &root_bytes.0.bytes,
                self.limits.decode,
                &mut |step| observe(step),
            )
        })?;
        drop(root_bytes);
        let generation_id = GenerationId::new(generation_root.digest);
        let root_object_bytes = self.object_bytes;
        let slot = match cache {
            Some(cache) => Some(
                cache
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .slot(root.0.volume_id, &self.meter)?,
            ),
            None => None,
        };
        let previous = slot.as_ref().and_then(|slot| slot.current(epochs)).filter(|cached| {
            cached.limits == self.limits
                && root.0.volume_id == cached.root.volume_id
                && root.0.root_file_id == cached.root.root_file_id
                && root.0.required_features == cached.root.required_features
                && root.0.parents.as_slice() == [cached.generation_id]
        });
        if let Some(previous) = previous {
            let increment = match self.prove_increment(&root.0, &previous).await {
                Err(crate::FsError::Closure(
                    ClosureError::TooManyObjects { .. } | ClosureError::ClosureBytesExceeded { .. },
                )) => None,
                result => result?,
            };
            if let Some((file_count, logical_file_bytes, object_count)) = increment {
                let objects = self.take_objects()?;
                let mut output = (
                    GenerationProof {
                        root: root.0,
                        generation_id,
                        file_count,
                        logical_file_bytes,
                        objects: objects.0,
                        work: self.meter.work(),
                    },
                    objects.1,
                );
                let mut root_guard = root.1;
                output.1.absorb(&mut root_guard)?;
                let pending = self.remember(slot, began, &output.0, object_count)?;
                output.0.work = self.meter.work();
                return Ok((output.0, output.1, pending));
            }
            self.objects = ProofVec::new(&self.meter);
            set_insert(&mut self.objects, generation_root)?;
            self.proven_lengths = ProofVec::new(&self.meter);
            self.object_bytes = root_object_bytes;
        }
        let records = self.prove_file_table(root.0.file_table).await?;
        let file_count = usize_to_u64(records.values.len());
        if file_count > self.limits.maximum_files {
            return Err(ClosureError::TooManyFiles {
                observed: file_count,
                maximum: self.limits.maximum_files,
            }
            .into());
        }
        let mut logical_file_bytes = 0_u128;
        for record in records.values.iter().map(|(_, value)| value) {
            self.examine(1)?;
            let bytes = match record.payload {
                FilePayload::InlineRegular(data) => data.as_bytes().len() as u128,
                FilePayload::Regular { logical_bytes, .. } => u128::from(logical_bytes),
                _ => 0,
            };
            logical_file_bytes = logical_file_bytes
                .checked_add(bytes)
                .ok_or(ClosureError::LogicalBytesOverflow)?;
        }
        self.prove_records_and_namespace(root.0.root_file_id, &records)
            .await?;
        drop(records);
        let objects = self.take_objects()?;
        let mut root = root;
        let mut output = (
            GenerationProof {
                root: root.0,
                generation_id,
                file_count,
                logical_file_bytes,
                objects: objects.0,
                work: self.meter.work(),
            },
            objects.1,
        );
        output.1.absorb(&mut root.1)?;
        let object_count = output.0.objects.len() as u64;
        let pending = self.remember(slot, began, &output.0, object_count)?;
        output.0.work = self.meter.work();
        Ok((output.0, output.1, pending))
    }

    async fn read_object(
        &mut self,
        object: ObjectId,
        page: bool,
    ) -> Result<(crate::storage::ObjectRead, ProofGuard), crate::FsError> {
        let count = usize_to_u64(self.objects.values.len())
            .checked_add(u64::from(
                self.objects.values.binary_search(&object).is_err(),
            ))
            .ok_or(WorkError::Overflow)?;
        if count > self.limits.maximum_objects {
            return Err(ClosureError::TooManyObjects {
                observed: count,
                maximum: self.limits.maximum_objects,
            }
            .into());
        }
        set_insert(&mut self.objects, object)?;
        self.meter.charge(WorkCounters {
            page_reads: u64::from(page),
            ..WorkCounters::default()
        })?;
        let read = self
            .meter
            .read(self.store, object, self.limits.decode.maximum_object_bytes)
            .await?;
        self.object_bytes = self
            .object_bytes
            .checked_add(usize_to_u64(read.0.bytes.len()))
            .ok_or(WorkError::Overflow)?;
        let observed = self.object_bytes;
        if observed > self.limits.maximum_object_bytes {
            return Err(ClosureError::ClosureBytesExceeded {
                observed,
                maximum: self.limits.maximum_object_bytes,
            }
            .into());
        }
        Ok(read)
    }

    async fn prove_file_table(
        &mut self,
        root: ObjectId,
    ) -> Result<ProofVec<'a, (FileId, FileRecord)>, crate::FsError> {
        Self::require_kind(root, ObjectKind::FileTablePage)?;
        let mut records = ProofVec::new(&self.meter);
        let mut pending = ProofVec::new(&self.meter);
        pending.push((root, None, None))?;
        let mut pages = ProofVec::new(&self.meter);
        while let Some((page_id, lower, upper)) = pending.values.pop() {
            self.examine(1)?;
            if !set_insert(&mut pages, page_id)? {
                return Err(ClosureError::RoutingAlias.into());
            }
            let bytes = self.read_object(page_id, true).await?;
            let page = self.meter.decode_steps(|observe| {
                crate::kernel::file_table::decode_file_table_page(
                    &bytes.0.bytes,
                    self.limits.decode,
                    &mut |step| observe(step),
                )
            })?;
            drop(bytes);
            match &page.0 {
                FileTablePage::Leaf(entries) => {
                    validate_ordered_bounds(
                        entries.iter().map(|record| record.file_id),
                        lower,
                        upper,
                    )?;
                    for record in entries {
                        self.examine(1)?;
                        if map_insert(&mut records, record.file_id, *record)?.is_some() {
                            return Err(ClosureError::DuplicateFileRecord(record.file_id).into());
                        }
                    }
                }
                FileTablePage::Internal(children) => {
                    validate_ordered_bounds(
                        children.iter().map(|child| child.first),
                        lower,
                        upper,
                    )?;
                    for (index, child) in children.iter().enumerate().rev() {
                        self.examine(1)?;
                        let child_upper = children.get(index + 1).map(|next| next.first).or(upper);
                        pending.push((child.page, Some(child.first), child_upper))?;
                    }
                }
            }
        }
        Ok(records)
    }

    async fn prove_records_and_namespace(
        &mut self,
        root_file_id: FileId,
        records: &ProofVec<'a, (FileId, FileRecord)>,
    ) -> Result<(), crate::FsError> {
        let root = map_get(records, &root_file_id).ok_or(ClosureError::MissingRootFile)?;
        if !matches!(root.payload, FilePayload::Directory { .. }) {
            return Err(ClosureError::RootIsNotDirectory.into());
        }
        let mut directory_entries = ProofVec::new(&self.meter);
        for record in records.values.iter().map(|(_, value)| value) {
            self.examine(1)?;
            if let Some(entries) = self.prove_record(record).await? {
                map_insert(&mut directory_entries, record.file_id, entries)?;
            }
        }
        self.validate_namespace(root_file_id, records, &directory_entries)
    }

    async fn prove_record(
        &mut self,
        record: &FileRecord,
    ) -> Result<Option<ProofVec<'a, (TreePage, ProofGuard)>>, crate::FsError> {
        if !record.kind.is_supported_by_profile(self.limits.profile)
            || (record.kind == crate::kernel::FileKind::SymbolicLink && !self.limits.symbolic_links)
            || (record.link_count > 1 && !self.limits.hard_links)
        {
            return Err(ClosureError::UnsupportedVolumeSemantics.into());
        }
        self.prove_metadata(record.metadata).await?;
        match record.payload {
            FilePayload::Regular {
                logical_bytes,
                extents,
            } => {
                self.prove_extent_tree(extents, logical_bytes).await?;
            }
            FilePayload::Directory { entries } => return Ok(Some(self.prove_tree(entries).await?)),
            FilePayload::SymbolicLink {
                target_bytes,
                target,
            }
            | FilePayload::ReparsePoint {
                payload_bytes: target_bytes,
                payload: target,
            } => {
                if self.prove_blob(target).await? != target_bytes {
                    return Err(ClosureError::BlobLengthMismatch.into());
                }
            }
            FilePayload::InlineRegular(_) | FilePayload::Empty | FilePayload::Device { .. } => {}
        }
        Ok(None)
    }

    async fn prove_metadata(&mut self, metadata_id: ObjectId) -> Result<(), crate::FsError> {
        Self::require_kind(metadata_id, ObjectKind::Metadata)?;
        // Immutable metadata is authenticated once even when many records share it.
        if self.objects.values.binary_search(&metadata_id).is_ok() {
            return Ok(());
        }
        let bytes = self.read_object(metadata_id, false).await?;
        self.examine(1)?;
        let metadata = decode_file_metadata(&bytes.0.bytes, self.limits.decode, &mut |_| {
            Ok::<_, crate::kernel::CanonicalDecodeError>(())
        })?;
        drop(bytes);
        if let MetadataField::Value(attributes) = metadata.named_attributes {
            self.prove_attributes(attributes).await?;
        }
        for blob in [metadata.acl, metadata.security_descriptor] {
            if let MetadataField::Value(blob) = blob {
                self.prove_blob(blob).await?;
            }
        }
        Ok(())
    }

    async fn prove_tree(
        &mut self,
        root: ObjectId,
    ) -> Result<ProofVec<'a, (TreePage, ProofGuard)>, crate::FsError> {
        Self::require_kind(root, ObjectKind::TreePage)?;
        let mut leaves = ProofVec::new(&self.meter);
        let mut pending: ProofVec<'a, PendingPage<LogicalName>> = ProofVec::new(&self.meter);
        pending.push((root, None, None))?;
        let mut pages = ProofVec::new(&self.meter);
        while let Some((page_id, lower, upper)) = pending.values.pop() {
            self.examine(1)?;
            if !set_insert(&mut pages, page_id)? {
                return Err(ClosureError::RoutingAlias.into());
            }
            let bytes = self.read_object(page_id, true).await?;
            let page = self.meter.decode_steps(|observe| {
                crate::kernel::tree::decode_tree_page(
                    &bytes.0.bytes,
                    self.limits.decode,
                    &mut |step| observe(step),
                )
            })?;
            drop(bytes);
            match &page.0 {
                TreePage::Leaf(values) => {
                    validate_ordered_bounds(
                        values.iter().map(|entry| &entry.name),
                        lower.as_ref().map(|key| &key.0),
                        upper.as_ref().map(|key| &key.0),
                    )?;
                    leaves.push(page)?;
                }
                TreePage::Internal(children) => {
                    validate_ordered_bounds(
                        children.iter().map(|child| &child.first),
                        lower.as_ref().map(|key| &key.0),
                        upper.as_ref().map(|key| &key.0),
                    )?;
                    for (index, child) in children.iter().enumerate().rev() {
                        self.examine(1)?;
                        let child_lower = self
                            .meter
                            .clone_key::<crate::kernel::tree_mutation::TreeFormat>(
                                &child.first,
                                self.limits.decode.maximum_name_bytes,
                            )?;
                        let upper_key = children
                            .get(index + 1)
                            .map(|next| &next.first)
                            .or(upper.as_ref().map(|key| &key.0));
                        let child_upper = upper_key
                            .map(|key| {
                                self.meter
                                    .clone_key::<crate::kernel::tree_mutation::TreeFormat>(
                                        key,
                                        self.limits.decode.maximum_name_bytes,
                                    )
                            })
                            .transpose()?;
                        pending.push((child.page, Some(child_lower), child_upper))?;
                    }
                }
            }
        }
        Ok(leaves)
    }

    async fn prove_extent_tree(&mut self, root: ObjectId, size: u64) -> Result<(), crate::FsError> {
        Self::require_kind(root, ObjectKind::ExtentPage)?;
        // Reuse only a complete proof in this invocation. The declared length is
        // part of the proof: a shared root cannot authenticate a different span.
        if let Some(length) = map_get(&self.proven_lengths, &root) {
            return if *length == size {
                Ok(())
            } else {
                Err(ClosureError::RoutingBoundsMismatch.into())
            };
        }
        let mut pending = ProofVec::new(&self.meter);
        pending.push((root, 0_u64, size))?;
        let mut pages = ProofVec::new(&self.meter);
        while let Some((page_id, first, end)) = pending.values.pop() {
            self.examine(1)?;
            if !set_insert(&mut pages, page_id)? {
                return Err(ClosureError::RoutingAlias.into());
            }
            let bytes = self.read_object(page_id, true).await?;
            let page = self.meter.decode_steps(|observe| {
                crate::kernel::extent::decode_extent_page(
                    &bytes.0.bytes,
                    self.limits.decode,
                    &mut |step| observe(step),
                )
            })?;
            drop(bytes);
            match &page.0 {
                ExtentPage::Leaf(extents) => {
                    validate_extent_bounds(extents, first, end)?;
                    for extent in extents {
                        self.examine(1)?;
                        if !self.limits.sparse_files
                            && !matches!(extent.kind, ExtentKind::Content { .. })
                        {
                            return Err(ClosureError::UnsupportedVolumeSemantics.into());
                        }
                        if let ExtentKind::Content {
                            object,
                            object_offset,
                        } = extent.kind
                        {
                            let content_end = object_offset
                                .checked_add(extent.length)
                                .ok_or(ClosureError::RangeOverflow)?;
                            if content_end > self.prove_blob(object).await? {
                                return Err(ClosureError::BlobSpanOutsideObject.into());
                            }
                        }
                    }
                }
                ExtentPage::Internal(children) => {
                    if children.first().map(|child| child.first_offset) != Some(first)
                        || children.last().map(|child| child.end_offset) != Some(end)
                    {
                        return Err(ClosureError::RoutingBoundsMismatch.into());
                    }
                    for child in children.iter().rev() {
                        self.examine(1)?;
                        pending.push((child.page, child.first_offset, child.end_offset))?;
                    }
                }
            }
        }
        map_insert(&mut self.proven_lengths, root, size)?;
        Ok(())
    }

    pub(super) async fn prove_blob(&mut self, root: ObjectId) -> Result<u64, crate::FsError> {
        Self::require_kind(root, ObjectKind::Blob)?;
        if let Some(length) = map_get(&self.proven_lengths, &root) {
            return Ok(*length);
        }
        let mut pending = ProofVec::new(&self.meter);
        pending.push((root, 0_u64, None))?;
        let mut pages = ProofVec::new(&self.meter);
        let mut root_length = None;
        while let Some((page_id, first, end)) = pending.values.pop() {
            self.examine(1)?;
            if !set_insert(&mut pages, page_id)? {
                return Err(ClosureError::RoutingAlias.into());
            }
            let bytes = self.read_object(page_id, true).await?;
            let page = self.meter.decode_steps(|observe| {
                crate::kernel::blob::decode_blob_page(
                    &bytes.0.bytes,
                    self.limits.decode,
                    &mut |step| observe(step),
                )
            })?;
            drop(bytes);
            if page.0.first_offset != first || end.is_some_and(|value| page.0.end_offset != value) {
                return Err(ClosureError::RoutingBoundsMismatch.into());
            }
            if root_length.is_none() {
                root_length = Some(page.0.end_offset);
            }
            match &page.0.node {
                BlobNode::Internal(children) => {
                    for child in children.iter().rev() {
                        self.examine(1)?;
                        pending.push((child.page, child.first_offset, Some(child.end_offset)))?;
                    }
                }
                BlobNode::Leaf(chunks) => {
                    for chunk in chunks {
                        self.examine(1)?;
                        let expected = chunk.end_offset - chunk.first_offset;
                        Self::require_kind(chunk.chunk, ObjectKind::BlobChunk)?;
                        let value = self.read_object(chunk.chunk, false).await?;
                        if usize_to_u64(value.0.bytes.len()) != expected {
                            return Err(ClosureError::BlobLengthMismatch.into());
                        }
                    }
                }
            }
        }
        let length = root_length.ok_or(ClosureError::RoutingBoundsMismatch)?;
        map_insert(&mut self.proven_lengths, root, length)?;
        Ok(length)
    }

    async fn prove_attributes(&mut self, root: ObjectId) -> Result<(), crate::FsError> {
        Self::require_kind(root, ObjectKind::AttributePage)?;
        let mut pending: ProofVec<'a, PendingPage<AttributeName>> = ProofVec::new(&self.meter);
        pending.push((root, None, None))?;
        let mut pages = ProofVec::new(&self.meter);
        while let Some((page_id, lower, upper)) = pending.values.pop() {
            self.examine(1)?;
            if !set_insert(&mut pages, page_id)? {
                return Err(ClosureError::RoutingAlias.into());
            }
            let bytes = self.read_object(page_id, true).await?;
            let page = self.meter.decode_steps(|observe| {
                crate::kernel::attribute::decode_attribute_page(
                    &bytes.0.bytes,
                    self.limits.decode,
                    &mut |step| observe(step),
                )
            })?;
            drop(bytes);
            match &page.0 {
                AttributePage::Leaf(entries) => {
                    validate_ordered_bounds(
                        entries.iter().map(|entry| &entry.name),
                        lower.as_ref().map(|key| &key.0),
                        upper.as_ref().map(|key| &key.0),
                    )?;
                    for entry in entries {
                        self.examine(1)?;
                        if self.prove_blob(entry.value).await? != entry.value_bytes {
                            return Err(ClosureError::BlobLengthMismatch.into());
                        }
                    }
                }
                AttributePage::Internal(children) => {
                    validate_ordered_bounds(
                        children.iter().map(|child| &child.first),
                        lower.as_ref().map(|key| &key.0),
                        upper.as_ref().map(|key| &key.0),
                    )?;
                    for (index, child) in children.iter().enumerate().rev() {
                        self.examine(1)?;
                        let child_lower = self
                            .meter
                            .clone_key::<crate::kernel::attribute_mutation::AttributeFormat>(
                            &child.first,
                            self.limits.decode.maximum_name_bytes,
                        )?;
                        let upper_key = children
                            .get(index + 1)
                            .map(|next| &next.first)
                            .or(upper.as_ref().map(|key| &key.0));
                        let child_upper = upper_key
                            .map(|key| {
                                self.meter
                                    .clone_key::<crate::kernel::attribute_mutation::AttributeFormat>(
                                        key,
                                        self.limits.decode.maximum_name_bytes,
                                    )
                            })
                            .transpose()?;
                        pending.push((child.page, Some(child_lower), child_upper))?;
                    }
                }
            }
        }
        Ok(())
    }

    fn tree_entries<'b>(
        pages: &'b ProofVec<'a, (TreePage, ProofGuard)>,
    ) -> impl Iterator<Item = &'b TreeEntry> + 'b {
        pages.values.iter().flat_map(|page| match &page.0 {
            TreePage::Leaf(entries) => entries.as_slice(),
            TreePage::Internal(_) => &[],
        })
    }

    fn validate_namespace(
        &self,
        root: FileId,
        records: &ProofVec<'a, (FileId, FileRecord)>,
        directories: &ProofVec<'a, (FileId, ProofVec<'a, (TreePage, ProofGuard)>)>,
    ) -> Result<(), crate::FsError> {
        let mut links = ProofVec::new(&self.meter);
        map_insert(&mut links, root, 1_u64)?;
        for pages in directories.values.iter().map(|(_, value)| value) {
            for entry in Self::tree_entries(pages) {
                self.examine(1)?;
                let record = map_get(records, &entry.file_id)
                    .ok_or(ClosureError::MissingFileRecord(entry.file_id))?;
                if record.kind != entry.kind {
                    return Err(ClosureError::NamespaceKindMismatch(entry.file_id).into());
                }
                let count = map_get(&links, &entry.file_id)
                    .copied()
                    .unwrap_or(0_u64)
                    .checked_add(1)
                    .ok_or(ClosureError::RangeOverflow)?;
                map_insert(&mut links, entry.file_id, count)?;
            }
        }
        for record in records.values.iter().map(|(_, value)| value) {
            self.examine(1)?;
            if map_get(&links, &record.file_id).copied().unwrap_or(0) != record.link_count {
                return Err(ClosureError::LinkCountMismatch(record.file_id).into());
            }
            if matches!(record.payload, FilePayload::Directory { .. }) && record.link_count != 1 {
                return Err(ClosureError::DirectoryHardLink(record.file_id).into());
            }
        }
        let mut reachable = ProofVec::new(&self.meter);
        let mut visited_directories = ProofVec::new(&self.meter);
        let mut pending = ProofVec::new(&self.meter);
        pending.push(root)?;
        while let Some(file_id) = pending.values.pop() {
            self.examine(1)?;
            if !set_insert(&mut visited_directories, file_id)? {
                return Err(ClosureError::DirectoryHardLink(file_id).into());
            }
            set_insert(&mut reachable, file_id)?;
            let pages = map_get(directories, &file_id)
                .ok_or(ClosureError::MissingDirectoryTree(file_id))?;
            for entry in Self::tree_entries(pages) {
                self.examine(1)?;
                set_insert(&mut reachable, entry.file_id)?;
                if map_get(directories, &entry.file_id).is_some() {
                    pending.push(entry.file_id)?;
                }
            }
        }
        if reachable.values.len() != records.values.len() {
            return Err(ClosureError::UnreachableFileRecords.into());
        }
        Ok(())
    }

    fn require_kind(object: ObjectId, expected: ObjectKind) -> Result<(), crate::FsError> {
        if object.kind == expected {
            Ok(())
        } else {
            Err(ClosureError::WrongObjectKind.into())
        }
    }
}

fn validate_ordered_bounds<K: Ord>(
    mut keys: impl DoubleEndedIterator<Item = K>,
    lower: Option<K>,
    upper: Option<K>,
) -> Result<(), ClosureError> {
    let first = keys.next();
    if lower.is_some_and(|lower| first.as_ref() != Some(&lower)) {
        return Err(ClosureError::RoutingBoundsMismatch);
    }
    let last = keys.next_back().or(first);
    if upper.is_some_and(|upper| last.is_some_and(|last| last >= upper)) {
        return Err(ClosureError::RoutingBoundsMismatch);
    }
    Ok(())
}

pub(super) fn validate_extent_bounds(
    extents: &[crate::kernel::Extent],
    first: u64,
    end: u64,
) -> Result<(), ClosureError> {
    if first == end && extents.is_empty() {
        return Ok(());
    }
    if extents.first().map(|extent| extent.offset) != Some(first)
        || extents
            .last()
            .and_then(|extent| extent.offset.checked_add(extent.length))
            != Some(end)
    {
        return Err(ClosureError::RoutingBoundsMismatch);
    }
    Ok(())
}
