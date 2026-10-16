use super::*;

impl<S: crate::AsyncObjectStore> Context<'_, '_, S> {
    pub(super) async fn rewrite(
        &mut self,
        page: ObjectId,
        original_first: u64,
        original_end: u64,
        output_end: u64,
    ) -> Result<Owned<Vec<ExtentChild>>, FsError> {
        let mut stack = Owned::<Vec<InternalFrame>>::allocate(0, self.accounting)?;
        let result = async {
            let mut request = NodeRequest {
                page,
                original_first,
                original_end,
                output_end,
                height: 1,
            };
            'traverse: loop {
                let mut result = match self.enter_node(request).await? {
                    EnteredNode::Complete(result) => result,
                    EnteredNode::Internal(mut frame) => {
                        if let Some(next) = self.advance_frame(&mut frame)? {
                            stack.ensure_for_push(
                                usize::from(self.limits.maximum_page_height),
                                self.accounting,
                            )?;
                            stack.value.push(frame);
                            request = next;
                            continue;
                        }
                        self.finish_frame(frame).await?
                    }
                };
                loop {
                    let Some(mut frame) = stack.value.pop() else {
                        return Ok(result);
                    };
                    for summary in result.value.drain(..) {
                        frame
                            .rewritten
                            .ensure_for_push(usize::MAX, self.accounting)?;
                        frame.rewritten.value.push(summary);
                    }
                    result.retire(self.accounting)?;
                    if let Some(next) = self.advance_frame(&mut frame)? {
                        stack.ensure_for_push(
                            usize::from(self.limits.maximum_page_height),
                            self.accounting,
                        )?;
                        stack.value.push(frame);
                        request = next;
                        continue 'traverse;
                    }
                    result = self.finish_frame(frame).await?;
                }
            }
        }
        .await;
        stack.retire(self.accounting)?;
        result
    }

    pub(super) async fn enter_node(
        &mut self,
        request: NodeRequest,
    ) -> Result<EnteredNode, FsError> {
        if request.height > self.limits.maximum_page_height {
            return Err(extent_error(ExtentMutationError::HeightExceeded));
        }
        if request.output_end <= request.original_first {
            return Ok(EnteredNode::Complete(Owned::allocate(0, self.accounting)?));
        }
        self.maximum_seen_height = self.maximum_seen_height.max(request.height);
        let visited = VisitedObjectSet::insert_with_accounting(
            &mut self.visited,
            request.page,
            self.accounting,
        )?;
        if !visited.inserted {
            return Err(extent_error(ExtentMutationError::CycleOrAlias));
        }
        let decoded = self.read_page(request.page).await?;
        let Owned {
            value: page,
            allocation,
            bytes,
        } = decoded;
        match page {
            ExtentPage::Leaf(extents) => {
                let extents = Owned::new(extents, allocation, bytes);
                Ok(EnteredNode::Complete(
                    self.rewrite_leaf(
                        request.page,
                        extents,
                        request.original_first,
                        request.original_end,
                        request.output_end,
                    )
                    .await?,
                ))
            }
            ExtentPage::Internal(children) => {
                let children = Owned::new(children, allocation, bytes);
                validate_children(
                    &children.value,
                    request.original_first,
                    request.original_end,
                )
                .map_err(extent_error)?;
                Ok(EnteredNode::Internal(InternalFrame {
                    original: request.page,
                    children,
                    original_first: request.original_first,
                    original_end: request.original_end,
                    output_end: request.output_end,
                    height: request.height,
                    next_child: 0,
                    rewritten: Owned::allocate(0, self.accounting)?,
                }))
            }
        }
    }

    pub(super) fn advance_frame(
        &mut self,
        frame: &mut InternalFrame,
    ) -> Result<Option<NodeRequest>, FsError> {
        while frame.next_child < frame.children.value.len() {
            let index = frame.next_child;
            frame.next_child += 1;
            let child = frame.children.value.get(index).ok_or(WorkError::Overflow)?;
            if child.first_offset >= frame.output_end {
                self.charge_items(1)?;
                frame.next_child = frame.children.value.len();
                break;
            }
            let mut child_output_end = child.end_offset.min(frame.output_end);
            if index + 1 == frame.children.value.len() && frame.output_end > frame.original_end {
                child_output_end = frame.output_end;
            }
            let (first, last) = overlapping_patch_range(
                &self.patches.value,
                child.first_offset,
                child_output_end,
                self.accounting,
            )?;
            self.charge_items(1)?;
            let patch_touched = first < last;
            if child_output_end != child.end_offset || patch_touched {
                return Ok(Some(NodeRequest {
                    page: child.page,
                    original_first: child.first_offset,
                    original_end: child.end_offset,
                    output_end: child_output_end,
                    height: frame
                        .height
                        .checked_add(1)
                        .ok_or_else(|| extent_error(ExtentMutationError::HeightExceeded))?,
                }));
            }
            frame
                .rewritten
                .ensure_for_push(usize::MAX, self.accounting)?;
            frame.rewritten.value.push(child.clone());
        }
        Ok(None)
    }

    pub(super) async fn finish_frame(
        &mut self,
        frame: InternalFrame,
    ) -> Result<Owned<Vec<ExtentChild>>, FsError> {
        let InternalFrame {
            children,
            mut rewritten,
            original_first,
            original_end,
            original,
            height,
            ..
        } = frame;
        if rewritten.value.is_empty() || rewritten.value == children.value {
            if !rewritten.value.is_empty() {
                rewritten.value.clear();
                rewritten.value.push(ExtentChild {
                    first_offset: original_first,
                    end_offset: original_end,
                    page: original,
                });
            }
            children.retire(self.accounting)?;
            return Ok(rewritten);
        }
        if height == 1 && rewritten.value.len() == 1 {
            children.retire(self.accounting)?;
            return Ok(rewritten);
        }
        let result = self.write_internal_chunks(&rewritten.value).await;
        rewritten.retire(self.accounting)?;
        children.retire(self.accounting)?;
        result
    }

    pub(super) async fn rewrite_leaf(
        &mut self,
        original: ObjectId,
        extents: Owned<Vec<Extent>>,
        original_first: u64,
        original_end: u64,
        output_end: u64,
    ) -> Result<Owned<Vec<ExtentChild>>, FsError> {
        validate_leaf(&extents.value, original_first, original_end).map_err(extent_error)?;
        let mut clipped = Owned::<Vec<Extent>>::allocate(
            extents
                .value
                .len()
                .checked_add(usize::from(output_end > original_end))
                .ok_or(WorkError::Overflow)?,
            self.accounting,
        )?;
        clip_and_grow(&extents.value, output_end, original_end, &mut clipped.value)
            .map_err(extent_error)?;
        let (first, last) = overlapping_patch_range(
            &self.patches.value,
            original_first,
            output_end,
            self.accounting,
        )?;
        let patches = self
            .patches
            .value
            .get(first..last)
            .ok_or(WorkError::Overflow)?;
        let capacity = clipped
            .value
            .len()
            .checked_add(patches.len().checked_mul(2).ok_or(WorkError::Overflow)?)
            .ok_or(WorkError::Overflow)?;
        let mut rewritten = Owned::<Vec<Extent>>::allocate(capacity, self.accounting)?;
        self.accounting.admit(WorkCounters {
            items_examined: usize_to_u64(clipped.value.len())
                .checked_add(
                    usize_to_u64(patches.len())
                        .checked_mul(3)
                        .ok_or(WorkError::Overflow)?,
                )
                .ok_or(WorkError::Overflow)?,
            ..WorkCounters::UNCHARGED
        })?;
        let examined = apply_patches_linear(&clipped.value, patches, &mut rewritten.value)
            .map_err(extent_error)?;
        self.charge_items(examined)?;
        coalesce(&mut rewritten.value).map_err(extent_error)?;
        let result = if rewritten.value == extents.value {
            let mut result = Owned::<Vec<ExtentChild>>::allocate(1, self.accounting)?;
            result.value.push(ExtentChild {
                first_offset: original_first,
                end_offset: original_end,
                page: original,
            });
            Ok(result)
        } else {
            self.write_leaf_chunks(&rewritten.value).await
        };
        rewritten.retire(self.accounting)?;
        clipped.retire(self.accounting)?;
        extents.retire(self.accounting)?;
        result
    }

    pub(super) async fn write_leaf_chunks(
        &mut self,
        extents: &[Extent],
    ) -> Result<Owned<Vec<ExtentChild>>, FsError> {
        let mut result = Owned::<Vec<ExtentChild>>::allocate(0, self.accounting)?;
        let mut start = 0;
        while start < extents.len() {
            let (end_index, examined) =
                extent_leaf_chunk_end(extents, start, self.limits).map_err(extent_error)?;
            self.charge_items(examined)?;
            let chunk = extents.get(start..end_index).ok_or(WorkError::Overflow)?;
            let first = chunk.first().ok_or(WorkError::Overflow)?.offset;
            let end = extent_end(chunk.last().ok_or(WorkError::Overflow)?).map_err(extent_error)?;
            let mut items = Owned::<Vec<Extent>>::allocate(chunk.len(), self.accounting)?;
            items.value.extend_from_slice(chunk);
            let Owned {
                value,
                allocation,
                bytes,
            } = items;
            let page = Owned::new(ExtentPage::Leaf(value), allocation, bytes);
            let written = self.write_page(&page.value).await;
            page.retire(self.accounting)?;
            result.ensure_for_push(usize::MAX, self.accounting)?;
            result.value.push(ExtentChild {
                first_offset: first,
                end_offset: end,
                page: written?,
            });
            start = end_index;
        }
        Ok(result)
    }

    pub(super) async fn write_internal_chunks(
        &mut self,
        children: &[ExtentChild],
    ) -> Result<Owned<Vec<ExtentChild>>, FsError> {
        let mut cursor = 0;
        let mut chunks = 0;
        while cursor < children.len() {
            let (end, examined) =
                extent_internal_chunk_end(children, cursor, self.limits).map_err(extent_error)?;
            self.charge_items(examined)?;
            chunks += 1;
            cursor = end;
        }
        if children.len() > 1 && chunks >= children.len() {
            return Err(extent_error(ExtentMutationError::PageItemTooLarge));
        }
        let mut result = Owned::<Vec<ExtentChild>>::allocate(chunks, self.accounting)?;
        let mut start = 0;
        while start < children.len() {
            let (end_index, examined) =
                extent_internal_chunk_end(children, start, self.limits).map_err(extent_error)?;
            self.charge_items(examined)?;
            let chunk = children.get(start..end_index).ok_or(WorkError::Overflow)?;
            let first = chunk.first().ok_or(WorkError::Overflow)?.first_offset;
            let end = chunk.last().ok_or(WorkError::Overflow)?.end_offset;
            let mut items = Owned::<Vec<ExtentChild>>::allocate(chunk.len(), self.accounting)?;
            items.value.extend_from_slice(chunk);
            let Owned {
                value,
                allocation,
                bytes,
            } = items;
            let page = Owned::new(ExtentPage::Internal(value), allocation, bytes);
            let written = self.write_page(&page.value).await;
            page.retire(self.accounting)?;
            result.value.push(ExtentChild {
                first_offset: first,
                end_offset: end,
                page: written?,
            });
            start = end_index;
        }
        Ok(result)
    }

    pub(super) async fn finish_root(
        &mut self,
        mut summaries: Owned<Vec<ExtentChild>>,
    ) -> Result<ObjectId, FsError> {
        if summaries.value.is_empty() {
            summaries.retire(self.accounting)?;
            return self.write_page(&ExtentPage::Leaf(Vec::new())).await;
        }
        let mut height = self.maximum_seen_height;
        while summaries.value.len() > 1 {
            height = height
                .checked_add(1)
                .ok_or_else(|| extent_error(ExtentMutationError::HeightExceeded))?;
            if height > self.limits.maximum_page_height {
                return Err(extent_error(ExtentMutationError::HeightExceeded));
            }
            let next = self.write_internal_chunks(&summaries.value).await;
            summaries.retire(self.accounting)?;
            summaries = next?;
        }
        let root = summaries.value.first().ok_or(WorkError::Overflow)?.page;
        summaries.retire(self.accounting)?;
        Ok(root)
    }

    pub(super) async fn read_page(&mut self, page: ObjectId) -> Result<Owned<ExtentPage>, FsError> {
        self.accounting.charge(WorkCounters {
            page_reads: 1,
            ..WorkCounters::UNCHARGED
        })?;
        let (read, retained) = crate::kernel::file_read::read_object_with_accounting(
            self.store,
            page,
            self.limits.maximum_page_object_bytes(),
            self.accounting,
            self.cancellation,
        )
        .await?;
        let before = match &self.accounting {
            KernelAccounting::Standalone { allocations, .. } => Some(allocations.live_bytes()),
            KernelAccounting::BorrowedStandalone { allocations, .. } => {
                Some(allocations.live_bytes())
            }
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { .. } => None,
        };
        let decoded = crate::kernel::extent::decode_extent_page_with_accounting(
            &read,
            self.limits,
            self.accounting,
        );
        if decoded.is_err()
            && let Some(before) = before
        {
            let ledger = match &mut self.accounting {
                KernelAccounting::Standalone { allocations, .. } => &mut *allocations,
                KernelAccounting::BorrowedStandalone { allocations, .. } => &mut **allocations,
                #[cfg(not(target_arch = "wasm32"))]
                KernelAccounting::NativeOriginal { .. } => {
                    unreachable!("standalone decoder custody")
                }
            };
            ledger
                .release(
                    ledger
                        .live_bytes()
                        .checked_sub(before)
                        .ok_or(WorkError::Overflow)?,
                )
                .map_err(extent_allocation_error)?;
        }
        drop(read);
        crate::kernel::file_read::release_standalone_read_bytes(retained, self.accounting)?;
        let (page, bytes, allocation) = decoded?;
        Ok(Owned::new(page, allocation, bytes))
    }

    pub(super) fn charge_items(&mut self, count: u64) -> Result<(), FsError> {
        charge_items(self.accounting, count)
    }

    pub(super) async fn write_page(&mut self, page: &ExtentPage) -> Result<ObjectId, FsError> {
        put_extent_page_with_accounting(
            self.store,
            page,
            self.limits,
            self.accounting,
            self.cancellation,
        )
        .await
    }
}
