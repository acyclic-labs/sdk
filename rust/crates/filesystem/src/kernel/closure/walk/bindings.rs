use super::*;
use crate::kernel::bindings::{Binding, BindingFormat};
use crate::kernel::persistent_btree::Page;

impl<'a, S: crate::AsyncObjectStore> ProofContext<'a, S> {
    pub(super) async fn prove_bindings(
        &mut self,
        record: &FileRecord,
    ) -> Result<(), crate::FsError> {
        let Some(root) = record.bindings else {
            return Ok(());
        };
        Self::require_kind(root, ObjectKind::BindingPage)?;
        if map_get(&self.binding_proofs, &record.file_id).is_some_and(|proof| proof.root == root) {
            return Ok(());
        }
        let mut leaves = ProofVec::new(&self.meter);
        let mut pending: ProofVec<'a, PendingPage<Binding>> = ProofVec::new(&self.meter);
        pending.push((root, None, None, false))?;
        let mut pages = ProofVec::new(&self.meter);
        while let Some((id, lower, upper, exiting)) = pending.values.pop() {
            if exiting {
                self.complete(id)?;
                continue;
            }
            self.examine(1)?;
            if !set_insert(&mut pages, id)? {
                return Err(ClosureError::RoutingAlias.into());
            }
            if self.publication.is_some() {
                pending.push((id, None, None, true))?;
            }
            let page = self
                .read_ordered_page::<BindingFormat>(
                    id,
                    lower.as_ref().map(|key| &key.0),
                    upper.as_ref().map(|key| &key.0),
                    &mut pending,
                )
                .await?;
            if let (Page::Leaf(values), guard) = page {
                leaves.push((Page::Leaf(values), guard))?;
            }
        }
        map_insert(
            &mut self.binding_proofs,
            record.file_id,
            BindingProof { root, leaves },
        )?;
        Ok(())
    }

    fn binding_entries<'p>(proof: &'p BindingProof<'_>) -> impl Iterator<Item = &'p Binding> {
        proof.leaves.values.iter().flat_map(|(page, _)| match page {
            Page::Leaf(values) => values.as_slice(),
            Page::Internal(_) => &[],
        })
    }

    fn proven_tree_entry<'p>(
        &self,
        pages: &'p ProofVec<'a, (TreePage, ProofGuard)>,
        name: &LogicalName,
    ) -> Result<Option<&'p crate::TreeEntry>, crate::FsError> {
        self.meter.with_accounting(|accounting| {
            let index = crate::kernel::binary_search_with_accounting(
                &pages.values,
                accounting,
                |(page, _)| match page {
                    TreePage::Leaf(values) => values
                        .last()
                        .map_or(std::cmp::Ordering::Less, |entry| entry.name.cmp(name)),
                    TreePage::Internal(_) => std::cmp::Ordering::Less,
                },
            )?;
            let (Ok(index) | Err(index)) = index;
            let Some((TreePage::Leaf(values), _)) = pages.values.get(index) else {
                return Ok(None);
            };
            let index =
                crate::kernel::binary_search_with_accounting(values, accounting, |entry| {
                    entry.name.cmp(name)
                })?;
            Ok(index.ok().and_then(|index| values.get(index)))
        })
    }

    pub(super) fn validate_binding_relations(
        &self,
        root: FileId,
        records: &ProofVec<'a, (FileId, FileRecord)>,
        directories: &DirectoryProofs<'a>,
    ) -> Result<(), crate::FsError> {
        for (file_id, record) in &records.values {
            self.examine(1)?;
            let mismatch = || ClosureError::BindingRelationMismatch(*file_id);
            if *file_id == root {
                if record.bindings.is_some() || record.link_count != 1 {
                    return Err(mismatch().into());
                }
                continue;
            }
            let proof = map_get(&self.binding_proofs, file_id).ok_or_else(mismatch)?;
            if record.bindings != Some(proof.root) {
                return Err(mismatch().into());
            }
            let mut count = 0_u64;
            for binding in Self::binding_entries(proof) {
                self.examine(1)?;
                count = count.checked_add(1).ok_or(WorkError::Overflow)?;
                let pages = map_get(directories, &binding.parent).ok_or_else(mismatch)?;
                let forward = self
                    .proven_tree_entry(pages, &binding.name)?
                    .ok_or_else(mismatch)?;
                if forward.file_id != *file_id || forward.kind != record.kind {
                    return Err(mismatch().into());
                }
            }
            // Strict ordered bounds make reverse edges unique. Equal forward and
            // reverse cardinality plus the checks above prove both directions.
            if count != record.link_count {
                return Err(mismatch().into());
            }
        }
        Ok(())
    }

    pub(super) fn validate_increment_binding_relations(
        &self,
        root: FileId,
        diff: &crate::GenerationDiff,
    ) -> Result<(), crate::FsError> {
        for change in &diff.files {
            self.examine(1)?;
            let record = change.after.ok_or(crate::ObjectStoreError::Corrupt)?;
            let mismatch = || ClosureError::BindingRelationMismatch(record.file_id);
            if record.file_id == root {
                if record.bindings.is_some() || record.link_count != 1 {
                    return Err(mismatch().into());
                }
            } else if change.before.is_none() {
                let proof = map_get(&self.binding_proofs, &record.file_id).ok_or_else(mismatch)?;
                let mut entries = Self::binding_entries(proof);
                if record.bindings != Some(proof.root)
                    || entries.next().is_none()
                    || entries.next().is_some()
                {
                    return Err(mismatch().into());
                }
            }
        }
        for forward in &diff.bindings {
            self.examine(1)?;
            let entry = forward
                .after
                .as_ref()
                .ok_or(crate::ObjectStoreError::Corrupt)?;
            let mismatch = || ClosureError::BindingRelationMismatch(entry.file_id);
            let proof = map_get(&self.binding_proofs, &entry.file_id).ok_or_else(mismatch)?;
            let binding = Self::binding_entries(proof).next().ok_or_else(mismatch)?;
            if binding.parent != forward.directory_id || binding.name != entry.name {
                return Err(mismatch().into());
            }
        }
        Ok(())
    }
}
