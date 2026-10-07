//! Immutable, bounded namespace selection for an actual volume fork.

use super::*;
use crate::kernel::{
    FileTableMutation, TreeEntry, TreeMutation, apply_file_table_mutations_async,
    apply_tree_mutations_async,
};
use crate::workspace::{WorkspaceError, customer_path};

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Fs<A, O> {
    pub(crate) async fn verify_fork_selection(
        &self,
        source: &crate::Generation<A, O>,
        initial: &crate::Generation<A, O>,
        paths: Option<Vec<String>>,
    ) -> Result<bool, WorkspaceError> {
        if !self.same_deployment(&source.workspace.volume.fs)
            || !self.same_deployment(&initial.workspace.volume.fs)
        {
            return Err(WorkspaceError::ForeignGeneration);
        }
        let (expected, _, _) = self
            .materialize_forked_generation_root(
                source,
                initial.workspace.id(),
                source.workspace.volume.config,
                paths,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await?;
        Ok(expected.generation_root.digest == initial.id.digest())
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one bounded selection transaction shares a single accumulated work budget"
    )]
    pub(super) async fn select_fork_paths(
        &self,
        source: &crate::Generation<A, O>,
        root: &GenerationRoot,
        paths: Vec<String>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<(ObjectId, WorkCounters), WorkspaceError> {
        let config = source.workspace.volume.config;
        let maximum = config.limits.maximum_mutations_per_batch;
        if paths.len() > maximum as usize {
            return Err(WorkspaceError::JoinLimit);
        }
        let selected = paths
            .iter()
            .map(|path| customer_path(path, config))
            .collect::<Result<BTreeSet<_>, _>>()?;
        if selected.iter().any(NamespacePath::is_root) {
            return Ok((root.file_table, WorkCounters::default()));
        }
        let root_path =
            NamespacePath::new(Vec::new(), config.limits).map_err(WorkspaceError::path)?;
        let mut directories =
            BTreeMap::<NamespacePath, BTreeSet<LogicalName>>::from([(root_path, BTreeSet::new())]);
        for path in &selected {
            let mut child = path.clone();
            while let Some(parent) = child.parent() {
                if !selected.iter().any(|selected| parent.is_within(selected)) {
                    let (_, name) = child.split_last().ok_or(WorkspaceError::JoinLimit)?;
                    directories
                        .entry(parent.clone())
                        .or_default()
                        .insert(name.clone());
                }
                child = parent;
            }
        }
        if directories.len() > maximum as usize {
            return Err(WorkspaceError::JoinLimit);
        }
        let checkout = source
            .workspace
            .volume
            .checkout(
                GenerationSelector::Exact(source.id),
                CheckoutMode::read_only_pinned(),
                budget,
                cancellation,
            )
            .await
            .map_err(WorkspaceError::engine)?;
        let mut work = checkout.work;
        let mut checkout = checkout.value;
        let mut overrides = BTreeMap::<FileId, FileRecord>::new();
        for (directory, names) in directories {
            let lookup = checkout
                .lookup_no_follow(
                    &directory,
                    remaining(work, budget).map_err(WorkspaceError::engine)?,
                    cancellation,
                )
                .await
                .map_err(WorkspaceError::engine)?;
            work = add(work, lookup.work).map_err(WorkspaceError::engine)?;
            let original = lookup.value.record.ok_or(WorkspaceError::NotFound)?;
            if original.kind != FileKind::Directory {
                return Err(WorkspaceError::NotDirectory);
            }
            let mut entries = Vec::with_capacity(names.len());
            for name in names {
                let mut components = directory.components().to_vec();
                components.push(name.clone());
                let path =
                    NamespacePath::new(components, config.limits).map_err(WorkspaceError::path)?;
                let lookup = checkout
                    .lookup_no_follow(
                        &path,
                        remaining(work, budget).map_err(WorkspaceError::engine)?,
                        cancellation,
                    )
                    .await
                    .map_err(WorkspaceError::engine)?;
                work = add(work, lookup.work).map_err(WorkspaceError::engine)?;
                let record = lookup.value.record.ok_or(WorkspaceError::NotFound)?;
                entries.push(TreeMutation::Insert(TreeEntry {
                    name,
                    file_id: record.file_id,
                    kind: record.kind,
                }));
            }
            let (empty, spent) = self
                .put_encoded(
                    ObjectKind::TreePage,
                    encode_tree_page(
                        &TreePage::Leaf(Vec::new()),
                        config.limits.maximum_directory_page_entries,
                    )
                    .map_err(WorkspaceError::engine)?,
                    WorkCounters::default(),
                    remaining(work, budget).map_err(WorkspaceError::engine)?,
                    cancellation,
                )
                .await
                .map_err(WorkspaceError::engine)?;
            work = add(work, spent).map_err(WorkspaceError::engine)?;
            let tree_root = if entries.is_empty() {
                empty
            } else {
                let tree = apply_tree_mutations_async(
                    &self.inner.objects,
                    empty,
                    entries,
                    maximum,
                    decode_limits(config),
                    remaining(work, budget).map_err(WorkspaceError::engine)?,
                    cancellation,
                )
                .await
                .map_err(WorkspaceError::engine)?;
                work = add(work, tree.work).map_err(WorkspaceError::engine)?;
                tree.root
            };
            overrides.insert(
                original.file_id,
                FileRecord {
                    payload: FilePayload::Directory { entries: tree_root },
                    ..original
                },
            );
        }
        // Enumerate only selected namespace closure, never omitted directories
        // or file bodies. Recount hard links in the selected initial view.
        let mut pending = vec![root.root_file_id];
        let mut links = BTreeMap::<FileId, u64>::from([(root.root_file_id, 1)]);
        let mut records = BTreeMap::<FileId, FileRecord>::new();
        while let Some(id) = pending.pop() {
            if records.contains_key(&id) {
                continue;
            }
            if records.len() >= maximum as usize {
                return Err(WorkspaceError::JoinLimit);
            }
            let record = if let Some(record) = overrides.get(&id) {
                *record
            } else {
                let lookup = lookup_file_record_async(
                    &self.inner.objects,
                    root.file_table,
                    id,
                    decode_limits(config),
                    remaining(work, budget).map_err(WorkspaceError::engine)?,
                    cancellation,
                )
                .await
                .map_err(WorkspaceError::engine)?;
                work = add(work, lookup.work).map_err(WorkspaceError::engine)?;
                lookup.record.ok_or(WorkspaceError::NotFound)?
            };
            records.insert(id, record);
            if let FilePayload::Directory { entries } = record.payload {
                let mut cursor = None;
                loop {
                    let page = list_tree_entries_async(
                        &self.inner.objects,
                        entries,
                        cursor.as_ref(),
                        config.limits.maximum_directory_page_entries,
                        decode_limits(config),
                        remaining(work, budget).map_err(WorkspaceError::engine)?,
                        cancellation,
                    )
                    .await
                    .map_err(WorkspaceError::engine)?;
                    work = add(work, page.work).map_err(WorkspaceError::engine)?;
                    cursor = page.entries.last().map(|entry| entry.name.clone());
                    for entry in page.entries {
                        if pending.len() >= maximum as usize {
                            return Err(WorkspaceError::JoinLimit);
                        }
                        let count = links.entry(entry.file_id).or_default();
                        *count = count.checked_add(1).ok_or(WorkspaceError::JoinLimit)?;
                        pending.push(entry.file_id);
                    }
                    if !page.has_more {
                        break;
                    }
                }
            }
        }
        let mutations = records
            .into_values()
            .map(|mut record| {
                record.link_count = *links
                    .get(&record.file_id)
                    .ok_or(WorkspaceError::JoinLimit)?;
                Ok(FileTableMutation::Insert(record))
            })
            .collect::<Result<Vec<_>, WorkspaceError>>()?;
        let (empty, spent) = self
            .put_encoded(
                ObjectKind::FileTablePage,
                encode_file_table_page(
                    &FileTablePage::Leaf(Vec::new()),
                    config.limits.maximum_directory_page_entries,
                )
                .map_err(WorkspaceError::engine)?,
                WorkCounters::default(),
                remaining(work, budget).map_err(WorkspaceError::engine)?,
                cancellation,
            )
            .await
            .map_err(WorkspaceError::engine)?;
        work = add(work, spent).map_err(WorkspaceError::engine)?;
        let table = apply_file_table_mutations_async(
            &self.inner.objects,
            empty,
            mutations,
            maximum,
            decode_limits(config),
            remaining(work, budget).map_err(WorkspaceError::engine)?,
            cancellation,
        )
        .await
        .map_err(WorkspaceError::engine)?;
        work = add(work, table.work).map_err(WorkspaceError::engine)?;
        Ok((table.root, work))
    }
}
