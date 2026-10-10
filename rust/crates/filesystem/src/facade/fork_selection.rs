//! Immutable, bounded namespace selection for an actual volume fork.

use super::*;
use crate::kernel::{
    FileTableMutation, TreeEntry, TreeMutation, apply_file_table_mutations_async,
    apply_tree_mutations_async,
};
use crate::kernel::allocation::AllocationLedger;
use crate::workspace::{
    WorkspaceError, customer_path_measured, grow_workspace_vec, namespace_path_error,
    new_workspace_vec, workspace_owned_strings_bytes, workspace_remaining, workspace_vec_bytes,
};

struct ForkDirectorySelection {
    path: Option<usize>,
    depth: usize,
    children: Vec<usize>,
}

fn fork_selection_merge_work(
    work: &mut WorkCounters,
    next: WorkCounters,
    live: u64,
    budget: WorkBudget,
) -> Result<(), WorkspaceError> {
    *work = work.with_backend(next, live)?;
    work.verify_ref(&budget)?;
    Ok(())
}

fn fork_selection_failure<E>(
    failure: OperationFailure<E>,
    work: &mut WorkCounters,
    live: u64,
    map: impl FnOnce(E) -> WorkspaceError,
) -> WorkspaceError {
    match work.with_backend(*failure.work, live) {
        Ok(actual) => {
            *work = actual;
            map(failure.error)
        }
        Err(error) => WorkspaceError::Work(error),
    }
}

fn fork_selection_allocation(error: crate::kernel::allocation::AllocationError) -> WorkspaceError {
    crate::collection::handle_allocation_error(error).into()
}

fn fork_selection_compare_names(
    left: &[LogicalName],
    right: &[LogicalName],
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<std::cmp::Ordering, WorkspaceError> {
    cancellation.check()?;
    work.charge_items(1, &budget)?;
    for (left, right) in left.iter().zip(right) {
        cancellation.check()?;
        work.charge_items(1, &budget)?;
        let order = left.cmp(right);
        if !order.is_eq() {
            return Ok(order);
        }
    }
    Ok(left.len().cmp(&right.len()))
}

fn fork_directory_components<'a>(
    directory: &ForkDirectorySelection,
    selected: &'a [NamespacePath],
) -> Result<&'a [LogicalName], WorkspaceError> {
    match directory.path {
        None => Ok(&[]),
        Some(index) => selected.get(index)
            .and_then(|path| path.components().get(..directory.depth))
            .ok_or(WorkspaceError::JoinLimit),
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Fs<A, O> {
    pub(crate) async fn verify_fork_selection(
        &self,
        source: &crate::Generation<A, O>,
        initial: &crate::Generation<A, O>,
        paths: Option<Vec<String>>,
    ) -> Result<bool, WorkspaceError> {
        self.verify_fork_selection_measured(
            source, initial, paths, WorkBudget::UNBOUNDED, &CancellationToken::new(),
        ).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
    }

    pub(crate) async fn verify_fork_selection_measured(
        &self,
        source: &crate::Generation<A, O>,
        initial: &crate::Generation<A, O>,
        paths: Option<Vec<String>>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredReceipt<bool, WorkspaceError> {
        let mut work = WorkCounters::default();
        let mut allocations = AllocationLedger::default();
        let result = async {
            let input_bytes = paths.as_ref().map(workspace_owned_strings_bytes).transpose()?.unwrap_or(0);
            // Incoming storage already exists. Record its actual peak even on
            // immediate cancellation/denial; this performs no new allocation.
            allocations.claim_bytes(input_bytes, 0, &mut work, WorkBudget::UNBOUNDED)
                .map_err(fork_selection_allocation)?;
            cancellation.check()?;
            work.verify_ref(&budget)?;
            if !self.same_deployment(&source.workspace.volume.fs)
                || !self.same_deployment(&initial.workspace.volume.fs) {
                return Err(WorkspaceError::ForeignGeneration);
            }
            // Transfer the owned input to the materializer. It adopts the same
            // backing; only still-parent-owned storage offsets nested work.
            allocations.release(input_bytes).map_err(fork_selection_allocation)?;
            let live = allocations.live_bytes();
            let (expected, source_root, spent) = self.materialize_forked_generation_root(
                source, initial.workspace.id(), source.workspace.volume.config, paths,
                workspace_remaining(work, budget, live)?, cancellation,
            ).await.map_err(|failure| fork_selection_failure(
                failure, &mut work, live, |error| error,
            ))?;
            fork_selection_merge_work(&mut work, spent, live, budget)?;
            let matches = expected.generation_root.digest == initial.id.digest();
            drop(expected);
            drop(source_root);
            Ok(matches)
        }.await;
        allocations.release(allocations.live_bytes()).map_err(fork_selection_allocation)
            .map_err(|error| OperationFailure::new(error, work))?;
        result.map(|value| OperationReceipt { value, work })
            .map_err(|error| OperationFailure::new(error, work))
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
    ) -> MeasuredResult<(ObjectId, WorkCounters), WorkspaceError> {
        let mut work = WorkCounters::default();
        let mut allocations = AllocationLedger::default();
        let result = async {
            let input_bytes = workspace_owned_strings_bytes(&paths)?;
            allocations.claim_bytes(input_bytes, 0, &mut work, WorkBudget::UNBOUNDED)
                .map_err(fork_selection_allocation)?;
            cancellation.check()?;
            work.verify_ref(&budget)?;
            let config = source.workspace.volume.config;
            let maximum = config.limits.maximum_mutations_per_batch;
            if paths.len() > maximum as usize {
                return Err(WorkspaceError::JoinLimit);
            }
            let mut whole_root = false;
            for path in &paths {
                cancellation.check()?;
                work.charge_items(1, &budget)?;
                whole_root |= path == "/";
            }
            if whole_root {
                // Root selection needs validation, not owned canonical copies.
                // This is the constructor's exact borrowed preflight engine.
                for path in &paths {
                    NamespacePath::validate_in_profile_with_work(
                        path, config.profile, config.limits, &mut work, budget, cancellation,
                    ).map_err(namespace_path_error)?;
                }
                return Ok(root.file_table);
            }
            let mut selected: Vec<NamespacePath> = Vec::new();
            for (index, path) in paths.iter().enumerate() {
                let mut duplicate = false;
                for prior in paths.iter().take(index) {
                    cancellation.check()?;
                    work.charge_items(1, &budget)?;
                    if prior == path {
                        duplicate = true;
                        break;
                    }
                }
                if duplicate { continue; }
                // Validate and construct the first path before allocating its
                // containing vector, so invalid input has no new allocations.
                let path = customer_path_measured(
                    path, config, &mut allocations, &mut work, budget, cancellation,
                )?;
                let element_copy = std::mem::size_of::<NamespacePath>() as u64;
                let growth_copy = if selected.len() == selected.capacity() {
                    selected.len().checked_mul(std::mem::size_of::<NamespacePath>())
                        .ok_or(crate::WorkError::Overflow)? as u64
                } else { 0 };
                work.admit(&WorkCounters { bytes_copied: element_copy.checked_add(growth_copy)
                    .ok_or(crate::WorkError::Overflow)?, ..WorkCounters::default() }, &budget)?;
                grow_workspace_vec(&mut selected, &mut allocations, &mut work, budget, cancellation)?;
                work.charge(&WorkCounters { bytes_copied: element_copy, ..WorkCounters::default() }, &budget)?;
                selected.push(path);
            }
            drop(paths);
            allocations.release(input_bytes).map_err(fork_selection_allocation)?;
            if maximum == 0 { return Err(WorkspaceError::JoinLimit); }
            let mut directories = new_workspace_vec(
                1, &mut allocations, &mut work, budget, cancellation,
            )?;
            work.charge(&WorkCounters { bytes_copied: std::mem::size_of::<ForkDirectorySelection>() as u64,
                ..WorkCounters::default() }, &budget)?;
            directories.push(ForkDirectorySelection { path: None, depth: 0, children: Vec::new() });
            // Plans retain indices into the canonical selected paths, not
            // redundant cloned ancestors or names. Borrowed prefix keys use
            // the exact NamespacePath/LogicalName ordering.
            for (index, path) in selected.iter().enumerate() {
                for depth in 0..path.depth() {
                    cancellation.check()?;
                    let prefix = path.components().get(..depth).ok_or(WorkspaceError::JoinLimit)?;
                    let mut inherited = false;
                    for included in &selected {
                        if included.depth() <= depth
                            && fork_selection_compare_names(
                                prefix.get(..included.depth()).ok_or(WorkspaceError::JoinLimit)?,
                                included.components(), &mut work, budget, cancellation,
                            )?.is_eq() {
                            inherited = true;
                            break;
                        }
                    }
                    if inherited { continue; }
                    let mut lower = 0;
                    let mut upper = directories.len();
                    let mut existing = false;
                    while lower < upper {
                        let middle = lower + (upper - lower) / 2;
                        let plan = directories.get(middle).ok_or(WorkspaceError::JoinLimit)?;
                        match fork_selection_compare_names(
                            fork_directory_components(plan, &selected)?, prefix,
                            &mut work, budget, cancellation,
                        )? {
                            std::cmp::Ordering::Less => lower = middle + 1,
                            std::cmp::Ordering::Greater => upper = middle,
                            std::cmp::Ordering::Equal => { lower = middle; existing = true; break; }
                        }
                    }
                    if !existing {
                        if directories.len() >= maximum as usize { return Err(WorkspaceError::JoinLimit); }
                        let moved = (directories.len() - lower).checked_add(1)
                            .and_then(|count| count.checked_mul(std::mem::size_of::<ForkDirectorySelection>()))
                            .ok_or(crate::WorkError::Overflow)? as u64;
                        let growth = if directories.len() == directories.capacity() {
                            directories.len().checked_mul(std::mem::size_of::<ForkDirectorySelection>())
                                .ok_or(crate::WorkError::Overflow)? as u64
                        } else { 0 };
                        work.admit(&WorkCounters { bytes_copied: moved.checked_add(growth)
                            .ok_or(crate::WorkError::Overflow)?, ..WorkCounters::default() }, &budget)?;
                        grow_workspace_vec(&mut directories, &mut allocations, &mut work, budget, cancellation)?;
                        work.charge(&WorkCounters { bytes_copied: moved, ..WorkCounters::default() }, &budget)?;
                        directories.insert(lower, ForkDirectorySelection {
                            path: Some(index), depth, children: Vec::new(),
                        });
                    }
                    let plan = directories.get_mut(lower).ok_or(WorkspaceError::JoinLimit)?;
                    let name = path.components().get(depth).ok_or(WorkspaceError::JoinLimit)?;
                    let mut name_lower = 0;
                    let mut name_upper = plan.children.len();
                    let mut present = false;
                    while name_lower < name_upper {
                        let middle = name_lower + (name_upper - name_lower) / 2;
                        let child = plan.children.get(middle).and_then(|index| selected.get(*index))
                            .and_then(|path| path.components().get(depth)).ok_or(WorkspaceError::JoinLimit)?;
                        match fork_selection_compare_names(
                            std::slice::from_ref(child), std::slice::from_ref(name),
                            &mut work, budget, cancellation,
                        )? {
                            std::cmp::Ordering::Less => name_lower = middle + 1,
                            std::cmp::Ordering::Greater => name_upper = middle,
                            std::cmp::Ordering::Equal => { present = true; break; }
                        }
                    }
                    if !present {
                        let moved = (plan.children.len() - name_lower).checked_add(1)
                            .and_then(|count| count.checked_mul(std::mem::size_of::<usize>()))
                            .ok_or(crate::WorkError::Overflow)? as u64;
                        let growth = if plan.children.len() == plan.children.capacity() {
                            plan.children.len().checked_mul(std::mem::size_of::<usize>())
                                .ok_or(crate::WorkError::Overflow)? as u64
                        } else { 0 };
                        work.admit(&WorkCounters { bytes_copied: moved.checked_add(growth)
                            .ok_or(crate::WorkError::Overflow)?, ..WorkCounters::default() }, &budget)?;
                        grow_workspace_vec(&mut plan.children, &mut allocations, &mut work, budget, cancellation)?;
                        work.charge(&WorkCounters { bytes_copied: moved, ..WorkCounters::default() }, &budget)?;
                        plan.children.insert(name_lower, index);
                    }
                }
            }
            let live = allocations.live_bytes();
            let opened = source.workspace.volume.checkout(
                GenerationSelector::Exact(source.id), CheckoutMode::read_only_pinned(),
                workspace_remaining(work, budget, live)?, cancellation,
            ).await.map_err(|failure| fork_selection_failure(failure, &mut work, live, WorkspaceError::from))?;
            fork_selection_merge_work(&mut work, opened.work, live, budget)?;
            let mut checkout = opened.value;
            let mut checkout_bytes = checkout.logical_owned_bytes();
            allocations.claim_bytes(checkout_bytes, 0, &mut work, WorkBudget::UNBOUNDED)
                .map_err(fork_selection_allocation)?;
            work.verify_ref(&budget)?;
            let directory_vector_bytes = workspace_vec_bytes(&directories);
            let mut overrides: Vec<FileRecord> = Vec::new();
            for plan in directories {
                cancellation.check()?;
                work.charge_items(1, &budget)?;
                let directory = match plan.path {
                    Some(index) => selected.get(index).ok_or(WorkspaceError::JoinLimit)?
                        .prefix_with_ledger(plan.depth, config.limits, &mut allocations, &mut work, budget, cancellation)
                        .map_err(namespace_path_error)?,
                    None => NamespacePath::new(Vec::new(), config.limits).map_err(namespace_path_error)?,
                };
                let live = allocations.live_bytes();
                let lookup = checkout.lookup_no_follow(
                    &directory, workspace_remaining(work, budget, live)?, cancellation,
                ).await.map_err(|failure| fork_selection_failure(failure, &mut work, live, WorkspaceError::from))?;
                fork_selection_merge_work(&mut work, lookup.work, live, budget)?;
                let retained = checkout.logical_owned_bytes();
                if retained >= checkout_bytes {
                    allocations.claim_bytes(retained - checkout_bytes, 0, &mut work, WorkBudget::UNBOUNDED)
                        .map_err(fork_selection_allocation)?;
                } else {
                    allocations.release(checkout_bytes - retained).map_err(fork_selection_allocation)?;
                }
                checkout_bytes = retained;
                work.verify_ref(&budget)?;
                let original = lookup.value.record.ok_or(WorkspaceError::NotFound)?;
                if original.kind != FileKind::Directory { return Err(WorkspaceError::NotDirectory); }
                let mut entries = new_workspace_vec(
                    plan.children.len(), &mut allocations, &mut work, budget, cancellation,
                )?;
                for index in &plan.children {
                    cancellation.check()?;
                    work.charge_items(1, &budget)?;
                    let selected_path = selected.get(*index).ok_or(WorkspaceError::JoinLimit)?;
                    let name = selected_path.components().get(plan.depth).ok_or(WorkspaceError::JoinLimit)?;
                    let path = selected_path.prefix_with_ledger(
                        plan.depth.checked_add(1).ok_or(crate::WorkError::Overflow)?,
                        config.limits, &mut allocations, &mut work, budget, cancellation,
                    ).map_err(namespace_path_error)?;
                    let live = allocations.live_bytes();
                    let lookup = checkout.lookup_no_follow(
                        &path, workspace_remaining(work, budget, live)?, cancellation,
                    ).await.map_err(|failure| fork_selection_failure(failure, &mut work, live, WorkspaceError::from))?;
                    fork_selection_merge_work(&mut work, lookup.work, live, budget)?;
                    let retained = checkout.logical_owned_bytes();
                    if retained >= checkout_bytes {
                        allocations.claim_bytes(retained - checkout_bytes, 0, &mut work, WorkBudget::UNBOUNDED)
                            .map_err(fork_selection_allocation)?;
                    } else {
                        allocations.release(checkout_bytes - retained).map_err(fork_selection_allocation)?;
                    }
                    checkout_bytes = retained;
                    work.verify_ref(&budget)?;
                    let record = lookup.value.record.ok_or(WorkspaceError::NotFound)?;
                    let path_bytes = path.retained_bytes();
                    drop(path);
                    allocations.release(path_bytes).map_err(fork_selection_allocation)?;
                    let handle_bytes = std::mem::size_of::<TreeMutation>() as u64;
                    work.admit(&WorkCounters { bytes_copied: handle_bytes.checked_add(name.as_bytes().len() as u64)
                        .ok_or(crate::WorkError::Overflow)?, ..WorkCounters::default() }, &budget)?;
                    let name = name.clone_with_ledger(&mut allocations, &mut work, budget, cancellation)
                        .map_err(namespace_path_error)?;
                    work.charge(&WorkCounters { bytes_copied: handle_bytes, ..WorkCounters::default() }, &budget)?;
                    entries.push(TreeMutation::Insert(TreeEntry { name, file_id: record.file_id, kind: record.kind }));
                }
                let directory_bytes = directory.retained_bytes();
                drop(directory);
                allocations.release(directory_bytes).map_err(fork_selection_allocation)?;
                let children_bytes = workspace_vec_bytes(&plan.children);
                drop(plan.children);
                allocations.release(children_bytes).map_err(fork_selection_allocation)?;
                let live = allocations.live_bytes();
                let child_budget = workspace_remaining(work, budget, live)?;
                let encoded = crate::kernel::encode_tree_page_measured(
                    &TreePage::Leaf(Vec::new()), config.limits.maximum_directory_page_entries,
                    child_budget, cancellation, 0,
                ).map_err(|failure| fork_selection_failure(
                    failure, &mut work, live, |error| WorkspaceError::from(tree_encode_error(error)),
                ))?;
                let (empty, spent) = self.put_encoded(
                    ObjectKind::TreePage, encoded, WorkCounters::default(), child_budget, cancellation,
                ).await.map_err(|failure| fork_selection_failure(failure, &mut work, live, WorkspaceError::from))?;
                fork_selection_merge_work(&mut work, spent, live, budget)?;
                let tree_root = if entries.is_empty() {
                    let bytes = workspace_vec_bytes(&entries);
                    drop(entries);
                    allocations.release(bytes).map_err(fork_selection_allocation)?;
                    empty
                } else {
                    let entry_bytes = entries.iter().try_fold(workspace_vec_bytes(&entries), |bytes, mutation| {
                        let TreeMutation::Insert(entry) = mutation else { return Err(WorkspaceError::JoinLimit); };
                        bytes.checked_add(entry.name.retained_bytes() as u64).ok_or_else(|| crate::WorkError::Overflow.into())
                    })?;
                    // Ownership moves to the kernel's actual mutation producer.
                    allocations.release(entry_bytes).map_err(fork_selection_allocation)?;
                    let live = allocations.live_bytes();
                    let tree = apply_tree_mutations_async(
                        &self.inner.objects, empty, entries, maximum, DecodeLimits::for_volume(config),
                        workspace_remaining(work, budget, live)?, cancellation,
                    ).await.map_err(|failure| fork_selection_failure(
                        failure, &mut work, live, |error| WorkspaceError::from(FsError::MergeTree(error)),
                    ))?;
                    fork_selection_merge_work(&mut work, tree.work, live, budget)?;
                    tree.root
                };
                let record = FileRecord { payload: FilePayload::Directory { entries: tree_root }, ..original };
                let mut existing = None;
                for (index, prior) in overrides.iter().enumerate() {
                    cancellation.check()?;
                    work.charge_items(1, &budget)?;
                    if prior.file_id == record.file_id { existing = Some(index); break; }
                }
                let copied = std::mem::size_of::<FileRecord>() as u64;
                let growth = if existing.is_none() && overrides.len() == overrides.capacity() {
                    overrides.len().checked_mul(std::mem::size_of::<FileRecord>())
                        .ok_or(crate::WorkError::Overflow)? as u64
                } else { 0 };
                work.admit(&WorkCounters { bytes_copied: copied.checked_add(growth)
                    .ok_or(crate::WorkError::Overflow)?, ..WorkCounters::default() }, &budget)?;
                if let Some(index) = existing {
                    work.charge(&WorkCounters { bytes_copied: copied, ..WorkCounters::default() }, &budget)?;
                    *overrides.get_mut(index).ok_or(WorkspaceError::JoinLimit)? = record;
                } else {
                    grow_workspace_vec(&mut overrides, &mut allocations, &mut work, budget, cancellation)?;
                    work.charge(&WorkCounters { bytes_copied: copied, ..WorkCounters::default() }, &budget)?;
                    overrides.push(record);
                }
            }
            allocations.release(directory_vector_bytes).map_err(fork_selection_allocation)?;
            let selected_bytes = selected.iter().try_fold(workspace_vec_bytes(&selected), |bytes, path| {
                bytes.checked_add(path.retained_bytes()).ok_or_else(|| crate::WorkError::Overflow.into())
            })?;
            drop(selected);
            allocations.release(selected_bytes).map_err(fork_selection_allocation)?;
            drop(checkout);
            allocations.release(checkout_bytes).map_err(fork_selection_allocation)?;
            self.select_fork_file_table(
                root, overrides, DecodeLimits::for_volume(config), maximum,
                &mut allocations, &mut work, budget, cancellation,
            ).await
        }.await;
        // The async scope has dropped every remaining owned value first.
        allocations.release(allocations.live_bytes()).map_err(fork_selection_allocation)
            .map_err(|error| OperationFailure::new(error, work))?;
        result.map(|root| (root, work)).map_err(|error| OperationFailure::new(error, work))
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Fs<A, O> {
    #[allow(
        clippy::too_many_arguments,
        clippy::too_many_lines,
        reason = "the selected closure shares one caller-owned allocation ledger and permit"
    )]
    pub(super) async fn select_fork_file_table(
        &self,
        root: &GenerationRoot,
        overrides: Vec<FileRecord>,
        limits: DecodeLimits,
        maximum: u32,
        ledger: &mut crate::kernel::allocation::AllocationLedger,
        work: &mut WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<ObjectId, WorkspaceError> {
        use crate::workspace::{
            grow_workspace_vec, new_workspace_vec, workspace_remaining, workspace_vec_bytes,
        };

        let override_bytes = workspace_vec_bytes(&overrides);
        let unrelated_live = ledger.live_bytes().checked_sub(override_bytes)
            .ok_or(WorkError::Overflow)?;
        let result = async {
            cancellation.check()?;
            if maximum == 0 || overrides.len() > maximum as usize {
                return Err(WorkspaceError::JoinLimit);
            }
            let mut pending = new_workspace_vec::<FileId>(1, ledger, work, budget, cancellation)?;
            fork_file_table_copy::<FileId>(1, work, budget, cancellation)?;
            pending.push(root.root_file_id);
            let mut links = new_workspace_vec::<(FileId, u64)>(1, ledger, work, budget, cancellation)?;
            fork_file_table_copy::<(FileId, u64)>(1, work, budget, cancellation)?;
            links.push((root.root_file_id, 1));
            let mut records = new_workspace_vec::<FileRecord>(0, ledger, work, budget, cancellation)?;

            while !pending.is_empty() {
                fork_file_table_copy::<FileId>(1, work, budget, cancellation)?;
                let id = pending.pop().ok_or(WorkspaceError::JoinLimit)?;
                let position = match fork_file_table_search(
                    &records, id, |record| record.file_id, work, budget, cancellation,
                )? {
                    Ok(_) => continue,
                    Err(position) => position,
                };
                if records.len() >= maximum as usize {
                    return Err(WorkspaceError::JoinLimit);
                }
                let mut overridden = None;
                for record in &overrides {
                    cancellation.check()?;
                    work.charge_items(1, &budget)?;
                    if record.file_id == id {
                        fork_file_table_copy::<FileRecord>(1, work, budget, cancellation)?;
                        overridden = Some(*record);
                        break;
                    }
                }
                let record = if let Some(record) = overridden {
                    record
                } else {
                    let live = ledger.live_bytes();
                    let lookup = lookup_file_record_async(
                        &self.inner.objects, root.file_table, id, limits,
                        workspace_remaining(*work, budget, live)?, cancellation,
                    ).await.map_err(|failure| fork_selection_failure(
                        failure, work, live,
                        |error| WorkspaceError::from(FsError::FileRecord(error)),
                    ))?;
                    fork_selection_merge_work(work, lookup.work, live, budget)?;
                    lookup.record.ok_or(WorkspaceError::NotFound)?
                };
                grow_workspace_vec(&mut records, ledger, work, budget, cancellation)?;
                fork_file_table_copy::<FileRecord>(
                    records.len() - position + 1, work, budget, cancellation,
                )?;
                records.insert(position, record);

                if let FilePayload::Directory { entries } = record.payload {
                    let mut cursor: Option<LogicalName> = None;
                    loop {
                        let live = ledger.live_bytes();
                        let mut page = list_tree_entries_async(
                            &self.inner.objects, entries, cursor.as_ref(),
                            limits.maximum_page_items, limits,
                            workspace_remaining(*work, budget, live)?, cancellation,
                        ).await.map_err(|failure| fork_selection_failure(
                            failure, work, live,
                            |error| WorkspaceError::from(FsError::Directory(error)),
                        ))?;
                        fork_selection_merge_work(work, page.work, live, budget)?;
                        // Returned backing already exists. Its capacity scan is
                        // ownership bookkeeping, not another namespace walk.
                        let page_bytes = page.entries.iter().try_fold(
                            workspace_vec_bytes(&page.entries), |bytes, entry| {
                                bytes.checked_add(entry.name.retained_bytes() as u64).ok_or(WorkError::Overflow)
                            },
                        )?;
                        ledger.claim_bytes(page_bytes, 0, work, WorkBudget::UNBOUNDED)
                            .map_err(fork_file_table_allocation_error)?;
                        work.verify_ref(&budget)?;

                        for entry in &page.entries {
                            cancellation.check()?;
                            work.charge_items(1, &budget)?;
                            if pending.len() >= maximum as usize {
                                return Err(WorkspaceError::JoinLimit);
                            }
                            let child = entry.file_id;
                            match fork_file_table_search(
                                &links, child, |binding| binding.0, work, budget, cancellation,
                            )? {
                                Ok(position) => {
                                    links[position].1 = links[position].1.checked_add(1)
                                        .ok_or(WorkspaceError::JoinLimit)?;
                                }
                                Err(position) => {
                                    if links.len() >= maximum as usize {
                                        return Err(WorkspaceError::JoinLimit);
                                    }
                                    grow_workspace_vec(&mut links, ledger, work, budget, cancellation)?;
                                    fork_file_table_copy::<(FileId, u64)>(
                                        links.len() - position + 1, work, budget, cancellation,
                                    )?;
                                    links.insert(position, (child, 1));
                                }
                            }
                            grow_workspace_vec(&mut pending, ledger, work, budget, cancellation)?;
                            fork_file_table_copy::<FileId>(1, work, budget, cancellation)?;
                            pending.push(child);
                        }

                        let has_more = page.has_more;
                        let next_cursor = if has_more {
                            fork_file_table_copy::<TreeEntry>(1, work, budget, cancellation)?;
                            Some(page.entries.pop().ok_or(WorkspaceError::JoinLimit)?.name)
                        } else {
                            None
                        };
                        let next_cursor_bytes = next_cursor.as_ref()
                            .map_or(0, |name| name.retained_bytes() as u64);
                        let old_cursor_bytes = cursor.as_ref()
                            .map_or(0, |name| name.retained_bytes() as u64);
                        drop(cursor);
                        ledger.release(old_cursor_bytes).map_err(fork_file_table_allocation_error)?;
                        cursor = next_cursor;
                        drop(page);
                        ledger.release(page_bytes.checked_sub(next_cursor_bytes)
                            .ok_or(WorkError::Overflow)?)
                            .map_err(fork_file_table_allocation_error)?;
                        if !has_more {
                            break;
                        }
                    }
                }
            }

            let pending_bytes = workspace_vec_bytes(&pending);
            drop(pending);
            ledger.release(pending_bytes).map_err(fork_file_table_allocation_error)?;
            drop(overrides);
            ledger.release(override_bytes).map_err(fork_file_table_allocation_error)?;

            let mut mutations = new_workspace_vec::<FileTableMutation>(
                records.len(), ledger, work, budget, cancellation,
            )?;
            for record in &records {
                cancellation.check()?;
                work.charge_items(1, &budget)?;
                let position = fork_file_table_search(
                    &links, record.file_id, |binding| binding.0, work, budget, cancellation,
                )?.map_err(|_| WorkspaceError::JoinLimit)?;
                fork_file_table_copy::<FileRecord>(1, work, budget, cancellation)?;
                let mut record = *record;
                record.link_count = links[position].1;
                fork_file_table_copy::<FileTableMutation>(1, work, budget, cancellation)?;
                mutations.push(FileTableMutation::Insert(record));
            }
            let record_bytes = workspace_vec_bytes(&records);
            drop(records);
            ledger.release(record_bytes).map_err(fork_file_table_allocation_error)?;
            let link_bytes = workspace_vec_bytes(&links);
            drop(links);
            ledger.release(link_bytes).map_err(fork_file_table_allocation_error)?;

            let live = ledger.live_bytes();
            let child_budget = workspace_remaining(*work, budget, live)?;
            let encoded = crate::kernel::encode_file_table_page_measured(
                &FileTablePage::Leaf(Vec::new()), limits.maximum_page_items,
                child_budget, cancellation,
            ).map_err(|failure| fork_selection_failure(
                failure, work, live,
                |error| WorkspaceError::from(file_table_encode_error(error)),
            ))?;
            let (empty, spent) = self.put_encoded(
                ObjectKind::FileTablePage, encoded, WorkCounters::default(),
                child_budget, cancellation,
            ).await.map_err(|failure| fork_selection_failure(
                failure, work, live, WorkspaceError::from,
            ))?;
            fork_selection_merge_work(work, spent, live, budget)?;

            // The kernel adopts this transferred vector. Only other backing
            // stays in the caller's live offset and nested allocation permit.
            let mutation_bytes = workspace_vec_bytes(&mutations);
            let live = ledger.live_bytes().checked_sub(mutation_bytes)
                .ok_or(WorkError::Overflow)?;
            let child_budget = workspace_remaining(*work, budget, live)?;
            ledger.release(mutation_bytes).map_err(fork_file_table_allocation_error)?;
            let table = apply_file_table_mutations_async(
                &self.inner.objects, empty, mutations, maximum, limits,
                child_budget, cancellation,
            ).await.map_err(|failure| fork_selection_failure(
                failure, work, live,
                |error| WorkspaceError::from(FsError::MergeFileTable(error)),
            ))?;
            fork_selection_merge_work(work, table.work, live, budget)?;
            Ok(table.root)
        }.await;
        // The completed future has dropped every local backing, including
        // transferred overrides on early failure. Preserve unrelated claims.
        let retired = ledger.live_bytes().checked_sub(unrelated_live)
            .ok_or(WorkError::Overflow)?;
        ledger.release(retired).map_err(fork_file_table_allocation_error)?;
        result
    }
}

fn fork_file_table_allocation_error(
    error: crate::kernel::allocation::AllocationError,
) -> WorkspaceError {
    WorkspaceError::from(crate::collection::handle_allocation_error(error))
}

fn fork_file_table_copy<T>(
    elements: usize,
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<(), WorkspaceError> {
    cancellation.check()?;
    let bytes = elements.checked_mul(std::mem::size_of::<T>())
        .ok_or(WorkError::Overflow)?;
    work.charge(&WorkCounters {
        bytes_copied: u64::try_from(bytes).map_err(|_| WorkError::Overflow)?,
        ..WorkCounters::default()
    }, &budget)?;
    Ok(())
}

fn fork_file_table_search<T>(
    values: &[T],
    id: FileId,
    mut key: impl FnMut(&T) -> FileId,
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<Result<usize, usize>, WorkspaceError> {
    let mut start = 0;
    let mut end = values.len();
    while start < end {
        cancellation.check()?;
        work.charge_items(1, &budget)?;
        let middle = start + (end - start) / 2;
        match key(&values[middle]).cmp(&id) {
            std::cmp::Ordering::Less => start = middle + 1,
            std::cmp::Ordering::Equal => return Ok(Ok(middle)),
            std::cmp::Ordering::Greater => end = middle,
        }
    }
    Ok(Err(start))
}
