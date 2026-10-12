use super::*;

fn search<T>(
    values: &[T],
    value: &T,
    compare: impl Fn(&T, &T) -> std::cmp::Ordering,
    io: &PreparationIo,
) -> Result<std::result::Result<usize, usize>> {
    let (mut low, mut high) = (0, values.len());
    while low < high {
        let middle = low + (high - low) / 2;
        fork_preparer_io::item(Some(io), 0)?;
        match compare(&values[middle], value) {
            std::cmp::Ordering::Less => low = middle + 1,
            std::cmp::Ordering::Greater => high = middle,
            std::cmp::Ordering::Equal => return Ok(Ok(middle)),
        }
    }
    Ok(Err(low))
}

pub(in crate::filesystem) fn fork_verify_insert<T>(
    values: &mut Vec<T>,
    value: T,
    compare: impl Fn(&T, &T) -> std::cmp::Ordering,
    io: &PreparationIo,
) -> Result<bool> {
    let index = match search(values, &value, compare, io)? {
        Ok(index) => {
            values[index] = value;
            return Ok(false);
        }
        Err(index) => index,
    };
    let capacity = values
        .len()
        .checked_add(1)
        .ok_or_else(|| io.invalid("fork verification vector overflow"))?;
    fork_preparer_io::reserve(values, capacity, Some(io))?;
    let shifted = (values.len() - index)
        .checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| io.invalid("fork verification copy overflow"))? as u64;
    let copies = WorkCounters {
        bytes_copied: shifted,
        ..Default::default()
    };
    io.preflight(copies)?;
    io.charge(copies)?;
    values.insert(index, value);
    Ok(true)
}

pub(in crate::filesystem) fn fork_verify_contains<T: Ord>(
    values: &[T],
    value: &T,
    io: &PreparationIo,
) -> Result<bool> {
    Ok(search(values, value, Ord::cmp, io)?.is_ok())
}

pub(in crate::filesystem) fn fork_verify_equal<T: Eq>(
    left: &[T],
    right: &[T],
    io: &PreparationIo,
) -> Result<bool> {
    if left.len() != right.len() {
        return Ok(false);
    }
    for (left, right) in left.iter().zip(right) {
        fork_preparer_io::item(Some(io), 0)?;
        if left != right {
            return Ok(false);
        }
    }
    Ok(true)
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> FilesystemForkVerifier<A, O> {
    #[cfg(not(target_arch = "wasm32"))]
    pub(in crate::filesystem) async fn require_seed_file_with_io(
        &self,
        seed: &ForkSeed,
        file: &FileRef,
        io: &PreparationIo,
    ) -> Result<()> {
        let work = io
            .original_native_work()
            .expect("native fork file verification");
        let binding = self.native_binding.as_ref().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            reason: "Filesystem verifier has no native authority",
        })?;
        binding.require_seed(seed, work)?;
        file.validate(&mut Some(&mut crate::store::history::native_work_io(work)?))?;
        if file.volume().provider() != &self.host.provider {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "fork file belongs to another provider",
            });
        }
        let mut admitted = false;
        for selected in seed
            .inherited_context
            .iter()
            .chain(seed.reference_grants.iter().map(|grant| &grant.file))
            .chain(seed.attachment_manifests.iter())
            .chain(seed.summary.iter().flat_map(|summary| {
                std::iter::once(&summary.context)
                    .chain(summary.payloads.iter())
                    .chain(summary.references.iter())
            }))
        {
            fork_preparer_io::item(Some(io), 0)?;
            if selected == file {
                admitted = true;
                break;
            }
        }
        if !admitted {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "fork file is not in the original admitted seed inventory",
            });
        }
        self.host
            .verify_fork_allocation_io(seed, &seed.child_private_volume, Some(io))
            .await?;
        if binding.verifier.audience() == &seed.parent
            && file.volume() == &seed.child_private_volume
        {
            return Ok(());
        }
        binding.require_file(file, io)
    }

    pub(in crate::filesystem) async fn matches_selection_with_io(
        selected: &Generation<A, O>,
        source: &Generation<A, O>,
        paths: Option<Vec<String>>,
        io: &PreparationIo,
    ) -> Result<bool> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.original_native_work() {
            return selected
                .matches_fork_selection(source, paths, work, work.cancellation())
                .await
                .map_err(|source| Error::NativeWorkspace {
                    source,
                    custody: None,
                });
        }
        io.receive(
            selected
                .matches_fork_selection(source, paths, io.remaining()?, io.cancellation())
                .await,
        )
    }

    pub(in crate::filesystem) async fn changed_paths_with_io(
        initial: &Generation<A, O>,
        head: &Generation<A, O>,
        io: &PreparationIo,
    ) -> Result<Vec<acyclic_fs::ChangedPath>> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.original_native_work() {
            let _changes_custody;
            let (changes, custody) = initial
                .diff_to(head, u32::MAX, work, work.cancellation())
                .await
                .map(|(value, allocation)| (value, allocation.expect("native result allocation")))
                .map_err(|source| Error::NativeWorkspace {
                    source,
                    custody: None,
                })?;
            _changes_custody = custody;
            let (paths, custody) = changes
                .changed_paths(u32::MAX, work, work.cancellation())
                .await
                .map(|(value, allocation)| (value, allocation.expect("native result allocation")))
                .map_err(|source| Error::NativeWorkspace {
                    source,
                    custody: None,
                })?;
            drop(changes);
            drop(_changes_custody);
            io.absorb_native_custody(custody)?;
            return Ok(paths);
        }
        let (changes, _) = io.receive(
            initial
                .diff_to(head, u32::MAX, io.remaining()?, io.cancellation())
                .await,
        )?;
        io.receive(
            changes
                .changed_paths(u32::MAX, io.remaining()?, io.cancellation())
                .await,
        )
        .map(|(paths, _)| paths)
    }

    pub(in crate::filesystem) async fn verify_with_io(
        &self,
        seed: &ForkSeed,
        io: &PreparationIo,
    ) -> Result<()> {
        let mut validation = io.history_io();
        seed.validate(&mut Some(&mut validation))?;
        drop(validation);
        if seed.child_private_volume.provider() == &self.host.provider {
            self.host
                .verify_fork_allocation_io(seed, &seed.child_private_volume, Some(&io))
                .await?;
        }
        for resource in &seed.resources {
            fork_preparer_io::item(Some(&io), 0)?;
            if let (
                ResourceRevision::Project {
                    volume: source,
                    generation: fork_point,
                },
                ResourceRevision::Project {
                    volume: child,
                    generation: initial,
                },
            ) = (&resource.source, &resource.revision)
                && source.provider() == &self.host.provider
            {
                self.host
                    .verify_fork_allocation_io(seed, child, Some(&io))
                    .await?;
                let source_ref =
                    fork_preparer_io::content_workspace(&self.host.provider, source, Some(&io))?;
                let child_ref =
                    fork_preparer_io::content_workspace(&self.host.provider, child, Some(&io))?;
                let source_workspace = fork_preparer_io::open(&self.host, &source_ref, &io).await?;
                let child_workspace = fork_preparer_io::open(&self.host, &child_ref, &io).await?;
                let source_generation =
                    fork_preparer_io::generation(&self.host, &source_workspace, fork_point, &io)
                        .await?;
                let child_generation =
                    fork_preparer_io::generation(&self.host, &child_workspace, initial, &io)
                        .await?;
                if !Self::matches_selection_with_io(&child_generation, &source_generation, None, io)
                    .await?
                {
                    return Err(interaction_host::interaction_boundary(
                        Some(io),
                        crate::wire::ErrorCode::Invalid,
                        "project capture is not forked from the selected generation",
                    ));
                }
            }
        }
        let mut unique_files: Vec<(String, &FileRef)> = Vec::new();
        for file in seed
            .inherited_context
            .iter()
            .chain(seed.reference_grants.iter().map(|grant| &grant.file))
            .chain(seed.attachment_manifests.iter())
        {
            fork_preparer_io::item(Some(&io), 0)?;
            if file.volume().provider() == &self.host.provider {
                let key = fork_preparer_io::reference_key(file, Some(&io))?;
                let owned = key.len() as u64;
                if !fork_verify_insert(
                    &mut unique_files,
                    (key, file),
                    |left, right| left.0.cmp(&right.0),
                    &io,
                )? {
                    io.release(owned)?;
                }
            }
        }
        let mut total = 0_u64;
        for (_, file) in &unique_files {
            fork_preparer_io::item(Some(&io), 0)?;
            total = total
                .checked_add(file.descriptor().byte_length())
                .ok_or_else(|| {
                    interaction_host::interaction_boundary(
                        Some(io),
                        crate::wire::ErrorCode::Invalid,
                        "fork reference bytes overflow",
                    )
                })?;
            if total > self.maximum_total_reference_bytes {
                return Err(interaction_host::interaction_boundary(
                    Some(io),
                    crate::wire::ErrorCode::Invalid,
                    "fork reference bytes exceed aggregate limit",
                ));
            }
        }
        if seed.child_private_volume.provider() == &self.host.provider {
            let mut prefix = None;
            for file in &seed.inherited_context {
                fork_preparer_io::item(Some(&io), 0)?;
                if file.path() == ".system/inherited-conversation/prefix.json" {
                    prefix = Some(file);
                    break;
                }
            }
            if seed.inherited_through_sequence > 0 && prefix.is_none() {
                return Err(interaction_host::interaction_boundary(
                    Some(io),
                    crate::wire::ErrorCode::Invalid,
                    "fork omitted its materialized conversation prefix",
                ));
            }
            if let Some(file) = prefix {
                if file.descriptor().media_type()
                    != "application/vnd.acyclic.harness.inherited-conversation+json"
                {
                    return Err(interaction_host::interaction_boundary(
                        Some(io),
                        crate::wire::ErrorCode::Invalid,
                        "inherited conversation has the wrong media type",
                    ));
                }
                let bytes = self
                    .host
                    .read_pinned_io(file, self.maximum_inherited_bytes, Some(&io))
                    .await?;
                let inherited = io.decode(
                    std::marker::PhantomData::<InheritedConversationPrefix>,
                    &bytes,
                    crate::wire::ErrorCode::Storage,
                )?;
                let canonical_guard;
                let canonical = io.encode(&inherited.0, io.remaining()?.bytes_encoded, true)?;
                canonical_guard = io.guard_bytes(&canonical)?;
                if canonical.as_slice() != bytes.as_ref()
                    || file.display_name() != "inherited-conversation.json"
                {
                    return Err(interaction_host::interaction_boundary(
                        Some(io),
                        crate::wire::ErrorCode::Invalid,
                        "inherited conversation is not canonical",
                    ));
                }
                if inherited.0.format_version != 3
                    || inherited.0.parent != seed.parent
                    || inherited.0.parent_revision != seed.parent_revision
                    || inherited.0.parent_agent == seed.child_agent
                    || inherited.0.through_sequence != seed.inherited_through_sequence
                    || !fork_verify_equal(&inherited.0.attached_agents, &seed.attached_agents, &io)?
                {
                    return Err(interaction_host::interaction_boundary(
                        Some(io),
                        crate::wire::ErrorCode::Invalid,
                        "inherited conversation does not match the fork boundary",
                    ));
                }
                drop(inherited);
                let bytes_owned = bytes.len() as u64;
                drop(canonical);
                drop(bytes);
                drop(canonical_guard);
                io.release(bytes_owned)?;
            }
        }
        if let Some(summary) = &seed.summary
            && summary.context.volume().provider() == &self.host.provider
        {
            let _bytes_hold;
            let bytes = self
                .host
                .read_pinned_io(&summary.context, self.maximum_inherited_bytes, Some(&io))
                .await?;
            _bytes_hold = io.guard_owned(bytes.len() as u64)?;
            let context = io.decode(
                std::marker::PhantomData::<crate::context::Context>,
                &bytes,
                crate::wire::ErrorCode::Storage,
            )?;
            crate::context::validate_projected_context(
                &context.0,
                summary.selection.limits,
                Some(&io),
            )?;
            let _canonical_hold;
            let canonical = io.encode(&context.0, io.remaining()?.bytes_encoded, true)?;
            _canonical_hold = io.guard_bytes(&canonical)?;
            if context.0.current_input_index.is_some() || canonical.as_slice() != bytes.as_ref() {
                return Err(interaction_host::interaction_boundary(
                    Some(io),
                    crate::wire::ErrorCode::Invalid,
                    "summary capture is not a canonical historical Context",
                ));
            }
            let _observed_hold;
            let mut observed = Vec::new();
            {
                let mut examine = |file: &FileRef| -> Result<()> {
                    fork_preparer_io::item(Some(&io), 0)?;
                    let digest = task_runtime::observed::digest(file, Some(&io), u64::MAX)?;
                    fork_verify_insert(&mut observed, digest, Ord::cmp, &io).map(|_| ())
                };
                for message in &context.0.messages {
                    for file in message.content.file_refs() {
                        examine(file)?;
                    }
                }
                for file in context.0.metadata.values() {
                    examine(file)?;
                }
            }
            _observed_hold =
                io.guard_owned((observed.capacity() * std::mem::size_of::<[u8; 32]>()) as u64)?;
            let _declared_hold;
            let mut declared = Vec::new();
            for file in summary.payloads.iter().chain(&summary.references) {
                fork_preparer_io::item(Some(&io), 0)?;
                let digest = task_runtime::observed::digest(file, Some(&io), u64::MAX)?;
                fork_verify_insert(&mut declared, digest, Ord::cmp, &io)?;
            }
            _declared_hold =
                io.guard_owned((declared.capacity() * std::mem::size_of::<[u8; 32]>()) as u64)?;
            if !fork_verify_equal(&observed, &declared, &io)? {
                return Err(interaction_host::interaction_boundary(
                    Some(io),
                    crate::wire::ErrorCode::Invalid,
                    "summary capture reference inventory differs from its Context",
                ));
            }
        }
        if seed.child_private_volume.provider() == &self.host.provider {
            let private = fork_preparer_io::content_workspace(
                &self.host.provider,
                &seed.child_private_volume,
                Some(&io),
            )?;
            let private_head = fork_preparer_io::resolve(&self.host, &private, Some(&io))
                .await?
                .generation;
            if private_head != seed.child_private_generation {
                return Err(interaction_host::interaction_boundary(
                    Some(io),
                    crate::wire::ErrorCode::Conflict,
                    "child private generation changed before fork admission",
                ));
            }
            let mut scratch = None;
            for resource in &seed.resources {
                fork_preparer_io::item(Some(&io), 0)?;
                if let (
                    ResourceRevision::PrivateVolume {
                        volume: source,
                        generation,
                        paths,
                    },
                    ResourceRevision::PrivateVolume {
                        volume: child,
                        generation: initial,
                        ..
                    },
                ) = (&resource.source, &resource.revision)
                    && child == &seed.child_private_volume
                {
                    scratch = Some((source, generation, paths, initial));
                    break;
                }
            }
            let scratch = scratch.ok_or_else(|| {
                interaction_host::interaction_boundary(
                    Some(io),
                    crate::wire::ErrorCode::Invalid,
                    "fork has no selected parent scratch lineage",
                )
            })?;
            let source_ref =
                fork_preparer_io::content_workspace(&self.host.provider, scratch.0, Some(&io))?;
            let source_workspace = fork_preparer_io::open(&self.host, &source_ref, &io).await?;
            let child_workspace = fork_preparer_io::open(&self.host, &private, &io).await?;
            let source =
                fork_preparer_io::generation(&self.host, &source_workspace, scratch.1, &io).await?;
            let initial =
                fork_preparer_io::generation(&self.host, &child_workspace, scratch.3, &io).await?;
            let mut paths = Vec::new();
            let capacity = paths
                .len()
                .checked_add(scratch.2.len())
                .ok_or_else(|| io.invalid("fork verification vector overflow"))?;
            fork_preparer_io::reserve(&mut paths, capacity, Some(&io))?;
            for path in scratch.2 {
                fork_preparer_io::item(Some(&io), 0)?;
                paths.push(fork_preparer_io::clone_text(path, Some(&io))?);
            }
            let selected_bytes = paths.capacity() * std::mem::size_of::<String>()
                + paths.iter().map(String::len).sum::<usize>();
            if !Self::matches_selection_with_io(&initial, &source, Some(paths), io).await? {
                return Err(interaction_host::interaction_boundary(
                    Some(io),
                    crate::wire::ErrorCode::Invalid,
                    "scratch capture differs from its pinned selection",
                ));
            }
            io.release(selected_bytes as u64)?;
            let head =
                fork_preparer_io::generation(&self.host, &child_workspace, &private_head, &io)
                    .await?;
            let changes = Self::changed_paths_with_io(&initial, &head, io).await?;
            let mut changed_owned =
                (changes.capacity() * std::mem::size_of::<acyclic_fs::ChangedPath>()) as u64;
            for change in &changes {
                changed_owned = changed_owned
                    .checked_add(std::mem::size_of_val(change.path.components()) as u64)
                    .ok_or_else(|| {
                        interaction_host::interaction_boundary(
                            Some(io),
                            crate::wire::ErrorCode::Invalid,
                            "changed path ownership overflow",
                        )
                    })?;
                for component in change.path.components() {
                    changed_owned = changed_owned
                        .checked_add(component.as_bytes().len() as u64)
                        .ok_or_else(|| {
                            interaction_host::interaction_boundary(
                                Some(io),
                                crate::wire::ErrorCode::Invalid,
                                "changed path ownership overflow",
                            )
                        })?;
                }
            }
            #[cfg(not(target_arch = "wasm32"))]
            if io.original_native_work().is_none() {
                io.retain_owned(changed_owned)?;
            }
            #[cfg(target_arch = "wasm32")]
            io.retain_owned(changed_owned)?;
            let mut expected = Vec::new();
            let version_bytes = private_head
                .as_resource()
                .key()
                .len()
                .checked_mul(2)
                .ok_or_else(|| {
                    interaction_host::interaction_boundary(
                        Some(io),
                        crate::wire::ErrorCode::Invalid,
                        "fork generation encoding overflow",
                    )
                })?;
            io.retain_owned(version_bytes as u64)?;
            io.allocation_attempt(version_bytes as u64)?;
            let version = hex::encode(private_head.as_resource().key());
            for file in &seed.inherited_context {
                fork_preparer_io::item(Some(&io), 0)?;
                if file.version() != version {
                    if seed.summary.is_none() {
                        return Err(interaction_host::interaction_boundary(
                            Some(io),
                            crate::wire::ErrorCode::Invalid,
                            "inherited file is not pinned to the selected private generation",
                        ));
                    }
                    let selected = crate::executor::with_history_io(Some(&io), |work| {
                        FileRef::new(
                            file.volume().copy(work)?,
                            fork_preparer_io::clone_text(file.path().into(), Some(&io))?,
                            fork_preparer_io::clone_text(&version, Some(&io))?,
                            FileDescriptor::new(
                                *file.descriptor().sha256(),
                                file.descriptor().byte_length(),
                                fork_preparer_io::clone_text(
                                    file.descriptor().media_type(),
                                    Some(&io),
                                )?,
                            )?,
                            fork_preparer_io::clone_text(file.display_name().into(), Some(&io))?,
                            work,
                        )
                    })?;
                    let original = self
                        .host
                        .read_pinned_io(file, self.maximum_inherited_bytes, Some(&io))
                        .await?;
                    let final_bytes = self
                        .host
                        .read_pinned_io(&selected, self.maximum_inherited_bytes, Some(&io))
                        .await?;
                    if original != final_bytes {
                        return Err(interaction_host::interaction_boundary(
                            Some(io),
                            crate::wire::ErrorCode::Invalid,
                            "summary inherited payload changed before admission",
                        ));
                    }
                    let original_owned = original.len() as u64;
                    let final_owned = final_bytes.len() as u64;
                    drop(original);
                    drop(final_bytes);
                    io.release(original_owned)?;
                    io.release(final_owned)?;
                }
                let path = crate::contract::preparation_format(
                    format_args!("/{}", file.path()),
                    Some(&io),
                )?;
                let owned = path.len() as u64;
                if !fork_verify_insert(&mut expected, path, Ord::cmp, &io)? {
                    io.release(owned)?;
                }
            }
            if expected.len() != seed.inherited_context.len() {
                return Err(interaction_host::interaction_boundary(
                    Some(io),
                    crate::wire::ErrorCode::Invalid,
                    "inherited context has duplicate paths",
                ));
            }
            let mut expected_directories = Vec::new();
            fork_verify_insert(
                &mut expected_directories,
                fork_preparer_io::clone_text("/", Some(&io))?,
                Ord::cmp,
                &io,
            )?;
            for path in &expected {
                let mut parent = path.as_str();
                while let Some((directory, _)) = parent.rsplit_once('/') {
                    fork_preparer_io::item(Some(&io), 0)?;
                    if directory.is_empty() {
                        break;
                    }
                    let directory_copy = fork_preparer_io::clone_text(directory, Some(&io))?;
                    if !fork_verify_insert(
                        &mut expected_directories,
                        directory_copy,
                        Ord::cmp,
                        &io,
                    )? {
                        io.release(directory.len() as u64)?;
                    }
                    parent = directory;
                }
            }
            let mut observed = Vec::new();
            for change in &changes {
                fork_preparer_io::item(Some(&io), 0)?;
                let mut length = 1_usize;
                for (index, name) in change.path.components().iter().enumerate() {
                    fork_preparer_io::item(Some(&io), 0)?;
                    std::str::from_utf8(name.as_bytes()).map_err(|_| {
                        interaction_host::interaction_boundary(
                            Some(io),
                            crate::wire::ErrorCode::Invalid,
                            "scratch path is not UTF-8",
                        )
                    })?;
                    length = length
                        .checked_add(name.as_bytes().len())
                        .and_then(|bytes| bytes.checked_add(usize::from(index > 0)))
                        .ok_or_else(|| {
                            interaction_host::interaction_boundary(
                                Some(io),
                                crate::wire::ErrorCode::Invalid,
                                "scratch path size overflow",
                            )
                        })?;
                }
                io.retain_owned(length as u64)?;
                io.allocation_attempt(length as u64)?;
                let copied = acyclic_fs::WorkCounters {
                    bytes_copied: (length - 1) as u64,
                    ..Default::default()
                };
                io.preflight(copied)?;
                io.charge(copied)?;
                let mut path = String::with_capacity(length);
                path.push('/');
                for (index, name) in change.path.components().iter().enumerate() {
                    if index > 0 {
                        path.push('/');
                    }
                    path.push_str(std::str::from_utf8(name.as_bytes()).map_err(|_| {
                        interaction_host::interaction_boundary(
                            Some(io),
                            crate::wire::ErrorCode::Invalid,
                            "scratch path is not UTF-8",
                        )
                    })?);
                }
                match change.after {
                    Some(record)
                        if record.kind == FileKind::Directory
                            && fork_verify_contains(&expected_directories, &path, &io)?
                            && change.before.is_none() =>
                    {
                        io.release(length as u64)?;
                    }
                    Some(record)
                        if record.kind == FileKind::Regular
                            && fork_verify_contains(&expected, &path, &io)?
                            && change.before.is_none() =>
                    {
                        if !fork_verify_insert(&mut observed, path, Ord::cmp, &io)? {
                            io.release(length as u64)?;
                        }
                    }
                    _ => {
                        return Err(interaction_host::interaction_boundary(
                            Some(io),
                            crate::wire::ErrorCode::Invalid,
                            "child scratch changed outside inherited context staging",
                        ));
                    }
                }
            }
            if !fork_verify_equal(&observed, &expected, &io)? {
                return Err(interaction_host::interaction_boundary(
                    Some(io),
                    crate::wire::ErrorCode::Invalid,
                    "child private volume lacks selected inherited context",
                ));
            }
            if fork_preparer_io::resolve(&self.host, &private, Some(&io))
                .await?
                .generation
                != private_head
            {
                return Err(interaction_host::interaction_boundary(
                    Some(io),
                    crate::wire::ErrorCode::Conflict,
                    "child private volume changed during fork admission",
                ));
            }
            fork_preparer_io::retain(&head, Some(&io)).await?;
            drop(changes);
            drop(version);
            io.release(changed_owned)?;
            io.release(version_bytes as u64)?;
            for collection in [expected, expected_directories, observed] {
                let owned = (collection.capacity() * std::mem::size_of::<String>()) as u64
                    + collection
                        .iter()
                        .map(|path| path.capacity() as u64)
                        .sum::<u64>();
                drop(collection);
                io.release(owned)?;
            }
        }
        for resource in &seed.resources {
            for revision in [&resource.source, &resource.revision] {
                fork_preparer_io::item(Some(&io), 0)?;
                if revision.provider() != &self.host.provider {
                    continue;
                }
                match revision {
                    ResourceRevision::Project { volume, generation }
                    | ResourceRevision::PrivateVolume {
                        volume, generation, ..
                    } => {
                        let reference = fork_preparer_io::content_workspace(
                            &self.host.provider,
                            volume,
                            Some(&io),
                        )?;
                        let workspace = fork_preparer_io::open(&self.host, &reference, &io).await?;
                        let generation =
                            fork_preparer_io::generation(&self.host, &workspace, generation, &io)
                                .await?;
                        fork_preparer_io::retain(&generation, Some(&io)).await?;
                    }
                    ResourceRevision::SharedVolume(volume) => {
                        let reference = fork_preparer_io::content_workspace(
                            &self.host.provider,
                            volume,
                            Some(&io),
                        )?;
                        fork_preparer_io::resolve(&self.host, &reference, Some(&io)).await?;
                    }
                    ResourceRevision::History(_) => {
                        return Err(interaction_host::interaction_boundary(
                            Some(io),
                            crate::wire::ErrorCode::Unsupported,
                            "Filesystem cannot prove Stream history",
                        ));
                    }
                    ResourceRevision::Context(_)
                    | ResourceRevision::Process(_)
                    | ResourceRevision::Artifact(_)
                    | ResourceRevision::Extension { .. } => {
                        return Err(interaction_host::interaction_boundary(
                            Some(io),
                            crate::wire::ErrorCode::Unsupported,
                            "fork resource requires its owning provider verifier",
                        ));
                    }
                }
            }
        }
        for (_, reference) in &unique_files {
            let bytes = self
                .host
                .read_pinned_io(reference, self.maximum_inherited_bytes, Some(&io))
                .await?;
            let owned = bytes.len() as u64;
            drop(bytes);
            io.release(owned)?;
        }
        Ok(())
    }
}
