use super::ordered::*;
use super::*;
use crate::executor::stock_string_copy;

fn inventory(seed: &ForkSeed) -> impl Iterator<Item = &FileRef> {
    seed.inherited_context
        .iter()
        .chain(seed.reference_grants.iter().map(|grant| &grant.file))
        .chain(&seed.attachment_manifests)
}

fn matches_path(
    change: &acyclic_fs::ChangedPath,
    path: &str,
    directory: bool,
    io: &PreparationIo,
) -> Result<bool> {
    let mut selected = path.split('/');
    for component in change.path.components() {
        crate::model::charge_items(Some(io), 1)?;
        if selected.next().map(str::as_bytes) != Some(component.as_bytes()) {
            return Ok(false);
        }
    }
    Ok(if directory {
        selected.next().is_some()
    } else {
        selected.next().is_none()
    })
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> FilesystemForkVerifier<A, O> {
    #[cfg(not(target_arch = "wasm32"))]
    pub(super) async fn require_seed_file(
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
        require_seed(binding, seed, work)?;
        file.validate(&mut Some(&mut io.history_io()))?;
        if file.volume().provider() != &self.host.provider {
            return Err(crate::executor::stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Unauthorized,
                "fork file belongs to another provider",
            ));
        }
        let mut admitted = false;
        for selected in inventory(seed).chain(seed.summary.iter().flat_map(|summary| {
            std::iter::once(&summary.context)
                .chain(&summary.payloads)
                .chain(&summary.references)
        })) {
            crate::model::charge_items(Some(io), 1)?;
            if selected == file {
                admitted = true;
                break;
            }
        }
        if !admitted {
            return Err(crate::executor::stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Unauthorized,
                "fork file is not in the original admitted seed inventory",
            ));
        }
        self.allocation(seed, &seed.child_private_volume, io)
            .await?;
        if binding.verifier.audience() == &seed.parent
            && file.volume() == &seed.child_private_volume
        {
            return Ok(());
        }
        binding.require_native_work(work)?;
        require_file_read(&binding.scope, file, io)
    }

    async fn allocation(
        &self,
        seed: &ForkSeed,
        volume: &VolumeRef,
        io: &PreparationIo,
    ) -> Result<()> {
        self.host
            .read_fork_allocation(
                seed,
                volume,
                crate::filesystem::fork_preparer::seed_digest(seed, 16 * 1024 * 1024, Some(io))?,
                16 * 1024 * 1024,
                Some(io),
            )
            .await
            .map(|_| ())
    }

    async fn pinned(
        &self,
        volume: &VolumeRef,
        reference: &GenerationRef,
        io: &PreparationIo,
    ) -> Result<Generation<A, O>> {
        let workspace_ref =
            fork_preparer_io::content_workspace(&self.host.provider, volume, Some(io))?;
        let hold = io.guard_owned(workspace_ref.logical_owned_bytes()?)?;
        let workspace = fork_preparer_io::open(&self.host, &workspace_ref, io).await?;
        let generation =
            fork_preparer_io::generation(&self.host, &workspace, reference, io).await?;
        drop(workspace_ref);
        drop(hold);
        Ok(generation)
    }

    pub(super) async fn verify_seed(&self, seed: &ForkSeed, io: &PreparationIo) -> Result<()> {
        seed.validate(&mut Some(&mut io.history_io()))?;
        if seed.child_private_volume.provider() == &self.host.provider {
            self.allocation(seed, &seed.child_private_volume, io)
                .await?;
        }
        for resource in &seed.resources {
            crate::model::charge_items(Some(io), 1)?;
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
                self.allocation(seed, child, io).await?;
                let source = self.pinned(source, fork_point, io).await?;
                let child = self.pinned(child, initial, io).await?;
                if !io.sdk(
                    child
                        .matches_fork_selection(&source, None, io, io.cancellation())
                        .await,
                )? {
                    return Err(
                        io.invalid("project capture is not forked from the selected generation")
                    );
                }
            }
        }
        let mut unique = Vec::new();
        for file in inventory(seed) {
            crate::model::charge_items(Some(io), 1)?;
            if file.volume().provider() == &self.host.provider {
                let digest = crate::executor::with_history_io(Some(io), |work| {
                    crate::executor::json::canonical_json_digest(
                        file,
                        crate::store::default_projection_read_limits().maximum_bytes,
                        work,
                    )
                })?;
                fork_verify_insert(
                    &mut unique,
                    (digest, file),
                    |left, right| left.0.cmp(&right.0),
                    io,
                )?;
            }
        }
        let unique_hold = io.guard_owned(
            (unique.capacity() * std::mem::size_of::<([u8; 32], &FileRef)>()) as u64,
        )?;
        let mut total = 0_u64;
        for (_, file) in &unique {
            crate::model::charge_items(Some(io), 1)?;
            total = total
                .checked_add(file.descriptor().byte_length())
                .ok_or_else(|| io.invalid("fork reference bytes overflow"))?;
            if total > self.maximum_total_reference_bytes {
                return Err(io.invalid("fork reference bytes exceed aggregate limit"));
            }
        }
        if seed.child_private_volume.provider() == &self.host.provider {
            let mut prefix = None;
            for file in &seed.inherited_context {
                crate::model::charge_items(Some(io), 1)?;
                if file.path() == ".system/inherited-conversation/prefix.json" {
                    prefix = Some(file);
                    break;
                }
            }
            if seed.inherited_through_sequence > 0 && prefix.is_none() {
                return Err(io.invalid("fork omitted its materialized conversation prefix"));
            }
            if let Some(file) = prefix {
                if file.descriptor().media_type()
                    != "application/vnd.acyclic.harness.inherited-conversation+json"
                {
                    return Err(io.invalid("inherited conversation has the wrong media type"));
                }
                let bytes = self
                    .host
                    .read_pinned_io(file, self.maximum_inherited_bytes, Some(io))
                    .await?;
                let _bytes = io.guard_bytes(&bytes)?;
                let inherited = io.decode(
                    std::marker::PhantomData::<InheritedConversationPrefix>,
                    &bytes,
                    crate::wire::ErrorCode::Storage,
                )?;
                let canonical = io.encode(&inherited.0, io.remaining()?.bytes_encoded, true)?;
                let _canonical = io.guard_bytes(&canonical)?;
                if canonical.as_ref() != bytes.as_ref()
                    || file.display_name() != "inherited-conversation.json"
                {
                    return Err(io.invalid("inherited conversation is not canonical"));
                }
                if inherited.0.format_version != 3
                    || inherited.0.parent != seed.parent
                    || inherited.0.parent_revision != seed.parent_revision
                    || inherited.0.parent_agent == seed.child_agent
                    || inherited.0.through_sequence != seed.inherited_through_sequence
                    || !fork_verify_equal(&inherited.0.attached_agents, &seed.attached_agents, io)?
                {
                    return Err(
                        io.invalid("inherited conversation does not match the fork boundary")
                    );
                }
            }
        }
        if let Some(summary) = &seed.summary
            && summary.context.volume().provider() == &self.host.provider
        {
            let bytes = self
                .host
                .read_pinned_io(&summary.context, self.maximum_inherited_bytes, Some(io))
                .await?;
            let _bytes = io.guard_bytes(&bytes)?;
            let context = io.decode(
                std::marker::PhantomData::<crate::context::Context>,
                &bytes,
                crate::wire::ErrorCode::Storage,
            )?;
            crate::context::validate_projected_context(
                &context.0,
                summary.selection.limits,
                Some(io),
            )?;
            let canonical = io.encode(&context.0, io.remaining()?.bytes_encoded, true)?;
            let _canonical = io.guard_bytes(&canonical)?;
            if context.0.current_input_index.is_some() || canonical.as_ref() != bytes.as_ref() {
                return Err(io.invalid("summary capture is not a canonical historical Context"));
            }
            let mut observed = Vec::new();
            let mut declared = Vec::new();
            for file in context
                .0
                .messages
                .iter()
                .flat_map(|message| message.content.file_refs())
                .chain(context.0.metadata.values())
            {
                crate::model::charge_items(Some(io), 1)?;
                let digest = crate::executor::with_history_io(Some(io), |work| {
                    crate::executor::json::canonical_json_digest(file, u64::MAX, work)
                })?;
                fork_verify_insert(&mut observed, digest, Ord::cmp, io)?;
            }
            for file in summary.payloads.iter().chain(&summary.references) {
                crate::model::charge_items(Some(io), 1)?;
                let digest = crate::executor::with_history_io(Some(io), |work| {
                    crate::executor::json::canonical_json_digest(file, u64::MAX, work)
                })?;
                fork_verify_insert(&mut declared, digest, Ord::cmp, io)?;
            }
            let _observed =
                io.guard_owned((observed.capacity() * std::mem::size_of::<[u8; 32]>()) as u64)?;
            let _declared =
                io.guard_owned((declared.capacity() * std::mem::size_of::<[u8; 32]>()) as u64)?;
            if !fork_verify_equal(&observed, &declared, io)? {
                return Err(
                    io.invalid("summary capture reference inventory differs from its Context")
                );
            }
        }
        if seed.child_private_volume.provider() == &self.host.provider {
            self.verify_scratch(seed, io).await?;
        }
        for resource in &seed.resources {
            for revision in [&resource.source, &resource.revision] {
                crate::model::charge_items(Some(io), 1)?;
                if revision.provider() != &self.host.provider {
                    continue;
                }
                match revision {
                    ResourceRevision::Project { volume, generation }
                    | ResourceRevision::PrivateVolume {
                        volume, generation, ..
                    } => {
                        let generation = self.pinned(volume, generation, io).await?;
                        fork_preparer_io::retain(&generation, Some(io)).await?;
                    }
                    ResourceRevision::SharedVolume(volume) => {
                        let reference = fork_preparer_io::content_workspace(
                            &self.host.provider,
                            volume,
                            Some(io),
                        )?;
                        let _hold = io.guard_owned(reference.logical_owned_bytes()?)?;
                        fork_preparer_io::resolve(&self.host, &reference, Some(io)).await?;
                    }
                    ResourceRevision::History(_) => {
                        return Err(crate::executor::stock_boundary(
                            Some(io),
                            crate::wire::ErrorCode::Unsupported,
                            "Filesystem cannot prove Stream history",
                        ));
                    }
                    _ => {
                        return Err(crate::executor::stock_boundary(
                            Some(io),
                            crate::wire::ErrorCode::Unsupported,
                            "fork resource requires its owning provider verifier",
                        ));
                    }
                }
            }
        }
        for (_, reference) in &unique {
            let bytes = self
                .host
                .read_pinned_io(reference, self.maximum_inherited_bytes, Some(io))
                .await?;
            let _bytes = io.guard_bytes(&bytes)?;
        }
        drop(unique);
        drop(unique_hold);
        Ok(())
    }

    async fn verify_scratch(&self, seed: &ForkSeed, io: &PreparationIo) -> Result<()> {
        let private = fork_preparer_io::content_workspace(
            &self.host.provider,
            &seed.child_private_volume,
            Some(io),
        )?;
        let _private = io.guard_owned(private.logical_owned_bytes()?)?;
        let head = fork_preparer_io::resolve(&self.host, &private, Some(io))
            .await?
            .generation;
        let _head = io.guard_owned(head.logical_owned_bytes()?)?;
        if head != seed.child_private_generation {
            return Err(crate::executor::stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Conflict,
                "child private generation changed before fork admission",
            ));
        }
        let mut scratch = None;
        for resource in &seed.resources {
            crate::model::charge_items(Some(io), 1)?;
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
        let (source, generation, selected_paths, initial) =
            scratch.ok_or_else(|| io.invalid("fork has no selected parent scratch lineage"))?;
        let source = self.pinned(source, generation, io).await?;
        let initial = self.pinned(&seed.child_private_volume, initial, io).await?;
        let mut paths = crate::model::vec_with_capacity(selected_paths.len(), Some(io))?;
        let mut owned = (paths.capacity() * std::mem::size_of::<String>()) as u64;
        for path in selected_paths {
            crate::model::charge_items(Some(io), 1)?;
            let copy = stock_string_copy(path, Some(io))?;
            owned = owned
                .checked_add(copy.capacity() as u64)
                .ok_or_else(|| io.invalid("fork selection ownership overflow"))?;
            paths.push(copy);
        }
        let _paths = io.guard_owned(owned)?;
        if !io.sdk(
            initial
                .matches_fork_selection(&source, Some(paths), io, io.cancellation())
                .await,
        )? {
            return Err(io.invalid("scratch capture differs from its pinned selection"));
        }
        drop(_paths);
        let head_generation = self.pinned(&seed.child_private_volume, &head, io).await?;
        let (diff, changes_allocation) = io.sdk(
            initial
                .diff_to(&head_generation, u32::MAX, io, io.cancellation())
                .await,
        )?;
        let (changes, paths_allocation) =
            io.sdk(diff.changed_paths(u32::MAX, io, io.cancellation()).await)?;
        drop(diff);
        drop(changes_allocation);
        let mut owned =
            (changes.capacity() * std::mem::size_of::<acyclic_fs::ChangedPath>()) as u64;
        for change in &changes {
            crate::model::charge_items(Some(io), 1)?;
            owned = owned
                .checked_add(change.path.logical_owned_bytes())
                .ok_or_else(|| io.invalid("changed path ownership overflow"))?;
            for component in change.path.components() {
                crate::model::charge_items(Some(io), 1)?;
                std::str::from_utf8(component.as_bytes())
                    .map_err(|_| io.invalid("scratch path is not UTF-8"))?;
            }
            if change.before.is_some() || change.after.is_none() {
                return Err(io.invalid("child scratch changed outside inherited context staging"));
            }
        }
        let _changes = if io.original_native_work().is_none() {
            io.retain_owned(owned)?;
            Some(io.guard_owned(owned)?)
        } else {
            None
        };
        let version = fork_preparer_io::hex_key(head.as_resource().key(), Some(io))?;
        let _version = io.guard_owned(version.capacity() as u64)?;
        for (index, file) in seed.inherited_context.iter().enumerate() {
            crate::model::charge_items(Some(io), 1)?;
            for prior in &seed.inherited_context[..index] {
                crate::model::charge_items(Some(io), 1)?;
                if prior.path() == file.path() {
                    return Err(io.invalid("inherited context has duplicate paths"));
                }
            }
            if file.version() != version {
                if seed.summary.is_none() {
                    return Err(io.invalid(
                        "inherited file is not pinned to the selected private generation",
                    ));
                }
                let before = io.owned_bytes()?;
                let selected = crate::executor::with_history_io(Some(io), |work| {
                    FileRef::new(
                        file.volume().copy(work)?,
                        stock_string_copy(file.path(), Some(io))?,
                        stock_string_copy(&version, Some(io))?,
                        FileDescriptor::new(
                            *file.descriptor().sha256(),
                            file.descriptor().byte_length(),
                            stock_string_copy(file.descriptor().media_type(), Some(io))?,
                        )?,
                        stock_string_copy(file.display_name(), Some(io))?,
                        work,
                    )
                })?;
                let _selected = io.guard_owned(
                    io.owned_bytes()?
                        .checked_sub(before)
                        .ok_or_else(|| io.invalid("selected file ownership underflows"))?,
                )?;
                let original = self
                    .host
                    .read_pinned_io(file, self.maximum_inherited_bytes, Some(io))
                    .await?;
                let _original = io.guard_bytes(&original)?;
                let final_bytes = self
                    .host
                    .read_pinned_io(&selected, self.maximum_inherited_bytes, Some(io))
                    .await?;
                let _final = io.guard_bytes(&final_bytes)?;
                if original != final_bytes {
                    return Err(io.invalid("summary inherited payload changed before admission"));
                }
            }
            let mut present = false;
            for change in &changes {
                crate::model::charge_items(Some(io), 1)?;
                if change
                    .after
                    .is_some_and(|record| record.kind == FileKind::Regular)
                    && matches_path(change, file.path(), false, io)?
                {
                    present = true;
                    break;
                }
            }
            if !present {
                return Err(io.invalid("child private volume lacks selected inherited context"));
            }
        }
        for change in &changes {
            crate::model::charge_items(Some(io), 1)?;
            let kind = change
                .after
                .expect("validated change has a new record")
                .kind;
            let mut admitted = kind == FileKind::Directory && change.path.components().is_empty();
            for file in &seed.inherited_context {
                crate::model::charge_items(Some(io), 1)?;
                if matches!(kind, FileKind::Regular | FileKind::Directory)
                    && matches_path(change, file.path(), kind == FileKind::Directory, io)?
                {
                    admitted = true;
                    break;
                }
            }
            if !admitted {
                return Err(io.invalid("child scratch changed outside inherited context staging"));
            }
        }
        if fork_preparer_io::resolve(&self.host, &private, Some(io))
            .await?
            .generation
            != head
        {
            return Err(crate::executor::stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Conflict,
                "child private volume changed during fork admission",
            ));
        }
        fork_preparer_io::retain(&head_generation, Some(io)).await?;
        drop(changes);
        drop(paths_allocation);
        drop(_changes);
        Ok(())
    }
}
