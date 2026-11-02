use super::*;

struct SelectTheirsDriver;

impl crate::MergeDriver for SelectTheirsDriver {
    fn fingerprint(&self) -> &[u8] {
        b"workspace-select-theirs-v1"
    }

    fn mode(&self) -> crate::MergeDriverMode {
        crate::MergeDriverMode::Deterministic
    }

    fn resolve(
        &self,
        conflict: &crate::ConflictView,
    ) -> Result<crate::MergeResolution, crate::DriverError> {
        assert_eq!(conflict.kind, crate::ConflictKind::Text);
        assert_eq!(conflict.base, crate::ConflictValue::Text("base".into()));
        assert_eq!(conflict.ours, crate::ConflictValue::Text("main".into()));
        assert_eq!(conflict.theirs, crate::ConflictValue::Text("agent".into()));
        Ok(crate::MergeResolution::Select(crate::ConflictSide::Theirs))
    }
}

#[tokio::test]
async fn merge_drivers_resolve_and_publish_real_workspace_joins() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = unbounded!(fs, create_workspace("driver-main"))?;
    main.write_text("/shared", "base").await?;
    let agent = fork_workspace(
        &main,
        "driver-agent",
        ForkOptions::from_generation(unbounded!(main, head())?, IdempotencyKey::new()),
    )
    .await?;
    agent.write_text("/shared", "agent").await?;
    main.write_text("/shared", "main").await?;

    let plan = unbounded!(agent.join_into(&main), plan())?;
    let mut registry = crate::MergeDriverRegistry::new();
    registry.register("theirs", Arc::new(SelectTheirsDriver))?;
    registry.set_default("theirs")?;
    let mut cache = crate::MemoryMergeResolutionCache::default();
    let join_key = IdempotencyKey::new();
    let outcome = plan
        .apply_with_drivers(
            ApplyOptions {
                if_target: plan.target_head(),
                idempotency_key: join_key,
            },
            &registry,
            &mut cache,
            false,
            crate::PublicationPermit::Unrestricted,
        )
        .await?;
    let JoinOutcome::Applied(application) = outcome else {
        return Err("resolved join did not publish".into());
    };
    assert!(
        main.verify_join_commit(
            &value!(plan.commit_witness(
                &application,
                join_key,
                &WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            ))?,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new()
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?
    );
    assert_eq!(
        unbounded!(main, read("/shared", 16))?,
        OwnedBytes::from_static(b"agent")
    );
    Ok(())
}

#[tokio::test]
async fn default_text_driver_projects_markers_and_publishes_them() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = unbounded!(fs, create_workspace("text-driver-main"))?;
    main.write_text("/shared", "base\n").await?;
    let agent = fork_workspace(
        &main,
        "text-driver-agent",
        ForkOptions::from_generation(unbounded!(main, head())?, IdempotencyKey::new()),
    )
    .await?;
    agent.write_text("/shared", "agent\n").await?;
    main.write_text("/shared", "main\n").await?;

    let plan = unbounded!(agent.join_into(&main), plan())?;
    let JoinOutcome::Conflicted {
        conflicts,
        truncated,
    } = plan
        .apply(
            ApplyOptions {
                if_target: plan.target_head(),
                idempotency_key: IdempotencyKey::new(),
            },
            BTreeMap::new(),
            crate::PublicationPermit::Unrestricted,
        )
        .await?
    else {
        return Err("text join did not expose its conflict".into());
    };
    let described = plan.describe_conflicts(conflicts, truncated).await?;
    assert_eq!(described.conflicts.len(), 1);
    assert_eq!(described.conflicts[0].path.as_deref(), Some("/shared"));
    assert_eq!(described.conflicts[0].kind, crate::ConflictKind::Text);
    assert!(matches!(
        described.conflicts[0].ours,
        crate::ConflictValue::Text(ref text) if text == "main\n"
    ));
    let mut registry = crate::MergeDriverRegistry::new();
    registry.register("text", Arc::new(crate::DefaultTextMergeDriver))?;
    registry.set_default("text")?;
    let mut cache = crate::MemoryMergeResolutionCache::default();
    let outcome = plan
        .apply_with_drivers(
            ApplyOptions {
                if_target: plan.target_head(),
                idempotency_key: IdempotencyKey::new(),
            },
            &registry,
            &mut cache,
            false,
            crate::PublicationPermit::Unrestricted,
        )
        .await?;
    assert!(matches!(outcome, JoinOutcome::Applied(_)));
    let merged = String::from_utf8(unbounded!(main, read("/shared", 1024))?.to_vec())?;
    assert!(merged.contains("main"));
    assert!(merged.contains("agent"));
    // UTF-8 validity alone does not make a file text. The same binary gate
    // applies to every side before a conflict can reach the text driver.
    for (label, base, ours, theirs) in [
        ("base", "base\0\n", "main\n", "agent\n"),
        ("ours", "base\n", "main\0\n", "agent\n"),
        ("theirs", "base\n", "main\n", "agent\0\n"),
    ] {
        main.write_text("/binary", base).await?;
        let binary_agent = fork_workspace(
            &main,
            &format!("binary-driver-{label}"),
            ForkOptions::from_generation(unbounded!(main, head())?, IdempotencyKey::new()),
        )
        .await?;
        binary_agent.write_text("/binary", theirs).await?;
        main.write_text("/binary", ours).await?;
        let plan = unbounded!(binary_agent.join_into(&main), plan())?;
        let JoinOutcome::Conflicted {
            conflicts,
            truncated,
        } = plan
            .apply(
                ApplyOptions {
                    if_target: plan.target_head(),
                    idempotency_key: IdempotencyKey::new(),
                },
                BTreeMap::new(),
                crate::PublicationPermit::Unrestricted,
            )
            .await?
        else {
            return Err("binary join did not expose its conflict".into());
        };
        let described = plan.describe_conflicts(conflicts, truncated).await?;
        assert_eq!(described.conflicts.len(), 1);
        let view = &described.conflicts[0];
        assert_eq!(view.path.as_deref(), Some("/binary"));
        assert_eq!(view.kind, crate::ConflictKind::Binary);
        for (value, expected) in [&view.base, &view.ours, &view.theirs]
            .into_iter()
            .zip([base, ours, theirs])
        {
            let crate::ConflictValue::Binary(bytes) = value else {
                return Err("binary conflict became marker input".into());
            };
            assert_eq!(bytes, expected.as_bytes());
        }
        assert_eq!(
            crate::MergeDriver::resolve(&crate::DefaultTextMergeDriver, view),
            Err(crate::DriverError::Unsupported)
        );
        assert_eq!(unbounded!(main, head())?.id(), plan.target_head());
        assert_eq!(unbounded!(binary_agent, head())?.id(), plan.source_head());
    }
    Ok(())
}

#[tokio::test]
async fn merge_preview_keeps_clean_paths_and_conflicts_without_publication()
-> Result<(), Box<dyn Error>> {
    use crate::kernel::{DecodeLimits, FilePayload, LogicalName, NameEncoding, Owned};

    let fs = Fs::memory();
    let main = unbounded!(fs, create_workspace("preview-main"))?;
    main.write_text("/shared", "base").await?;
    main.write_text("/deleted", "base").await?;
    let agent = fork_workspace(
        &main,
        "preview-agent",
        ForkOptions::from_generation(unbounded!(main, head())?, IdempotencyKey::new()),
    )
    .await?;
    agent.write_text("/shared", "agent").await?;
    main.write_text("/shared", "main").await?;
    agent.remove("/deleted").await?;
    main.write_text("/deleted", "modified").await?;
    agent.write_text("/added", "agent-add").await?;
    main.write_text("/added", "main-add").await?;
    let mut linking = unbounded!(main, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(linking, hard_link("/added", "/a"))?;
    let TransactionCommit::Committed(_) = commit(&mut linking).await? else {
        return Err("earlier conflict alias did not commit".into());
    };
    agent.write_text("/clean", "clean").await?;
    main.write_text("/target", "target").await?;

    let plan = unbounded!(agent.join_into(&main), plan())?;
    let cancellation = CancellationToken::new();
    let mut accounting =
        crate::kernel::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let prior = Owned::<Vec<u8>>::allocate(29, &mut accounting)?;
    let options = ApplyOptions {
        if_target: plan.target_head(),
        idempotency_key: IdempotencyKey::new(),
    };
    let prepared = plan
        .preview_with_accounting(options, &mut accounting, &cancellation)
        .await?;
    let crate::workspace::JoinPreparation::Preview {
        generation_root,
        conflicts,
        hold: _,
    } = &prepared
    else {
        return Err("merge preview did not retain its read projection".into());
    };
    assert_eq!(conflicts.value.len(), 3);
    let shared_id = unbounded!(main, stat("/shared"))?.file_id;
    let shared_conflict = crate::MergeConflict::File(shared_id);
    // Range output retires on the same original ledger, preserving the
    // earlier caller buffer rather than releasing or copying its credit.
    let record = fs
        .workspace_merge_record_with_accounting(
            &main.volume,
            plan.target_head(),
            &shared_conflict,
            &mut accounting,
            &cancellation,
        )
        .await?
        .ok_or("selected range record missing")?;
    for offset in [0, 2, 0] {
        let read = fs
            .workspace_merge_range_with_accounting(
                &main.volume,
                record,
                crate::ByteRange { offset, length: 2 },
                &mut accounting,
                &cancellation,
            )
            .await?;
        assert_eq!(
            read.bytes.as_ref(),
            &b"main"[offset as usize..offset as usize + 2]
        );
        assert_eq!(
            accounting.standalone_parts().unwrap().0.scratch_bytes(),
            prior.bytes + read.logical_owned_bytes
        );
        read.retire(&mut accounting)?;
        assert_eq!(
            accounting.standalone_parts().unwrap().0.scratch_bytes(),
            prior.bytes
        );
    }
    for range in [
        crate::ByteRange {
            offset: 4,
            length: 1,
        },
        crate::ByteRange {
            offset: u64::MAX,
            length: 1,
        },
    ] {
        assert!(
            fs.workspace_merge_range_with_accounting(
                &main.volume,
                record,
                range,
                &mut accounting,
                &cancellation,
            )
            .await
            .is_err()
        );
        assert_eq!(
            accounting.standalone_parts().unwrap().0.scratch_bytes(),
            prior.bytes
        );
    }
    assert!(
        conflicts
            .value
            .iter()
            .any(|conflict| matches!(conflict, crate::MergeConflict::Binding { .. }))
    );
    fs.workspace_merge_record_with_accounting(
        &main.volume,
        plan.target_head(),
        conflicts.value.first().ok_or("preview conflict missing")?,
        &mut accounting,
        &cancellation,
    )
    .await?;
    let selections = conflicts
        .value
        .iter()
        .cloned()
        .map(|conflict| (conflict, crate::ConflictSide::Theirs))
        .collect();
    let (root, allocation) = crate::kernel::read_generation_root_with_accounting(
        fs.objects(),
        *generation_root,
        main.volume.config(),
        &mut accounting,
        &cancellation,
    )
    .await?;
    let root_bytes = (root.parents.capacity() * std::mem::size_of::<crate::GenerationId>()) as u64;
    let root = Owned::new(root, allocation, root_bytes);
    let limits = DecodeLimits::for_volume(main.volume.config());
    let root_record = value!(
        crate::kernel::lookup_file_record(
            fs.objects(),
            root.value.file_table,
            root.value.root_file_id,
            limits,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
    )?
    .ok_or("preview root record missing")?;
    let FilePayload::Directory { entries } = root_record.payload else {
        return Err("preview root is not a directory".into());
    };
    for (name, expected) in [
        ("clean", b"clean".as_slice()),
        ("target", b"target".as_slice()),
        ("shared", b"main".as_slice()),
        ("added", b"main-add".as_slice()),
    ] {
        let entry = value!(
            crate::kernel::lookup_tree_entry(
                fs.objects(),
                entries,
                &LogicalName::new(NameEncoding::Utf8, name.as_bytes().to_vec(), 255)?,
                limits,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
        )?
        .ok_or("preview binding missing")?;
        let record = value!(
            crate::kernel::lookup_file_record(
                fs.objects(),
                root.value.file_table,
                entry.file_id,
                limits,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
        )?
        .ok_or("preview file record missing")?;
        let FilePayload::InlineRegular(bytes) = record.payload else {
            return Err("preview file content changed representation".into());
        };
        assert_eq!(bytes.as_bytes(), expected);
        assert_eq!(record.link_count, if name == "added" { 2 } else { 1 });
        assert!(record.bindings.is_some());
    }
    root.retire(&mut accounting)?;
    // A projection retains unresolved regions; it is not a selected resolution.
    assert!(matches!(
        fs.publish_workspace_join_prepared(&main.volume, prepared, &mut accounting, &cancellation,)
            .await,
        Err(WorkspaceError::InvalidMergeResolution)
    ));
    assert_eq!(unbounded!(main, head())?.id(), plan.target_head());
    assert_eq!(unbounded!(agent, head())?.id(), plan.source_head());
    assert!(unbounded!(main, read("/clean", 16)).is_err());
    assert_eq!(
        accounting
            .standalone_parts()
            .ok_or("original ledger lost")?
            .0
            .scratch_bytes(),
        prior.bytes
    );
    prior.retire(&mut accounting)?;

    // The same projection feeds two independent continuations. No conflict's
    // path or content is expanded merely to visit another clean/conflict row.
    let paged_plan = unbounded!(agent.join_into(&main), plan())?;
    let paged_cancel = CancellationToken::new();
    let mut pages = paged_plan
        .preview_pages(options, WorkBudget::UNBOUNDED, &paged_cancel)
        .await
        .map_err(|failure| failure.error)?
        .value;
    assert_eq!(pages.conflict_count(), 3);
    assert_eq!(pages.plan().target_head(), plan.target_head());
    let mut conflict_rows = Vec::new();
    let mut selected_paths = Vec::new();
    let mut conflict_paths = Vec::new();
    loop {
        let peeked = pages.peek_conflict().map_err(|failure| failure.error)?;
        let expected = peeked.value.cloned();
        let peek_work = peeked.work;
        assert_eq!(
            pages
                .peek_conflict()
                .map_err(|failure| failure.error)?
                .value,
            expected.as_ref()
        );
        assert_eq!(pages.work(), peek_work);
        let taken = pages.next_conflict().map_err(|failure| failure.error)?;
        assert_eq!(taken.work, peek_work);
        assert_eq!(
            taken.value.as_ref().map(|row| row.value()),
            expected.as_ref()
        );
        let Some(row) = taken.value else {
            break;
        };
        let mut records = [None; 3];
        for (side, record) in [
            crate::ConflictSide::Base,
            crate::ConflictSide::Ours,
            crate::ConflictSide::Theirs,
        ]
        .into_iter()
        .zip(&mut records)
        {
            let selected = pages
                .open_conflict_content(row.value(), side)
                .await
                .map_err(|failure| failure.error)?;
            let mut page_work = selected.work;
            let mut content = selected.value;
            *record = content.as_ref().map(|content| *content.record());
            if let Some(record) = record {
                let content = content.as_mut().ok_or("selected body missing")?;
                let FilePayload::InlineRegular(expected) = record.payload else {
                    return Err("selected conflict body changed representation".into());
                };
                for (index, chunk) in expected.as_bytes().chunks(2).enumerate() {
                    let read = content
                        .read_range(crate::ByteRange {
                            offset: (index * 2) as u64,
                            length: chunk.len() as u64,
                        })
                        .await
                        .map_err(|failure| failure.error)?;
                    assert_eq!(read.value, chunk);
                    assert_eq!(
                        read.work.items_examined, page_work.items_examined,
                        "byte paging repeated the selected namespace lookup"
                    );
                    assert_eq!(
                        read.work.bytes_copied - page_work.bytes_copied,
                        chunk.len() as u64,
                        "byte paging copied outside the selected range"
                    );
                    page_work = read.work;
                }
                let empty = content
                    .read_range(crate::ByteRange {
                        offset: expected.as_bytes().len() as u64,
                        length: 0,
                    })
                    .await
                    .map_err(|failure| failure.error)?;
                assert_eq!(empty.value, b"".as_slice());
                assert_eq!(empty.work.items_examined, page_work.items_examined);
                assert_eq!(empty.work.bytes_copied, page_work.bytes_copied);
            } else {
                assert!(content.is_none());
            }
            drop(content);
            let conflict_path = pages
                .conflict_path(row.value(), side)
                .await
                .map_err(|failure| failure.error)?
                .value;
            match row.value() {
                crate::MergeConflict::File(_) => {
                    assert_eq!(conflict_path.is_some(), record.is_some());
                }
                crate::MergeConflict::Binding { .. } => {
                    // The location exists even on the base side where no file
                    // occupies that binding. A file alias is not its location.
                    assert_eq!(
                        conflict_path
                            .as_ref()
                            .map(|path| namespace_path_text(path.value()))
                            .transpose()?
                            .as_deref(),
                        Some("/added")
                    );
                }
            }
            if let Some(path) = conflict_path {
                conflict_paths.push(path);
            }
            let file_id =
                record
                    .as_ref()
                    .map(|record| record.file_id)
                    .or_else(|| match row.value() {
                        crate::MergeConflict::File(id) => Some(*id),
                        crate::MergeConflict::Binding { .. } => None,
                    });
            if let Some(file_id) = file_id {
                let before = pages.work();
                let path = pages
                    .file_path(file_id, side)
                    .await
                    .map_err(|failure| failure.error)?;
                assert!(path.work.items_examined > before.items_examined);
                assert_eq!(path.value.is_some(), record.is_some());
                if let Some(path) = path.value {
                    if matches!(row.value(), crate::MergeConflict::Binding { .. })
                        && side == crate::ConflictSide::Ours
                    {
                        // The occupying file's earlier alias differs from the
                        // binding location returned above for the same side.
                        assert_eq!(namespace_path_text(path.value())?, "/a");
                    }
                    assert!(matches!(
                        namespace_path_text(path.value())?.as_str(),
                        "/shared" | "/deleted" | "/added" | "/a"
                    ));
                    selected_paths.push(path);
                }
            }
        }
        match row.value() {
            crate::MergeConflict::File(id) => {
                assert!(records[0].is_some());
                assert!(records[1].is_some());
                assert!(
                    records
                        .into_iter()
                        .flatten()
                        .all(|record| record.file_id == *id)
                );
                for (record, expected) in records.into_iter().zip([
                    Some(b"base".as_slice()),
                    Some(if records[2].is_some() {
                        b"main".as_slice()
                    } else {
                        b"modified"
                    }),
                    records[2].map(|_| b"agent".as_slice()),
                ]) {
                    assert_eq!(record.is_some(), expected.is_some());
                    if let (Some(record), Some(expected)) = (record, expected) {
                        let FilePayload::InlineRegular(bytes) = record.payload else {
                            return Err("selected conflict record changed representation".into());
                        };
                        assert_eq!(bytes.as_bytes(), expected);
                    }
                }
            }
            crate::MergeConflict::Binding { .. } => {
                assert!(records[0].is_none());
                let ours = records[1].ok_or("target binding not selected")?;
                let theirs = records[2].ok_or("source binding not selected")?;
                assert_ne!(ours.file_id, theirs.file_id);
                for (record, expected) in [(ours, b"main-add".as_slice()), (theirs, b"agent-add")] {
                    let FilePayload::InlineRegular(bytes) = record.payload else {
                        return Err("selected binding content changed representation".into());
                    };
                    assert_eq!(bytes.as_bytes(), expected);
                }
            }
        }
        conflict_rows.push(row);
    }
    assert_eq!(conflict_rows.len(), 3);
    let first_work = pages
        .peek_clean()
        .await
        .map_err(|failure| failure.error)?
        .work;
    assert!(
        pages
            .peek_clean()
            .await
            .map_err(|failure| failure.error)?
            .value
            .is_some()
    );
    assert_eq!(pages.work(), first_work);
    let clean = pages
        .next_clean()
        .await
        .map_err(|failure| failure.error)?
        .value
        .ok_or("clean merge row missing")?;
    assert_eq!(namespace_path_text(&clean.value().path)?, "/clean");
    assert!(
        pages
            .next_clean()
            .await
            .map_err(|failure| failure.error)?
            .value
            .is_none()
    );
    // The original token remains authoritative even after the frontier ends.
    paged_cancel.cancel();
    assert!(matches!(
        pages.next_conflict().map_err(|failure| failure.error),
        Err(WorkspaceError::Cancelled(_))
    ));
    assert!(pages.next_clean().await.is_err());
    drop(pages);
    // Output backing is independent of the preview, including a binding name.
    assert_eq!(namespace_path_text(&clean.value().path)?, "/clean");
    assert!(conflict_rows.iter().any(|row| matches!(
        row.value(), crate::MergeConflict::Binding { name, .. }
            if name.as_bytes() == b"added"
    )));
    assert_eq!(selected_paths.len(), 7);
    for path in &selected_paths {
        assert!(matches!(
            namespace_path_text(path.value())?.as_str(),
            "/shared" | "/deleted" | "/added" | "/a"
        ));
    }
    assert_eq!(conflict_paths.len(), 8);
    for path in &conflict_paths {
        assert!(matches!(
            namespace_path_text(path.value())?.as_str(),
            "/shared" | "/deleted" | "/added"
        ));
    }
    drop((clean, conflict_rows, selected_paths, conflict_paths));

    // Output admission may suspend after peek. That cached identity cannot
    // bypass cancellation when the encoder later attempts to consume it.
    let cancelled_plan = unbounded!(agent.join_into(&main), plan())?;
    let cancelled = CancellationToken::new();
    let mut cancelled_pages = cancelled_plan
        .preview_pages(options, WorkBudget::UNBOUNDED, &cancelled)
        .await
        .map_err(|failure| failure.error)?
        .value;
    assert!(
        cancelled_pages
            .peek_conflict()
            .map_err(|failure| failure.error)?
            .value
            .is_some()
    );
    cancelled.cancel();
    assert!(matches!(
        cancelled_pages
            .next_conflict()
            .map_err(|failure| failure.error),
        Err(WorkspaceError::Cancelled(_))
    ));
    assert!(cancelled_pages.next_clean().await.is_err());
    drop(cancelled_pages);

    let selected_plan = unbounded!(agent.join_into(&main), plan())?;
    let selected_cancel = CancellationToken::new();
    let mut selected_pages = selected_plan
        .preview_pages(options, WorkBudget::UNBOUNDED, &selected_cancel)
        .await
        .map_err(|failure| failure.error)?
        .value;
    let selected = selected_pages
        .next_conflict()
        .map_err(|failure| failure.error)?
        .value
        .ok_or("selected conflict missing")?;
    selected_cancel.cancel();
    assert!(matches!(
        selected_pages
            .conflict_record(selected.value(), crate::ConflictSide::Ours)
            .await
            .map_err(|failure| failure.error),
        Err(WorkspaceError::Cancelled(_))
    ));
    assert!(selected_pages.next_clean().await.is_err());
    drop((selected_pages, selected));

    let calibration_plan = unbounded!(agent.join_into(&main), plan())?;
    let calibration = calibration_plan
        .preview_pages(options, WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map_err(|failure| failure.error)?;
    let preparation_items = calibration.work.items_examined;
    drop(calibration);
    let range_plan = unbounded!(agent.join_into(&main), plan())?;
    let mut range_pages = range_plan
        .preview_pages(
            options,
            WorkBudget {
                items_examined: preparation_items,
                ..WorkBudget::UNBOUNDED
            },
            &CancellationToken::new(),
        )
        .await
        .map_err(|failure| failure.error)?
        .value;
    assert!(
        range_pages
            .open_conflict_content(&shared_conflict, crate::ConflictSide::Ours)
            .await
            .is_err(),
        "selected content minted another item allowance"
    );
    assert!(range_pages.next_clean().await.is_err());
    drop(range_pages);

    #[cfg(not(target_arch = "wasm32"))]
    {
        let range_cancel = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &range_cancel)
            .map_err(|(error, _)| FsError::from(error))?;
        let native_plan = unbounded!(agent.join_into(&main), plan())?;
        let mut native_pages = native_plan
            .preview_pages(options, &original, &range_cancel)
            .await
            .map_err(|failure| failure.error)?
            .value;
        let selected = native_pages
            .open_conflict_content(&shared_conflict, crate::ConflictSide::Ours)
            .await
            .map_err(|failure| failure.error)?;
        let mut page_work = selected.work;
        let mut content = selected.value.ok_or("native body missing")?;
        let mut live = None;
        for offset in [0, 2, 0] {
            let read = content
                .read_range(crate::ByteRange { offset, length: 2 })
                .await
                .map_err(|failure| failure.error)?;
            assert_eq!(read.value, &b"main"[offset as usize..offset as usize + 2]);
            assert_eq!(read.work.items_examined, page_work.items_examined);
            assert_eq!(read.work.bytes_copied - page_work.bytes_copied, 2);
            page_work = read.work;
            let current = original.live_allocation_bytes();
            if let Some(previous) = live {
                assert_eq!(current, previous, "earlier content pages retained credit");
            }
            live = Some(current);
        }
        range_cancel.cancel();
        assert!(
            content
                .read_range(crate::ByteRange {
                    offset: 0,
                    length: 2
                })
                .await
                .is_err()
        );
        drop(content);
        assert!(native_pages.next_clean().await.is_err());
        assert!(original.live_allocation_bytes() < live.unwrap());
    }
    let bounded_plan = unbounded!(agent.join_into(&main), plan())?;
    let mut bounded_pages = bounded_plan
        .preview_pages(
            options,
            WorkBudget {
                items_examined: preparation_items,
                ..WorkBudget::UNBOUNDED
            },
            &CancellationToken::new(),
        )
        .await
        .map_err(|failure| failure.error)?
        .value;
    assert!(
        bounded_pages
            .file_path(root_record.file_id, crate::ConflictSide::Ours)
            .await
            .is_err(),
        "selected path minted another item allowance"
    );
    assert!(bounded_pages.next_clean().await.is_err());
    drop(bounded_pages);

    let path_plan = unbounded!(agent.join_into(&main), plan())?;
    let path_cancel = CancellationToken::new();
    let mut path_pages = path_plan
        .preview_pages(options, WorkBudget::UNBOUNDED, &path_cancel)
        .await
        .map_err(|failure| failure.error)?
        .value;
    let root_id = root_record.file_id;
    path_cancel.cancel();
    assert!(matches!(
        path_pages
            .file_path(root_id, crate::ConflictSide::Ours)
            .await
            .map_err(|failure| failure.error),
        Err(WorkspaceError::Cancelled(_))
    ));
    assert!(path_pages.next_clean().await.is_err());
    drop(path_pages);

    let refused_plan = unbounded!(agent.join_into(&main), plan())?;
    assert!(
        refused_plan
            .preview_pages(
                options,
                WorkBudget {
                    items_examined: 0,
                    ..WorkBudget::UNBOUNDED
                },
                &CancellationToken::new(),
            )
            .await
            .is_err()
    );

    let limited = unbounded!(agent.join_into(&main).bounds(128, 128, 1), plan())?;
    assert!(matches!(
        limited
            .preview_with_accounting(
                ApplyOptions {
                    if_target: limited.target_head(),
                    ..options
                },
                &mut accounting,
                &cancellation,
            )
            .await,
        Err(WorkspaceError::Filesystem(FsError::MergeChangeLimit))
    ));
    assert_eq!(
        accounting
            .standalone_parts()
            .ok_or("original ledger lost")?
            .0
            .scratch_bytes(),
        0
    );
    cancellation.cancel();
    assert!(matches!(
        plan.preview_with_accounting(options, &mut accounting, &cancellation)
            .await,
        Err(WorkspaceError::Cancelled(_))
    ));
    assert_eq!(
        accounting
            .standalone_parts()
            .ok_or("original ledger lost")?
            .0
            .scratch_bytes(),
        0
    );
    let pinned_range_plan = unbounded!(agent.join_into(&main), plan())?;
    let mut pinned_range_pages = pinned_range_plan
        .preview_pages(options, WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map_err(|failure| failure.error)?
        .value;
    let mut pinned_content = pinned_range_pages
        .open_conflict_content(&shared_conflict, crate::ConflictSide::Ours)
        .await
        .map_err(|failure| failure.error)?
        .value
        .ok_or("pinned body missing")?;
    assert!(matches!(
        plan.apply_sides(options, selections).await?,
        JoinOutcome::Applied(_)
    ));
    assert_eq!(
        pinned_content
            .read_range(crate::ByteRange {
                offset: 0,
                length: 4
            })
            .await
            .map_err(|failure| failure.error)?
            .value,
        b"main".as_slice()
    );
    for (path, expected) in [
        ("/clean", b"clean".as_slice()),
        ("/shared", b"agent".as_slice()),
        ("/added", b"agent-add".as_slice()),
    ] {
        assert_eq!(unbounded!(main, read(path, 16))?.as_ref(), expected);
    }
    assert!(unbounded!(main, read("/deleted", 16)).is_err());
    Ok(())
}

/// A workspace join (no Git metadata) where one side replaced the file under
/// its path: the replacement merges with the identity it replaced, the text
/// driver sees both texts, and the path keeps its original identity.
#[tokio::test]
async fn joins_merge_a_replaced_file_with_the_identity_it_replaced() -> Result<(), Box<dyn Error>> {
    replaced_file_join(JoinHistory::Merge).await
}

#[tokio::test]
async fn rebase_joins_merge_a_replaced_file_with_the_identity_it_replaced()
-> Result<(), Box<dyn Error>> {
    replaced_file_join(JoinHistory::Rebase).await
}

#[tokio::test]
async fn squash_joins_merge_a_replaced_file_with_the_identity_it_replaced()
-> Result<(), Box<dyn Error>> {
    replaced_file_join(JoinHistory::Squash).await
}

#[tokio::test]
async fn cherry_pick_joins_merge_a_replaced_file_with_the_identity_it_replaced()
-> Result<(), Box<dyn Error>> {
    replaced_file_join(JoinHistory::CherryPick).await
}

async fn replaced_file_join(history: JoinHistory) -> Result<(), Box<dyn Error>> {
    let base = "one\ntwo\nthree\n";
    for (agent_replaces, agent_text, main_text, expected) in [
        (true, base, "ONE\ntwo\nthree\n", "ONE\ntwo\nthree\n"),
        (
            true,
            "one\ntwo\nTHREE\n",
            "ONE\ntwo\nthree\n",
            "ONE\ntwo\nTHREE\n",
        ),
        (
            false,
            "one\ntwo\nTHREE\n",
            "ONE\ntwo\nthree\n",
            "ONE\ntwo\nTHREE\n",
        ),
    ] {
        let fs = Fs::memory();
        let main = unbounded!(fs, create_workspace("replaced-main"))?;
        main.write_text("/shared", base).await?;
        let original = unbounded!(main, stat("/shared"))?.file_id;
        let agent = fork_workspace(
            &main,
            "replaced-agent",
            ForkOptions::from_generation(unbounded!(main, head())?, IdempotencyKey::new()),
        )
        .await?;
        let (replacing, replacing_text, editing, editing_text) = if agent_replaces {
            (&agent, agent_text, &main, main_text)
        } else {
            (&main, main_text, &agent, agent_text)
        };
        replacing.remove("/shared").await?;
        replacing.write_text("/shared", replacing_text).await?;
        assert_ne!(unbounded!(replacing, stat("/shared"))?.file_id, original);
        editing.write_text("/shared", editing_text).await?;

        let plan = unbounded!(agent.join_into(&main).history(history), plan())?;
        // Equal content merges without a driver; edits on both sides reach
        // the text driver as one text conflict at the path.
        let outcome = match plan
            .apply(
                ApplyOptions {
                    if_target: plan.target_head(),
                    idempotency_key: IdempotencyKey::new(),
                },
                BTreeMap::new(),
                crate::PublicationPermit::Unrestricted,
            )
            .await?
        {
            JoinOutcome::Conflicted {
                conflicts,
                truncated,
            } => {
                let described = plan.describe_conflicts(conflicts, truncated).await?;
                assert_eq!(described.conflicts.len(), 1);
                assert_eq!(described.conflicts[0].kind, crate::ConflictKind::Text);
                assert_eq!(described.conflicts[0].path.as_deref(), Some("/shared"));
                let mut registry = crate::MergeDriverRegistry::new();
                registry.register("text", Arc::new(crate::DefaultTextMergeDriver))?;
                registry.set_default("text")?;
                let mut cache = crate::MemoryMergeResolutionCache::default();
                plan.apply_with_drivers(
                    ApplyOptions {
                        if_target: plan.target_head(),
                        idempotency_key: IdempotencyKey::new(),
                    },
                    &registry,
                    &mut cache,
                    false,
                    crate::PublicationPermit::Unrestricted,
                )
                .await?
            }
            outcome => outcome,
        };
        let variant = match &outcome {
            JoinOutcome::Applied(_) => "applied",
            JoinOutcome::AlreadyApplied(_) => "already applied",
            JoinOutcome::NoChanges(_) => "no changes",
            JoinOutcome::StaleTarget(_) => "stale target",
            JoinOutcome::Conflicted { .. } => "conflicted",
            JoinOutcome::Fenced => "fenced",
            JoinOutcome::IdempotencyConflict => "idempotency conflict",
        };
        // Merging an equal replacement into the target's edit leaves the
        // target as it is.
        let expected_variant = if agent_text == base {
            "no changes"
        } else {
            "applied"
        };
        assert_eq!(
            variant, expected_variant,
            "{history:?} agent_replaces={agent_replaces} agent_text={agent_text:?}"
        );
        let merged = String::from_utf8(unbounded!(main, read("/shared", 1024))?.to_vec())?;
        assert_eq!(merged, expected);
        let record = unbounded!(main, stat("/shared"))?;
        assert_eq!(record.file_id, original);
        assert_eq!(record.link_count, 1);
    }
    Ok(())
}

/// A file moved onto the path is not a replacement: the kernel pairs only a
/// file the deleting side created, so the join must not offer the moved
/// file's text as the deleted identity's resolution (the kernel would leave
/// the path bound to the moved file and drop the merged text with the
/// unlinked identity).
#[tokio::test]
async fn joins_keep_a_file_moved_onto_a_deleted_path_conflicted() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = unbounded!(fs, create_workspace("moved-main"))?;
    main.write_text("/a", "a one\na two\n").await?;
    main.write_text("/b", "b one\nb two\n").await?;
    let agent = fork_workspace(
        &main,
        "moved-agent",
        ForkOptions::from_generation(unbounded!(main, head())?, IdempotencyKey::new()),
    )
    .await?;
    main.remove("/a").await?;
    let mut moving = unbounded!(main, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(moving, rename("/b", "/a", true))?;
    let TransactionCommit::Committed(_) = commit(&mut moving).await? else {
        return Err("rename did not commit".into());
    };
    main.write_text("/a", "b one\nb TWO\n").await?;
    agent.write_text("/a", "A ONE\na two\n").await?;

    let plan = unbounded!(agent.join_into(&main), plan())?;
    let JoinOutcome::Conflicted {
        conflicts,
        truncated,
    } = plan
        .apply(
            ApplyOptions {
                if_target: plan.target_head(),
                idempotency_key: IdempotencyKey::new(),
            },
            BTreeMap::new(),
            crate::PublicationPermit::Unrestricted,
        )
        .await?
    else {
        return Err("a delete/modify beside a move must conflict".into());
    };
    let described = plan.describe_conflicts(conflicts, truncated).await?;
    assert!(
        described
            .conflicts
            .iter()
            .all(|conflict| conflict.kind != crate::ConflictKind::Text),
        "the moved file must not stand in for the deleted one: {:?}",
        described
            .conflicts
            .iter()
            .map(|conflict| conflict.kind)
            .collect::<Vec<_>>()
    );
    let mut registry = crate::MergeDriverRegistry::new();
    registry.register("text", Arc::new(crate::DefaultTextMergeDriver))?;
    registry.set_default("text")?;
    let mut cache = crate::MemoryMergeResolutionCache::default();
    let outcome = plan
        .apply_with_drivers(
            ApplyOptions {
                if_target: plan.target_head(),
                idempotency_key: IdempotencyKey::new(),
            },
            &registry,
            &mut cache,
            false,
            crate::PublicationPermit::Unrestricted,
        )
        .await;
    assert!(
        !matches!(outcome, Ok(JoinOutcome::Applied(_))),
        "the agent's edit must not be published away"
    );
    assert_eq!(
        String::from_utf8(unbounded!(main, read("/a", 64))?.to_vec())?,
        "b one\nb TWO\n"
    );
    Ok(())
}

/// The replacement rule follows directory identity, not full paths: under a
/// directory both sides renamed, a replaced file still reaches the text
/// driver as one text conflict and keeps its original identity.
#[tokio::test]
async fn joins_merge_a_replaced_file_under_a_renamed_directory() -> Result<(), Box<dyn Error>> {
    for agent_replaces in [true, false] {
        let fs = Fs::memory();
        let main = unbounded!(fs, create_workspace("renamed-main"))?;
        let mut seeding = unbounded!(main, begin_transaction(IdempotencyKey::new()))?;
        unbounded!(seeding, create_directory("/d"))?;
        let TransactionCommit::Committed(_) = commit(&mut seeding).await? else {
            return Err("directory did not commit".into());
        };
        main.write_text("/d/a", "one\ntwo\nthree\n").await?;
        let original = unbounded!(main, stat("/d/a"))?.file_id;
        let agent = fork_workspace(
            &main,
            "renamed-agent",
            ForkOptions::from_generation(unbounded!(main, head())?, IdempotencyKey::new()),
        )
        .await?;
        for side in [&main, &agent] {
            let mut renaming = unbounded!(side, begin_transaction(IdempotencyKey::new()))?;
            unbounded!(renaming, rename("/d", "/e", true))?;
            let TransactionCommit::Committed(_) = commit(&mut renaming).await? else {
                return Err("rename did not commit".into());
            };
        }
        let (replacing, editing) = if agent_replaces {
            (&agent, &main)
        } else {
            (&main, &agent)
        };
        replacing.remove("/e/a").await?;
        replacing.write_text("/e/a", "one\ntwo\nTHREE\n").await?;
        assert_ne!(unbounded!(replacing, stat("/e/a"))?.file_id, original);
        editing.write_text("/e/a", "ONE\ntwo\nthree\n").await?;

        let plan = unbounded!(agent.join_into(&main), plan())?;
        let JoinOutcome::Conflicted {
            conflicts,
            truncated,
        } = plan
            .apply(
                ApplyOptions {
                    if_target: plan.target_head(),
                    idempotency_key: IdempotencyKey::new(),
                },
                BTreeMap::new(),
                crate::PublicationPermit::Unrestricted,
            )
            .await?
        else {
            return Err("overlapping whole-file edits must reach the driver".into());
        };
        let described = plan.describe_conflicts(conflicts, truncated).await?;
        assert_eq!(
            described.conflicts.len(),
            1,
            "agent_replaces={agent_replaces}"
        );
        assert_eq!(
            described.conflicts[0].kind,
            crate::ConflictKind::Text,
            "agent_replaces={agent_replaces}"
        );
        let mut registry = crate::MergeDriverRegistry::new();
        registry.register("text", Arc::new(crate::DefaultTextMergeDriver))?;
        registry.set_default("text")?;
        let mut cache = crate::MemoryMergeResolutionCache::default();
        assert!(matches!(
            plan.apply_with_drivers(
                ApplyOptions {
                    if_target: plan.target_head(),
                    idempotency_key: IdempotencyKey::new(),
                },
                &registry,
                &mut cache,
                false,
                crate::PublicationPermit::Unrestricted,
            )
            .await?,
            JoinOutcome::Applied(_)
        ));
        assert_eq!(
            String::from_utf8(unbounded!(main, read("/e/a", 64))?.to_vec())?,
            "ONE\ntwo\nTHREE\n"
        );
        let record = unbounded!(main, stat("/e/a"))?;
        assert_eq!(record.file_id, original);
        assert_eq!(record.link_count, 1);
    }
    Ok(())
}
