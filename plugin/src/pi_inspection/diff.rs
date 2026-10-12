use super::*;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiNativeDiffCursor {
    before: LazySnapshotRef,
    pub(crate) after: LazySnapshotRef,
    before_pass: bool,
    continuation: Option<Vec<u8>>,
}

/// Two bounded native-order passes, so a source's host enumeration order is never assumed to be lexical.
pub(crate) async fn diff_native(
    before: &PiNativeSelectedSource,
    after: &PiNativeSelectedSource,
    directory: &str,
    cursor: Option<&PiNativeDiffCursor>,
    maximum_entries: u32,
    work: &NativeMaterializationWork,
) -> Result<(
    Vec<PiDifference>,
    Option<PiNativeDiffCursor>,
    NativeAllocation,
)> {
    let mut io = HistoryWork::new(PreparationIo::from_native_work(work)?);
    if !std::sync::Arc::ptr_eq(&before.context, &after.context)
        || !std::sync::Arc::ptr_eq(&before.owner, &after.owner)
    {
        return Err(selected_unauthorized(
            "diff selections have different callers",
        ));
    }
    let owner = &after.owner;
    let cancellation = work.cancellation();
    owner.verify(false, &mut Some(&mut io)).await?;
    let _before_path = before.path(directory, work)?;
    let _after_path = after.path(directory, work)?;
    if cursor.is_some_and(|c| c.before != before.snapshot || c.after != after.snapshot) {
        return Err(selected_invalid("diff cursor names another selection"));
    }
    let before_pass = cursor.is_some_and(|c| c.before_pass);
    let (current, other) = if before_pass {
        (before, after)
    } else {
        (after, before)
    };
    let page = match current
        .page(
            directory,
            cursor.and_then(|c| c.continuation.as_deref()),
            maximum_entries,
            work,
        )
        .await
    {
        Ok(page) => Some(page),
        Err(Error::NativeSource { source, .. })
            if matches!(
                source.semantic_error(),
                acyclic_fs::LazyWorkspaceError::NotFound
            ) =>
        {
            None
        }
        Err(error) => return Err(error),
    };
    let allocation = crate::pi_native::pi_native_vec(
        page.as_ref().map_or(0, |(page, _)| page.entries.len()),
        work,
    )?;
    let mut output = (allocation.0, None, allocation.1);
    for entry in page
        .as_ref()
        .into_iter()
        .flat_map(|(page, _)| &page.entries)
    {
        let (name, mut name_custody) = entry
            .name
            .unicode_text_with_native_work(work, cancellation)?;
        let path = if directory.is_empty() {
            work.display(&format_args!("{name}"))?
        } else {
            work.display(&format_args!("{directory}/{name}"))?
        };
        let counterpart = match other.lookup(&path.0, work).await {
            Ok(found) => Some(found),
            Err(Error::NotFound(_)) => None,
            Err(Error::NativeSource { source, .. })
                if matches!(
                    source.semantic_error(),
                    acyclic_fs::LazyWorkspaceError::NotFound
                ) =>
            {
                None
            }
            Err(error) => return Err(error),
        };
        let kind = if before_pass {
            counterpart.is_none().then_some(PiDifferenceKind::Removed)
        } else if let Some(old) = counterpart {
            let new = current.lookup(&path.0, work).await?;
            let old_stat = old.0.stat();
            let new_stat = new.0.stat();
            if old_stat.kind != new_stat.kind {
                Some(PiDifferenceKind::KindChanged)
            } else if old_stat.logical_bytes != new_stat.logical_bytes
                || old.0.metadata() != new.0.metadata()
                || native_extra_facts_differ(&old.0, &new.0)
            {
                Some(PiDifferenceKind::Modified)
            } else {
                use acyclic_fs::demand::SourceNodeKind;
                let mut previous = None;
                let mut changed = false;
                for source in [before, after] {
                    let bytes = match new_stat.kind {
                        SourceNodeKind::RegularFile => {
                            source
                                .read(&path.0, source.context.scope().limits().file_bytes, work)
                                .await?
                        }
                        SourceNodeKind::SymbolicLink => source.read_link(&path.0, work).await?,
                        _ => break,
                    };
                    if let Some(previous) = &previous {
                        changed = previous != &bytes;
                    }
                    previous = Some(bytes);
                }
                changed.then_some(PiDifferenceKind::Modified)
            }
        } else {
            Some(PiDifferenceKind::Added)
        };
        if let Some(kind) = kind {
            output.2.absorb(&mut name_custody)?;
            output.0.push(PiDifference { name, kind });
        }
        owner.verify(false, &mut Some(&mut io)).await?;
    }
    let continuation = match page.as_ref().and_then(|(page, _)| page.next.as_ref()) {
        Some(next) => {
            let (bytes, mut next_custody) = next
                .encode_with_native_work(work, cancellation)
                .map_err(|(source, custody)| Error::NativeSource { source, custody })?;
            output.2.absorb(&mut next_custody)?;
            Some(bytes)
        }
        None => None,
    };
    output.1 = if continuation.is_some() || !before_pass {
        Some(PiNativeDiffCursor {
            before: before.snapshot,
            after: after.snapshot,
            before_pass: before_pass || continuation.is_none(),
            continuation,
        })
    } else {
        None
    };
    drop(page);
    owner.verify(false, &mut Some(&mut io)).await?;
    Ok(output)
}
fn native_extra_facts_differ(
    before: &acyclic_fs::LazyLookup,
    after: &acyclic_fs::LazyLookup,
) -> bool {
    let links = |lookup: &acyclic_fs::LazyLookup| match lookup {
        acyclic_fs::LazyLookup::Authored { stat, .. } => Some(stat.link_count),
        acyclic_fs::LazyLookup::Shadow {
            source_link_count, ..
        } => Some(*source_link_count),
        acyclic_fs::LazyLookup::Source { node, .. } => node.link_count,
    };
    if links(before) != links(after) {
        return true;
    }
    match (before, after) {
        (
            acyclic_fs::LazyLookup::Source { node: before, .. },
            acyclic_fs::LazyLookup::Source { node: after, .. },
        ) => before.device != after.device,
        _ => false,
    }
}
