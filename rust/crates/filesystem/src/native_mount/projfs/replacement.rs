use super::*;

pub(super) fn replace_written(
    projection: Projection<'_>,
    path: &MountPath,
    written: &mut WrittenEntry,
) -> Result<Placed, String> {
    let source = projection.runtime.source.as_ref();
    let relative = projection
        .runtime
        .projfs_name(&host_relative_path(path).map_err(|error| error.to_string())?);
    let basis = ReadBasis::sample(source);
    let current = match source.lookup_pinned(path) {
        Ok(found) => found,
        Err(MountSourceError::NotFound) => None,
        Err(error) => return Err(error.to_string()),
    };
    let owned = written.owned;
    let replacing = match (owned, current) {
        (Some(Owned::File(state)), Some((lookup, _)))
            if lookup.node.kind == MountNodeKind::Regular =>
        {
            Some(state)
        }
        _ => None,
    };
    if replacing.is_none() {
        if owned.is_some() {
            if release(projection, path, owned)? == Release::Busy {
                return Ok(Placed::Retry);
            }
        } else {
            // SAFETY: the name outlives this synchronous call on a live context.
            match unsafe {
                PrjDeleteFile(
                    projection.context,
                    &relative,
                    Some(PRJ_UPDATE_ALLOW_READ_ONLY),
                    None,
                )
            } {
                Ok(()) => {}
                Err(error) if [HR_FILE_NOT_FOUND, HR_PATH_NOT_FOUND].contains(&error.code()) => {}
                Err(error) if error.code() == HR_SHARING_VIOLATION => return Ok(Placed::Retry),
                Err(error)
                    if [HR_VIRTUALIZATION_INVALID_OPERATION, HR_DIRECTORY_NOT_EMPTY]
                        .contains(&error.code()) =>
                {
                    return Ok(Placed::Nothing);
                }
                Err(error) => return Err(driver_error(&error).to_string()),
            }
        }
    }
    let Some((lookup, pin)) =
        current.filter(|(lookup, _)| lookup.node.kind != MountNodeKind::Directory)
    else {
        return Ok(Placed::Nothing);
    };
    let (Some(parent), Some(component)) = (path.parent(), path.components().last()) else {
        return Ok(Placed::Nothing);
    };
    let Some(entry) = projected_entry(
        projection,
        &parent,
        owned_component(projection, component).map_err(|error| error.to_string())?,
        lookup,
        pin,
    )
    .map_err(|error| error.to_string())?
    else {
        return Ok(Placed::Nothing);
    };
    let placed = match replacing {
        Some(state) => write_ordinary(
            projection,
            path,
            &host_relative_path(path).map_err(|error| error.to_string())?,
            &entry,
            Some(state),
        )?,
        None => write_entry(projection, path, &entry)?,
    };
    if matches!(placed, Placed::Owned(_) | Placed::Unowned) {
        *written = WrittenEntry {
            owned: placed.owned(),
            facts: Some(entry.facts),
            basis,
        };
    }
    Ok(placed)
}

pub(super) struct SendContext(pub(super) PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT);
