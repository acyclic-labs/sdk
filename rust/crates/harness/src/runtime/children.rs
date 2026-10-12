use super::*;

pub(crate) fn validate_children_request(
    parent: TaskId,
    after_slot: Option<&str>,
    maximum: usize,
) -> Result<()> {
    validate_child_page_request("task child", parent.into_bytes(), after_slot, maximum)
}

pub(crate) fn validate_children_page(
    page: &TaskChildrenPage,
    expected_revision: Option<u64>,
    after_slot: Option<&str>,
    maximum: usize,
) -> Result<()> {
    validate_child_page(
        "task child",
        (page.revision, page.next_after.as_deref()),
        page.entries
            .iter()
            .map(|child| (child.slot.as_str(), child.task_id.into_bytes())),
        (expected_revision, after_slot, maximum),
    )
}

/// Bounds one direct-child page request of a task or operation hierarchy.

pub(crate) fn validate_child_page_request(
    label: &str,
    parent: [u8; 16],
    after_slot: Option<&str>,
    maximum: usize,
) -> Result<()> {
    if parent == [0; 16]
        || maximum == 0
        || after_slot.is_some_and(|slot| slot.chars().any(char::is_control))
    {
        return Err(Error::Invalid(format!("{label} page request is invalid")));
    }
    Ok(())
}

/// Validates a pinned child page against its requested cursor and bounds.

pub(crate) fn validate_child_page<'a>(
    label: &str,
    (revision, next_after): (u64, Option<&str>),
    entries: impl Iterator<Item = (&'a str, [u8; 16])>,
    (expected_revision, after_slot, maximum): (Option<u64>, Option<&str>, usize),
) -> Result<()> {
    let mut previous = after_slot;
    let mut last = None;
    let mut ids = BTreeSet::new();
    for (slot, id) in entries {
        if id == [0; 16]
            || slot.trim().is_empty()
            || slot.chars().any(char::is_control)
            || previous.is_some_and(|previous| slot <= previous)
            || !ids.insert(id)
        {
            return Err(Error::Invalid(format!(
                "{label} entries are not in stable slot order"
            )));
        }
        previous = Some(slot);
        last = Some(slot);
    }
    if expected_revision.is_some_and(|expected| expected != revision)
        || ids.len() > maximum
        || next_after.is_some_and(|next| last != Some(next))
    {
        return Err(Error::Invalid(format!(
            "{label} page does not match its request"
        )));
    }
    Ok(())
}
