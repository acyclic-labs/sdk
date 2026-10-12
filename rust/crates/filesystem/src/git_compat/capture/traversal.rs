use super::*;

pub(crate) async fn load_git_ignore_policy<A, O>(
    generation: &Generation<A, O>,
    policies: &mut BTreeMap<String, GitIgnorePolicy>,
    directory: &str,
) -> Result<(), GitCaptureError>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
{
    if policies.contains_key(directory) {
        return Ok(());
    }
    let path = if directory.is_empty() {
        "/.gitignore".to_owned()
    } else {
        format!("/{directory}/.gitignore")
    };
    let policy = match generation
        .read(
            &path,
            1024 * 1024,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
    {
        Ok(contents) => {
            let contents =
                std::str::from_utf8(&contents).map_err(|_| GitCaptureError::NonPortableName)?;
            GitIgnorePolicy::parse(contents)
        }
        Err(WorkspaceError::NotFound) => GitIgnorePolicy::default(),
        Err(error) => return Err(error.into()),
    };
    policies.insert(directory.to_owned(), policy);
    Ok(())
}

pub(crate) async fn scan_git_capture_entries<A, O>(
    live: &crate::Generation<A, O>,
) -> Result<(Vec<(String, bool)>, BTreeMap<String, GitIgnorePolicy>), GitCaptureError>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
{
    let mut entries = Vec::new();
    let mut nested_policies = BTreeMap::new();
    let mut pending = vec!["/".to_owned()];
    while let Some(directory) = pending.pop() {
        let mut after = None;
        loop {
            let page = live
                .list_directory(
                    &directory,
                    after.as_ref(),
                    1_024,
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await
                .map(|receipt| receipt.value.0)
                .map_err(|failure| failure.error)?;
            for entry in &page.entries {
                let name = entry
                    .name
                    .unicode_text()
                    .ok_or(GitCaptureError::NonPortableName)?;
                let path = if directory == "/" {
                    name.to_string()
                } else {
                    format!("{}/{}", directory.trim_start_matches('/'), name)
                };
                let is_directory = entry.kind == FileKind::Directory;
                entries.push((path.clone(), is_directory));
                if is_directory {
                    pending.push(format!("/{path}"));
                } else if name == ".gitignore" && directory != "/" {
                    let contents = live
                        .read(
                            &format!("/{path}"),
                            1024 * 1024,
                            WorkBudget::UNBOUNDED,
                            &CancellationToken::new(),
                        )
                        .await
                        .map(|receipt| receipt.value)
                        .map_err(|failure| failure.error)?;
                    let contents = std::str::from_utf8(&contents)
                        .map_err(|_| GitCaptureError::NonPortableName)?;
                    nested_policies.insert(
                        directory.trim_start_matches('/').to_owned(),
                        GitIgnorePolicy::parse(contents),
                    );
                }
            }
            if !page.has_more {
                break;
            }
            after = page.entries.last().map(|entry| entry.name.clone());
        }
    }
    Ok((entries, nested_policies))
}

pub(crate) fn portable_changed_path(
    path: &crate::kernel::NamespacePath,
) -> Result<String, GitCaptureError> {
    let mut portable = String::new();
    for component in path.components() {
        if !portable.is_empty() {
            portable.push('/');
        }
        portable.push_str(
            &component
                .unicode_text()
                .ok_or(GitCaptureError::NonPortableName)?,
        );
    }
    Ok(portable)
}

pub(crate) fn path_depth(path: &str) -> usize {
    path.split('/').count()
}

pub(crate) fn is_path_below(candidate: &str, parent: &str) -> bool {
    candidate
        .strip_prefix(parent)
        .is_some_and(|suffix| suffix.starts_with('/'))
}
