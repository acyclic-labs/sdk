use super::*;
/// Resolves a path argument within the managed workspace.
pub fn workspace_path_argument(mount: &Path, argument: &str) -> Result<String, String> {
    let argument = Path::new(argument);
    let relative = if argument.is_absolute() {
        argument
            .strip_prefix(mount)
            .map_err(|_| "path is outside the managed workspace".to_owned())?
    } else {
        argument
    };
    let mut result = String::new();
    for component in relative.components() {
        match component {
            Component::Normal(name) => {
                let name = name
                    .to_str()
                    .ok_or_else(|| "compatibility path is not valid Unicode".to_owned())?;
                result.push('/');
                result.push_str(name);
            }
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err("path escapes the managed workspace".to_owned());
            }
        }
    }
    if result.is_empty() {
        return Err("path must name a file inside the managed workspace".to_owned());
    }
    Ok(result)
}

/// Reserved SDK metadata excluded from compatibility commits.
pub fn reserved_ignore_rules() -> String {
    [".acyclic-sdk", ".git"]
        .iter()
        .map(|name| format!("{name}/\n"))
        .collect()
}

/// Loads text merge drivers and workspace attributes.
pub async fn merge_drivers_for(
    workspace: &LocalLazyWorkspace,
) -> Result<Arc<MergeDriverRegistry>, String> {
    let attributes = match workspace
        .read(
            "/.gitattributes",
            1024 * 1024,
            &mut acyclic_fs::kernel::KernelAccounting::Standalone {
                allocations: Default::default(),
                work: acyclic_fs::WorkCounters::UNCHARGED,
                budget: WorkBudget::UNBOUNDED,
                cancellation: &CancellationToken::new(),
            },
        )
        .await
    {
        Ok(chunk) => String::from_utf8(chunk.bytes.to_vec())
            .map_err(|_| ".gitattributes must be UTF-8".to_owned())?,
        Err(acyclic_fs::LazyWorkspaceError::NotFound) => String::new(),
        Err(error) => return Err(display(error)),
    };
    let mut registry = MergeDriverRegistry::new();
    registry
        .register("text", Arc::new(DefaultTextMergeDriver))
        .map_err(display)?;
    registry.set_default("text").map_err(display)?;
    if !attributes.trim().is_empty() {
        registry.set_git_attributes(&attributes).map_err(display)?;
    }
    Ok(Arc::new(registry))
}

pub(super) fn existing_tracked_paths<'a>(
    generation: &'a LocalGeneration,
    candidates: &'a BTreeSet<String>,
) -> impl std::future::Future<Output = Result<BTreeSet<String>, GitError>> + Send + 'a {
    async move {
        let mut tracked = BTreeSet::new();
        for path in candidates {
            match generation
                .stat(
                    &format!("/{path}"),
                    acyclic_fs::WorkBudget::UNBOUNDED,
                    &acyclic_fs::CancellationToken::new(),
                )
                .await
                .map(|receipt| receipt.value)
                .map_err(|failure| failure.error)
            {
                Ok(stat) if stat.kind != FileKind::Directory => {
                    tracked.insert(path.clone());
                }
                Ok(_) | Err(WorkspaceError::NotFound) => {}
                Err(error) => return Err(GitError(display(error))),
            }
        }
        Ok(tracked)
    }
}
