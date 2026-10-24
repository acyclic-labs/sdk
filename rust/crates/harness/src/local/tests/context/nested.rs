use super::*;

const PATHS: [&str; 8] = [
    "AGENTS.md",
    "CLAUDE.md",
    "src/AGENTS.md",
    "src/CLAUDE.md",
    "src/deep/AGENTS.md",
    "src/deep/CLAUDE.md",
    "sibling/AGENTS.md",
    "src/deep/child/CLAUDE.md",
];

async fn scoped_instructions(native: bool) -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::super::storage)?;
    let provider = text();
    let config = if native {
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt;
            // Legal native siblings cannot name portable context sources.
            for name in [b"unrelated-\xff".as_slice(), b"unrelated\\name"] {
                std::fs::write(
                    directory.path().join(std::ffi::OsStr::from_bytes(name)),
                    b"unrelated",
                )
                .map_err(super::super::super::storage)?;
            }
        }
        for path in PATHS {
            let file = directory.path().join(path);
            std::fs::create_dir_all(file.parent().unwrap())
                .map_err(super::super::super::storage)?;
            std::fs::write(file, format!("instruction for {path}"))
                .map_err(super::super::super::storage)?;
        }
        checkout_config(directory.path(), (model()?, provider))?
    } else {
        let project = volume(directory.path()).await?;
        volume_config(directory.path(), project, provider)?
    }
    .without_default_tools()
    .active_directory("src/deep");
    let harness = LocalHarness::open(config).await?;
    if !native {
        for path in PATHS {
            instruction(&harness, path, &format!("instruction for {path}")).await?;
        }
    }
    let operation = OperationId::new();
    harness.stage_context_manifest(operation).await?;
    let (_, manifest) = manifest(&harness, operation).await?;
    let reader = harness.context_reader()?;
    let mut bodies = Vec::new();
    for source in manifest.discovery.instructions {
        let bytes = reader.read(&source, 1024, None, None)?.await?;
        bodies.push(String::from_utf8(bytes.to_vec()).unwrap());
    }
    assert_eq!(
        bodies,
        PATHS[..6]
            .iter()
            .map(|path| format!("instruction for {path}"))
            .collect::<Vec<_>>()
    );
    // The former root-only result is observably insufficient to this same oracle.
    assert_ne!(bodies, bodies[..2]);
    Ok(())
}

#[tokio::test]
async fn checkout_instructions_follow_the_shared_root_to_leaf_scope() -> Result<()> {
    scoped_instructions(true).await
}

#[tokio::test]
async fn volume_instructions_follow_the_shared_root_to_leaf_scope() -> Result<()> {
    scoped_instructions(false).await
}

#[tokio::test]
async fn active_instruction_scope_cannot_escape_the_workspace() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::super::storage)?;
    let project = volume(directory.path()).await?;
    for path in ["../outside", "/absolute", "src/../outside", "src\\outside"] {
        let config = volume_config(directory.path(), project.clone(), text())?
            .without_default_tools()
            .active_directory(path);
        assert!(
            matches!(LocalHarness::open(config).await, Err(Error::Invalid(_))),
            "{path}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn host_working_directory_selects_the_same_bounded_instruction_scope() -> Result<()> {
    let root = tempfile::tempdir().map_err(super::super::super::storage)?;
    let outside = tempfile::tempdir().map_err(super::super::super::storage)?;
    std::fs::create_dir_all(root.path().join("src/deep")).map_err(super::super::super::storage)?;
    let config = || checkout_config(root.path(), (model().unwrap(), text()));
    assert_eq!(
        config()?
            .working_directory(&root.path().join("src/deep"))?
            .active_directory,
        "src/deep"
    );
    assert_eq!(
        config()?
            .working_directory(outside.path())?
            .active_directory,
        ""
    );
    assert_eq!(
        config()?.working_directory(root.path())?.active_directory,
        ""
    );
    Ok(())
}
