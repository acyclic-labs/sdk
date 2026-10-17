//! Tests never share machine state: no unit test can reach the per-user harness store, the user's home or
//! CODEX_HOME, and every test composition owns its state root. The seams are `owner::user_state_root`,
//! `settings::user_home` and `native_settings::configured_codex_home`; the scan keeps them the only way in.
use super::*;
use std::path::PathBuf;

#[test]
fn unit_tests_cannot_resolve_the_per_user_store() -> Result<()> {
    let checkout = tempfile::tempdir().map_err(super::super::storage)?;
    let unrooted = LocalHarnessConfig::new(checkout.path(), (model()?, text()));
    assert!(
        matches!(super::super::owner::user_state_root(), Err(Error::Unsupported(message))
            if message == super::super::owner::UNIT_TESTS_INJECT_STATE)
    );
    assert!(
        matches!(unrooted.resolved_state_directory(), Err(Error::Unsupported(message))
            if message == super::super::owner::UNIT_TESTS_INJECT_STATE)
    );
    // Positive control: the injected directory is the one resolution, and the test helper always injects one.
    let injected = tempfile::tempdir().map_err(super::super::storage)?;
    let rooted = LocalHarnessConfig::new(checkout.path(), (model()?, text()))
        .state_directory(injected.path());
    assert_eq!(rooted.resolved_state_directory()?, injected.path());
    let helper = crate::test_support::local_harness_config(checkout.path(), (model()?, text()))?;
    let owned = helper.resolved_state_directory()?;
    assert!(!owned.starts_with(checkout.path()));
    assert_ne!(
        owned,
        crate::test_support::local_harness_config(checkout.path(), (model()?, text()))?
            .resolved_state_directory()?
    );
    Ok(())
}

#[tokio::test]
async fn opening_without_an_injected_root_fails_before_any_state_exists() -> Result<()> {
    let checkout = tempfile::tempdir().map_err(super::super::storage)?;
    let opened =
        LocalHarness::open(LocalHarnessConfig::new(checkout.path(), (model()?, text()))).await;
    assert!(
        matches!(&opened, Err(Error::Unsupported(message)) if message.starts_with("unit tests must inject")),
        "{:?}",
        opened.as_ref().err()
    );
    Ok(())
}

#[cfg(any(feature = "hooks", feature = "mcp-stdio", feature = "mcp-http"))]
#[test]
fn unit_tests_cannot_resolve_the_user_home_or_codex_home() -> Result<()> {
    let checkout = tempfile::tempdir().map_err(super::super::storage)?;
    let project = super::super::LocalProject::Workspace(checkout.path().to_path_buf());
    assert!(
        matches!(super::super::settings::configuration(&project, None), Err(Error::Unsupported(message))
            if message == super::super::settings::UNIT_TESTS_INJECT_SETTINGS)
    );
    Ok(())
}

#[test]
fn default_state_is_one_directory_per_project_under_the_root() -> Result<()> {
    let root = tempfile::tempdir().map_err(super::super::storage)?;
    let projects = tempfile::tempdir().map_err(super::super::storage)?;
    let [first, second] = ["first", "second"].map(|name| projects.path().join(name));
    for path in [&first, &second] {
        std::fs::create_dir(path).map_err(super::super::storage)?;
    }
    let at = |path: &PathBuf| {
        super::super::owner::state_directory_in(
            root.path(),
            &super::super::LocalProject::Workspace(path.clone()),
        )
    };
    let state = at(&first)?;
    assert_eq!(state.parent(), Some(root.path()));
    assert_eq!(state, at(&first.join("."))?);
    assert_ne!(state, at(&second)?);
    // Negative control: a root inside the checkout would put owner state in the agent's view.
    assert!(matches!(
        super::super::owner::state_directory_in(
            &first.join("nested"),
            &super::super::LocalProject::Workspace(first.clone())
        ),
        Err(Error::Invalid(_))
    ));
    Ok(())
}

/// Production seams that may consult machine-wide locations, by crate-relative path.
const SEAMS: [(&str, &str); 3] = [
    ("src/local/owner.rs", "data_local_dir"),
    ("src/local/settings.rs", "home_dir"),
    ("src/native_settings.rs", concat!("CODEX", "_HOME")),
];

/// Sources allowed to build a composition without the injecting test helper: the type itself, this gate (which
/// proves such a composition fails), the helper, and the production binary.
const UNROOTED: [&str; 4] = [
    "src/local/config.rs",
    "src/local/tests/hermetic.rs",
    "src/test_support/mod.rs",
    "src/bin/",
];

/// Every reach for a machine-wide location or an unrooted composition outside its seam, as "path:line: text".
fn escapes(relative: &str, source: &str) -> Vec<String> {
    let machine = [concat!("dirs", "::"), concat!("\"CODEX", "_HOME\")")];
    let unrooted = concat!("LocalHarnessConfig", "::new(");
    source
        .lines()
        .enumerate()
        .filter(|(_, line)| {
            let seam = SEAMS.iter().any(|(path, _)| relative == *path);
            (!seam && machine.iter().any(|token| line.contains(token)))
                || (!UNROOTED.iter().any(|path| relative.starts_with(path))
                    && line.contains(unrooted))
        })
        .map(|(index, line)| format!("{relative}:{}: {}", index + 1, line.trim()))
        .collect()
}

fn rust_sources(directory: &Path, found: &mut Vec<PathBuf>) -> Result<()> {
    for entry in std::fs::read_dir(directory).map_err(super::super::storage)? {
        let path = entry.map_err(super::super::storage)?.path();
        if path.is_dir() {
            rust_sources(&path, found)?;
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            found.push(path);
        }
    }
    Ok(())
}

#[test]
fn only_the_seams_reach_machine_wide_state() -> Result<()> {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut sources = Vec::new();
    for directory in ["src", "tests"] {
        rust_sources(&crate_root.join(directory), &mut sources)?;
    }
    let mut found = Vec::new();
    for path in &sources {
        let relative = path
            .strip_prefix(crate_root)
            .map_err(super::super::storage)?
            .to_string_lossy()
            .replace('\\', "/");
        found.extend(escapes(
            &relative,
            &std::fs::read_to_string(path).map_err(super::super::storage)?,
        ));
    }
    assert!(
        sources.len() > 400,
        "scanned only {} sources",
        sources.len()
    );
    assert!(
        found.is_empty(),
        "machine-wide state outside its seam:\n{}",
        found.join("\n")
    );
    // Each seam still names the location it owns, so the scan cannot pass by a seam moving away.
    for (path, token) in SEAMS {
        let seam = std::fs::read_to_string(crate_root.join(path)).map_err(super::super::storage)?;
        assert!(seam.contains(token), "{path} no longer owns {token}");
    }
    Ok(())
}

#[test]
fn the_scan_rejects_each_escape_shape() {
    for (path, line) in [
        (
            "src/local/tests/x.rs",
            concat!("let root = dirs", "::data_local_dir();"),
        ),
        (
            "tests/e2e/x.rs",
            concat!("let home = dirs", "::home_dir();"),
        ),
        (
            "src/local/mod.rs",
            concat!("std::env::var_os(\"CODEX", "_HOME\")"),
        ),
        (
            "tests/e2e/agents.rs",
            concat!(
                "LocalHarness::open(LocalHarnessConfig",
                "::new(dir, model))"
            ),
        ),
        (
            "src/local/tests.rs",
            concat!("LocalHarnessConfig", "::new(dir, model)"),
        ),
    ] {
        assert_eq!(escapes(path, line).len(), 1, "{path}: {line}");
    }
    for (path, line) in [
        ("src/local/owner.rs", concat!("dirs", "::data_local_dir()")),
        (
            "src/test_support/mod.rs",
            concat!("LocalHarnessConfig", "::new(workspace, model)"),
        ),
        (
            "src/bin/graphcoder.rs",
            concat!("LocalHarnessConfig", "::new(&args.workspace, binding)"),
        ),
    ] {
        assert!(escapes(path, line).is_empty(), "{path}: {line}");
    }
}
