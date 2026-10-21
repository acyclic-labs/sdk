use super::*;

#[test]
fn explicit_state_directory_is_used_by_a_fresh_registry() -> Result<(), Box<dyn std::error::Error>>
{
    const EXPECTED: &str = "ACYCLIC_FS_REGISTRY_EXPECTED_BASE";
    if let Some(expected) = std::env::var_os(EXPECTED) {
        let expected = PathBuf::from(expected);
        // Check the selected root before initialize can create any directory.
        // The source-negative therefore cannot write into the real profile.
        assert_eq!(
            persistent_base(),
            Some(expected.clone()),
            "registry ignored the explicit state root"
        );
        assert!(!expected.exists(), "fixture state starts absent");
        initialize()?;
        let expected = expected
            .join("acyclic")
            .join("native-mount")
            .canonicalize()?;
        assert_eq!(directory()?, expected);
        assert_eq!(path()?, expected.join(NATIVE_REGISTRY_NAME));
        let work = crate::NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .map_err(|(error, _)| error)?;
        drop(open(&mut &work)?);
        assert!(expected.join(NATIVE_REGISTRY_NAME).is_file());
        return Ok(());
    }
    let fixture = tempfile::tempdir()?;
    let profile = fixture.path().join("empty-profile");
    let codex = fixture.path().join("fixture-codex");
    std::fs::create_dir(&profile)?;
    std::fs::create_dir(&codex)?;
    #[cfg(windows)]
    let variable = "LOCALAPPDATA";
    #[cfg(not(windows))]
    let variable = "XDG_STATE_HOME";
    let name = format!(
        "{}::explicit_state_directory_is_used_by_a_fresh_registry",
        module_path!().split_once("::").unwrap().1
    );
    for name_suffix in ["first-state", "second-state"] {
        let state = fixture.path().join(name_suffix);
        let output = acyclic_native_runtime::process_output(
            std::process::Command::new(std::env::current_exe()?)
                .args(["--exact", &name, "--nocapture"])
                .env(EXPECTED, &state)
                .env(variable, &state)
                .env("HOME", &profile)
                .env("USERPROFILE", &profile)
                .env("CODEX_HOME", &codex)
                .env("APPDATA", profile.join("AppData").join("Roaming")),
            std::time::Duration::from_secs(5),
            16 * 1024,
        )?;
        assert!(output.status.success(), "{variable}={state:?}: {output:?}");
        assert!(
            state
                .join("acyclic")
                .join("native-mount")
                .join(NATIVE_REGISTRY_NAME)
                .is_file()
        );
        assert!(!profile.join("AppData").exists());
    }
    Ok(())
}
