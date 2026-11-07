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
            acyclic_native_runtime::ProcessCommand::new(std::env::current_exe()?)
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

#[test]
fn invalid_state_roots_use_the_fallback_in_a_fresh_process()
-> Result<(), Box<dyn std::error::Error>> {
    const CHILD: &str = "ACYCLIC_FS_REGISTRY_INVALID_ROOT_CHILD";
    if std::env::var_os(CHILD).is_some() {
        #[cfg(windows)]
        let expected = dirs::data_local_dir().filter(|path| path.is_absolute());
        #[cfg(not(windows))]
        let expected =
            if std::env::var_os(CHILD).as_deref() == Some(std::ffi::OsStr::new("absolute-home")) {
                Some(
                    PathBuf::from(std::env::var_os("HOME").unwrap())
                        .join(".local")
                        .join("state"),
                )
            } else {
                dirs::data_local_dir().filter(|path| path.is_absolute())
            };
        // Compare before initialize. A source-negative must never create a
        // relative registry. The OS fallback is only inspected.
        assert_eq!(
            persistent_base(),
            expected,
            "invalid state root selected a registry according to process cwd"
        );
        #[cfg(not(windows))]
        if std::env::var_os(CHILD).as_deref() == Some(std::ffi::OsStr::new("absolute-home")) {
            let expected = expected.ok_or("fixture HOME is absent")?;
            initialize()?;
            assert_eq!(
                directory()?,
                expected
                    .join("acyclic")
                    .join("native-mount")
                    .canonicalize()?
            );
        }
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
        "{}::invalid_state_roots_use_the_fallback_in_a_fresh_process",
        module_path!().split_once("::").unwrap().1
    );
    #[cfg(windows)]
    let homes = [profile.as_os_str()];
    #[cfg(not(windows))]
    let homes = [
        profile.as_os_str(),
        std::ffi::OsStr::new("relative-home"),
        std::ffi::OsStr::new(""),
    ];
    for home in homes {
        for value in ["", "relative-state", "../relative-state"] {
            let output = acyclic_native_runtime::process_output(
                acyclic_native_runtime::ProcessCommand::new(std::env::current_exe()?)
                    .args(["--exact", &name, "--nocapture"])
                    .current_dir(fixture.path())
                    .env(
                        CHILD,
                        if Path::new(home).is_absolute() {
                            "absolute-home"
                        } else {
                            "invalid-home"
                        },
                    )
                    .env(variable, value)
                    .env("HOME", home)
                    .env("USERPROFILE", &profile)
                    .env("CODEX_HOME", &codex)
                    .env("APPDATA", profile.join("AppData").join("Roaming")),
                std::time::Duration::from_secs(5),
                16 * 1024,
            )?;
            assert!(
                output.status.success(),
                "{variable}={value:?}, HOME={home:?}: {output:?}"
            );
            assert!(!fixture.path().join("relative-state").exists());
            assert!(!fixture.path().join("relative-home").exists());
        }
    }
    Ok(())
}

#[test]
#[cfg(not(loom))]
fn mount_guards_share_one_registry_directory_and_lock() -> Result<(), Box<dyn std::error::Error>> {
    initialize()?;
    let fixture = tempfile::tempdir()?;
    let directory = fixture.path().join("registry");
    std::fs::create_dir(&directory)?;
    let slot = RegistrySlot::new();
    let cancellation = crate::CancellationToken::new();
    let work = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let registry = slot.open(&directory, &mut &work)?;
    let before = work.receipt();
    let same = slot.open(&directory, &mut &work)?;
    assert!(
        crate::sync::native::Arc::ptr_eq(&registry, &same),
        "each mount reopened the registry directory and lock"
    );
    assert_eq!(
        work.receipt(),
        before,
        "sharing re-traversed the registry path"
    );
    let mut guards = Vec::new();
    for name in ["first", "second"] {
        let destination = fixture.path().join(name);
        std::fs::create_dir(&destination)?;
        let mut guard = None;
        MountDestinationGuard::acquire_with_registry(
            &destination,
            false,
            &work,
            &mut guard,
            false,
            |work| slot.open(&directory, &mut &*work),
        )?;
        let guard = guard.unwrap();
        assert!(
            crate::sync::native::Arc::ptr_eq(&registry, guard.registry_file.as_ref().unwrap()),
            "destination guard owns a separate registry directory or lock"
        );
        guards.push(guard);
    }
    for mut guard in guards {
        guard.release_cleanup(None)?;
    }
    assert!(
        directory.join(NATIVE_REGISTRY_NAME).is_file(),
        "retirement unlinked the process-shared registry"
    );
    let (_, probe) = open_in_directory(&directory, &mut &work)?;
    // The independent descriptor must detect a retained lock; no other test
    // can hold this fixture's registry and counterfeit that failure.
    {
        let held = registry.lock(&mut &work)?;
        assert!(fence_io::try_lock(&probe).is_err_and(|error| lock_is_contended(&error)));
        held.unlock(&mut &work)?;
    }
    fence_io::try_lock(&probe)?;
    fence_io::unlock(&probe)?;
    Ok(())
}
