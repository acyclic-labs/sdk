use super::*;

type VersionKey = (PathBuf, String, PathBuf);
thread_local! {
    // Read-only selections belong to one invocation and one calling thread.
    static CARGOS: std::cell::RefCell<Option<BTreeMap<String, PathBuf>>> = const { std::cell::RefCell::new(None) };
    static VERSIONS: std::cell::RefCell<Option<BTreeMap<VersionKey, String>>> = const { std::cell::RefCell::new(None) };
}

pub(super) fn with_toolchain_cache<T>(run: impl FnOnce() -> T) -> T {
    struct Restore(
        Option<BTreeMap<String, PathBuf>>,
        Option<BTreeMap<VersionKey, String>>,
    );
    impl Drop for Restore {
        fn drop(&mut self) {
            CARGOS.set(self.0.take());
            VERSIONS.set(self.1.take());
        }
    }
    let _restore = Restore(
        CARGOS.replace(Some(BTreeMap::new())),
        VERSIONS.replace(Some(BTreeMap::new())),
    );
    run()
}

fn cached<K: Ord + Clone + 'static, V: Clone + 'static>(
    slot: &std::thread::LocalKey<std::cell::RefCell<Option<BTreeMap<K, V>>>>,
    key: K,
    probe: impl FnOnce() -> Result<V, CliError>,
) -> Result<V, CliError> {
    if let Some(value) =
        slot.with_borrow(|cache| cache.as_ref().and_then(|cache| cache.get(&key)).cloned())
    {
        return Ok(value);
    }
    let value = probe()?;
    slot.with_borrow_mut(|cache| {
        if let Some(cache) = cache {
            cache.insert(key, value.clone());
        }
    });
    Ok(value)
}

fn cached_version(
    key: VersionKey,
    probe: impl FnOnce() -> Result<String, CliError>,
) -> Result<String, CliError> {
    cached(&VERSIONS, key, probe)
}

pub(super) fn cargo_version(
    cargo: &Path,
    digest: &str,
    directory: &Path,
) -> Result<String, CliError> {
    cached_version(
        (cargo.to_owned(), digest.to_owned(), directory.to_owned()),
        || {
            let output = Command::new(cargo)
                .current_dir(directory)
                .arg("-vV")
                .output()
                .map_err(io_error)?;
            if !output.status.success() {
                return Err(CliError("pinned Cargo identity query failed".into()));
            }
            String::from_utf8(output.stdout).map_err(|error| CliError(error.to_string()))
        },
    )
}

pub(super) fn rustc_host() -> Result<String, CliError> {
    sdk_docs::rustdoc_profiles::rustdoc_host().map_err(profile_error)
}

pub(super) fn pinned_cargo(root: &Path) -> Result<PathBuf, CliError> {
    let toolchain = fs::read_to_string(root.join("rust-toolchain.toml"))
        .map_err(io_error)?
        .lines()
        .find_map(|line| {
            let (key, value) = line.split_once('=')?;
            (key.trim() == "channel").then(|| value.trim().trim_matches('"').to_owned())
        })
        .ok_or_else(|| CliError("rust-toolchain.toml has no pinned channel".into()))?;
    cached(&CARGOS, toolchain.clone(), || {
        let output = Command::new("rustup")
            .args(["which", "cargo", "--toolchain", &toolchain])
            .output()
            .map_err(io_error)?;
        if !output.status.success() {
            return Err(CliError(format!(
                "pinned Cargo toolchain `{toolchain}` is unavailable: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        let path = PathBuf::from(String::from_utf8_lossy(&output.stdout).trim());
        if !path.is_file() {
            return Err(CliError(format!(
                "rustup selected a missing Cargo executable {}",
                path.display()
            )));
        }
        Ok(path)
    })
}

pub(super) fn toolchain_identity(cargo: &Path) -> Result<String, CliError> {
    let digest = sha256_file(cargo)?;
    Ok(
        cargo_version(cargo, &digest, &env::current_dir().map_err(io_error)?)?
            .trim()
            .to_owned(),
    )
}

pub(super) fn rustup_targets() -> Result<BTreeSet<String>, CliError> {
    sdk_docs::rustdoc_profiles::rustdoc_targets(&["wasm32-unknown-unknown"]).map_err(profile_error)
}

#[cfg(test)]
mod cache_tests {
    use super::*;
    proptest::proptest! {
        #[test]
        fn version_reuse_requires_path_bytes_and_working_directory(
            inputs in proptest::collection::vec(("[a-c]", "[a-c]", "[a-c]"), 0..40)
        ) {
            let calls = std::cell::Cell::new(0);
            let selections = std::cell::Cell::new(0);
            let channels = inputs.iter().map(|(path,_,_)| path).collect::<BTreeSet<_>>().len();
            let unique = inputs.iter().cloned().collect::<BTreeSet<_>>().len();
            with_toolchain_cache(|| {
                for (path, digest, directory) in &inputs {
                    let expected = format!("{path}:{digest}:{directory}");
                    let actual = cached_version((PathBuf::from(path), digest.clone(), PathBuf::from(directory)), || {
                        calls.set(calls.get()+1); Ok(expected.clone())
                    }).unwrap();
                    proptest::prop_assert_eq!(actual, expected);
                    let selected = cached(&CARGOS, path.clone(), || {
                        selections.set(selections.get()+1); Ok(PathBuf::from(path))
                    }).unwrap();
                    proptest::prop_assert_eq!(selected, PathBuf::from(path));
                }
                Ok(())
            })?;
            proptest::prop_assert_eq!(calls.get(), unique);
            proptest::prop_assert_eq!(selections.get(), channels);
            proptest::prop_assert!(VERSIONS.with_borrow(|cache| cache.is_none()));
        }
    }

    #[test]
    fn nested_unwinding_and_failed_probes_restore_the_invocation() {
        let key = (
            PathBuf::from("cargo"),
            "digest".to_owned(),
            PathBuf::from("root"),
        );
        with_toolchain_cache(|| {
            assert!(cached_version(key.clone(), || Err(CliError("retry".into()))).is_err());
            assert_eq!(
                cached_version(key.clone(), || Ok("outer".into())).unwrap(),
                "outer"
            );
            let failed = std::panic::catch_unwind(|| {
                with_toolchain_cache(|| {
                    assert_eq!(
                        cached_version(key.clone(), || Ok("inner".into())).unwrap(),
                        "inner"
                    );
                    panic!("unwind inner selection");
                })
            });
            assert!(failed.is_err());
            assert_eq!(
                cached_version(key, || panic!("must reuse outer selection")).unwrap(),
                "outer"
            );
        });
        assert!(VERSIONS.with_borrow(|cache| cache.is_none()));
        assert!(CARGOS.with_borrow(|cache| cache.is_none()));
    }
}
