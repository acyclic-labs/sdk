use super::{
    observe_binary_rustdoc, observe_rustdoc, package_by_name, ProfileError, ProfileSpec,
    RustdocObservation,
};
use crate::CommandOutput;
use cargo_metadata::{Metadata, MetadataCommand, TargetKind};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    process::Command,
};

/// The exact lock authority for an archived Cargo owner.
#[derive(Clone, Copy)]
pub enum LockSelection<'a> {
    RetainedConfig(&'a Path),
    SourceAdjacent(&'a Path),
}

/// Cargo and lock selection for metadata; Rustdoc JSON uses its pinned nightly.
#[derive(Clone, Copy, Default)]
pub struct CargoExecutionContext<'a> {
    pub cargo_path: Option<&'a Path>,
    pub lock_selection: Option<LockSelection<'a>>,
}

impl CargoExecutionContext<'_> {
    fn config_options(self) -> Result<Vec<String>, ProfileError> {
        let Some(selection) = self.lock_selection else {
            return Ok(Vec::new());
        };
        let value = match selection {
            LockSelection::RetainedConfig(path) => path
                .to_str()
                .ok_or_else(|| {
                    ProfileError::InvalidRustdoc("producer config path is not UTF-8".into())
                })?
                .to_owned(),
            LockSelection::SourceAdjacent(path) => {
                if !path.is_absolute() {
                    return Err(ProfileError::InvalidRustdoc(
                        "selected owner lock must be absolute".into(),
                    ));
                }
                let path = path.to_str().ok_or_else(|| {
                    ProfileError::InvalidRustdoc("selected owner lock path is not UTF-8".into())
                })?;
                // JSON string escaping is valid TOML basic-string escaping.
                // This physical command argument is never captured source data.
                format!(
                    "resolver.lockfile-path={}",
                    serde_json::to_string(path)
                        .map_err(|e| { ProfileError::InvalidRustdoc(e.to_string()) })?
                )
            }
        };
        Ok(vec!["--config".into(), value])
    }
    pub fn configure(self, command: &mut Command) -> Result<(), ProfileError> {
        crate::child::configure(command);
        command.env_remove("CARGO_RESOLVER_LOCKFILE_PATH");
        command.args(self.config_options()?);
        Ok(())
    }

    pub fn configure_metadata(self, command: &mut MetadataCommand) -> Result<(), ProfileError> {
        for (key, value) in crate::child::overrides() {
            match value {
                Some(value) => {
                    command.env(key, value);
                }
                None => {
                    command.env_remove(key);
                }
            }
        }
        command.env_remove("CARGO_RESOLVER_LOCKFILE_PATH");
        if let Some(directory) = crate::command_directory() {
            command.current_dir(directory);
        }
        if let Some(cargo) = self.cargo_path {
            command.cargo_path(cargo);
        }
        let mut options = vec!["--locked".to_owned()];
        options.extend(self.config_options()?);
        command.other_options(options);
        Ok(())
    }
}

/// Reads metadata with the selected Cargo executable and owner lock.
pub fn load_metadata(
    manifest: impl AsRef<Path>,
    context: CargoExecutionContext<'_>,
) -> Result<Metadata, ProfileError> {
    let mut command = MetadataCommand::new();
    context.configure_metadata(&mut command)?;
    command.no_deps().manifest_path(manifest.as_ref());
    crate::execute_metadata(&command)
        .map_err(|error| ProfileError::InvalidRustdoc(format!("cargo metadata failed: {error}")))
}

const RUSTDOC_TOOLCHAIN: &str = "nightly-2026-07-01";

#[derive(Clone)]
pub(crate) struct RustdocProducer {
    cargo: PathBuf,
    targets: BTreeSet<String>,
    compiler: Compiler,
}

#[derive(Clone)]
struct Compiler {
    rustc: PathBuf,
    host: String,
    cfgs: (String, String),
}

// Only concrete compiler output survives an operation, fenced on native file
// identity. Rustup selection and installed targets belong to the operation.
static COMPILERS: crate::concrete_versions::Versions<PathBuf, sdk_file_identity::Stamp, Compiler> =
    crate::concrete_versions::Mutex::new(std::collections::BTreeMap::new());
#[cfg(test)]
static PROBES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
#[cfg(test)]
thread_local! { static LOOKUPS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }

#[cfg(test)]
pub(super) fn rustup_lookup_count() -> usize {
    LOOKUPS.get()
}

fn rustup_lookup(args: &[&str]) -> Result<std::process::Output, ProfileError> {
    #[cfg(test)]
    LOOKUPS.set(LOOKUPS.get() + 1);
    crate::command("rustup")
        .env("RUSTUP_AUTO_INSTALL", "0")
        .args(args)
        .run_output()
        .map_err(|error| ProfileError::InvalidRustdoc(error.to_string()))
}

fn compiler_probe(rustc: &Path, args: &[&str]) -> Result<String, ProfileError> {
    #[cfg(test)]
    PROBES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let output = crate::command(rustc)
        .args(args)
        .run_output()
        .map_err(|error| ProfileError::InvalidRustdoc(error.to_string()))?;
    if !output.status.success() {
        return Err(ProfileError::InvalidRustdoc(format!(
            "pinned compiler probe failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    String::from_utf8(output.stdout)
        .map_err(|error| ProfileError::InvalidRustdoc(error.to_string()))
}

fn resolve_producer() -> Result<RustdocProducer, ProfileError> {
    let installed = rustup_lookup(&[
        "target",
        "list",
        "--installed",
        "--toolchain",
        RUSTDOC_TOOLCHAIN,
    ])?;
    if !installed.status.success() {
        return Err(ProfileError::InvalidRustdoc(format!(
            "cannot query {RUSTDOC_TOOLCHAIN} targets: {}",
            String::from_utf8_lossy(&installed.stderr)
        )));
    }
    let targets: BTreeSet<String> = String::from_utf8_lossy(&installed.stdout)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
        .collect();
    let output = rustup_lookup(&["which", "cargo", "--toolchain", RUSTDOC_TOOLCHAIN])?;
    if !output.status.success() {
        return Err(ProfileError::InvalidRustdoc(format!(
            "Rustdoc toolchain {RUSTDOC_TOOLCHAIN} is unavailable: {}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    let cargo = PathBuf::from(
        String::from_utf8(output.stdout)
            .map_err(|error| ProfileError::InvalidRustdoc(error.to_string()))?
            .trim(),
    )
    .canonicalize()
    .map_err(|error| ProfileError::InvalidRustdoc(error.to_string()))?;
    let rustc = cargo
        .parent()
        .ok_or_else(|| ProfileError::InvalidRustdoc("pinned Cargo has no directory".into()))?
        .join(if cfg!(windows) { "rustc.exe" } else { "rustc" })
        .canonicalize()
        .map_err(|error| ProfileError::InvalidRustdoc(error.to_string()))?;
    let compiler = crate::concrete_versions::query(
        &COMPILERS,
        rustc.clone(),
        || {
            sdk_file_identity::stamp(&rustc)
                .map_err(|error| ProfileError::InvalidRustdoc(error.to_string()))
        },
        || {
            let verbose = compiler_probe(&rustc, &["-vV"])?;
            let host = verbose
                .lines()
                .find_map(|line| line.strip_prefix("host: "))
                .ok_or_else(|| {
                    ProfileError::InvalidRustdoc("pinned compiler host is absent".into())
                })?
                .to_owned();
            let cfgs = (
                compiler_probe(&rustc, &["--print", "cfg", "--target", &host])?,
                compiler_probe(
                    &rustc,
                    &["--print", "cfg", "--target", "wasm32-unknown-unknown"],
                )?,
            );
            Ok::<_, ProfileError>(Compiler {
                rustc: rustc.clone(),
                host,
                cfgs,
            })
        },
        || ProfileError::InvalidRustdoc("pinned compiler changed during observation".into()),
    )?;
    if !targets.contains(&compiler.host) {
        return Err(ProfileError::InvalidRustdoc(
            "pinned compiler host target is unavailable".into(),
        ));
    }
    Ok(RustdocProducer {
        cargo,
        targets,
        compiler,
    })
}

fn with_producer<T>(
    required: &[&str],
    use_producer: impl FnOnce(&RustdocProducer) -> T,
) -> Result<T, ProfileError> {
    let producer = crate::render_cache::producer(resolve_producer)?;
    for &target in required {
        if !producer.targets.contains(target) {
            return Err(ProfileError::UnsupportedTarget {
                target: target.to_owned(),
            });
        }
    }
    Ok(use_producer(&producer))
}

/// Returns the host of the compiler that produces the pinned Rustdoc output.
pub fn rustdoc_host() -> Result<String, ProfileError> {
    with_producer(&[], |producer| producer.compiler.host.clone())
}

/// Reuses the pinned compiler's host/browser cfg across all profile batches.
pub fn rustdoc_platform_cfgs() -> Result<(String, String), ProfileError> {
    with_producer(&[], |producer| producer.compiler.cfgs.clone())
}

/// Queries installed targets and rejects unavailable requirements without provisioning.
pub fn rustdoc_targets(required: &[&str]) -> Result<BTreeSet<String>, ProfileError> {
    with_producer(required, |producer| producer.targets.clone())
}

fn nightly_rustdoc_command(target: &str) -> Result<Command, ProfileError> {
    with_producer(&[target], |producer| {
        let bin = producer.cargo.parent().ok_or_else(|| {
            ProfileError::InvalidRustdoc("pinned Cargo has no toolchain directory".into())
        })?;
        let mut command = crate::command(&producer.cargo);
        // Cargo fingerprints RUSTUP_HOME/toolchains/RUSTUP_TOOLCHAIN/bin/rustc.
        // An abbreviated nightly name disables its compiler-info cache.
        command
            .env(
                "RUSTUP_TOOLCHAIN",
                format!("{RUSTDOC_TOOLCHAIN}-{}", producer.compiler.host),
            )
            .env("RUSTC", &producer.compiler.rustc)
            .env(
                "RUSTDOC",
                bin.join(if cfg!(windows) {
                    "rustdoc.exe"
                } else {
                    "rustdoc"
                }),
            );
        Ok(command)
    })?
}

/// A Cargo-owned target; binary receipts are for source guides, never public API extraction.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RustdocTarget {
    Library,
    Binary(String),
}

/// Executes a Cargo-owned target with pinned nightly Rustdoc and the selected lock.
#[allow(clippy::too_many_arguments)]
pub fn execute_profile(
    manifest: impl AsRef<Path>,
    metadata: &Metadata,
    profile: &ProfileSpec,
    available_targets: &BTreeSet<String>,
    target_dir: impl AsRef<Path>,
    output_json: impl AsRef<Path>,
    context: CargoExecutionContext<'_>,
    rustdoc_target: &RustdocTarget,
) -> Result<RustdocObservation, ProfileError> {
    if !available_targets.contains(&profile.target) {
        return Err(ProfileError::UnsupportedTarget {
            target: profile.target.clone(),
        });
    }
    let package = package_by_name(metadata, &profile.package)
        .ok_or_else(|| ProfileError::UnknownPackage(profile.package.clone()))?;
    if let RustdocTarget::Binary(name) = rustdoc_target {
        if !package
            .targets
            .iter()
            .any(|target| target.name == *name && target.kind.contains(&TargetKind::Bin))
        {
            return Err(ProfileError::InvalidRustdoc(
                "binary target is absent from Cargo metadata".into(),
            ));
        }
    }
    for feature in &profile.features {
        // Cargo owns qualified dependency/workspace feature syntax and resolution.
        // Keep the cheap early rejection for unknown owner-local names only.
        if !feature.contains('/') && !package.features.contains_key(feature) {
            return Err(ProfileError::InvalidFeature {
                package: profile.package.clone(),
                feature: feature.clone(),
            });
        }
    }
    let target_dir = target_dir.as_ref();
    fs::create_dir_all(target_dir).map_err(|error| {
        ProfileError::InvalidRustdoc(format!("cannot create target directory: {error}"))
    })?;
    let mut command = nightly_rustdoc_command(&profile.target)?;
    command
        .arg("rustdoc")
        .arg("--locked")
        .arg("--manifest-path")
        .arg(manifest.as_ref())
        .arg("--package")
        .arg(&profile.package);
    match rustdoc_target {
        RustdocTarget::Library => {
            command.arg("--lib");
        }
        RustdocTarget::Binary(name) => {
            command.args(["--bin", name]);
        }
    }
    command
        .arg("--target")
        .arg(&profile.target)
        .env("CARGO_TARGET_DIR", target_dir);
    if !profile.default_features {
        command.arg("--no-default-features");
    }
    if !profile.features.is_empty() {
        command.arg("--features").arg(
            profile
                .features
                .iter()
                .cloned()
                .collect::<Vec<_>>()
                .join(","),
        );
    }
    context.configure(&mut command)?;
    command.args(["--", "-Z", "unstable-options", "--output-format", "json"]);
    let output = command.run_output().map_err(|error| {
        ProfileError::InvalidRustdoc(format!(
            "could not start Cargo for {}: {error}",
            profile.id().0
        ))
    })?;
    if !output.status.success() {
        return Err(ProfileError::InvalidRustdoc(format!(
            "Cargo profile {} failed ({}): {}",
            profile.id().0,
            output.status,
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    let library_target = package.targets.iter().find(|target| {
        target.kind.iter().any(|kind| {
            matches!(
                kind,
                TargetKind::ProcMacro
                    | TargetKind::Lib
                    | TargetKind::RLib
                    | TargetKind::DyLib
                    | TargetKind::CDyLib
                    | TargetKind::StaticLib
            )
        })
    });
    let library_name = library_target
        .map(|target| target.name.as_str())
        .unwrap_or(profile.package.as_str());
    let receipt_name = match rustdoc_target {
        RustdocTarget::Library => library_name,
        RustdocTarget::Binary(name) => name,
    };
    let filename = format!("{}.json", receipt_name.replace('-', "_"));
    // Procedural macros are host tools. Cargo writes their JSON in the host
    // documentation directory even when --target is explicitly supplied.
    let generated = if matches!(rustdoc_target, RustdocTarget::Library)
        && library_target.is_some_and(|target| target.kind.contains(&TargetKind::ProcMacro))
    {
        target_dir.join("doc")
    } else {
        target_dir.join(&profile.target).join("doc")
    }
    .join(filename);
    if !generated.is_file() {
        return Err(ProfileError::InvalidRustdoc(format!(
            "Cargo succeeded but did not produce {}",
            generated.display()
        )));
    }
    fs::copy(&generated, output_json.as_ref()).map_err(|error| {
        ProfileError::InvalidRustdoc(format!(
            "cannot retain Rustdoc receipt {}: {error}",
            output_json.as_ref().display()
        ))
    })?;
    let observation = match rustdoc_target {
        RustdocTarget::Library => observe_rustdoc(output_json),
        RustdocTarget::Binary(_) => observe_binary_rustdoc(output_json),
    }?;
    if observation.target != profile.target {
        return Err(ProfileError::InvalidRustdoc(format!(
            "Rustdoc target {} differs from declared profile target {}",
            observation.target, profile.target
        )));
    }
    Ok(observation)
}

#[test]
fn prepared_rustdoc_producer_is_reused_for_any_profile_order() {
    // Isolate the counters from other tests that select different fixture compilers.
    if std::env::var_os("SDK_DOCS_PREPARED_CHILD").is_none() {
        let thread = std::thread::current();
        let output = crate::command(std::env::current_exe().unwrap())
            .args(["--exact", thread.name().unwrap(), "--nocapture"])
            .env("SDK_DOCS_PREPARED_CHILD", "1")
            .run_output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
        return;
    }
    crate::with_rustdoc_cache(|| {
        let targets = rustdoc_targets(&[])
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>();
        let cfgs = rustdoc_platform_cfgs().unwrap();
        let lookups = rustup_lookup_count();
        assert_eq!(lookups, 2);
        let probes = PROBES.load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(probes, 3);
        let profiles = proptest::collection::vec(0..targets.len(), 0..32);
        proptest::test_runner::TestRunner::default()
            .run(&profiles, |order| {
                for index in order {
                    nightly_rustdoc_command(&targets[index]).unwrap();
                }
                let unavailable = "sdk-docs-unavailable-target";
                proptest::prop_assert_eq!(
                    nightly_rustdoc_command(unavailable).unwrap_err(),
                    ProfileError::UnsupportedTarget {
                        target: unavailable.into()
                    },
                );
                proptest::prop_assert_eq!(rustup_lookup_count(), lookups);
                proptest::prop_assert_eq!(rustdoc_platform_cfgs().unwrap(), cfgs.clone());
                proptest::prop_assert_eq!(
                    PROBES.load(std::sync::atomic::Ordering::Relaxed),
                    probes
                );
                Ok(())
            })
            .unwrap();
    });
}

#[cfg(test)]
#[path = "execution/tests.rs"]
mod tests;
