use super::*;

pub(super) fn load_metadata(root: &Path, cargo_path: Option<&Path>) -> Result<Metadata, CliError> {
    let mut metadata = sdk_docs::rustdoc_profiles::load_metadata(
        root.join("Cargo.toml"),
        sdk_docs::rustdoc_profiles::CargoExecutionContext {
            cargo_path,
            lock_selection: None,
        },
    )
    .map_err(profile_error)?;
    let missing = published_packages(root)?
        .into_iter()
        .filter(|name| {
            !metadata
                .packages
                .iter()
                .any(|package| package.name.as_ref() == name)
        })
        .collect::<Vec<_>>();
    if missing.is_empty() {
        return Ok(metadata);
    }

    // Workspace metadata deliberately excludes dependencies. Explicitly published
    // vendored path owners may also be excluded from the workspace, so resolve
    // the locked graph, retaining only those exact local published owners.
    // Registry dependencies must never become documentation owners implicitly.
    let mut command = cargo_metadata::MetadataCommand::new();
    sdk_docs::rustdoc_profiles::CargoExecutionContext {
        cargo_path,
        lock_selection: None,
    }
    .configure_metadata(&mut command)
    .map_err(profile_error)?;
    command.manifest_path(root.join("Cargo.toml"));
    let resolved = sdk_docs::execute_metadata(&command)
        .map_err(|error| CliError(format!("published owner metadata failed: {error}")))?;
    let canonical_root = root.canonicalize().map_err(io_error)?;
    for name in missing {
        let candidates = resolved
            .packages
            .iter()
            .filter(|package| package.name.as_ref() == name && package.source.is_none())
            .collect::<Vec<_>>();
        if candidates.len() != 1 {
            return Err(CliError(format!(
                "published owner `{name}` must resolve to exactly one local Cargo package"
            )));
        }
        let package = candidates[0];
        let manifest = package.manifest_path.canonicalize().map_err(io_error)?;
        if !manifest.starts_with(&canonical_root) {
            return Err(CliError(format!(
                "published owner `{name}` has a manifest outside the captured source root"
            )));
        }
        metadata.packages.push(package.clone());
    }
    metadata
        .packages
        .sort_by(|left, right| left.name.cmp(&right.name));
    Ok(metadata)
}

pub(super) fn documentation_cargo(root: &Path, historical: bool) -> Result<PathBuf, CliError> {
    if historical {
        maintained_cargo(root)
    } else {
        pinned_cargo(root)
    }
}

pub(super) fn is_binary_receipt(receipt: &Path, profiles: &[ExecutedProfile]) -> bool {
    profiles.iter().any(|profile| {
        profile.receipt == receipt
            && matches!(
                profile.target,
                sdk_docs::rustdoc_profiles::RustdocTarget::Binary(_)
            )
    })
}
pub(super) fn observe_profile_receipt(
    receipt: &Path,
    profiles: &[ExecutedProfile],
) -> Result<sdk_docs::rustdoc_profiles::RustdocObservation, sdk_docs::rustdoc_profiles::ProfileError>
{
    if is_binary_receipt(receipt, profiles) {
        sdk_docs::rustdoc_profiles::observe_binary_rustdoc(receipt)
    } else {
        observe_rustdoc(receipt)
    }
}
pub(super) fn package_for_profile<'a>(
    metadata: &'a Metadata,
    receipt: &Path,
    crate_name: &str,
    profiles: &[ExecutedProfile],
) -> Result<&'a Package, CliError> {
    if let Some(profile) = profiles.iter().find(|profile| profile.receipt == receipt) {
        metadata
            .packages
            .iter()
            .find(|package| package.name.as_ref() == profile.spec.package)
            .ok_or_else(|| CliError("profile Cargo owner disappeared".into()))
    } else {
        package_for_crate(metadata, crate_name)
    }
}
pub(super) fn historical_docs_input(
    metadata: &Metadata,
    profiles: &[ExecutedProfile],
    releases: &[sdk_docs::historical::ReleasedPackage],
    captured_source: Option<sdk_docs::historical::CapturedSource>,
) -> Result<sdk_docs::historical::Input, CliError> {
    let mut binaries = Vec::new();
    for profile in profiles {
        if let sdk_docs::rustdoc_profiles::RustdocTarget::Binary(name) = &profile.target {
            let package = metadata
                .packages
                .iter()
                .find(|package| package.name.as_ref() == profile.spec.package)
                .ok_or_else(|| CliError("binary Cargo owner is absent".into()))?;
            let target = package
                .targets
                .iter()
                .find(|target| target.name == *name && target.kind.contains(&TargetKind::Bin))
                .ok_or_else(|| CliError("binary Cargo target is absent".into()))?;
            let package_root = package
                .manifest_path
                .parent()
                .ok_or_else(|| CliError("binary Cargo manifest has no parent".into()))?;
            let readme = package
                .readme
                .as_ref()
                .map(|path| package_root.join(path).into_std_path_buf());
            binaries.push(sdk_docs::historical::BinaryInput {
                rustdoc_file: profile.receipt.clone(),
                source_file: target.src_path.clone().into_std_path_buf(),
                readme,
            });
        }
    }
    Ok(sdk_docs::historical::Input {
        captured_source,
        released_packages: releases.to_vec(),
        binaries,
    })
}
pub(super) fn scope_generated_sources(
    mut sources: Vec<GeneratedSource>,
    catalog: &Path,
    output: &Path,
) -> Result<Vec<GeneratedSource>, CliError> {
    let prefix = output
        .strip_prefix(catalog)
        .map_err(|_| CliError("historical output escapes catalog".into()))?;
    if !prefix.as_os_str().is_empty() {
        for source in &mut sources {
            source.logical_path = prefix.join(&source.logical_path);
        }
    }
    Ok(sources)
}

pub(super) fn execute_historical_profiles(
    root: &Path,
    output: &Path,
    rustdoc_dir: &Path,
    metadata: &Metadata,
    source_sha256: &str,
    cargo: &Path,
    plan: &crate::parallel::Arc<historical::Plan>,
) -> Result<Vec<ExecutedProfile>, CliError> {
    // Default receipts use Cargo target names. Preserve sequential behavior
    // when owners collide, including Windows case aliases.
    let disjoint = distinct_receipt_names(
        plan.released_packages
            .iter()
            .filter_map(|release| {
                metadata
                    .packages
                    .iter()
                    .find(|package| release.package == package.name.as_ref())
            })
            .flat_map(historical_targets)
            .map(|(target, _)| target.name.as_str()),
    );
    let executed = if plan.released_packages.len() < 2 || !disjoint {
        compile_historical_profiles(
            root,
            output,
            rustdoc_dir,
            metadata,
            cargo,
            (plan, &plan.released_packages),
        )?
    } else {
        let jobs = plan.released_packages.clone();
        let plan = crate::parallel::Arc::clone(plan);
        let (root, output, rustdoc_dir, cargo) = (
            root.to_owned(),
            output.to_owned(),
            rustdoc_dir.to_owned(),
            cargo.to_owned(),
        );
        let metadata = metadata.clone();
        rustc_host()?;
        crate::parallel::map(
            jobs,
            sdk_docs::rustdoc_worker(move |release| {
                compile_historical_profiles(
                    &root,
                    &output,
                    &rustdoc_dir,
                    &metadata,
                    &cargo,
                    (&plan, std::slice::from_ref(&release)),
                )
            }),
        )
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .collect()
    };
    // Retained aliases form one shared inventory, so publish them serially
    // after every compiler has finished and before admitting any manifest.
    for profile in &executed {
        retain_profile_generated_sources(root, &profile.receipt)?;
        fs::write(
            profile.receipt.with_extension("source.sha256"),
            source_sha256,
        )
        .map_err(io_error)?;
    }
    Ok(executed)
}

fn distinct_receipt_names<'a>(names: impl IntoIterator<Item = &'a str>) -> bool {
    let mut seen = BTreeSet::new();
    names
        .into_iter()
        .all(|name| seen.insert(name.replace('-', "_").to_ascii_lowercase()))
}

fn historical_targets(
    package: &Package,
) -> Vec<(
    &cargo_metadata::Target,
    sdk_docs::rustdoc_profiles::RustdocTarget,
)> {
    use sdk_docs::rustdoc_profiles::RustdocTarget;
    let library = package.targets.iter().find(|target| {
        target.kind.iter().any(|kind| {
            matches!(
                kind,
                TargetKind::ProcMacro
                    | TargetKind::Lib
                    | TargetKind::RLib
                    | TargetKind::CDyLib
                    | TargetKind::StaticLib
                    | TargetKind::DyLib
            )
        })
    });
    if let Some(target) = library {
        vec![(target, RustdocTarget::Library)]
    } else {
        package
            .targets
            .iter()
            .filter(|target| target.kind.contains(&TargetKind::Bin))
            .map(|target| (target, RustdocTarget::Binary(target.name.clone())))
            .collect()
    }
}

fn compile_historical_profiles(
    root: &Path,
    output: &Path,
    rustdoc_dir: &Path,
    metadata: &Metadata,
    cargo: &Path,
    (plan, releases): (&historical::Plan, &[sdk_docs::historical::ReleasedPackage]),
) -> Result<Vec<ExecutedProfile>, CliError> {
    use sdk_docs::rustdoc_profiles::{execute_profile, CargoExecutionContext, RustdocTarget};
    let host = rustc_host()?;
    let available = BTreeSet::from([host.clone()]);
    let mut executed = Vec::new();
    fs::create_dir_all(rustdoc_dir).map_err(io_error)?;
    for release in releases {
        let execution_metadata = plan
            .owner_metadata
            .get(&release.package)
            .unwrap_or(metadata);
        let package = metadata
            .packages
            .iter()
            .find(|package| package.name.as_ref() == release.package)
            .ok_or_else(|| CliError("released Cargo owner is absent".into()))?;
        let targets = historical_targets(package);
        if targets.is_empty() {
            return Err(CliError(
                "released Cargo package has no native Rustdoc target".into(),
            ));
        }
        for (target, rustdoc_target) in targets {
            let mut compiled = BTreeMap::new();
            let feature_map = &execution_metadata
                .packages
                .iter()
                .find(|owner| owner.name == package.name)
                .ok_or_else(|| CliError("profile Cargo owner disappeared".into()))?
                .features;
            let profiles = if matches!(rustdoc_target, RustdocTarget::Library) {
                sdk_docs::rustdoc_profiles::historical_profiles_for_package(
                    execution_metadata,
                    package.name.as_ref(),
                    &host,
                    &available,
                )
                .map_err(profile_error)?
            } else {
                vec![ProfileSpec {
                    package: package.name.to_string(),
                    target: host.clone(),
                    default_features: true,
                    features: target.required_features.iter().cloned().collect(),
                }]
            };
            for spec in profiles {
                let receipt = if spec.default_features
                    && (spec.features.is_empty()
                        || matches!(rustdoc_target, RustdocTarget::Binary(_)))
                {
                    rustdoc_dir.join(format!("{}.json", target.name.replace('-', "_")))
                } else {
                    rustdoc_dir
                        .join("profiles")
                        .join(profile_receipt_filename(&spec, ""))
                };
                fs::create_dir_all(
                    receipt
                        .parent()
                        .ok_or_else(|| CliError("profile receipt has no parent".into()))?,
                )
                .map_err(io_error)?;
                let config = historical::producer_config(
                    root,
                    plan.captured_source.as_ref(),
                    release.package.as_str(),
                )
                .map_err(CliError)?;
                let manifest = if plan.captured_source.is_some() {
                    package.manifest_path.clone().into_std_path_buf()
                } else {
                    root.join("Cargo.toml")
                };
                let lock = manifest.with_file_name("Cargo.lock");
                let key = local_feature_configuration(feature_map, &spec);
                if let Some(previous) = key.as_ref().and_then(|key| compiled.get(key)) {
                    fs::copy(previous, &receipt).map_err(io_error)?;
                } else {
                    execute_profile(
                        &manifest,
                        execution_metadata,
                        &spec,
                        &available,
                        output.join(".profile-build").join(
                            crate::sha256_bytes(release.package.as_bytes())
                                .trim_start_matches("sha256:"),
                        ),
                        &receipt,
                        CargoExecutionContext {
                            cargo_path: Some(cargo),
                            lock_selection: historical::producer_lock_selection(
                                config.as_deref(),
                                &lock,
                                plan.captured_source.as_ref(),
                            ),
                        },
                        &rustdoc_target,
                    )
                    .map_err(profile_error)?;
                    if let Some(key) = key {
                        compiled.insert(key, receipt.clone());
                    }
                }
                executed.push(ExecutedProfile {
                    target: rustdoc_target.clone(),
                    receipt,
                    spec,
                });
            }
        }
    }
    Ok(executed)
}

pub(super) fn execute_default_profiles(
    root: &Path,
    output: &Path,
    rustdoc_dir: &Path,
    metadata: &Metadata,
    source_sha256: &str,
    cargo_path: &Path,
) -> Result<Vec<ExecutedProfile>, CliError> {
    let executed = compile_default_profiles(root, output, rustdoc_dir, metadata, cargo_path)?;
    for profile in &executed {
        retain_profile_generated_sources(root, &profile.receipt)?;
        fs::write(
            profile.receipt.with_extension("source.sha256"),
            source_sha256,
        )
        .map_err(io_error)?;
    }
    Ok(executed)
}

fn compile_default_profiles(
    root: &Path,
    output: &Path,
    rustdoc_dir: &Path,
    metadata: &Metadata,
    cargo_path: &Path,
) -> Result<Vec<ExecutedProfile>, CliError> {
    let host = rustc_host()?;
    fs::create_dir_all(rustdoc_dir).map_err(io_error)?;
    // Share Cargo's validated compiler/dependency cache across current owners.
    let target_dir = output.join(".profile-build");
    let wasm_target = "wasm32-unknown-unknown";
    let installed_targets = rustup_targets()?;
    let (host_output, browser_output) =
        sdk_docs::rustdoc_profiles::rustdoc_platform_cfgs().map_err(profile_error)?;
    let browser_cfg = browser_output
        .lines()
        .map(str::parse)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| CliError(format!("invalid rustc browser cfg: {error}")))?;
    let host_cfg = host_output
        .lines()
        .map(str::parse)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| CliError(format!("invalid rustc host cfg: {error}")))?;
    let published = published_packages(root)?
        .into_iter()
        .collect::<BTreeSet<_>>();
    let mut executed = Vec::new();
    for package in &metadata.packages {
        let owned_binding = api_owner_for_package(metadata, package.name.as_str())
            .ok()
            .is_some_and(|owner| owner.rustdoc_package == package.name.as_ref());
        if !published.contains(package.name.as_ref()) && !owned_binding {
            continue;
        }
        let excluded_owner = !metadata.workspace_members.contains(&package.id);
        let manifest = if excluded_owner {
            package.manifest_path.clone().into_std_path_buf()
        } else {
            root.join("Cargo.toml")
        };
        // Excluded packages may retain their own complete standalone lock,
        // including feature/dev dependencies absent from the workspace graph.
        let owner_lock = manifest.with_file_name("Cargo.lock");
        let selected_lock = if excluded_owner && owner_lock.is_file() {
            owner_lock
        } else {
            root.join("Cargo.lock")
        }
        .canonicalize()
        .map_err(io_error)?;
        let context = sdk_docs::rustdoc_profiles::CargoExecutionContext {
            cargo_path: Some(cargo_path),
            lock_selection: excluded_owner.then_some(
                sdk_docs::rustdoc_profiles::LockSelection::SourceAdjacent(&selected_lock),
            ),
        };
        let Some(target) = package.targets.iter().find(|target| {
            target.kind.iter().any(|kind| {
                matches!(
                    kind,
                    TargetKind::ProcMacro
                        | TargetKind::Lib
                        | TargetKind::RLib
                        | TargetKind::CDyLib
                        | TargetKind::StaticLib
                        | TargetKind::DyLib
                )
            })
        }) else {
            for target in package
                .targets
                .iter()
                .filter(|target| target.kind.contains(&TargetKind::Bin))
            {
                let profile = ProfileSpec {
                    package: package.name.to_string(),
                    target: host.clone(),
                    default_features: true,
                    features: target.required_features.iter().cloned().collect(),
                };
                let receipt = rustdoc_dir.join(format!("{}.json", target.name.replace('-', "_")));
                let rustdoc_target =
                    sdk_docs::rustdoc_profiles::RustdocTarget::Binary(target.name.clone());
                sdk_docs::rustdoc_profiles::execute_profile(
                    &manifest,
                    metadata,
                    &profile,
                    &BTreeSet::from([host.clone()]),
                    &target_dir,
                    &receipt,
                    context,
                    &rustdoc_target,
                )
                .map_err(profile_error)?;
                executed.push(ExecutedProfile {
                    target: rustdoc_target,
                    receipt,
                    spec: profile,
                });
            }
            continue;
        };
        let owner = api_owner_for_package(metadata, package.name.as_str()).ok();
        let wasm_binding = owner
            .as_ref()
            .is_some_and(|owner| owner.kind == ApiOwnerKind::WasmBinding);
        let execution_target = if wasm_binding {
            if !installed_targets.contains(wasm_target) {
                return Err(profile_error(
                    sdk_docs::rustdoc_profiles::ProfileError::UnsupportedTarget {
                        target: wasm_target.into(),
                    },
                ));
            }
            wasm_target
        } else {
            host.as_str()
        };
        let targets = BTreeSet::from([execution_target.to_owned()]);
        let mut profiles = sdk_docs::rustdoc_profiles::profiles_for_package(
            metadata,
            package.name.as_str(),
            execution_target,
            &targets,
        )
        .map_err(profile_error)?;
        if published.contains(package.name.as_ref()) && !wasm_binding {
            // Evaluate Cargo target predicates against actual compiler cfg for
            // both targets; native-only dependencies do not imply browser support.
            let browser_dependencies = package.dependencies.iter().any(|dependency| {
                dependency.kind == cargo_metadata::DependencyKind::Normal
                    && dependency.target.as_ref().is_some_and(|platform| {
                        platform.matches(wasm_target, &browser_cfg)
                            && !platform.matches(&host, &host_cfg)
                    })
            });
            if let Some(profile) = sdk_docs::rustdoc_profiles::browser_profile_for_package(
                package,
                browser_dependencies,
                &installed_targets,
            )
            .map_err(profile_error)?
            {
                profiles.push(profile);
            }
        }
        let mut compiled = BTreeMap::new();
        for profile in profiles {
            let targets = BTreeSet::from([profile.target.clone()]);
            let receipt = if profile.default_features
                && profile.features.is_empty()
                && profile.target == execution_target
            {
                rustdoc_dir.join(format!("{}.json", target.name.replace('-', "_")))
            } else {
                let prefix = if wasm_binding { "wasm-" } else { "" };
                rustdoc_dir
                    .join("profiles")
                    .join(profile_receipt_filename(&profile, prefix))
            };
            if let Some(parent) = receipt.parent() {
                fs::create_dir_all(parent).map_err(io_error)?;
            }
            let key = local_feature_configuration(&package.features, &profile);
            if let Some(previous) = key.as_ref().and_then(|key| compiled.get(key)) {
                fs::copy(previous, &receipt).map_err(io_error)?;
            } else {
                sdk_docs::rustdoc_profiles::execute_profile(
                    &manifest,
                    metadata,
                    &profile,
                    &targets,
                    &target_dir,
                    &receipt,
                    context,
                    &sdk_docs::rustdoc_profiles::RustdocTarget::Library,
                )
                .map_err(profile_error)?;
                if let Some(key) = key {
                    compiled.insert(key, receipt.clone());
                }
            }
            executed.push(ExecutedProfile {
                target: sdk_docs::rustdoc_profiles::RustdocTarget::Library,
                receipt,
                spec: profile,
            });
        }
    }
    Ok(executed)
}

// Reuse only within one package/target batch with unchanged compiler, manifest,
// environment and lock selection. Every local cfg feature, including `default`,
// belongs in the key. Dependency/weak/qualified expressions stay Cargo-owned:
// a graph containing any nonlocal edge disables this optimization entirely.
fn local_feature_configuration(
    features: &BTreeMap<String, Vec<String>>,
    profile: &ProfileSpec,
) -> Option<(String, String, BTreeSet<String>)> {
    if features
        .values()
        .flatten()
        .any(|feature| !features.contains_key(feature))
    {
        return None;
    }
    let mut pending = profile.features.iter().cloned().collect::<Vec<_>>();
    if profile.default_features && features.contains_key("default") {
        pending.push("default".into());
    }
    let mut enabled = BTreeSet::new();
    while let Some(feature) = pending.pop() {
        let children = features.get(&feature)?;
        if enabled.insert(feature) {
            pending.extend(children.iter().cloned());
        }
    }
    Some((profile.package.clone(), profile.target.clone(), enabled))
}

// Feature identities stay complete in receipt metadata, while physical path
// components have a fixed size on every supported filesystem.
pub(super) fn profile_receipt_filename(profile: &ProfileSpec, prefix: &str) -> String {
    format!(
        "{prefix}profile-{:x}.json",
        Sha256::digest(profile.id().0.as_bytes())
    )
}

pub(super) fn retain_profile_generated_sources(
    root: &Path,
    receipt: &Path,
) -> Result<(), CliError> {
    let value = sdk_docs::read_rustdoc_json(receipt).map_err(docs_error)?;
    let mut filenames = BTreeSet::new();
    collect_span_filenames(&value, &mut filenames);
    let retained_root = root.join("target/sdk-generation-generated-sources/retained");
    let index_path = retained_root.join("aliases.json");
    let mut aliases = if index_path.is_file() {
        serde_json::from_slice::<BTreeMap<String, String>>(
            &fs::read(&index_path).map_err(io_error)?,
        )
        .map_err(|error| CliError(format!("invalid retained Rustdoc source index: {error}")))?
    } else {
        BTreeMap::new()
    };
    for filename in filenames {
        let source = PathBuf::from(&filename);
        if !source.is_absolute() || source.starts_with(root) || !source.is_file() {
            continue;
        }
        let basename = source.file_name().and_then(OsStr::to_str).ok_or_else(|| {
            CliError(format!(
                "Rustdoc generated source has invalid filename {filename}"
            ))
        })?;
        let bytes = fs::read(&source).map_err(io_error)?;
        let digest = sha256_bytes(&bytes);
        let retained = retained_root.join(format!(
            "{}-{basename}",
            digest.trim_start_matches("sha256:")
        ));
        if let Some(parent) = retained.parent() {
            fs::create_dir_all(parent).map_err(io_error)?;
        }
        if !retained.is_file() {
            fs::write(&retained, bytes).map_err(io_error)?;
        }
        let relative = retained
            .strip_prefix(root)
            .map(path_string)
            .map_err(|_| CliError("retained generated source escaped repository root".into()))?;
        aliases.insert(filename, relative);
    }
    fs::create_dir_all(&retained_root).map_err(io_error)?;
    fs::write(
        &index_path,
        serde_json::to_vec(&aliases).map_err(|error| {
            CliError(format!(
                "cannot serialize retained Rustdoc source index: {error}"
            ))
        })?,
    )
    .map_err(io_error)?;
    Ok(())
}

pub(super) fn profile_generated_sources(
    sources: &[GeneratedSource],
    receipt: &Path,
) -> Result<Vec<GeneratedSource>, CliError> {
    let value = sdk_docs::read_rustdoc_json(receipt).map_err(docs_error)?;
    let mut filenames = BTreeSet::new();
    collect_span_filenames(&value, &mut filenames);
    Ok(crate::source_selection::referenced(
        sources,
        &filenames,
        cfg!(windows),
    ))
}

#[cfg(test)]
mod tests;
