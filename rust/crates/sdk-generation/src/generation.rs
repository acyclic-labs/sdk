use super::*;

pub(super) fn generate(args: GenerateArgs, stdout: &mut dyn Write) -> Result<(), CliError> {
    let mut root = canonical(&args.root)?;
    let catalog_output = absolute(&args.output)?;
    let output = if args.historical_archives.is_some() {
        catalog_output
            .join("releases")
            .join(sdk_docs::safe_version(&args.version).map_err(docs_error)?)
    } else {
        catalog_output.clone()
    };
    if args.historical_archives.is_some() && args.historical_source == HistoricalSource::Git {
        let top = sdk_docs::command("git")
            .current_dir(&root)
            .args(["rev-parse", "--show-toplevel"])
            .run_output()
            .map_err(io_error)?;
        if !top.status.success()
            || canonical(Path::new(String::from_utf8_lossy(&top.stdout).trim()))? != root
        {
            return Err(CliError(
                "historical generation requires the exact immutable Git source root".into(),
            ));
        }
    }
    let rustdoc_dir = match (&args.rustdoc_dir, args.execute_profiles) {
        (Some(path), false) => canonical(path)?,
        (None, true) => output
            .join(".rustdoc")
            .canonicalize()
            .unwrap_or_else(|_| output.join(".rustdoc")),
        _ => return Err(CliError("invalid Rustdoc input mode".into())),
    };
    let cargo_path = args
        .execute_profiles
        .then(|| documentation_cargo(&root, args.historical_archives.is_some()))
        .transpose()?;
    let registry_corpus = args
        .registry_frontier_repository
        .as_deref()
        .map(|repository| {
            historical::capture_registry_cohort(
                repository,
                args.registry_frontier_revisions.clone(),
                &output.join("registry-frontier"),
                cargo_path
                    .as_deref()
                    .ok_or_else(|| CliError("registry frontier docs Cargo is absent".into()))?,
                &args.version,
                args.historical_archives
                    .as_deref()
                    .ok_or_else(|| CliError("registry frontier archives are absent".into()))?,
            )
            .map_err(CliError)
        })
        .transpose()?;
    let (metadata, historical_plan, revision) =
        if args.historical_source == HistoricalSource::RegistryArchives {
            let inventory = registry_corpus
                .as_ref()
                .map(|corpus| {
                    historical::verify_registry_corpus(
                        corpus,
                        &output.join("registry-frontier"),
                        cargo_path
                            .as_deref()
                            .ok_or("registry frontier docs Cargo is absent")?,
                        &args.version,
                        args.historical_archives
                            .as_deref()
                            .ok_or("registry archive mode lacks archives")?,
                    )
                })
                .transpose()
                .map_err(CliError)?;
            let (plan, index) = historical::archive_plan(
                &args.version,
                args.historical_archives
                    .as_deref()
                    .ok_or_else(|| CliError("registry archive mode lacks archives".into()))?,
                &output.join("sources"),
                cargo_path
                    .as_deref()
                    .ok_or_else(|| CliError("registry archive mode lacks docs Cargo".into()))?,
                inventory.as_ref(),
            )
            .map_err(CliError)?;
            root = canonical(&output.join("sources"))?;
            let revision = plan
                .captured_source
                .as_ref()
                .ok_or_else(|| CliError("archive captured identity is absent".into()))?
                .revision()
                .map_err(docs_error)?;
            (index, Some(plan), revision)
        } else {
            let metadata = load_metadata(&root, cargo_path.as_deref())?;
            let revision = git_revision(&root)?;
            let plan = args
                .historical_archives
                .as_ref()
                .map(|archives| {
                    historical::plan(&metadata, &root, &revision, &args.version, archives)
                        .map_err(CliError)
                })
                .transpose()?;
            (metadata, plan, revision)
        };
    let historical_plan = historical_plan.map(crate::parallel::Arc::new);
    if args.channel == Channel::Release && args.historical_archives.is_none() {
        let published = published_packages(&root)?;
        let versions = metadata
            .packages
            .iter()
            .filter(|package| published.iter().any(|name| name == package.name.as_ref()))
            .map(|package| package.version.to_string())
            .collect::<BTreeSet<_>>();
        validate_release_package_versions(&args.version, &versions)?;
    }
    let toolchain = cargo_path
        .as_deref()
        .map(toolchain_identity)
        .transpose()?
        .unwrap_or_default();
    let scope = historical_plan
        .as_ref()
        .map(|plan| {
            if let Some(captured_source) = &plan.captured_source {
                historical::Scope::RegistryArchives {
                    released_packages: plan.released_packages.clone(),
                    archive_source_paths: plan.source_files.keys().cloned().collect(),
                    captured_source: captured_source.clone(),
                    source_root: "sources".into(),
                }
            } else {
                historical::Scope::HistoricalRelease {
                    released_packages: plan.released_packages.clone(),
                    archive_source_paths: plan.source_files.keys().cloned().collect(),
                }
            }
        })
        .unwrap_or_default();
    let declared_source_toolchain = if scope.is_current() {
        String::new()
    } else if historical_plan
        .as_ref()
        .is_some_and(|plan| plan.captured_source.is_some())
    {
        declared_archive_toolchain(&metadata)?
    } else {
        fs::read_to_string(root.join("rust-toolchain.toml")).map_err(io_error)?
    };
    let source_files =
        historical::scope_source_file_hashes(&root, &output, &scope).map_err(CliError)?;
    if let Some(plan) = &historical_plan {
        if plan.captured_source.is_some() && source_files != plan.source_files {
            return Err(CliError(
                "initial imported source closure differs from verified archive inputs".into(),
            ));
        }
    }
    let source_sha256 = digest_map(&source_files);
    let source_state = if matches!(&scope, historical::Scope::RegistryArchives { .. }) {
        "registry-archives"
    } else if args.channel == Channel::Release {
        "captured-snapshot"
    } else {
        "working-tree"
    };
    if args.channel == Channel::Release && source_state != "registry-archives" && git_dirty(&root)?
    {
        return Err(CliError(
            "release generation requires a clean source checkout".into(),
        ));
    }
    if args.channel == Channel::Release && args.skip_scenarios {
        return Err(CliError(
            "release generation cannot skip the registered scenario source closure".into(),
        ));
    }
    if let Some(plan) = &historical_plan {
        for (archive, release) in &plan.archives {
            write_immutable(
                &output
                    .join("registry-archives")
                    .join(format!("{}-{}.crate", release.package, release.num)),
                &fs::read(archive).map_err(io_error)?,
            )?;
        }
        write_immutable(
            &output.join("sdk-docs-registry-releases.v1.json"),
            &json_bytes(
                &plan
                    .archives
                    .iter()
                    .map(|(_, release)| release)
                    .collect::<Vec<_>>(),
            )?,
        )?;
    }
    let executed_profiles = if let Some(plan) = &historical_plan {
        execute_historical_profiles(
            &root,
            &output,
            &rustdoc_dir,
            &metadata,
            &source_sha256,
            cargo_path
                .as_deref()
                .ok_or_else(|| CliError("historical scope has no pinned docs producer".into()))?,
            plan,
        )?
    } else if args.execute_profiles {
        execute_default_profiles(
            &root,
            &output,
            &rustdoc_dir,
            &metadata,
            &source_sha256,
            cargo_path
                .as_deref()
                .ok_or_else(|| CliError("profile execution has no pinned Cargo".into()))?,
        )?
    } else {
        Vec::new()
    };

    let receipts = rustdoc_files(&rustdoc_dir)?;
    if receipts.is_empty() {
        return Err(CliError(format!(
            "no .json Rustdoc receipts found in {}",
            rustdoc_dir.display()
        )));
    }
    let mut rustdoc_files = Vec::new();
    let mut package_metadata = Vec::new();
    let mut profile_items = Vec::new();
    let mut profiles = BTreeMap::new();
    let mut receipt_manifest = BTreeMap::new();
    let mut receipt_specs = BTreeMap::<String, ProfileSpec>::new();
    for receipt in &receipts {
        let observation =
            observe_profile_receipt(receipt, &executed_profiles).map_err(profile_error)?;
        let source_attestation = receipt_source_attestation(receipt)?;
        if source_attestation.as_deref() != Some(source_sha256.as_str()) {
            return Err(CliError(format!(
                "Rustdoc receipt {} is missing a matching source attestation (expected {})",
                receipt.display(),
                source_sha256
            )));
        }
        let package = package_for_profile(
            &metadata,
            receipt,
            &observation.crate_name,
            &executed_profiles,
        )?;
        validate_rustdoc_version(&metadata, package.name.as_ref(), &observation)
            .map_err(profile_error)?;
        let owner =
            api_owner_for_package(&metadata, package.name.as_ref()).map_err(profile_error)?;
        let profile_spec = executed_profiles
            .iter()
            .find(|executed| executed.receipt == *receipt)
            .map(|executed| executed.spec.clone())
            .unwrap_or(ProfileSpec {
                package: package.name.to_string(),
                target: observation.target.clone(),
                // A hand-supplied receipt has no producer profile metadata.
                // Keep its item coverage useful while refusing to claim that
                // Cargo default features were actually selected.
                default_features: false,
                features: BTreeSet::new(),
            });
        let profile_id = profile_spec.id();
        let crate_name = observation.crate_name.clone();
        if !is_binary_receipt(receipt, &executed_profiles) {
            let items = extract_owned_api(receipt, &owner, profile_id.clone(), &crate_name)
                .map_err(profile_error)?;
            profile_items.extend(items);
        }
        profiles.insert(profile_id.clone(), profile_spec.clone());
        if executed_profiles
            .iter()
            .any(|executed| executed.receipt == *receipt)
        {
            receipt_specs.insert(path_string(receipt), profile_spec.clone());
        }
        let receipt_key = rustdoc_key(&rustdoc_dir, receipt)?;
        receipt_manifest.insert(
            receipt_key,
            rustdoc_digest(receipt, &root).map_err(|error| CliError(error.to_string()))?,
        );
        rustdoc_files.push(receipt.clone());
        package_metadata.push(PackageMetadata {
            rustdoc_source_root: (historical_plan.is_none()
                && !metadata.workspace_members.contains(&package.id))
            .then(|| {
                package
                    .manifest_path
                    .parent()
                    .unwrap()
                    .to_path_buf()
                    .into_std_path_buf()
            }),
            rustdoc_file: receipt.clone(),
            package_name: package.name.to_string(),
            crate_name,
            version: package.version.to_string(),
        });
    }

    // Additional feature and target receipts are profile evidence, not
    // additional docs families. Merge their exact signatures into the
    // existing catalog below.
    for executed in executed_profiles
        .iter()
        .filter(|executed| !receipts.iter().any(|receipt| receipt == &executed.receipt))
    {
        let receipt = &executed.receipt;
        let observation =
            observe_profile_receipt(receipt, &executed_profiles).map_err(profile_error)?;
        if receipt_source_attestation(receipt)?.as_deref() != Some(source_sha256.as_str()) {
            return Err(CliError(format!(
                "WASM Rustdoc receipt {} has no matching source attestation",
                receipt.display()
            )));
        }
        let package = package_for_profile(
            &metadata,
            receipt,
            &observation.crate_name,
            &executed_profiles,
        )?;
        validate_rustdoc_version(&metadata, package.name.as_ref(), &observation)
            .map_err(profile_error)?;
        let owner =
            api_owner_for_package(&metadata, package.name.as_ref()).map_err(profile_error)?;
        let profile = executed.spec.clone();
        let profile_id = profile.id();
        profile_items.extend(
            extract_owned_api(receipt, &owner, profile_id.clone(), &observation.crate_name)
                .map_err(profile_error)?,
        );
        profiles.insert(profile_id, profile);
        receipt_specs.insert(path_string(receipt), executed.spec.clone());
        let receipt_key = rustdoc_key(&rustdoc_dir, receipt)?;
        receipt_manifest.insert(
            receipt_key,
            rustdoc_digest(receipt, &root).map_err(|error| CliError(error.to_string()))?,
        );
    }

    let published = historical_plan
        .as_ref()
        .map(|plan| {
            plan.released_packages
                .iter()
                .map(|release| release.package.clone())
                .collect::<Vec<_>>()
        })
        .map(Ok)
        .unwrap_or_else(|| published_packages(&root))?;
    let found = package_metadata
        .iter()
        .map(|metadata| metadata.package_name.clone())
        .collect::<BTreeSet<_>>();
    for owner in &published {
        if !found.contains(owner.as_str()) {
            return Err(CliError(format!(
                "published owner `{owner}` has no Rustdoc receipt"
            )));
        }
    }
    rustdoc_files.sort();
    package_metadata.sort_by(|left, right| left.package_name.cmp(&right.package_name));
    let mut generated_receipts = rustdoc_files.clone();
    if matches!(&scope, historical::Scope::RegistryArchives { .. }) {
        generated_receipts.extend(
            executed_profiles
                .iter()
                .map(|profile| profile.receipt.clone()),
        );
        generated_receipts.sort();
        generated_receipts.dedup();
    }
    let mut sources = materialize_generated_sources(&root, &generated_receipts)?;
    if matches!(&scope, historical::Scope::RegistryArchives { .. }) {
        sources.extend(source_files.iter().map(|(path, sha256)| GeneratedSource {
            physical_path: root.join(path),
            logical_path: PathBuf::from("sources").join(path),
            sha256: sha256.clone(),
        }));
    }
    let generated_sources = scope_generated_sources(sources, &catalog_output, &output)?;
    let input = BuildInput {
        version: args.version.clone(),
        channel: args.channel.clone(),
        revision: revision.clone(),
        source_state: source_state.into(),
        source_sha256: Some(source_sha256.clone()),
        repository_root: root.clone(),
        rustdoc_files,
        package_metadata,
        generated_sources,
        mark_latest: false,
    };
    let historical_input = historical_plan
        .as_ref()
        .map(|plan| {
            historical_docs_input(
                &metadata,
                &executed_profiles,
                &plan.released_packages,
                plan.captured_source.clone(),
            )
        })
        .transpose()?;
    let mut data = if let Some(scope) = &historical_input {
        sdk_docs::historical::build(&input, scope)
    } else {
        let native = historical_docs_input(&metadata, &executed_profiles, &[], None)?;
        sdk_docs::build_data(&input, &native.binaries)
    }
    .map_err(docs_error)?;
    let mut generated_source_artifacts = input.generated_sources.clone();
    if args.channel == Channel::Release && historical_plan.is_none() {
        validate_published_coverage(&published, &data)?;
    }
    // Feature and target receipts are additional views of the same crate
    // family. Build each through the maintained sdk-docs projection and union
    // its public items before availability is attached, so binding-only and
    // feature-gated APIs remain present in the actual docs catalog.
    for executed in executed_profiles
        .iter()
        .filter(|executed| !receipts.iter().any(|receipt| receipt == &executed.receipt))
    {
        let receipt = &executed.receipt;
        let observation =
            observe_profile_receipt(receipt, &executed_profiles).map_err(profile_error)?;
        let package = package_for_profile(
            &metadata,
            receipt,
            &observation.crate_name,
            &executed_profiles,
        )?;
        let variant_input = BuildInput {
            version: args.version.clone(),
            channel: args.channel.clone(),
            revision: revision.clone(),
            source_state: source_state.into(),
            source_sha256: Some(source_sha256.clone()),
            repository_root: root.clone(),
            rustdoc_files: vec![receipt.clone()],
            package_metadata: vec![PackageMetadata {
                rustdoc_source_root: (historical_plan.is_none()
                    && !metadata.workspace_members.contains(&package.id))
                .then(|| {
                    package
                        .manifest_path
                        .parent()
                        .unwrap()
                        .to_path_buf()
                        .into_std_path_buf()
                }),
                rustdoc_file: receipt.clone(),
                package_name: package.name.to_string(),
                crate_name: observation.crate_name,
                version: package.version.to_string(),
            }],
            generated_sources: if matches!(&scope, historical::Scope::RegistryArchives { .. }) {
                profile_generated_sources(&input.generated_sources, receipt)?
            } else {
                scope_generated_sources(
                    materialize_generated_sources(&root, std::slice::from_ref(receipt))?,
                    &catalog_output,
                    &output,
                )?
            },
            mark_latest: false,
        };
        let variant_data = if let Some(scope) = &historical_input {
            let scope = sdk_docs::historical::Input {
                captured_source: scope.captured_source.clone(),
                released_packages: scope
                    .released_packages
                    .iter()
                    .filter(|release| release.package == package.name.as_ref())
                    .cloned()
                    .collect(),
                binaries: Vec::new(),
            };
            sdk_docs::historical::build(&variant_input, &scope)
        } else {
            build_data(&variant_input, &[])
        }
        .map_err(docs_error)?;
        // Archive variants select a subset of the already retained base
        // inventory. Other scopes may materialize new physical/logical aliases.
        if !matches!(&scope, historical::Scope::RegistryArchives { .. }) {
            generated_source_artifacts.extend(variant_input.generated_sources);
        }
        merge_profile_catalog(&mut data, variant_data).map_err(docs_error)?;
    }
    let availability =
        project_into_docs(&data, &metadata, profile_items, &profiles).map_err(profile_error)?;
    fs::create_dir_all(&output).map_err(io_error)?;
    if historical_plan.is_some() {
        sdk_docs::historical::write_bundle(&data, &catalog_output, false)
    } else {
        write_bundle_files(&data, &catalog_output, input.mark_latest)
    }
    .map_err(docs_error)?;
    crate::source_selection::deduplicate(&mut generated_source_artifacts);
    for source in generated_source_artifacts {
        let bytes = fs::read(&source.physical_path).map_err(io_error)?;
        if sha256_bytes(&bytes).trim_start_matches("sha256:")
            != source.sha256.trim_start_matches("sha256:")
        {
            return Err(CliError(format!(
                "generated source changed before bundling: {}",
                source.physical_path.display()
            )));
        }
        write_immutable(&catalog_output.join(&source.logical_path), &bytes)?;
    }
    let profile_path = output.join("sdk-docs-profile-availability.v1.json");
    let profile_bytes = json_bytes(&availability)?;
    write_immutable(&profile_path, &profile_bytes)?;
    let receipts_path = output.join("sdk-docs-rustdoc-profiles.v1.json");
    let mut receipt_rows = Vec::new();
    for (path, digest) in &receipt_manifest {
        let receipt = rustdoc_dir.join(path.strip_prefix("rustdoc/").unwrap_or(path));
        let observation =
            observe_profile_receipt(&receipt, &executed_profiles).map_err(profile_error)?;
        let package = package_for_profile(
            &metadata,
            &receipt,
            &observation.crate_name,
            &executed_profiles,
        )?;
        let owner =
            api_owner_for_package(&metadata, package.name.as_ref()).map_err(profile_error)?;
        let spec = receipt_specs
            .get(&path_string(&receipt))
            .cloned()
            .unwrap_or(ProfileSpec {
                package: package.name.to_string(),
                target: observation.target.clone(),
                default_features: true,
                features: BTreeSet::new(),
            });
        receipt_rows.push(ProfileReceipt {
            rustdoc_target: executed_profiles
                .iter()
                .find(|executed| executed.receipt == receipt)
                .filter(|executed| {
                    historical_plan.is_some()
                        || matches!(
                            executed.target,
                            sdk_docs::rustdoc_profiles::RustdocTarget::Binary(_)
                        )
                })
                .map(|executed| executed.target.clone()),
            package: package.name.to_string(),
            published_owner: owner.published_package,
            owner_kind: format!("{:?}", owner.kind),
            target: observation.target,
            default_features: spec.default_features,
            features: spec.features.iter().cloned().collect(),
            profile: spec.id(),
            rustdoc_file: path.clone(),
            rustdoc_sha256: digest.clone(),
            rustdoc_version: observation.crate_version,
            rustdoc_profiles_covered: receipt_specs.contains_key(&path_string(&receipt)),
            installed_runtime_qualified: false,
        });
    }
    write_immutable(&receipts_path, &json_bytes(&receipt_rows)?)?;
    let scenario_path = output.join("sdk-docs-scenarios.v1.json");
    let mut scenario_artifacts = Vec::new();
    let scenario_sources = if let Some(plan) = &historical_plan {
        let owners = plan
            .released_packages
            .iter()
            .map(|release| release.package.clone())
            .collect::<BTreeSet<_>>();
        let examples = historical::examples(&metadata, &root, &owners).map_err(CliError)?;
        let executions = historical::execute_examples(
            &root,
            cargo_path
                .as_deref()
                .ok_or_else(|| CliError("historical examples lack pinned docs Cargo".into()))?,
            &output.join(".profile-build"),
            &examples,
            if plan.owner_metadata.is_empty() {
                None
            } else {
                Some(&plan.owner_metadata)
            },
            plan.captured_source.as_ref(),
        )
        .map_err(CliError)?;
        write_immutable(&scenario_path, &json_bytes(&examples)?)?;
        write_immutable(
            &output.join("sdk-docs-scenario-executions.v1.json"),
            &json_bytes(&executions)?,
        )?;
        None
    } else if !args.skip_scenarios {
        let sources = scenarios::validate(&root, &metadata).map_err(scenario_error)?;
        scenarios::compile_all(&root, &sources, None).map_err(scenario_error)?;
        let executions =
            scenarios::execute_all(&root, &sources, None, None).map_err(scenario_error)?;
        let snippets = scenarios::render_typescript(&executions).map_err(scenario_error)?;
        write_immutable(&scenario_path, &json_bytes(&scenarios::catalog(&sources))?)?;
        let executions_path = output.join("sdk-docs-scenario-executions.v1.json");
        write_immutable(
            &executions_path,
            &json_bytes(&scenarios::execution_catalog(&executions))?,
        )?;
        let projections = scenarios::projection_catalog(&snippets, |snippet| {
            sha256_bytes(snippet.source.as_bytes())
        });
        let projections_path = output.join("sdk-docs-scenario-projections.v1.json");
        write_immutable(&projections_path, &json_bytes(&projections)?)?;
        for snippet in snippets {
            let path = output.join(snippet.path);
            write_immutable(&path, snippet.source.as_bytes())?;
            scenario_artifacts.push(path);
        }
        Some(sources)
    } else {
        write_immutable(
            &scenario_path,
            br#"{"schema":"acyclic.sdk.scenarios.v1","scenarios":[],"skipped":true}"#,
        )?;
        None
    };
    if historical::scope_source_file_hashes(&root, &output, &scope).map_err(CliError)?
        != source_files
    {
        return Err(CliError(
            "source closure changed during qualification before manifest admission".into(),
        ));
    }
    let pre_manifest_artifacts = artifact_hashes_with_scope(&output, &scope)?;
    let release_manifest = release_manifest::build(
        &root,
        &output,
        release_manifest::SourceIdentity {
            version: &args.version,
            channel: &args.channel,
            revision: &revision,
            source_state,
            source_sha256: &source_sha256,
            source_files: &source_files,
        },
        &metadata,
        scenario_sources.as_deref(),
        &pre_manifest_artifacts,
        &scenario_artifacts,
    )
    .map_err(|error| CliError(format!("release manifest error: {error}")))?;
    let release_manifest_path = release_manifest::output_path(
        &catalog_output,
        &args.channel,
        &data.source.publication_status,
        &args.version,
    )
    .map_err(|error| CliError(format!("release manifest path error: {error}")))?;
    let release_manifest_bytes = json_bytes(&release_manifest)?;
    write_immutable(&release_manifest_path, &release_manifest_bytes)?;
    write_immutable(
        &release_manifest_path.with_extension("sha256"),
        sha256_bytes(&release_manifest_bytes).as_bytes(),
    )?;
    let artifacts = artifact_hashes_with_scope(&output, &scope)?;
    let manifest = GenerationManifest {
        scope,
        registry_corpus,
        declared_source_toolchain,
        schema: "sdk-generation-manifest.v1".into(),
        generator: "sdk-generation/0.1.0".into(),
        rustdoc_tool: "rustdoc-types/0.60.0".into(),
        cargo_metadata: "cargo_metadata/0.23.1".into(),
        toolchain,
        version: args.version,
        channel: args.channel,
        revision,
        source_state: source_state.into(),
        source_sha256,
        source_files,
        rustdoc_root: "rustdoc".into(),
        rustdoc_files: receipt_manifest,
        profile_availability: path_string(
            profile_path.strip_prefix(&output).unwrap_or(&profile_path),
        ),
        scenarios: path_string(
            scenario_path
                .strip_prefix(&output)
                .unwrap_or(&scenario_path),
        ),
        release_manifest: path_string(
            release_manifest_path
                .strip_prefix(&output)
                .unwrap_or(&release_manifest_path),
        ),
        artifacts,
    };
    let manifest_path = output.join("generation-manifest.v1.json");
    let manifest_bytes = json_bytes(&manifest)?;
    write_immutable(&manifest_path, &manifest_bytes)?;
    write_immutable(
        &output.join("generation-manifest.v1.sha256"),
        sha256_bytes(&manifest_bytes).as_bytes(),
    )?;
    // Re-read the source closure, revision, receipts and all immutable artifacts
    // after execution. Failed qualification leaves files unadmitted.
    drift((root.clone(), output.clone(), rustdoc_dir.clone()), stdout)?;
    if historical_plan.is_some() {
        sdk_docs::historical::write_bundle(&data, &catalog_output, true)
    } else {
        write_bundle(&data, &catalog_output, input.mark_latest)
    }
    .map_err(docs_error)?;
    writeln!(stdout, "generated {}", output.display()).map_err(io_error)?;
    Ok(())
}
