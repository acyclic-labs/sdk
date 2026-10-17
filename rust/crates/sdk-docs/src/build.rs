use super::*;

/// Projects public library APIs and Cargo-owned binary guides.
pub fn build_data(
    input: &BuildInput,
    binaries: &[historical::BinaryInput],
) -> Result<DocsData, Error> {
    build_data_inner(input, None, binaries)
}

pub(super) fn build_data_inner(
    input: &BuildInput,
    historical: Option<&historical::Input>,
    binaries: &[historical::BinaryInput],
) -> Result<DocsData, Error> {
    if binaries
        .iter()
        .any(|binary| !input.rustdoc_files.contains(&binary.rustdoc_file))
    {
        return Err(Error::Invalid(
            "binary input is absent from Rustdoc inputs".into(),
        ));
    }
    if input.version.trim().is_empty() {
        return Err(Error::Invalid("version must not be empty".into()));
    }
    if input.version != input.version.trim() {
        return Err(Error::Invalid(
            "version must not contain surrounding whitespace".into(),
        ));
    }
    if input.channel == Channel::Release {
        if historical.is_some() {
            Version::parse(&input.version).map_err(|e| Error::Invalid(e.to_string()))?;
        } else {
            stable_version(&input.version)?;
        }
    }
    if input.revision.len() < 40
        || input.revision.len() > 64
        || !input.revision.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(Error::Invalid(
            "revision must be a 40- to 64-character hexadecimal source identity".into(),
        ));
    }
    if input.rustdoc_files.is_empty() {
        return Err(Error::Invalid(
            "at least one rustdoc JSON file is required".into(),
        ));
    }
    if input.package_metadata.len() != input.rustdoc_files.len() {
        return Err(Error::Invalid(format!(
            "package metadata count {} does not match rustdoc input count {}",
            input.package_metadata.len(),
            input.rustdoc_files.len()
        )));
    }
    if input.mark_latest && input.channel != Channel::Release {
        return Err(Error::Invalid("only a release can be marked latest".into()));
    }
    if !matches!(
        input.source_state.as_str(),
        "captured-snapshot" | "working-tree" | "release-tag"
    ) && !(input.source_state == "registry-archives"
        && historical.is_some_and(|scope| {
            matches!(
                &scope.captured_source,
                Some(historical::CapturedSource::RegistryArchives { .. })
            )
        }))
    {
        return Err(Error::Invalid(
            "source state must be captured-snapshot, working-tree, or release-tag".into(),
        ));
    }
    if let Some(source_sha256) = &input.source_sha256 {
        validate_source_digest(source_sha256)?;
    } else if input.source_state != "working-tree" {
        return Err(Error::Invalid(
            "captured snapshots and release tags require a trusted source manifest digest".into(),
        ));
    }
    let repository_root = canonical_repository_root(&input.repository_root)?;
    let generated_sources = attest_generated_sources(&repository_root, &input.generated_sources)?;
    // Callers may discover rustdoc files through different filesystem traversals.
    // Normalize the order before hashing or projecting so the same inputs always
    // produce the same identity and family order.
    let mut rustdoc_files = input.rustdoc_files.clone();
    rustdoc_files.sort_by_key(|path| normalize_path(path));
    if rustdoc_files.windows(2).any(|paths| paths[0] == paths[1]) {
        return Err(Error::Invalid(
            "rustdoc JSON input contains the same file more than once".into(),
        ));
    }
    let mut family_packages = Vec::new();
    let mut format_versions = std::collections::BTreeSet::new();
    let mut input_digest = ring::digest::Context::new(&ring::digest::SHA256);
    for path in &rustdoc_files {
        let metadata = package_metadata_for_path(&input.package_metadata, path)?;
        validate_package_metadata(metadata)?;
        input_digest.update(metadata.package_name.as_bytes());
        input_digest.update(&[0]);
        input_digest.update(metadata.crate_name.as_bytes());
        input_digest.update(&[0]);
        input_digest.update(metadata.version.as_bytes());
        input_digest.update(&[0]);
        let bytes = read_rustdoc(path)?;
        // Rustdoc embeds absolute source filenames in spans.  Those paths are
        // producer-local metadata, so normalize them before deriving the
        // input identity; the parsed receipt remains untouched for source
        // mapping and generated-source attestation.
        let digest_bytes = normalized_rustdoc_digest_bytes(&bytes, &repository_root)?;
        if let Some(name) = path.file_name() {
            input_digest.update(name.to_string_lossy().as_bytes());
        }
        input_digest.update(&[0]);
        input_digest.update(&digest_bytes);
        let krate = render_cache::decode(std::rc::Rc::clone(&bytes)).map_err(|e| {
            Error::Invalid(format!(
                "{} is not valid typed rustdoc JSON: {e}",
                path.display()
            ))
        })?;
        if krate.format_version != FORMAT_VERSION {
            return Err(Error::Invalid(format!(
                "{} uses unsupported rustdoc format {}; this generator accepts {}",
                path.display(),
                krate.format_version,
                FORMAT_VERSION
            )));
        }
        match krate.crate_version.as_deref() {
            Some(crate_version) if crate_version != metadata.version => {
                return Err(Error::Invalid(format!(
                    "{} reports crate version {crate_version}, but Cargo metadata reports {}",
                    path.display(),
                    metadata.version
                )));
            }
            None => {
                return Err(Error::Invalid(format!(
                    "{} does not declare crate version for Cargo package {}",
                    path.display(),
                    metadata.package_name
                )));
            }
            _ => {}
        }
        let binary = binaries.iter().find(|binary| binary.rustdoc_file == *path);
        if krate.includes_private && binary.is_none() {
            return Err(Error::Invalid(format!(
                "{} includes private rustdoc items; public-api requires normal public JSON",
                path.display()
            )));
        }
        format_versions.insert(krate.format_version);
        let family_root = if let Some(scope) = historical {
            historical::package_root(&repository_root, scope, metadata)?
        } else {
            repository_root.clone()
        };
        if historical.is_some_and(|scope| {
            matches!(
                &scope.captured_source,
                Some(historical::CapturedSource::RegistryArchives { .. })
            )
        }) {
            historical::validate_relative_package_spans(&family_root, &krate)?;
        }
        let rustdoc_source_root = if let Some(source_root) = &metadata.rustdoc_source_root {
            reject_reparse_ancestors(source_root)?;
            let source_root = source_root.canonicalize()?;
            let relative = source_root.strip_prefix(&family_root).map_err(|_| {
                Error::Invalid("Rustdoc owner source root escapes the captured source root".into())
            })?;
            input_digest.update(normalize_path(relative).as_bytes());
            input_digest.update(&[0]);
            source_root
        } else {
            family_root.clone()
        };
        let family = if let Some(binary) = binary {
            historical::binary_family(
                &family_root,
                &rustdoc_source_root,
                &generated_sources,
                &krate,
                binary,
            )?
        } else {
            build_family(
                &family_root,
                &rustdoc_source_root,
                &generated_sources,
                path,
                &bytes,
                &krate,
            )?
        };
        if family.crate_name != metadata.crate_name {
            return Err(Error::Invalid(format!(
                "{} has Rust crate {}, but Cargo metadata names {}",
                path.display(),
                family.crate_name,
                metadata.crate_name
            )));
        }
        family_packages.push((family, metadata.clone()));
    }
    let final_generated_sources =
        attest_generated_sources(&repository_root, &input.generated_sources)?;
    if final_generated_sources != generated_sources {
        return Err(Error::Invalid(
            "generated source changed while projecting rustdoc".into(),
        ));
    }
    family_packages.sort_by(|(left, _), (right, _)| left.slug.cmp(&right.slug));
    let families = family_packages
        .iter()
        .map(|(family, _)| family.clone())
        .collect::<Vec<_>>();
    if families.windows(2).any(|pair| pair[0].slug == pair[1].slug) {
        return Err(Error::Invalid(
            "duplicate crate family in rustdoc input".into(),
        ));
    }
    let navigation = Navigation {
        entries: navigation_entries(&families),
    };
    let packages = package_catalog(&family_packages);
    let search = search_index(&families);
    let data = DocsData {
        schema: DATA_SCHEMA_VERSION.into(),
        schema_version: DATA_SCHEMA_VERSION.into(),
        version: input.version.clone(),
        channel: input.channel.clone(),
        source: SourceInfo {
            captured_source: Some(historical::CapturedSource::Git {
                revision: input.revision.clone(),
            }),
            publication_status: PublicationStatus::Candidate,
            released_packages: Vec::new(),
            revision: input.revision.clone(),
            source_state: input.source_state.clone(),
            source_sha256: input.source_sha256.clone(),
            input_sha256: hex_digest(input_digest.finish().as_ref()),
            rustdoc_format_versions: format_versions.into_iter().collect(),
            generator: format!("sdk-docs/{GENERATOR_VERSION}"),
        },
        navigation,
        packages,
        search,
        families,
    };
    let mut data = data;
    if let Some(historical) = historical {
        if let Some(captured) = &historical.captured_source {
            data.source.captured_source = Some(captured.clone());
        }
        data.source.publication_status = PublicationStatus::RegistryReleased;
        data.source.released_packages = historical.released_packages.clone();
        historical::validate_release_identity(&data)?;
    }
    validate_source_info(&data.source, &input.channel)?;
    Ok(data)
}

/// Merge one feature/target Rustdoc projection into the existing family
/// catalog. Profile receipts are additional views of the same published
/// crate, so they contribute public items without creating a second family or
/// package entry. IDs are stable from the rendered identity, independent of
/// Rustdoc's per-receipt numeric IDs.
pub fn merge_profile_catalog(data: &mut DocsData, variant: DocsData) -> Result<(), Error> {
    if data.schema != variant.schema
        || data.schema_version != variant.schema_version
        || data.version != variant.version
        || data.channel != variant.channel
        || data.source.revision != variant.source.revision
        || data.source.captured_source != variant.source.captured_source
        || data.source.source_state != variant.source.source_state
        || data.source.source_sha256 != variant.source.source_sha256
        || data.source.generator != variant.source.generator
    {
        return Err(Error::Invalid(
            "profile catalog has a different docs/source identity".into(),
        ));
    }

    for variant_family in variant.families {
        let family = data
            .families
            .iter_mut()
            .find(|family| family.crate_name == variant_family.crate_name)
            .ok_or_else(|| {
                Error::Invalid(format!(
                    "profile family {} is absent from the generated docs catalog",
                    variant_family.crate_name
                ))
            })?;

        let mut existing = family
            .items
            .iter()
            .map(|item| {
                (
                    (item.path.clone(), item.kind.clone(), item.signature.clone()),
                    item.id.clone(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut id_map = BTreeMap::<String, String>::new();
        let mut pending: Vec<ApiItem> = Vec::new();
        for mut item in variant_family.items {
            let key = (item.path.clone(), item.kind.clone(), item.signature.clone());
            if let Some(existing_id) = existing.get(&key) {
                id_map.insert(item.id, existing_id.clone());
                continue;
            }

            let digest = sha256_hex(format!("{}\0{}\0{}", key.0, key.1, key.2).as_bytes());
            let mut id = format!("profile-{digest}");
            if family.items.iter().any(|candidate| candidate.id == id)
                || pending.iter().any(|candidate: &ApiItem| candidate.id == id)
            {
                id.push_str("-variant");
            }
            id_map.insert(item.id.clone(), id.clone());
            item.id = id;
            existing.insert(key, item.id.clone());
            pending.push(item);
        }
        for mut item in pending {
            item.parent_id = item
                .parent_id
                .map(|parent| id_map.get(&parent).cloned().unwrap_or(parent));
            family.items.push(item);
        }
        family.items.sort_by(|left, right| {
            left.path
                .cmp(&right.path)
                .then(left.kind.cmp(&right.kind))
                .then(left.signature.cmp(&right.signature))
                .then(left.id.cmp(&right.id))
        });

        let mut guide_paths = family
            .guides
            .iter()
            .map(|guide| guide.path.clone())
            .collect::<BTreeSet<String>>();
        for guide in variant_family.guides {
            if guide_paths.insert(guide.path.clone()) {
                family.guides.push(guide);
            }
        }
        family
            .guides
            .sort_by(|left, right| left.path.cmp(&right.path));
    }

    data.navigation = Navigation {
        entries: navigation_entries(&data.families),
    };
    data.search = search_index(&data.families);
    Ok(())
}
