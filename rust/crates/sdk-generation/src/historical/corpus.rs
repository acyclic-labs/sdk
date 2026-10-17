use super::*;

/// Generated historical corpus provenance. This is producer evidence, kept
/// separate from CapturedSource's exact compiled registry archive identity.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RegistryCorpus {
    pub repository: String,
    pub declared_anchors: BTreeSet<String>,
    pub anchors: BTreeSet<String>,
    pub original_release_authority: BTreeSet<String>,
    pub frontier: Vec<RegistryOwnerSource>,
    pub responses: BTreeMap<String, Option<String>>,
    pub publisher_anchors: BTreeMap<String, Option<PublisherVcs>>,
}

/// The caller declares immutable history anchors, never package names. Git
/// derives original release/workspace/package-manifest changes plus anchors.
pub(super) fn frontier_revisions(
    repository: &Path,
    anchors: &BTreeSet<String>,
) -> Result<BTreeSet<String>, String> {
    if anchors.is_empty()
        || anchors
            .iter()
            .any(|sha| sha.len() != 40 || !sha.bytes().all(|b| b.is_ascii_hexdigit()))
    {
        return Err("registry frontier needs immutable Git history anchors".into());
    }
    let git = frontier_git()?;
    let layout = command_output(
        sdk_docs::command(&git)
            .arg("--no-replace-objects")
            .current_dir(repository)
            .args([
                "rev-parse",
                "--is-shallow-repository",
                "--git-path",
                "info/grafts",
            ]),
    )?;
    let graft = repository.join(super::probe::graft_path(&layout)?);
    if graft.try_exists().map_err(|e| e.to_string())?
        || crate::command_var_os("GIT_GRAFT_FILE").is_some()
    {
        return Err("source frontier does not accept substituted Git ancestry".into());
    }
    let mut command = sdk_docs::command(git);
    command.arg("--no-replace-objects");
    command
        .current_dir(repository)
        .arg("log")
        .arg("--format=%H")
        .args(anchors)
        .args([
            "--",
            "release/cargo-crates.json",
            "Cargo.toml",
            ":(glob)**/Cargo.toml",
        ]);
    let revisions = String::from_utf8(command_output(&mut command)?).map_err(|e| e.to_string())?;
    Ok(anchors
        .iter()
        .cloned()
        .chain(revisions.lines().map(str::to_owned))
        .collect())
}

pub(super) fn decode_release_authority(bytes: &[u8]) -> Result<BTreeSet<String>, String> {
    let names: Vec<String> = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
    let authority: BTreeSet<_> = names.iter().cloned().collect();
    if authority.is_empty() || authority.len() != names.len() {
        return Err("original release authority is empty or duplicated".into());
    }
    for name in &authority {
        validate_name(name)?;
    }
    Ok(authority)
}

pub(super) fn original_sources(
    repository: &Path,
    frontier_root: &Path,
    revisions: &BTreeSet<String>,
    capture: bool,
) -> Result<(Vec<RegistryOwnerSource>, BTreeSet<String>), String> {
    let metadata = fs::symlink_metadata(frontier_root).map_err(|error| error.to_string())?;
    if is_linked(&metadata) || !metadata.is_dir() {
        return Err("registry frontier root is linked or non-directory".into());
    }
    let frontier_root = frontier_root
        .canonicalize()
        .map_err(|error| error.to_string())?;
    let mut sources = Vec::new();
    let mut authority = BTreeSet::new();
    for revision in revisions {
        let root = frontier_root.join(revision);
        validate_import_path(&frontier_root, &root)?;
        if capture {
            fs::create_dir_all(&root).map_err(|error| error.to_string())?;
        }
        let (source, original_authority) = registry_source(repository, &root, revision, capture)?;
        sources.push(source);
        authority.extend(original_authority);
    }
    if authority.is_empty() {
        return Err("declared frontier has no original release authority".into());
    }
    Ok((sources, authority))
}
pub fn capture_registry_corpus(
    repository: &Path,
    anchors: BTreeSet<String>,
    frontier_root: &Path,
    cargo: &Path,
) -> Result<RegistryCorpus, String> {
    let repository = repository.canonicalize().map_err(|e| e.to_string())?;
    // Before the first mkdir, anchor at the nearest existing nonlinked parent.
    // Use its physical spelling so Windows extended paths and /var aliases do
    // not weaken the existing no-follow importer guard.
    let mut existing = frontier_root;
    loop {
        match fs::symlink_metadata(existing) {
            Ok(metadata) => {
                if is_linked(&metadata) || !metadata.is_dir() {
                    return Err("registry frontier ancestor is linked or non-directory".into());
                }
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                existing = existing
                    .parent()
                    .ok_or("registry frontier has no owned ancestor")?;
            }
            Err(error) => return Err(error.to_string()),
        }
    }
    let physical = existing.canonicalize().map_err(|e| e.to_string())?;
    let destination = physical.join(
        frontier_root
            .strip_prefix(existing)
            .map_err(|e| e.to_string())?,
    );
    validate_import_path(&physical, &destination)?;
    let frontier_root = destination.as_path();
    fs::create_dir_all(frontier_root).map_err(|e| e.to_string())?;
    if is_linked(&fs::symlink_metadata(frontier_root).map_err(|e| e.to_string())?) {
        return Err("registry frontier root is linked".into());
    }
    let frontier_root = frontier_root.canonicalize().map_err(|e| e.to_string())?;
    let revisions = frontier_revisions(&repository, &anchors)?;
    let (sources, original_release_authority) =
        original_sources(&repository, &frontier_root, &revisions, true)?;
    let mut frontier = Vec::new();
    for source in sources {
        frontier.push(discover_owner(
            &frontier_root.join(&source.revision),
            cargo,
            source,
            Some(&original_release_authority),
        )?);
    }
    if frontier.is_empty() {
        return Err("registry source frontier is empty".into());
    }
    let mut responses = BTreeMap::new();
    for owner in frontier
        .iter()
        .flat_map(|source| &source.owners)
        .collect::<BTreeSet<_>>()
    {
        let bytes = registry_response(&format!("{owner}/versions"))?;
        let text = bytes
            .map(String::from_utf8)
            .transpose()
            .map_err(|e| e.to_string())?;
        if let Some(text) = &text {
            decode_registry_versions(owner, text.as_bytes())?;
        }
        responses.insert(owner.clone(), text);
    }
    Ok(RegistryCorpus {
        repository: crate::path_string(&repository),
        declared_anchors: anchors.clone(),
        anchors,
        original_release_authority,
        frontier,
        responses,
        publisher_anchors: BTreeMap::new(),
    })
}

pub(super) fn corpus_inventory(
    corpus: &RegistryCorpus,
) -> Result<BTreeMap<String, RegistryOwnerVersions>, String> {
    corpus
        .responses
        .iter()
        .map(|(owner, response)| {
            let versions = response
                .as_ref()
                .map(|text| decode_registry_versions(owner, text.as_bytes()))
                .transpose()?
                .unwrap_or_default();
            Ok((
                owner.clone(),
                RegistryOwnerVersions {
                    response_sha256: response
                        .as_ref()
                        .map(|text| crate::sha256_bytes(text.as_bytes())),
                    versions,
                },
            ))
        })
        .collect()
}

pub(super) fn cohort_publishers(
    inventory: &BTreeMap<String, RegistryOwnerVersions>,
    version: &str,
    archives: &Path,
) -> Result<BTreeMap<String, Option<PublisherVcs>>, String> {
    validate_registry_cohort(version, inventory, archives)?;
    inventory
        .values()
        .flat_map(|owner| &owner.versions)
        .filter(|release| release.num == version)
        .map(|release| {
            let name = format!("{}-{}.crate", release.package, release.num);
            let vcs = archive_vcs(&archive_members(release, &archives.join(&name))?)?;
            if vcs.as_ref().is_some_and(|vcs| {
                vcs.revision.len() != 40 || !vcs.revision.bytes().all(|b| b.is_ascii_hexdigit())
            }) {
                return Err("published archive has an invalid original Git anchor".into());
            }
            Ok((name, vcs))
        })
        .collect()
}

/// Publisher facts extend the declared scope; they never supply owner authority
/// or retag dirty/divergent archive bytes as clean Git inputs.
pub fn capture_registry_cohort(
    repository: &Path,
    mut anchors: BTreeSet<String>,
    frontier_root: &Path,
    cargo: &Path,
    version: &str,
    archives: &Path,
) -> Result<RegistryCorpus, String> {
    let declared_anchors = anchors.clone();
    loop {
        let mut corpus =
            capture_registry_corpus(repository, anchors.clone(), frontier_root, cargo)?;
        let publishers = cohort_publishers(&corpus_inventory(&corpus)?, version, archives)?;
        let expanded = publisher_anchor_union(&anchors, &publishers);
        if expanded != anchors {
            // Publisher commits commonly already occur in manifest history.
            // Reuse this capture only when the exact immutable revision set is
            // unchanged. Admission and drift still recheck every original export.
            let captured = corpus
                .frontier
                .iter()
                .map(|source| source.revision.clone())
                .collect();
            if frontier_revisions(repository, &expanded)? != captured {
                anchors = expanded;
                continue;
            }
            corpus.anchors = expanded;
        }
        corpus.declared_anchors = declared_anchors;
        corpus.publisher_anchors = publishers;
        return Ok(corpus);
    }
}

pub(super) fn publisher_anchor_union(
    anchors: &BTreeSet<String>,
    publishers: &BTreeMap<String, Option<PublisherVcs>>,
) -> BTreeSet<String> {
    anchors
        .iter()
        .cloned()
        .chain(
            publishers
                .values()
                .flatten()
                .map(|vcs| vcs.revision.clone()),
        )
        .collect()
}

/// Call before archive_plan and again from the existing drift admission gate.
/// The original Git exports/config context are rechecked through the same
/// source discovery function, not through a second source snapshot engine.
pub fn verify_registry_corpus(
    corpus: &RegistryCorpus,
    frontier_root: &Path,
    cargo: &Path,
    version: &str,
    archives: &Path,
) -> Result<BTreeMap<String, RegistryOwnerVersions>, String> {
    if corpus.frontier.is_empty() {
        return Err("registry source frontier is empty".into());
    }
    if corpus.declared_anchors.is_empty() {
        return Err("registry corpus has no declared original history anchors".into());
    }
    let repository = Path::new(&corpus.repository);
    let expected_revisions = frontier_revisions(repository, &corpus.anchors)?;
    let mut revisions = BTreeSet::new();
    for source in &corpus.frontier {
        if !revisions.insert(&source.revision) {
            return Err("duplicate source frontier revision".into());
        }
    }
    if revisions.into_iter().cloned().collect::<BTreeSet<_>>() != expected_revisions {
        return Err("registry corpus omits original release-authority history".into());
    }
    let (sources, authority) =
        original_sources(repository, frontier_root, &expected_revisions, false)?;
    if authority != corpus.original_release_authority {
        return Err("original frontier release authority changed".into());
    }
    let mut sources = sources
        .into_iter()
        .map(|source| (source.revision.clone(), source))
        .collect::<BTreeMap<_, _>>();
    let mut owners = BTreeSet::new();
    for source in &corpus.frontier {
        let current = discover_owner(
            &frontier_root.join(&source.revision),
            cargo,
            sources
                .remove(&source.revision)
                .ok_or("source frontier revision is absent")?,
            Some(&corpus.original_release_authority),
        )?;
        if current != *source {
            return Err("historical source frontier or discovery context changed".into());
        }
        owners.extend(source.owners.iter().cloned());
    }
    let expected_paths = corpus
        .frontier
        .iter()
        .flat_map(|source| {
            source
                .input_sha256
                .keys()
                .map(|path| format!("{}/{path}", source.revision))
        })
        .collect();
    validate_archive_inventory(frontier_root, &expected_paths)?;
    if owners != corpus.responses.keys().cloned().collect() {
        return Err("registry corpus does not exactly cover source-owned candidates".into());
    }
    let inventory = corpus_inventory(corpus)?;
    let publishers = cohort_publishers(&inventory, version, archives)?;
    if publishers != corpus.publisher_anchors
        || publisher_anchor_union(&corpus.declared_anchors, &publishers) != corpus.anchors
    {
        return Err("checksum-bound archive publisher frontier differs".into());
    }
    Ok(inventory)
}

/// Complete version responses retain yanked releases and prereleases. The raw
/// response digest belongs in the existing generation artifact/input attestation.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RegistryOwnerVersions {
    pub response_sha256: Option<String>,
    pub versions: Vec<RegistryVersion>,
}

pub(super) fn decode_registry_versions(
    package: &str,
    bytes: &[u8],
) -> Result<Vec<RegistryVersion>, String> {
    #[derive(Deserialize)]
    struct Response {
        versions: Vec<RegistryVersion>,
        meta: Meta,
    }
    #[derive(Deserialize)]
    struct Meta {
        total: usize,
        next_page: Option<String>,
    }
    let response: Response = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
    // The maintained endpoint is unpaginated when per_page is omitted. Detect
    // truncation/schema policy changes rather than treating a first page as complete.
    if response.meta.total != response.versions.len() || response.meta.next_page.is_some() {
        return Err("registry version inventory is incomplete".into());
    }
    let mut versions = BTreeMap::new();
    for version in response.versions {
        validate_version(&version.num)?;
        if version.package != package
            || version.checksum.len() != 64
            || !version.checksum.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err("registry inventory identity or checksum differs".into());
        }
        if versions.insert(version.num.clone(), version).is_some() {
            return Err("duplicate registry version identity".into());
        }
    }
    Ok(versions.into_values().collect())
}

/// Use BEFORE archive_plan/import/Cargo execution. Archive directory contents
/// are checked against registry evidence, never used to define expected owners.
pub fn validate_registry_cohort(
    version: &str,
    inventory: &BTreeMap<String, RegistryOwnerVersions>,
    archives: &Path,
) -> Result<(), String> {
    validate_version(version)?;
    let expected: BTreeSet<_> = inventory
        .values()
        .flat_map(|o| &o.versions)
        .filter(|v| v.num == version)
        .map(|v| format!("{}-{}.crate", v.package, v.num))
        .collect();
    if expected.is_empty() {
        return Err("requested cohort has no registry releases in captured source frontier".into());
    }
    validate_archive_inventory(archives, &expected)
}

/// Fetch an actual registry version, retaining yanked historical releases.
pub fn registry_version(package: &str, version: &str) -> Result<RegistryVersion, String> {
    validate_name(package)?;
    validate_version(version)?;
    registry_version_optional(package, version)?
        .ok_or("requested package version is absent from the registry".into())
}

pub(super) fn registry_version_optional(
    package: &str,
    version: &str,
) -> Result<Option<RegistryVersion>, String> {
    validate_name(package)?;
    validate_version(version)?;
    let Some(bytes) = registry_response(&format!("{package}/{version}"))? else {
        return Ok(None);
    };
    #[derive(Deserialize)]
    struct Response {
        version: RegistryVersion,
    }
    let response: Response = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
    if response.version.package != package || response.version.num != version {
        return Err("registry response identity differs from requested release".into());
    }
    Ok(Some(response.version))
}
