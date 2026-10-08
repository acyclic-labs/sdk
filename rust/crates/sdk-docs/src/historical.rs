//! Historical release identities and binary-only native documentation.
use super::*;

/// Registry facts verified against an actual released source archive.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReleasedPackage {
    pub package: String,
    pub version: String,
    pub yanked: bool,
    pub registry_checksum: String,
    pub source_revision: String,
    pub path_in_vcs: String,
}

/// Publisher VCS facts embedded in the exact released archive, independent of
/// the imported source actually compiled by the documentation producer.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PublisherVcs {
    pub revision: String,
    pub dirty: bool,
    pub path_in_vcs: String,
}

/// An immutable registry input and the lock actually used for its Cargo run.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ArchiveSource {
    pub package: String,
    pub version: String,
    pub registry_checksum: String,
    pub publisher_vcs: Option<PublisherVcs>,
    pub resolution_lock: ResolutionLock,
}

/// Published locks and newly generated documentation resolutions remain distinct.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum ResolutionLock {
    Published { sha256: String },
    DocsProducer { sha256: String },
}

/// Authority of the captured source; archive identities never impersonate Git.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum CapturedSource {
    Git { revision: String },
    RegistryArchives { archives: Vec<ArchiveSource> },
}

impl CapturedSource {
    pub fn revision(&self) -> Result<String, Error> {
        match self {
            Self::Git { revision } => Ok(revision.clone()),
            Self::RegistryArchives { archives } => {
                if archives.is_empty()
                    || archives.windows(2).any(|rows| {
                        (&rows[0].package, &rows[0].version) >= (&rows[1].package, &rows[1].version)
                    })
                {
                    return Err(Error::Invalid(
                        "archive source identities must be nonempty, unique and sorted".into(),
                    ));
                }
                for archive in archives {
                    if archive.package.is_empty() {
                        return Err(Error::Invalid("archive source package is empty".into()));
                    }
                    Version::parse(&archive.version).map_err(|e| Error::Invalid(e.to_string()))?;
                    validate_source_digest(&format!("sha256:{}", archive.registry_checksum))?;
                    let sha256 = match &archive.resolution_lock {
                        ResolutionLock::Published { sha256 }
                        | ResolutionLock::DocsProducer { sha256 } => sha256,
                    };
                    validate_source_digest(sha256)?;
                    if let Some(vcs) = &archive.publisher_vcs {
                        if vcs.revision.len() != 40
                            || !vcs.revision.bytes().all(|byte| byte.is_ascii_hexdigit())
                        {
                            return Err(Error::Invalid(
                                "publisher VCS revision must be an original SHA1".into(),
                            ));
                        }
                        let path = Path::new(&vcs.path_in_vcs);
                        if path.is_absolute()
                            || path.components().any(|part| {
                                matches!(part, Component::ParentDir | Component::Prefix(_))
                            })
                        {
                            return Err(Error::Invalid(
                                "publisher VCS path escapes its source".into(),
                            ));
                        }
                    }
                }
                Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(self)?)))
            }
        }
    }
}

/// Cargo-derived binary target, whose private implementation is never API data.
#[derive(Clone, Debug)]
pub struct BinaryInput {
    pub rustdoc_file: PathBuf,
    pub source_file: PathBuf,
    pub readme: Option<PathBuf>,
}

/// Explicit historical scope; normal [`build_data`] remains public/stable-only.
#[derive(Clone, Debug)]
pub struct Input {
    pub released_packages: Vec<ReleasedPackage>,
    pub binaries: Vec<BinaryInput>,
    pub captured_source: Option<CapturedSource>,
}

/// Project native historical receipts with separately verified release facts.
pub fn build(input: &BuildInput, scope: &Input) -> Result<DocsData, Error> {
    if scope
        .binaries
        .iter()
        .any(|binary| !input.rustdoc_files.contains(&binary.rustdoc_file))
    {
        return Err(Error::Invalid(
            "binary input is absent from Rustdoc inputs".into(),
        ));
    }
    build_data_inner(input, Some(scope))
}

/// Write immutable files; publish only after the producer's full drift check.
pub fn write_bundle(data: &DocsData, output: &Path, publish: bool) -> Result<(), Error> {
    let stable = Version::parse(&data.version)
        .map_err(|e| Error::Invalid(e.to_string()))?
        .pre
        .is_empty()
        && !data
            .source
            .released_packages
            .iter()
            .any(|package| package.yanked);
    write_bundle_inner(data, output, stable, publish, true)
}

pub(super) fn validate_release_identity(data: &DocsData) -> Result<(), Error> {
    if data.channel != Channel::Release
        || data.source.publication_status != PublicationStatus::RegistryReleased
        || data.source.released_packages.is_empty()
    {
        return Err(Error::Invalid(
            "historical data requires verified released package identities".into(),
        ));
    }
    let version = Version::parse(&data.version).map_err(|e| Error::Invalid(e.to_string()))?;
    if !version.build.is_empty() {
        return Err(Error::Invalid(
            "released package version cannot contain build metadata".into(),
        ));
    }
    let mut names = HashSet::new();
    for release in &data.source.released_packages {
        let source_matches = match &data.source.captured_source {
            Some(CapturedSource::RegistryArchives { archives }) => archives.iter().any(|archive| {
                archive.package == release.package
                    && archive.version == release.version
                    && archive.registry_checksum == release.registry_checksum
                    && archive
                        .publisher_vcs
                        .as_ref()
                        .map(|vcs| (&vcs.revision, &vcs.path_in_vcs))
                        == if release.source_revision.is_empty() {
                            None
                        } else {
                            Some((&release.source_revision, &release.path_in_vcs))
                        }
            }),
            _ => release.source_revision == data.source.revision,
        };
        if !names.insert(&release.package) || !source_matches || release.version != data.version {
            return Err(Error::Invalid(
                "historical package revision/version/identity differs from native data".into(),
            ));
        }
        validate_source_digest(&format!("sha256:{}", release.registry_checksum))?;
        let path = Path::new(&release.path_in_vcs);
        if release.package.is_empty()
            || path.is_absolute()
            || path
                .components()
                .any(|part| matches!(part, Component::ParentDir | Component::Prefix(_)))
        {
            return Err(Error::Invalid(
                "historical package source path or name is invalid".into(),
            ));
        }
        if !data.packages.entries.iter().any(|package| {
            package.package_name == release.package && package.version == release.version
        }) {
            return Err(Error::Invalid(
                "released package has no matching native Cargo docs".into(),
            ));
        }
    }
    if data
        .packages
        .entries
        .iter()
        .any(|package| !names.contains(&package.package_name))
    {
        return Err(Error::Invalid(
            "historical docs package has no verified released archive".into(),
        ));
    }
    Ok(())
}

pub(super) fn binary_family(
    root: &Path,
    generated_sources: &HashMap<PathBuf, GeneratedSource>,
    krate: &Crate,
    binary: &BinaryInput,
) -> Result<Family, Error> {
    let item = krate
        .index
        .get(&krate.root)
        .ok_or_else(|| Error::Invalid("binary Rustdoc root is missing".into()))?;
    if !matches!(&item.inner, ItemEnum::Module(module) if module.is_crate) {
        return Err(Error::Invalid(
            "binary Rustdoc root must be a crate module".into(),
        ));
    }
    let name = item
        .name
        .clone()
        .ok_or_else(|| Error::Invalid("binary Rustdoc root name is missing".into()))?;
    reject_reparse_ancestors(&binary.source_file)?;
    let source = binary.source_file.canonicalize()?;
    let relative_source = source
        .strip_prefix(root)
        .map_err(|_| Error::Invalid("binary Cargo source escapes repository root".into()))?;
    let span = item
        .span
        .as_ref()
        .ok_or_else(|| Error::Invalid("binary Rustdoc root source span is missing".into()))?;
    let original = source_span_at_root(root, &HashMap::new(), span)?;
    if original.path != normalize_path(relative_source) {
        return Err(Error::Invalid(
            "binary Rustdoc span differs from Cargo target source".into(),
        ));
    }
    let projected = source_span_at_root(root, generated_sources, span)?;
    let mut guides = Vec::new();
    if let Some(markdown) = item.docs.as_ref().filter(|value| !value.trim().is_empty()) {
        guides.push(Guide {
            path: projected.path,
            title: titleize(&name),
            markdown: markdown.clone(),
            links: BTreeMap::new(),
        });
    }
    if let Some(readme) = &binary.readme {
        reject_reparse_ancestors(readme)?;
        let readme = readme.canonicalize()?;
        let path = readme
            .strip_prefix(root)
            .map_err(|_| Error::Invalid("binary README escapes repository root".into()))?;
        let markdown = fs::read_to_string(&readme)?;
        if !markdown.trim().is_empty()
            && !guides
                .iter()
                .any(|guide| guide.markdown.trim() == markdown.trim())
        {
            guides.push(Guide {
                path: generated_sources
                    .get(&readme)
                    .map(|source| normalize_path(&source.logical_path))
                    .unwrap_or_else(|| normalize_path(path)),
                title: guide_title(&markdown, &name),
                markdown,
                links: BTreeMap::new(),
            });
        }
    }
    if guides.is_empty() {
        return Err(Error::Invalid(
            "binary target has no source-owned comments or guides".into(),
        ));
    }
    Ok(Family {
        slug: slugify(&name),
        title: titleize(&name),
        crate_name: name,
        items: Vec::new(),
        guides,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(root: &Path, version: &str) -> (BuildInput, Input) {
        fs::create_dir_all(root.join("cli/src")).unwrap();
        let source = root.join("cli/src/main.rs");
        fs::write(&source, "//! Source-owned CLI guide.\nfn main() {}\n").unwrap();
        let readme = root.join("cli/README.md");
        fs::write(&readme, "# CLI\n\ncargo install --locked --path cli\n").unwrap();
        let receipt = root.join("cli.json");
        let item = |id, name, docs, visibility, inner| {
            serde_json::json!({
                "id":id,"crate_id":0,"name":name,"span": {"filename":"cli/src/main.rs","begin":[1,0],"end":[2,12]},
                "visibility":visibility,"docs":docs,"links":{},"attrs":[],"deprecation":null,"stability":null,"const_stability":null,"inner":inner
            })
        };
        fs::write(&receipt, serde_json::to_vec(&serde_json::json!({
            "root":0,"crate_version":version,"includes_private":true,"format_version":60,
            "index":{"0":item(0,"cli","Source-owned CLI guide.","public",serde_json::json!({"module":{"is_crate":true,"items":[1],"is_stripped":false}})),
                "1":item(1,"hidden","PRIVATE CLI INTERNALS","default",serde_json::json!({"module":{"is_crate":false,"items":[],"is_stripped":false}}))},
            "paths":{},"external_crates":{},"target":{"triple":"x86_64-pc-windows-msvc","target_features":[]}
        })).unwrap()).unwrap();
        let input = BuildInput {
            version: version.into(),
            channel: Channel::Release,
            revision: "a".repeat(40),
            source_state: "captured-snapshot".into(),
            source_sha256: Some(format!("sha256:{}", "b".repeat(64))),
            repository_root: root.into(),
            rustdoc_files: vec![receipt.clone()],
            package_metadata: vec![PackageMetadata {
                rustdoc_file: receipt.clone(),
                package_name: "demo-cli".into(),
                crate_name: "cli".into(),
                version: version.into(),
            }],
            generated_sources: Vec::new(),
            mark_latest: false,
        };
        let scope = Input {
            captured_source: None,
            released_packages: vec![ReleasedPackage {
                package: "demo-cli".into(),
                version: version.into(),
                yanked: false,
                registry_checksum: "c".repeat(64),
                source_revision: input.revision.clone(),
                path_in_vcs: "cli".into(),
            }],
            binaries: vec![BinaryInput {
                rustdoc_file: receipt,
                source_file: source,
                readme: Some(readme),
            }],
        };
        (input, scope)
    }

    #[test]
    fn binary_guides_preserve_native_source_without_exposing_private_items() {
        let root = tempfile::tempdir().unwrap();
        let (input, scope) = fixture(root.path(), "0.1.0");
        assert!(build_data(&input)
            .unwrap_err()
            .to_string()
            .contains("private rustdoc"));
        let data = build(&input, &scope).unwrap();
        assert!(data.families[0].items.is_empty());
        assert_eq!(data.families[0].guides.len(), 2);
        let bytes = serde_json::to_string(&data).unwrap();
        assert!(bytes.contains("cargo install --locked --path cli"));
        assert!(!bytes.contains("PRIVATE CLI INTERNALS"));
        let mut unverified = data.clone();
        let mut extra = unverified.packages.entries[0].clone();
        extra.package_name = "unreleased-extra".into();
        unverified.packages.entries.push(extra);
        assert!(validate_release_identity(&unverified)
            .unwrap_err()
            .to_string()
            .contains("no verified released archive"));
        let mut escaped = scope.clone();
        escaped.binaries[0].source_file = root.path().join("cli/README.md");
        assert!(build(&input, &escaped)
            .unwrap_err()
            .to_string()
            .contains("span differs"));
        let mut wrong_identity = scope;
        wrong_identity.released_packages[0].source_revision = "d".repeat(40);
        assert!(build(&input, &wrong_identity).is_err());
    }

    #[test]
    fn archive_capture_keeps_dirty_publisher_vcs_separate_and_binds_catalog_identity() {
        let root = tempfile::tempdir().unwrap();
        let (mut input, mut scope) = fixture(root.path(), "0.1.0");
        let publisher = "f".repeat(40);
        let captured = CapturedSource::RegistryArchives {
            archives: vec![ArchiveSource {
                package: "demo-cli".into(),
                version: "0.1.0".into(),
                registry_checksum: "c".repeat(64),
                publisher_vcs: Some(PublisherVcs {
                    revision: publisher.clone(),
                    dirty: true,
                    path_in_vcs: "cli".into(),
                }),
                resolution_lock: ResolutionLock::Published {
                    sha256: format!("sha256:{}", "d".repeat(64)),
                },
            }],
        };
        input.revision = captured.revision().unwrap();
        input.source_state = "registry-archives".into();
        scope.released_packages[0].source_revision = publisher.clone();
        scope.captured_source = Some(captured.clone());
        let data = build(&input, &scope).unwrap();
        assert_ne!(data.source.revision, publisher);
        assert_eq!(data.source.captured_source.as_ref(), Some(&captured));
        let mut false_git = data.clone();
        false_git.source.captured_source = Some(CapturedSource::Git {
            revision: publisher,
        });
        assert!(validate_source_info(&false_git.source, &false_git.channel).is_err());
        let mut false_archive = data.clone();
        false_archive.source.revision = "a".repeat(64);
        assert!(validate_source_info(&false_archive.source, &false_archive.channel).is_err());
        let output = tempfile::tempdir().unwrap();
        write_bundle(&data, output.path(), true).unwrap();
        let mut index = load_version_index(output.path()).unwrap();
        index.releases[0].captured_source = Some(CapturedSource::Git {
            revision: data.source.revision.clone(),
        });
        index.latest = Some(index.releases[0].clone());
        assert!(validate_version_index(&index, output.path())
            .unwrap_err()
            .to_string()
            .contains("captured source differs"));
    }

    #[test]
    fn include_str_readme_keeps_original_root_guide_without_duplicate() {
        let root = tempfile::tempdir().unwrap();
        let (input, scope) = fixture(root.path(), "0.1.0");
        let readme = fs::read_to_string(scope.binaries[0].readme.as_ref().unwrap()).unwrap();
        let receipt = &input.rustdoc_files[0];
        let mut native: serde_json::Value =
            serde_json::from_slice(&fs::read(receipt).unwrap()).unwrap();
        native["index"]["0"]["docs"] = readme.clone().into();
        fs::write(receipt, serde_json::to_vec(&native).unwrap()).unwrap();
        let data = build(&input, &scope).unwrap();
        assert_eq!(data.families[0].guides.len(), 1);
        assert_eq!(data.families[0].guides[0].path, "cli/src/main.rs");
        assert_eq!(data.families[0].guides[0].markdown, readme);
    }

    #[test]
    fn immutable_yanked_prereleases_never_replace_latest_stable_and_bind_index_metadata() {
        let root = tempfile::tempdir().unwrap();
        let output = tempfile::tempdir().unwrap();
        for version in ["0.1.5", "1.0.0-rc.3", "0.1.1"] {
            let (input, mut scope) = fixture(root.path(), version);
            if version.contains('-') {
                scope.released_packages[0].yanked = true;
            }
            if version.contains('-') {
                assert!(build_data(&input).is_err());
            }
            let data = build(&input, &scope).unwrap();
            write_bundle(&data, output.path(), false).unwrap();
            let unadmitted = load_version_index(output.path()).unwrap();
            assert!(!unadmitted
                .releases
                .iter()
                .chain(&unadmitted.historical_prereleases)
                .any(|entry| entry.version == version));
            write_bundle(&data, output.path(), true).unwrap();
            if version.contains('-') {
                assert!(super::super::write_bundle(&data, output.path(), false).is_err());
                let mut changed = data.clone();
                changed.source.released_packages[0].yanked = false;
                assert!(write_bundle(&changed, output.path(), true).is_err());
            }
        }
        let mut index = load_version_index(output.path()).unwrap();
        assert_eq!(index.latest.as_ref().unwrap().version, "0.1.5");
        assert_eq!(index.historical_prereleases.len(), 1);
        assert!(index.historical_prereleases[0].released_packages[0].yanked);
        index.historical_prereleases[0].released_packages[0].yanked = false;
        fs::write(
            output.path().join("sdk-docs-versions.v1.json"),
            serde_json::to_vec(&index).unwrap(),
        )
        .unwrap();
        assert!(load_version_index(output.path())
            .unwrap_err()
            .to_string()
            .contains("immutable data"));
    }
    #[test]
    fn qualified_candidates_are_explicit_and_cannot_replace_actual_registry_latest() {
        let root = tempfile::tempdir().unwrap();
        let output = tempfile::tempdir().unwrap();
        let (input, scope) = fixture(root.path(), "0.1.5");
        let released = build(&input, &scope).unwrap();
        write_bundle(&released, output.path(), true).unwrap();
        let mut candidate = released.clone();
        candidate.version = "0.2.0".into();
        candidate.source.publication_status = PublicationStatus::Candidate;
        candidate.source.released_packages.clear();
        let before = fs::read(output.path().join("sdk-docs-versions.v1.json")).unwrap();
        assert!(super::super::write_bundle(&candidate, output.path(), true)
            .unwrap_err()
            .to_string()
            .contains("candidate cannot be marked latest"));
        assert_eq!(
            before,
            fs::read(output.path().join("sdk-docs-versions.v1.json")).unwrap()
        );
        super::super::write_bundle(&candidate, output.path(), false).unwrap();
        let index = load_version_index(output.path()).unwrap();
        assert_eq!(index.latest.as_ref().unwrap().version, "0.1.5");
        assert_eq!(index.release_candidates.len(), 1);
        assert_eq!(
            index.release_candidates[0].publication_status,
            PublicationStatus::Candidate
        );
        assert_eq!(
            index.release_candidates[0].data_file,
            "release-candidates/0.2.0/sdk-docs-data.v2.json"
        );
        assert!(
            fs::read_to_string(output.path().join(&index.release_candidates[0].data_file))
                .unwrap()
                .contains("\"publicationStatus\": \"candidate\"")
        );
    }
    #[test]
    fn yanked_stable_is_selectable_immutable_and_never_latest() {
        let root = tempfile::tempdir().unwrap();
        let output = tempfile::tempdir().unwrap();
        for (version, yanked, latest) in [
            ("0.1.0", false, "0.1.0"),
            ("0.1.1", true, "0.1.0"),
            ("0.1.5", false, "0.1.5"),
        ] {
            let (input, mut scope) = fixture(root.path(), version);
            scope.released_packages[0].yanked = yanked;
            let data = build(&input, &scope).unwrap();
            write_bundle(&data, output.path(), true).unwrap();
            let mut index = load_version_index(output.path()).unwrap();
            assert_eq!(index.latest.as_ref().unwrap().version, latest);
            assert!(index.releases.iter().any(|entry| entry.version == version));
            if yanked {
                let mut changed = data.clone();
                changed.source.released_packages[0].yanked = false;
                assert!(write_bundle(&changed, output.path(), true).is_err());
                index.latest = index
                    .releases
                    .iter()
                    .find(|entry| entry.version == version)
                    .cloned();
                assert!(validate_version_index(&index, output.path())
                    .unwrap_err()
                    .to_string()
                    .contains("maximum release"));
            }
        }
    }
}
