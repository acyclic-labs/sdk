//! Deterministic, Rust-owned data for the versioned SDK documentation site.
//!
//! The input boundary is the pinned [`rustdoc_types::Crate`] representation. The output is a
//! deliberately small public projection: private rustdoc items and compiler-only metadata never
//! cross this boundary.
use rustdoc_types::{Crate, Id, Item, ItemEnum, ItemKind, FORMAT_VERSION};
use schemars::JsonSchema;
use semver::Version;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

mod public_api;

pub const DATA_SCHEMA_VERSION: &str = "sdk-docs-data.v1";
pub const VERSION_INDEX_SCHEMA_VERSION: &str = "sdk-docs-versions.v1";
pub const GENERATOR_VERSION: &str = "0.1.0";

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Channel {
    Preview,
    Release,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SourceInfo {
    pub revision: String,
    pub source_state: String,
    /// Digest of the trusted source manifest supplied by the generation launcher.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_sha256: Option<String>,
    pub input_sha256: String,
    pub rustdoc_format_versions: Vec<u32>,
    pub generator: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct NavigationEntry {
    pub slug: String,
    pub title: String,
    pub crate_name: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Navigation {
    pub entries: Vec<NavigationEntry>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SourceSpan {
    pub path: String,
    pub begin_line: usize,
    pub begin_column: usize,
    pub end_line: usize,
    pub end_column: usize,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ApiItem {
    pub id: String,
    /// Rustdoc parent identity retained when public-api reports one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    pub name: String,
    pub kind: String,
    pub path: String,
    pub signature: String,
    pub docs: Option<String>,
    /// Public same-crate Rustdoc links keyed by their rendered label.
    #[serde(default)]
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub links: BTreeMap<String, Vec<String>>,
    pub source: Option<SourceSpan>,
    pub reexport: Option<String>,
    pub reexport_target: Option<String>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Guide {
    pub path: String,
    pub title: String,
    pub markdown: String,
    /// Public same-crate Rustdoc links keyed by their rendered label.
    #[serde(default)]
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub links: BTreeMap<String, Vec<String>>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Family {
    pub slug: String,
    pub title: String,
    pub crate_name: String,
    pub items: Vec<ApiItem>,
    pub guides: Vec<Guide>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DocsData {
    pub schema: String,
    pub schema_version: String,
    pub version: String,
    pub channel: Channel,
    pub source: SourceInfo,
    pub navigation: Navigation,
    pub families: Vec<Family>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct VersionEntry {
    pub version: String,
    pub channel: Channel,
    pub revision: String,
    pub data_file: String,
    pub data_sha256: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct VersionIndex {
    pub schema: String,
    pub latest: Option<VersionEntry>,
    pub releases: Vec<VersionEntry>,
    pub preview: Option<VersionEntry>,
}

#[derive(Clone, Debug)]
pub struct BuildInput {
    pub version: String,
    pub channel: Channel,
    pub revision: String,
    pub source_state: String,
    pub source_sha256: Option<String>,
    pub repository_root: PathBuf,
    pub rustdoc_files: Vec<PathBuf>,
    pub mark_latest: bool,
}

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Json(serde_json::Error),
    Invalid(String),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::Json(e) => write!(f, "JSON error: {e}"),
            Self::Invalid(e) => f.write_str(e),
        }
    }
}
impl std::error::Error for Error {}
impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Self::Json(e)
    }
}

/// Build a stable public data projection from one or more typed rustdoc JSON files.
pub fn build_data(input: &BuildInput) -> Result<DocsData, Error> {
    if input.version.trim().is_empty() {
        return Err(Error::Invalid("version must not be empty".into()));
    }
    if input.version != input.version.trim() {
        return Err(Error::Invalid(
            "version must not contain surrounding whitespace".into(),
        ));
    }
    if input.channel == Channel::Release {
        stable_version(&input.version)?;
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
    if input.mark_latest && input.channel != Channel::Release {
        return Err(Error::Invalid("only a release can be marked latest".into()));
    }
    if !matches!(
        input.source_state.as_str(),
        "captured-snapshot" | "working-tree" | "release-tag"
    ) {
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
    let mut families = Vec::new();
    let mut format_versions = std::collections::BTreeSet::new();
    let mut input_digest = Sha256::new();
    for path in &rustdoc_files {
        let bytes = fs::read(path)?;
        if let Some(name) = path.file_name() {
            input_digest.update(name.to_string_lossy().as_bytes());
        }
        input_digest.update([0]);
        input_digest.update(&bytes);
        let krate: Crate = serde_json::from_slice(&bytes).map_err(|e| {
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
        if let Some(crate_version) = krate.crate_version.as_deref() {
            if crate_version != input.version {
                return Err(Error::Invalid(format!(
                    "{} reports crate version {crate_version}, but the build is {}",
                    path.display(),
                    input.version
                )));
            }
        }
        if krate.includes_private {
            return Err(Error::Invalid(format!(
                "{} includes private rustdoc items; public-api requires normal public JSON",
                path.display()
            )));
        }
        format_versions.insert(krate.format_version);
        families.push(build_family(&input.repository_root, path, &krate)?);
    }
    families.sort_by(|a, b| a.slug.cmp(&b.slug));
    if families.windows(2).any(|pair| pair[0].slug == pair[1].slug) {
        return Err(Error::Invalid(
            "duplicate crate family in rustdoc input".into(),
        ));
    }
    let navigation = Navigation {
        entries: families
            .iter()
            .map(|family| NavigationEntry {
                slug: family.slug.clone(),
                title: family.title.clone(),
                crate_name: family.crate_name.clone(),
            })
            .collect(),
    };
    let data = DocsData {
        schema: DATA_SCHEMA_VERSION.into(),
        schema_version: DATA_SCHEMA_VERSION.into(),
        version: input.version.clone(),
        channel: input.channel.clone(),
        source: SourceInfo {
            revision: input.revision.clone(),
            source_state: input.source_state.clone(),
            source_sha256: input.source_sha256.clone(),
            input_sha256: format!("{:x}", input_digest.finalize()),
            rustdoc_format_versions: format_versions.into_iter().collect(),
            generator: format!("sdk-docs/{GENERATOR_VERSION}"),
        },
        navigation,
        families,
    };
    validate_source_info(&data.source, &input.channel)?;
    Ok(data)
}

/// Write data, its Schemars-generated schema, and the guarded version index.
pub fn write_bundle(data: &DocsData, output_dir: &Path, mark_latest: bool) -> Result<(), Error> {
    if data.schema != DATA_SCHEMA_VERSION || data.schema_version != DATA_SCHEMA_VERSION {
        return Err(Error::Invalid(
            "cannot publish data with an unsupported docs schema".into(),
        ));
    }
    if data.channel == Channel::Release {
        stable_version(&data.version)?;
    }
    if mark_latest && data.channel != Channel::Release {
        return Err(Error::Invalid("only a release can be marked latest".into()));
    }
    validate_source_info(&data.source, &data.channel)?;
    reject_reparse_ancestors(output_dir)?;
    let index = load_version_index(output_dir)?;
    fs::create_dir_all(output_dir)?;
    let channel_dir = match data.channel {
        Channel::Release => "releases",
        Channel::Preview => "preview",
    };
    let version_dir = output_dir
        .join(channel_dir)
        .join(safe_version(&data.version)?);
    let data_file = format!(
        "{channel_dir}/{}/sdk-docs-data.v1.json",
        safe_version(&data.version)?
    );
    let data_bytes = serde_json::to_vec_pretty(data)?;
    let digest = sha256_hex(&data_bytes);
    let data_path = output_dir.join(&data_file);
    if data_path.exists() {
        let existing = fs::read(&data_path)?;
        if sha256_hex(&existing) != digest {
            return Err(Error::Invalid(format!(
                "refusing to rewrite {} with different data",
                data_path.display()
            )));
        }
    }
    let entry = VersionEntry {
        version: data.version.clone(),
        channel: data.channel.clone(),
        revision: data.source.revision.clone(),
        data_file,
        data_sha256: digest,
    };
    let updated_index = merge_version_index(index, &entry, output_dir, mark_latest)?;
    reject_reparse_ancestors(&version_dir)?;
    fs::create_dir_all(&version_dir)?;
    atomic_write(&data_path, &data_bytes, true)?;
    let schema = schemars::schema_for!(DocsData);
    atomic_write(
        &version_dir.join("sdk-docs-data.v1.schema.json"),
        &serde_json::to_vec_pretty(&schema)?,
        true,
    )?;
    atomic_write(
        &output_dir.join("sdk-docs-versions.v1.schema.json"),
        &serde_json::to_vec_pretty(&schemars::schema_for!(VersionIndex))?,
        true,
    )?;
    atomic_write(
        &output_dir.join("sdk-docs-versions.v1.json"),
        &serde_json::to_vec_pretty(&updated_index)?,
        false,
    )?;
    Ok(())
}

fn load_version_index(output_dir: &Path) -> Result<VersionIndex, Error> {
    let index_path = output_dir.join("sdk-docs-versions.v1.json");
    let index = if index_path.exists() {
        serde_json::from_slice::<VersionIndex>(&fs::read(&index_path)?)?
    } else {
        VersionIndex {
            schema: VERSION_INDEX_SCHEMA_VERSION.into(),
            latest: None,
            releases: Vec::new(),
            preview: None,
        }
    };
    if index.schema != VERSION_INDEX_SCHEMA_VERSION {
        return Err(Error::Invalid(format!(
            "unsupported version index schema: {}",
            index.schema
        )));
    }
    validate_version_index(&index, output_dir)?;
    Ok(index)
}

fn validate_version_index(index: &VersionIndex, output_dir: &Path) -> Result<(), Error> {
    let mut release_versions = HashSet::new();
    for entry in &index.releases {
        validate_version_entry(entry, &Channel::Release, output_dir)?;
        if !release_versions.insert(entry.version.as_str()) {
            return Err(Error::Invalid(format!(
                "version index contains duplicate release {}",
                entry.version
            )));
        }
    }
    if let Some(preview) = &index.preview {
        validate_version_entry(preview, &Channel::Preview, output_dir)?;
    }
    if let Some(latest) = &index.latest {
        validate_version_entry(latest, &Channel::Release, output_dir)?;
        let Some(release) = index
            .releases
            .iter()
            .find(|entry| entry.version == latest.version)
        else {
            return Err(Error::Invalid(format!(
                "version index latest release {} is absent from releases",
                latest.version
            )));
        };
        if release != latest {
            return Err(Error::Invalid(format!(
                "version index latest release {} differs from its release entry",
                latest.version
            )));
        }
    }
    Ok(())
}

fn validate_version_entry(
    entry: &VersionEntry,
    expected_channel: &Channel,
    output_dir: &Path,
) -> Result<(), Error> {
    if &entry.channel != expected_channel {
        return Err(Error::Invalid(format!(
            "version index entry {} has the wrong channel",
            entry.version
        )));
    }
    match entry.channel {
        Channel::Release => {
            stable_version(&entry.version)?;
        }
        Channel::Preview => {
            safe_version(&entry.version)?;
        }
    }
    if entry.revision.len() < 40
        || entry.revision.len() > 64
        || !entry.revision.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(Error::Invalid(format!(
            "version index entry {} has an invalid source revision",
            entry.version
        )));
    }
    if entry.data_sha256.len() != 64
        || !entry
            .data_sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(Error::Invalid(format!(
            "version index entry {} has an invalid data digest",
            entry.version
        )));
    }
    let channel_dir = match entry.channel {
        Channel::Release => "releases",
        Channel::Preview => "preview",
    };
    let expected_data_file = format!(
        "{channel_dir}/{}/sdk-docs-data.v1.json",
        safe_version(&entry.version)?
    );
    if entry.data_file != expected_data_file {
        return Err(Error::Invalid(format!(
            "version index entry {} has unexpected data file {}",
            entry.version, entry.data_file
        )));
    }
    let data_path = output_dir.join(&entry.data_file);
    reject_reparse_ancestors(&data_path)?;
    let metadata = fs::symlink_metadata(&data_path).map_err(|_| {
        Error::Invalid(format!(
            "version index entry {} refers to a missing data file {}",
            entry.version,
            data_path.display()
        ))
    })?;
    if !metadata.is_file() {
        return Err(Error::Invalid(format!(
            "version index entry {} data path is not a regular file {}",
            entry.version,
            data_path.display()
        )));
    }
    let data_bytes = fs::read(&data_path)?;
    if sha256_hex(&data_bytes) != entry.data_sha256 {
        return Err(Error::Invalid(format!(
            "version index entry {} data digest does not match {}",
            entry.version,
            data_path.display()
        )));
    }
    Ok(())
}

fn merge_version_index(
    mut index: VersionIndex,
    entry: &VersionEntry,
    output_dir: &Path,
    mark_latest: bool,
) -> Result<VersionIndex, Error> {
    validate_version_index(&index, output_dir)?;
    if entry.channel == Channel::Release {
        if let Some(existing) = index
            .releases
            .iter()
            .find(|item| item.version == entry.version)
        {
            if existing != entry {
                return Err(Error::Invalid(format!(
                    "refusing to rewrite release {} with a different revision or digest",
                    entry.version
                )));
            }
        } else {
            index.releases.push(entry.clone());
            index.releases.sort_by(|a, b| {
                match (release_version(&a.version), release_version(&b.version)) {
                    (Ok(a), Ok(b)) => a.cmp(&b),
                    _ => a.version.cmp(&b.version),
                }
            });
        }
        if mark_latest {
            let should_advance = match index.latest.as_ref() {
                None => true,
                Some(current) => match (
                    release_version(&entry.version),
                    release_version(&current.version),
                ) {
                    (Ok(entry), Ok(current)) => entry > current,
                    _ => false,
                },
            };
            if should_advance {
                index.latest = Some(entry.clone());
            }
        }
    } else if let Some(existing) = &index.preview {
        if existing.version == entry.version && existing != entry {
            return Err(Error::Invalid(
                "refusing to rewrite the preview version with a different revision or digest"
                    .into(),
            ));
        }
        index.preview = Some(entry.clone());
    } else {
        index.preview = Some(entry.clone());
    }
    Ok(index)
}

fn build_family(repository_root: &Path, json_path: &Path, krate: &Crate) -> Result<Family, Error> {
    let root_item = krate.index.get(&krate.root).ok_or_else(|| {
        Error::Invalid(format!(
            "{} does not contain its declared rustdoc root item",
            json_path.display()
        ))
    })?;
    if !matches!(&root_item.inner, ItemEnum::Module(module) if module.is_crate) {
        return Err(Error::Invalid(format!(
            "{} does not declare a crate root module",
            json_path.display()
        )));
    }
    let crate_name = root_item
        .name
        .clone()
        .ok_or_else(|| Error::Invalid(format!("{} has no crate name", json_path.display())))?;
    let slug = slugify(&crate_name);
    let title = titleize(&crate_name);
    let mut public_items = public_api::extract(json_path)?;
    deduplicate_public_items(&mut public_items);
    let public_occurrence_paths = public_occurrence_paths(&public_items, &crate_name);
    let use_occurrences = public_use_occurrences(krate, &crate_name)?;
    let mut items = Vec::new();
    for public_item in &public_items {
        let id = public_item.id;
        let path = public_item.path.clone();
        if path.first() != Some(&crate_name) {
            return Err(Error::Invalid(format!(
                "public-api item {} does not resolve to the crate root",
                id.0
            )));
        }
        let (item_id, item, target) =
            if let Some(use_id) = use_occurrences.get(&(id, path.join("::"))).copied() {
                let use_item = krate.index.get(&use_id).ok_or_else(|| {
                    Error::Invalid(format!(
                        "public use item {} is absent from rustdoc index",
                        use_id.0
                    ))
                })?;
                let target = krate.index.get(&id);
                (use_id, use_item, target)
            } else {
                let item = krate.index.get(&id).ok_or_else(|| {
                    Error::Invalid(format!(
                        "public-api item {} is absent from rustdoc index",
                        id.0
                    ))
                })?;
                (id, item, None)
            };
        if item.crate_id != 0
            || item_name(item).is_none()
            || matches!(item.inner, ItemEnum::Impl(_))
        {
            continue;
        }
        let name = item_name(item).unwrap_or_default();
        let docs = item
            .docs
            .clone()
            .or_else(|| target.and_then(|target| target.docs.clone()));
        let (reexport, reexport_target) = match &item.inner {
            ItemEnum::Use(use_) => (
                Some(use_.source.clone()),
                use_.id
                    .and_then(|target| krate.paths.get(&target))
                    .map(|summary| summary.path.join("::")),
            ),
            _ => (None, None),
        };
        items.push(ApiItem {
            id: format_id(item_id),
            parent_id: public_item.parent_id.map(format_id),
            name,
            kind: kind_name(item.inner.item_kind()).into(),
            path: path.join("::"),
            signature: public_item.display.clone(),
            docs,
            links: rustdoc_links(item, &public_occurrence_paths),
            source: item
                .span
                .as_ref()
                .map(|span| source_span(repository_root, span))
                .transpose()?,
            reexport,
            reexport_target,
        });
    }
    items.sort_by(|a, b| a.path.cmp(&b.path).then(a.id.cmp(&b.id)));
    let guides = guides_from_rustdoc(krate, &crate_name, &public_items, &public_occurrence_paths)?;
    Ok(Family {
        slug,
        title,
        crate_name,
        items,
        guides,
    })
}

fn public_occurrence_paths(
    public_items: &[public_api::PublicItemSignature],
    crate_name: &str,
) -> HashMap<Id, Vec<String>> {
    // Rustdoc IDs identify definitions within this blob. A public `use` alias
    // or glob contributes an occurrence path for that definition, so retain
    // those paths while keeping the link target anchored to the definition ID.
    // Only local public-api occurrences are safe to expose as stable targets.
    let mut paths = HashMap::<Id, Vec<String>>::new();
    for public_item in public_items {
        if public_item.path.first().map(String::as_str) != Some(crate_name) {
            continue;
        }
        paths
            .entry(public_item.id)
            .or_default()
            .push(public_item.path.join("::"));
    }
    for occurrences in paths.values_mut() {
        occurrences.sort();
        occurrences.dedup();
    }
    paths
}

fn rustdoc_links(
    item: &Item,
    public_occurrence_paths: &HashMap<Id, Vec<String>>,
) -> BTreeMap<String, Vec<String>> {
    item.links
        .iter()
        .filter_map(|(label, target)| {
            public_occurrence_paths
                .get(target)
                .filter(|paths| !paths.is_empty())
                .map(|paths| (label.clone(), paths.clone()))
        })
        .collect()
}

fn deduplicate_public_items(items: &mut Vec<public_api::PublicItemSignature>) {
    items.sort_by(|left, right| {
        left.id
            .cmp(&right.id)
            .then(left.path.cmp(&right.path))
            .then(left.display.cmp(&right.display))
            .then(left.parent_id.cmp(&right.parent_id))
    });
    items.dedup_by(|left, right| {
        left.id == right.id
            && left.path == right.path
            && left.display == right.display
            && left.parent_id == right.parent_id
    });
}

fn public_use_occurrences(
    krate: &Crate,
    crate_name: &str,
) -> Result<HashMap<(Id, String), Id>, Error> {
    let mut occurrences = HashMap::new();
    let mut visited = HashSet::new();
    collect_public_use_occurrences(
        krate,
        krate.root,
        vec![crate_name.to_owned()],
        &mut occurrences,
        &mut visited,
    )?;
    Ok(occurrences)
}

fn collect_public_use_occurrences(
    krate: &Crate,
    id: Id,
    path: Vec<String>,
    occurrences: &mut HashMap<(Id, String), Id>,
    visited: &mut HashSet<Id>,
) -> Result<(), Error> {
    if !visited.insert(id) {
        return Ok(());
    }
    let item = krate.index.get(&id).ok_or_else(|| {
        Error::Invalid(format!("rustdoc module item {} is absent from index", id.0))
    })?;
    let ItemEnum::Module(module) = &item.inner else {
        return Ok(());
    };
    for child_id in &module.items {
        let child = krate.index.get(child_id).ok_or_else(|| {
            Error::Invalid(format!(
                "rustdoc child item {} is absent from index",
                child_id.0
            ))
        })?;
        match &child.inner {
            ItemEnum::Use(use_) => {
                if let Some(target) = use_.id {
                    let mut exported = path.clone();
                    exported.push(use_.name.clone());
                    occurrences.insert((target, exported.join("::")), *child_id);
                }
            }
            ItemEnum::Module(_) => {
                if let Some(name) = item_name(child) {
                    let mut nested = path.clone();
                    nested.push(name);
                    collect_public_use_occurrences(krate, *child_id, nested, occurrences, visited)?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn source_span(repository_root: &Path, span: &rustdoc_types::Span) -> Result<SourceSpan, Error> {
    let repository_root = repository_root.canonicalize().map_err(|error| {
        Error::Invalid(format!(
            "cannot resolve rustdoc source root {}: {error}",
            repository_root.display()
        ))
    })?;
    let source_path = if span.filename.is_absolute() {
        span.filename.clone()
    } else {
        repository_root.join(&span.filename)
    };
    let source_path = source_path.canonicalize().map_err(|error| {
        Error::Invalid(format!(
            "cannot resolve rustdoc source span {}: {error}",
            span.filename.display()
        ))
    })?;
    let relative = source_path.strip_prefix(&repository_root).map_err(|_| {
        Error::Invalid(format!(
            "rustdoc source span escapes its source root: {}",
            span.filename.display()
        ))
    })?;
    Ok(SourceSpan {
        path: normalize_path(relative),
        begin_line: span.begin.0,
        begin_column: span.begin.1,
        end_line: span.end.0,
        end_column: span.end.1,
    })
}

fn item_name(item: &Item) -> Option<String> {
    item.name.clone().or_else(|| match &item.inner {
        ItemEnum::Use(use_) => Some(use_.name.clone()),
        _ => None,
    })
}

fn kind_name(kind: ItemKind) -> &'static str {
    match kind {
        ItemKind::Module => "module",
        ItemKind::ExternCrate => "extern_crate",
        ItemKind::Use => "use",
        ItemKind::Struct => "struct",
        ItemKind::StructField => "struct_field",
        ItemKind::Union => "union",
        ItemKind::Enum => "enum",
        ItemKind::Variant => "variant",
        ItemKind::Function => "function",
        ItemKind::TypeAlias => "type_alias",
        ItemKind::Constant => "constant",
        ItemKind::Static => "static",
        ItemKind::Trait => "trait",
        ItemKind::TraitAlias => "trait_alias",
        ItemKind::Impl => "impl",
        ItemKind::ExternType => "extern_type",
        ItemKind::Macro => "macro",
        ItemKind::ProcAttribute => "proc_attribute",
        ItemKind::ProcDerive => "proc_derive",
        ItemKind::Primitive => "primitive",
        ItemKind::AssocConst => "assoc_const",
        ItemKind::AssocType => "assoc_type",
        ItemKind::Keyword => "keyword",
        ItemKind::Attribute => "attribute",
    }
}

fn guides_from_rustdoc(
    krate: &Crate,
    crate_name: &str,
    public_items: &[public_api::PublicItemSignature],
    public_occurrence_paths: &HashMap<Id, Vec<String>>,
) -> Result<Vec<Guide>, Error> {
    let mut guides = Vec::new();
    for public_item in public_items {
        if public_item.path.first().map(String::as_str) != Some(crate_name) {
            continue;
        }
        let Some(item) = krate.index.get(&public_item.id) else {
            continue;
        };
        if !matches!(&item.inner, ItemEnum::Module(_)) {
            continue;
        }
        let Some(markdown) = item.docs.clone().filter(|docs| !docs.trim().is_empty()) else {
            continue;
        };
        let path = public_item.path.join("::");
        let title = markdown
            .lines()
            .find_map(|line| line.strip_prefix("# "))
            .map(str::trim)
            .filter(|title| !title.is_empty())
            .unwrap_or_else(|| {
                public_item
                    .path
                    .last()
                    .map(String::as_str)
                    .unwrap_or(crate_name)
            })
            .to_owned();
        guides.push(Guide {
            path,
            title,
            markdown,
            links: rustdoc_links(item, public_occurrence_paths),
        });
    }
    guides.sort_by(|a, b| a.path.cmp(&b.path));
    guides.dedup_by(|a, b| a.path == b.path);
    Ok(guides)
}
fn sha256_hex(bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(bytes);
    format!("{:x}", digest.finalize())
}

fn atomic_write(path: &Path, bytes: &[u8], immutable: bool) -> Result<(), Error> {
    reject_reparse_ancestors(path)?;
    if let Ok(existing) = fs::read(path) {
        if immutable && existing != bytes {
            return Err(Error::Invalid(format!(
                "refusing to rewrite immutable file {}",
                path.display()
            )));
        }
        if immutable {
            return Ok(());
        }
    }
    let parent = path.parent().unwrap_or(path);
    let mut file = tempfile::NamedTempFile::new_in(parent)?;
    file.write_all(bytes)?;
    file.as_file().sync_all()?;
    if immutable {
        file.persist_noclobber(path)
            .map_err(|error| Error::Io(error.error))?;
    } else {
        file.persist(path).map_err(|error| Error::Io(error.error))?;
    }
    Ok(())
}

fn reject_reparse_ancestors(path: &Path) -> Result<(), Error> {
    let mut current = Some(path);
    while let Some(candidate) = current {
        if let Ok(metadata) = fs::symlink_metadata(candidate) {
            if is_reparse_or_symlink(&metadata) {
                return Err(Error::Invalid(format!(
                    "path contains a reparse point or symlink: {}",
                    candidate.display()
                )));
            }
        }
        current = candidate.parent();
    }
    Ok(())
}

fn is_reparse_or_symlink(metadata: &fs::Metadata) -> bool {
    if metadata.file_type().is_symlink() {
        return true;
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        return metadata.file_attributes() & 0x400 != 0;
    }
    #[cfg(not(windows))]
    {
        false
    }
}

fn stable_version(version: &str) -> Result<Version, Error> {
    let parsed = Version::parse(version).map_err(|error| {
        Error::Invalid(format!(
            "release version {version:?} is not valid semver: {error}"
        ))
    })?;
    if !parsed.pre.is_empty() {
        return Err(Error::Invalid(format!(
            "release version {version:?} must not contain a pre-release identifier"
        )));
    }
    Ok(parsed)
}

fn validate_source_digest(value: &str) -> Result<(), Error> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err(Error::Invalid(
            "source manifest digest must use the sha256:<64 hex characters> form".into(),
        ));
    };
    if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(Error::Invalid(
            "source manifest digest must use the sha256:<64 hex characters> form".into(),
        ));
    }
    Ok(())
}

fn validate_source_info(source: &SourceInfo, channel: &Channel) -> Result<(), Error> {
    if source.revision.len() < 40
        || source.revision.len() > 64
        || !source.revision.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(Error::Invalid(
            "source revision must be a 40- to 64-character hexadecimal identity".into(),
        ));
    }
    if !matches!(
        source.source_state.as_str(),
        "captured-snapshot" | "working-tree" | "release-tag"
    ) {
        return Err(Error::Invalid("unsupported source state".into()));
    }
    if matches!(channel, Channel::Release) && source.source_state == "working-tree" {
        return Err(Error::Invalid(
            "release data requires captured-snapshot or release-tag source state".into(),
        ));
    }
    if let Some(source_sha256) = &source.source_sha256 {
        validate_source_digest(source_sha256)?;
    } else if source.source_state != "working-tree" {
        return Err(Error::Invalid(
            "captured snapshots and release tags require a trusted source manifest digest".into(),
        ));
    }
    if source.input_sha256.len() != 64
        || !source
            .input_sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(Error::Invalid(
            "rustdoc input digest must be 64 hexadecimal characters".into(),
        ));
    }
    if source.rustdoc_format_versions.is_empty() {
        return Err(Error::Invalid(
            "rustdoc format metadata must contain at least one version".into(),
        ));
    }
    if source
        .rustdoc_format_versions
        .iter()
        .any(|format| *format != FORMAT_VERSION)
    {
        return Err(Error::Invalid(format!(
            "unsupported rustdoc format in source metadata; expected {}",
            FORMAT_VERSION
        )));
    }
    Ok(())
}

fn release_version(version: &str) -> Result<Version, Error> {
    stable_version(version)
}
fn safe_version(version: &str) -> Result<String, Error> {
    if version.is_empty() || version == "." || version == ".." || version.contains(['/', '\\']) {
        return Err(Error::Invalid(format!(
            "invalid version path segment: {version:?}"
        )));
    }
    Ok(version.replace(':', "_"))
}
fn format_id(id: Id) -> String {
    id.0.to_string()
}
fn normalize_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}
fn slugify(value: &str) -> String {
    value.replace('_', "-").to_ascii_lowercase()
}
fn titleize(value: &str) -> String {
    value
        .split(['-', '_'])
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            chars
                .next()
                .map(|first| first.to_uppercase().collect::<String>() + chars.as_str())
                .unwrap_or_default()
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Return the schema generated directly from the output structs.
pub fn schema_json() -> Result<serde_json::Value, Error> {
    Ok(serde_json::to_value(schemars::schema_for!(DocsData))?)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn schema_is_stable_and_identifies_the_public_contract() {
        let first = schema_json().expect("schema should serialize");
        let second = schema_json().expect("schema should serialize");
        assert_eq!(first, second);
        assert_eq!(first["title"], "DocsData");
    }
    #[test]
    fn slug_and_title_are_deterministic() {
        assert_eq!(slugify("sdk_stream"), "sdk-stream");
        assert_eq!(titleize("sdk-stream"), "Sdk Stream");
    }

    #[test]
    fn duplicate_public_occurrences_collapse_without_losing_aliases() {
        let item = public_api::PublicItemSignature {
            id: Id(1),
            parent_id: None,
            display: "pub struct Thing".into(),
            path: vec!["demo".into(), "Thing".into()],
        };
        let nested_occurrence = public_api::PublicItemSignature {
            parent_id: Some(Id(9)),
            ..item.clone()
        };
        let alias = public_api::PublicItemSignature {
            id: Id(2),
            parent_id: Some(Id(3)),
            display: "pub use demo::Thing as Alias".into(),
            path: vec!["demo".into(), "Alias".into()],
        };
        let mut items = vec![item.clone(), alias.clone(), nested_occurrence.clone(), item];
        deduplicate_public_items(&mut items);
        assert_eq!(items.len(), 3);
        assert!(items.contains(&nested_occurrence));
        assert!(items.contains(&alias));
    }

    #[test]
    fn release_publication_rejects_unbound_source_before_creating_output() {
        let output =
            std::env::temp_dir().join(format!("sdk-docs-provenance-{}", std::process::id()));
        let _ = fs::remove_dir_all(&output);
        let mut data = DocsData {
            schema: DATA_SCHEMA_VERSION.into(),
            schema_version: DATA_SCHEMA_VERSION.into(),
            version: "1.0.0".into(),
            channel: Channel::Release,
            source: SourceInfo {
                revision: "a".repeat(40),
                source_state: "working-tree".into(),
                source_sha256: None,
                input_sha256: "b".repeat(64),
                rustdoc_format_versions: vec![FORMAT_VERSION],
                generator: "sdk-docs/test".into(),
            },
            navigation: Navigation {
                entries: Vec::new(),
            },
            families: Vec::new(),
        };
        let error = write_bundle(&data, &output, true)
            .expect_err("a release cannot publish an unbound working tree");
        assert!(error.to_string().contains("requires captured-snapshot"));
        assert!(!output.exists());

        data.source.source_state = "captured-snapshot".into();
        data.source.source_sha256 = Some(format!("sha256:{}", "c".repeat(64)));
        data.source.rustdoc_format_versions.clear();
        let error = write_bundle(&data, &output, true)
            .expect_err("publication must include Rustdoc format metadata");
        assert!(error.to_string().contains("format metadata"));
        assert!(!output.exists());
    }

    #[test]
    fn version_index_rejects_a_conflicting_release_rewrite() {
        let output = std::env::temp_dir().join(format!("sdk-docs-index-{}", std::process::id()));
        let _ = fs::remove_dir_all(&output);
        let data = DocsData {
            schema: DATA_SCHEMA_VERSION.into(),
            schema_version: DATA_SCHEMA_VERSION.into(),
            version: "1.0.0".into(),
            channel: Channel::Release,
            source: SourceInfo {
                revision: "a".repeat(40),
                source_state: "captured-snapshot".into(),
                source_sha256: Some(format!("sha256:{}", "a".repeat(64))),
                input_sha256: "a".repeat(64),
                rustdoc_format_versions: vec![60],
                generator: "sdk-docs/test".into(),
            },
            navigation: Navigation {
                entries: Vec::new(),
            },
            families: Vec::new(),
        };
        write_bundle(&data, &output, true).expect("initial release should write");
        let mut conflicting = data;
        conflicting.source.revision = "b".repeat(40);
        let error =
            write_bundle(&conflicting, &output, true).expect_err("release rewrite must fail");
        assert!(error.to_string().contains("refusing to rewrite"));
        fs::remove_dir_all(output).expect("test output should be removable");
    }

    #[test]
    fn malformed_prior_version_index_is_rejected_without_mutation() {
        let output =
            std::env::temp_dir().join(format!("sdk-docs-malformed-index-{}", std::process::id()));
        let _ = fs::remove_dir_all(&output);
        fs::create_dir_all(&output).expect("output directory should be creatable");
        let malformed = VersionIndex {
            schema: VERSION_INDEX_SCHEMA_VERSION.into(),
            latest: None,
            releases: vec![VersionEntry {
                version: "1.0.0".into(),
                channel: Channel::Release,
                revision: "a".repeat(40),
                data_file: "releases/1.0.0/sdk-docs-data.v1.json".into(),
                data_sha256: "0".repeat(64),
            }],
            preview: None,
        };
        let index_path = output.join("sdk-docs-versions.v1.json");
        fs::write(
            &index_path,
            serde_json::to_vec_pretty(&malformed).expect("malformed index should serialize"),
        )
        .expect("malformed index should write");
        let before = fs::read(&index_path).expect("malformed index should remain readable");
        let data = DocsData {
            schema: DATA_SCHEMA_VERSION.into(),
            schema_version: DATA_SCHEMA_VERSION.into(),
            version: "2.0.0".into(),
            channel: Channel::Release,
            source: SourceInfo {
                revision: "b".repeat(40),
                source_state: "captured-snapshot".into(),
                source_sha256: Some(format!("sha256:{}", "b".repeat(64))),
                input_sha256: "b".repeat(64),
                rustdoc_format_versions: vec![FORMAT_VERSION],
                generator: "sdk-docs/test".into(),
            },
            navigation: Navigation {
                entries: Vec::new(),
            },
            families: Vec::new(),
        };
        let error = write_bundle(&data, &output, true)
            .expect_err("malformed prior index must block publication");
        assert!(error.to_string().contains("missing data file"));
        assert_eq!(
            before,
            fs::read(&index_path).expect("malformed index should be unchanged")
        );
        assert!(!output.join("releases").exists());

        let data_path = output.join("releases/1.0.0/sdk-docs-data.v1.json");
        fs::create_dir_all(data_path.parent().expect("data file should have a parent"))
            .expect("prior data directory should be creatable");
        fs::write(&data_path, b"prior data").expect("prior data should be writable");
        let prior_data = fs::read(&data_path).expect("prior data should remain readable");
        let error = write_bundle(&data, &output, true)
            .expect_err("wrong prior data hash must block publication");
        assert!(error.to_string().contains("data digest does not match"));
        assert_eq!(
            before,
            fs::read(&index_path).expect("malformed index should remain unchanged")
        );
        assert_eq!(
            prior_data,
            fs::read(&data_path).expect("prior data should remain unchanged")
        );
        fs::remove_dir_all(output).expect("test output should be removable");
    }

    #[test]
    fn version_index_advances_latest_and_preview_without_rewriting_releases() {
        let output =
            std::env::temp_dir().join(format!("sdk-docs-lifecycle-{}", std::process::id()));
        let _ = fs::remove_dir_all(&output);
        let mut data = DocsData {
            schema: DATA_SCHEMA_VERSION.into(),
            schema_version: DATA_SCHEMA_VERSION.into(),
            version: "1.0.0".into(),
            channel: Channel::Release,
            source: SourceInfo {
                revision: "1".repeat(40),
                source_state: "captured-snapshot".into(),
                source_sha256: Some(format!("sha256:{}", "a".repeat(64))),
                input_sha256: "a".repeat(64),
                rustdoc_format_versions: vec![60],
                generator: "sdk-docs/test".into(),
            },
            navigation: Navigation {
                entries: Vec::new(),
            },
            families: Vec::new(),
        };
        write_bundle(&data, &output, true).expect("first release should write");
        data.version = "1.1.0".into();
        data.source.revision = "2".repeat(40);
        write_bundle(&data, &output, true).expect("latest should advance to the next release");
        data.version = "0.9.0".into();
        data.source.revision = "9".repeat(40);
        write_bundle(&data, &output, true).expect("an older release should remain immutable");
        data.version = "0.10.0".into();
        data.source.revision = "8".repeat(40);
        write_bundle(&data, &output, true).expect("semantic release ordering should be accepted");
        data.version = "preview-1".into();
        data.channel = Channel::Preview;
        data.source.revision = "3".repeat(40);
        write_bundle(&data, &output, false).expect("preview should write");
        data.version = "preview-2".into();
        data.source.revision = "4".repeat(40);
        write_bundle(&data, &output, false).expect("preview should advance");
        let index: VersionIndex = serde_json::from_slice(
            &fs::read(output.join("sdk-docs-versions.v1.json")).expect("index should exist"),
        )
        .expect("index should parse");
        assert_eq!(
            index
                .releases
                .iter()
                .map(|entry| entry.version.as_str())
                .collect::<Vec<_>>(),
            vec!["0.9.0", "0.10.0", "1.0.0", "1.1.0"]
        );
        assert_eq!(
            index.latest.as_ref().map(|entry| entry.version.as_str()),
            Some("1.1.0")
        );
        assert_eq!(
            index.preview.as_ref().map(|entry| entry.version.as_str()),
            Some("preview-2")
        );
        fs::remove_dir_all(output).expect("test output should be removable");
    }

    #[test]
    fn typed_rustdoc_projection_respects_public_reachability_and_use_aliases() {
        let root = std::env::temp_dir().join(format!("sdk-docs-rustdoc-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("fixture directory should be creatable");
        let rustdoc_path = root.join("demo.json");
        let fixture = serde_json::json!({
            "root": 0,
            "crate_version": null,
            "includes_private": false,
            "index": {
                "0": {"id": 0, "crate_id": 0, "name": "demo", "span": null, "visibility": "public", "docs": null, "links": {}, "attrs": [], "deprecation": null, "stability": null, "const_stability": null, "inner": {"module": {"is_crate": true, "items": [2, 4, 5, 6], "is_stripped": false}}},
                "2": {"id": 2, "crate_id": 0, "name": null, "span": null, "visibility": "public", "docs": "alias", "links": {}, "attrs": [], "deprecation": null, "stability": null, "const_stability": null, "inner": {"use": {"source": "hidden::Visible", "name": "Visible", "id": 3, "is_glob": false}}},
                "3": {"id": 3, "crate_id": 0, "name": "private_function", "span": null, "visibility": "public", "docs": "hidden", "links": {}, "attrs": [], "deprecation": null, "stability": null, "const_stability": null, "inner": {"function": {"sig": {"inputs": [], "output": null, "is_c_variadic": false}, "generics": {"params": [], "where_predicates": []}, "header": {"is_const": false, "is_unsafe": false, "is_async": false, "abi": "Rust"}, "has_body": true, "default_unstable": null}}},
                "4": {"id": 4, "crate_id": 0, "name": "linked", "span": null, "visibility": "public", "docs": "links", "links": {"alias target": 3, "associated target": 5, "private target": 1, "external target": 99}, "attrs": [], "deprecation": null, "stability": null, "const_stability": null, "inner": {"function": {"sig": {"inputs": [], "output": null, "is_c_variadic": false}, "generics": {"params": [], "where_predicates": []}, "header": {"is_const": false, "is_unsafe": false, "is_async": false, "abi": "Rust"}, "has_body": true, "default_unstable": null}}},
                "5": {"id": 5, "crate_id": 0, "name": "associated_target", "span": null, "visibility": "public", "docs": "target", "links": {}, "attrs": [], "deprecation": null, "stability": null, "const_stability": null, "inner": {"function": {"sig": {"inputs": [], "output": null, "is_c_variadic": false}, "generics": {"params": [], "where_predicates": []}, "header": {"is_const": false, "is_unsafe": false, "is_async": false, "abi": "Rust"}, "has_body": true, "default_unstable": null}}},
                "6": {"id": 6, "crate_id": 0, "name": "nested", "span": null, "visibility": "public", "docs": "Nested guide", "links": {"associated target": 5}, "attrs": [], "deprecation": null, "stability": null, "const_stability": null, "inner": {"module": {"is_crate": false, "items": [7], "is_stripped": false}}},
                "7": {"id": 7, "crate_id": 0, "name": null, "span": null, "visibility": "public", "docs": null, "links": {}, "attrs": [], "deprecation": null, "stability": null, "const_stability": null, "inner": {"use": {"source": "crate", "name": "crate", "id": 0, "is_glob": true}}}
            },
            "paths": {"3": {"crate_id": 0, "path": ["demo", "hidden", "private_function"], "kind": "function"}, "4": {"crate_id": 0, "path": ["demo", "linked"], "kind": "function"}, "5": {"crate_id": 0, "path": ["demo", "associated_target"], "kind": "function"}},
            "external_crates": {},
            "target": {"triple": "x86_64-pc-windows-msvc", "target_features": []},
            "format_version": 60
        });
        fs::write(
            &rustdoc_path,
            serde_json::to_vec(&fixture).expect("fixture should serialize"),
        )
        .expect("fixture should write");
        let input = BuildInput {
            version: "1.0.0".into(),
            channel: Channel::Release,
            revision: "f".repeat(40),
            source_state: "captured-snapshot".into(),
            source_sha256: Some(format!("sha256:{}", "b".repeat(64))),
            repository_root: root.clone(),
            rustdoc_files: vec![rustdoc_path],
            mark_latest: false,
        };
        let first = build_data(&input).expect("typed fixture should build");
        let second = build_data(&input).expect("typed fixture should build deterministically");
        assert_eq!(first, second);
        let mut changed_source = input.clone();
        changed_source.source_sha256 = Some(format!("sha256:{}", "c".repeat(64)));
        let changed = build_data(&changed_source).expect("source identity should be retained");
        assert_ne!(first.source.source_sha256, changed.source.source_sha256);
        let family = &first.families[0];
        let alias = family
            .items
            .iter()
            .find(|item| {
                item.reexport.as_deref() == Some("hidden::Visible")
                    && item.reexport_target.as_deref() == Some("demo::hidden::private_function")
            })
            .expect("the public alias should be projected");
        assert!(alias.parent_id.is_some());
        let linked = family
            .items
            .iter()
            .find(|item| item.name == "linked")
            .expect("the link source should be projected");
        // Rustdoc resolves the alias reference to definition id 3; the
        // public occurrence map still exposes the reachable `demo::Visible`
        // use path rather than confusing the use item id with the definition.
        assert!(linked
            .links
            .get("alias target")
            .is_some_and(|paths| paths.contains(&"demo::Visible".to_owned())));
        let associated_paths = linked
            .links
            .get("associated target")
            .expect("the associated public target should be projected");
        assert!(associated_paths.contains(&"demo::associated_target".to_owned()));
        assert!(associated_paths.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(
            associated_paths,
            &associated_paths
                .iter()
                .cloned()
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>()
        );
        assert!(
            !linked.links.contains_key("private target"),
            "private link unexpectedly projected: links={:?}; items={:?}",
            linked.links,
            family
                .items
                .iter()
                .map(|item| (&item.id, &item.path, &item.name))
                .collect::<Vec<_>>()
        );
        assert!(!linked.links.contains_key("external target"));
        assert_eq!(
            linked.links.keys().map(String::as_str).collect::<Vec<_>>(),
            vec!["alias target", "associated target"]
        );
        let guide = family
            .guides
            .iter()
            .find(|guide| guide.path == "demo::nested")
            .expect("the public module guide should be projected");
        assert!(guide
            .links
            .get("associated target")
            .is_some_and(|paths| paths.contains(&"demo::associated_target".to_owned())));
        let linked_roundtrip: ApiItem = serde_json::from_value(
            serde_json::to_value(linked).expect("non-empty links should serialize"),
        )
        .expect("non-empty links should deserialize");
        assert_eq!(&linked_roundtrip, linked);
        let alias_json = serde_json::to_value(alias).expect("empty links should serialize");
        assert!(alias_json.get("links").is_none());
        let alias_roundtrip: ApiItem =
            serde_json::from_value(alias_json).expect("omitted links should default");
        assert!(alias_roundtrip.links.is_empty());
        let mismatched_path = root.join("mismatched.json");
        let mut mismatched = fixture.clone();
        mismatched["crate_version"] = serde_json::json!("9.9.9");
        fs::write(
            &mismatched_path,
            serde_json::to_vec(&mismatched).expect("mismatched fixture should serialize"),
        )
        .expect("mismatched fixture should write");
        let mut mismatched_input = input;
        mismatched_input.rustdoc_files = vec![mismatched_path];
        let error = build_data(&mismatched_input).expect_err("crate version mismatch must fail");
        assert!(error.to_string().contains("reports crate version 9.9.9"));
        fs::remove_dir_all(root).expect("fixture directory should be removable");
    }
}
