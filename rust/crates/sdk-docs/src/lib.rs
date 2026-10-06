//! Deterministic, Rust-owned data for the versioned SDK documentation site.
//!
//! The input boundary is the pinned [`rustdoc_types::Crate`] representation. The output is a
//! deliberately small public projection: private rustdoc items and compiler-only metadata never
//! cross this boundary.

use rustdoc_types::{
    Crate, GenericArg, GenericArgs, GenericBound, GenericParamDef, GenericParamDefKind, Id, Item,
    ItemEnum, ItemKind, Type, Visibility, FORMAT_VERSION,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};

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
    pub name: String,
    pub kind: String,
    pub path: String,
    pub signature: String,
    pub docs: Option<String>,
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
    let mut families = Vec::new();
    let mut format_versions = std::collections::BTreeSet::new();
    let mut input_digest = Sha256::new();
    for path in &input.rustdoc_files {
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
    Ok(DocsData {
        schema: DATA_SCHEMA_VERSION.into(),
        schema_version: DATA_SCHEMA_VERSION.into(),
        version: input.version.clone(),
        channel: input.channel.clone(),
        source: SourceInfo {
            revision: input.revision.clone(),
            source_state: input.source_state.clone(),
            input_sha256: format!("{:x}", input_digest.finalize()),
            rustdoc_format_versions: format_versions.into_iter().collect(),
            generator: format!("sdk-docs/{GENERATOR_VERSION}"),
        },
        navigation,
        families,
    })
}

/// Write data, its Schemars-generated schema, and the guarded version index.
pub fn write_bundle(data: &DocsData, output_dir: &Path, mark_latest: bool) -> Result<(), Error> {
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
    let index = load_version_index(output_dir)?;
    let updated_index = merge_version_index(index, &entry, mark_latest)?;
    fs::create_dir_all(&version_dir)?;
    if !data_path.exists() {
        fs::write(&data_path, &data_bytes)?;
    }
    let schema = schemars::schema_for!(DocsData);
    fs::write(
        version_dir.join("sdk-docs-data.v1.schema.json"),
        serde_json::to_vec_pretty(&schema)?,
    )?;
    fs::write(
        output_dir.join("sdk-docs-versions.v1.schema.json"),
        serde_json::to_vec_pretty(&schemars::schema_for!(VersionIndex))?,
    )?;
    fs::write(
        output_dir.join("sdk-docs-versions.v1.json"),
        serde_json::to_vec_pretty(&updated_index)?,
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
    Ok(index)
}

fn merge_version_index(
    mut index: VersionIndex,
    entry: &VersionEntry,
    mark_latest: bool,
) -> Result<VersionIndex, Error> {
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
            index.releases.sort_by(|a, b| a.version.cmp(&b.version));
        }
        if mark_latest {
            index.latest = Some(entry.clone());
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
    let crate_name = krate
        .index
        .get(&krate.root)
        .and_then(|item| item.name.clone())
        .or_else(|| {
            json_path
                .file_stem()
                .map(|name| name.to_string_lossy().into_owned())
        })
        .ok_or_else(|| Error::Invalid(format!("{} has no crate name", json_path.display())))?;
    let slug = slugify(&crate_name);
    let title = titleize(&crate_name);
    let implicit_public = implicit_public_ids(krate);
    let paths = public_paths(krate, &implicit_public);
    let mut items = Vec::new();
    for (id, path) in &paths {
        let Some(item) = krate.index.get(id) else {
            continue;
        };
        if item.crate_id != 0
            || item_name(item).is_none()
            || matches!(item.inner, ItemEnum::Impl(_))
        {
            continue;
        }
        let name = item_name(item).unwrap_or_default();
        items.push(ApiItem {
            id: format_id(*id),
            name: name.clone(),
            kind: kind_name(item.inner.item_kind()).into(),
            path: path.join("::"),
            signature: signature(item, &name, is_public(*id, item, &implicit_public)),
            docs: item.docs.clone(),
            source: item.span.as_ref().map(source_span).transpose()?,
            reexport: match &item.inner {
                ItemEnum::Use(use_) => Some(use_.source.clone()),
                _ => None,
            },
            reexport_target: match &item.inner {
                ItemEnum::Use(use_) => use_
                    .id
                    .and_then(|target| krate.paths.get(&target))
                    .map(|summary| summary.path.join("::")),
                _ => None,
            },
        });
    }
    items.sort_by(|a, b| a.path.cmp(&b.path).then(a.id.cmp(&b.id)));
    let guides = read_guides(repository_root, &crate_name)?;
    Ok(Family {
        slug,
        title,
        crate_name,
        items,
        guides,
    })
}

fn source_span(span: &rustdoc_types::Span) -> Result<SourceSpan, Error> {
    if span.filename.is_absolute()
        || span
            .filename
            .components()
            .any(|component| component == std::path::Component::ParentDir)
    {
        return Err(Error::Invalid(format!(
            "rustdoc source span escapes its source root: {}",
            span.filename.display()
        )));
    }
    Ok(SourceSpan {
        path: normalize_path(&span.filename),
        begin_line: span.begin.0,
        begin_column: span.begin.1,
        end_line: span.end.0,
        end_column: span.end.1,
    })
}

fn implicit_public_ids(krate: &Crate) -> HashSet<Id> {
    let mut result = HashSet::new();
    for item in krate.index.values() {
        if !matches!(item.visibility, Visibility::Public) {
            continue;
        }
        match &item.inner {
            ItemEnum::Enum(enum_) => result.extend(enum_.variants.iter().copied()),
            ItemEnum::Trait(trait_) => result.extend(trait_.items.iter().copied()),
            _ => {}
        }
    }
    result
}

fn is_public(id: Id, item: &Item, implicit_public: &HashSet<Id>) -> bool {
    matches!(item.visibility, Visibility::Public) || implicit_public.contains(&id)
}

fn public_paths(krate: &Crate, implicit_public: &HashSet<Id>) -> HashMap<Id, Vec<String>> {
    let mut result = HashMap::new();
    let mut visited = HashSet::new();
    walk_public_item(
        krate,
        krate.root,
        Vec::new(),
        &mut result,
        &mut visited,
        implicit_public,
    );
    result
}
fn walk_public_item(
    krate: &Crate,
    id: Id,
    parent: Vec<String>,
    result: &mut HashMap<Id, Vec<String>>,
    visited: &mut HashSet<Id>,
    implicit_public: &HashSet<Id>,
) {
    if !visited.insert(id) {
        return;
    }
    let Some(item) = krate.index.get(&id) else {
        return;
    };
    let mut path = parent;
    if let Some(name) = item_name(item).as_ref() {
        path.push(name.clone());
    }
    result.insert(id, path.clone());
    match &item.inner {
        ItemEnum::Module(module) => {
            for child in &module.items {
                let Some(child_item) = krate.index.get(child) else {
                    continue;
                };
                if is_public(*child, child_item, implicit_public) {
                    walk_public_item(
                        krate,
                        *child,
                        path.clone(),
                        result,
                        visited,
                        implicit_public,
                    );
                }
            }
        }
        ItemEnum::Enum(enum_) => {
            for child in &enum_.variants {
                walk_public_item(
                    krate,
                    *child,
                    path.clone(),
                    result,
                    visited,
                    implicit_public,
                );
            }
        }
        ItemEnum::Trait(trait_) => {
            for child in &trait_.items {
                walk_public_item(
                    krate,
                    *child,
                    path.clone(),
                    result,
                    visited,
                    implicit_public,
                );
            }
        }
        ItemEnum::Struct(struct_) => {
            let fields = match &struct_.kind {
                rustdoc_types::StructKind::Plain { fields, .. } => fields.clone(),
                rustdoc_types::StructKind::Tuple(fields) => {
                    fields.iter().flatten().copied().collect()
                }
                rustdoc_types::StructKind::Unit => Vec::new(),
            };
            for child in fields {
                if let Some(child_item) = krate.index.get(&child) {
                    if is_public(child, child_item, implicit_public) {
                        walk_public_item(
                            krate,
                            child,
                            path.clone(),
                            result,
                            visited,
                            implicit_public,
                        );
                    }
                }
            }
        }
        ItemEnum::Union(union_) => {
            for child in &union_.fields {
                if let Some(child_item) = krate.index.get(child) {
                    if is_public(*child, child_item, implicit_public) {
                        walk_public_item(
                            krate,
                            *child,
                            path.clone(),
                            result,
                            visited,
                            implicit_public,
                        );
                    }
                }
            }
        }
        ItemEnum::Variant(variant) => {
            let fields = match &variant.kind {
                rustdoc_types::VariantKind::Plain => Vec::new(),
                rustdoc_types::VariantKind::Tuple(fields) => {
                    fields.iter().flatten().copied().collect()
                }
                rustdoc_types::VariantKind::Struct { fields, .. } => fields.clone(),
            };
            for child in fields {
                if let Some(child_item) = krate.index.get(&child) {
                    if is_public(child, child_item, implicit_public) {
                        walk_public_item(
                            krate,
                            child,
                            path.clone(),
                            result,
                            visited,
                            implicit_public,
                        );
                    }
                }
            }
        }
        ItemEnum::Use(use_) => {
            // The alias is the public item. The target is recorded as a resolved path below;
            // walking it here would incorrectly publish a private definition path.
            let _ = use_.id;
        }
        _ => {}
    }
}

fn item_name(item: &Item) -> Option<String> {
    item.name.clone().or_else(|| match &item.inner {
        ItemEnum::Use(use_) => Some(use_.name.clone()),
        _ => None,
    })
}

fn signature(item: &Item, name: &str, public: bool) -> String {
    let prefix = if public { "pub " } else { "" };
    match &item.inner {
        ItemEnum::Function(function) => {
            let mut result = String::new();
            if function.header.is_const {
                result.push_str("const ");
            }
            if function.header.is_unsafe {
                result.push_str("unsafe ");
            }
            if function.header.is_async {
                result.push_str("async ");
            }
            result.push_str(prefix);
            result.push_str("fn ");
            result.push_str(name);
            result.push_str(&generics_text(&function.generics));
            result.push('(');
            for (index, (arg, ty)) in function.sig.inputs.iter().enumerate() {
                if index > 0 {
                    result.push_str(", ");
                }
                let _ = write!(result, "{arg}: {}", type_text(ty));
            }
            if function.sig.is_c_variadic {
                if !function.sig.inputs.is_empty() {
                    result.push_str(", ");
                }
                result.push_str("...");
            }
            result.push(')');
            if let Some(output) = &function.sig.output {
                let _ = write!(result, " -> {}", type_text(output));
            }
            result
        }
        ItemEnum::Struct(struct_) => {
            format!("{prefix}struct {name}{}", generics_text(&struct_.generics))
        }
        ItemEnum::Enum(enum_) => {
            format!("{prefix}enum {name}{}", generics_text(&enum_.generics))
        }
        ItemEnum::Union(_) => format!("{prefix}union {name}"),
        ItemEnum::Trait(trait_) => {
            format!("{prefix}trait {name}{}", generics_text(&trait_.generics))
        }
        ItemEnum::TypeAlias(alias) => format!(
            "{prefix}type {name}{} = {}",
            generics_text(&alias.generics),
            type_text(&alias.type_)
        ),
        ItemEnum::Constant { type_, .. } => format!("{prefix}const {name}: {}", type_text(type_)),
        ItemEnum::Static(static_) => {
            format!("{prefix}static {name}: {}", type_text(&static_.type_))
        }
        ItemEnum::StructField(ty) => format!("{prefix}{name}: {}", type_text(ty)),
        ItemEnum::Use(use_) => format!("{prefix}use {}", use_.source),
        _ => format!("{prefix}{} {name}", kind_name(item.inner.item_kind())),
    }
}

fn type_text(ty: &Type) -> String {
    match ty {
        Type::ResolvedPath(path) => format!(
            "{}{}",
            path.path,
            path.args
                .as_deref()
                .map(generic_args_text)
                .unwrap_or_default()
        ),
        Type::Generic(name) | Type::Primitive(name) => name.clone(),
        Type::Tuple(types) => format!(
            "({})",
            types.iter().map(type_text).collect::<Vec<_>>().join(", ")
        ),
        Type::Slice(inner) => format!("[{}]", type_text(inner)),
        Type::Array { type_, len } => format!("[{}; {len}]", type_text(type_)),
        Type::RawPointer { is_mutable, type_ } => format!(
            "*{} {}",
            if *is_mutable { "mut" } else { "const" },
            type_text(type_)
        ),
        Type::BorrowedRef {
            lifetime,
            is_mutable,
            type_,
        } => {
            let lifetime = lifetime
                .as_deref()
                .map(|value| format!("{value} "))
                .unwrap_or_default();
            format!(
                "&{lifetime}{}{}",
                if *is_mutable { "mut " } else { "" },
                type_text(type_)
            )
        }
        Type::Infer => "_".into(),
        Type::FunctionPointer(pointer) => format!(
            "fn({}){}",
            pointer
                .sig
                .inputs
                .iter()
                .map(|(_, ty)| type_text(ty))
                .collect::<Vec<_>>()
                .join(", "),
            pointer
                .sig
                .output
                .as_ref()
                .map(|ty| format!(" -> {}", type_text(ty)))
                .unwrap_or_default()
        ),
        Type::DynTrait(dynamic) => {
            let mut traits = dynamic
                .traits
                .iter()
                .map(|poly| {
                    format!(
                        "{}{}",
                        poly.trait_.path,
                        poly.trait_
                            .args
                            .as_deref()
                            .map(generic_args_text)
                            .unwrap_or_default()
                    )
                })
                .collect::<Vec<_>>();
            if let Some(lifetime) = &dynamic.lifetime {
                traits.push(lifetime.clone());
            }
            format!("dyn {}", traits.join(" + "))
        }
        Type::ImplTrait(bounds) => format!(
            "impl {}",
            bounds
                .iter()
                .map(generic_bound_text)
                .collect::<Vec<_>>()
                .join(" + ")
        ),
        Type::Pat { type_, .. } => type_text(type_),
        Type::QualifiedPath {
            name,
            self_type,
            trait_,
            args,
        } => {
            let owner = type_text(self_type);
            let trait_path = trait_
                .as_ref()
                .map(|path| format!(" as {}", path.path))
                .unwrap_or_default();
            format!(
                "<{owner}{trait_path}>::{name}{}",
                args.as_deref().map(generic_args_text).unwrap_or_default()
            )
        }
    }
}

fn generics_text(generics: &rustdoc_types::Generics) -> String {
    let params = generics.params.iter().map(param_text).collect::<Vec<_>>();
    if params.is_empty() {
        String::new()
    } else {
        format!("<{}>", params.join(", "))
    }
}

fn param_text(param: &GenericParamDef) -> String {
    match &param.kind {
        GenericParamDefKind::Lifetime { .. } => param.name.clone(),
        GenericParamDefKind::Type {
            bounds, default, ..
        } => {
            let bounds = if bounds.is_empty() {
                String::new()
            } else {
                format!(
                    ": {}",
                    bounds
                        .iter()
                        .map(generic_bound_text)
                        .collect::<Vec<_>>()
                        .join(" + ")
                )
            };
            let default = default
                .as_ref()
                .map(|ty| format!(" = {}", type_text(ty)))
                .unwrap_or_default();
            format!("{}{bounds}{default}", param.name)
        }
        GenericParamDefKind::Const { type_, default } => {
            let default = default
                .as_ref()
                .map(|value| format!(" = {value}"))
                .unwrap_or_default();
            format!("const {}: {}{default}", param.name, type_text(type_))
        }
    }
}

fn generic_args_text(args: &GenericArgs) -> String {
    match args {
        GenericArgs::AngleBracketed { args, constraints } => {
            let mut values = args
                .iter()
                .map(|arg| match arg {
                    GenericArg::Lifetime(value) => value.clone(),
                    GenericArg::Type(ty) => type_text(ty),
                    GenericArg::Const(value) => value.expr.clone(),
                    GenericArg::Infer => "_".into(),
                })
                .collect::<Vec<_>>();
            values.extend(constraints.iter().map(|constraint| constraint.name.clone()));
            format!("<{}>", values.join(", "))
        }
        GenericArgs::Parenthesized { inputs, output } => {
            let suffix = output
                .as_ref()
                .map(|ty| format!(" -> {}", type_text(ty)))
                .unwrap_or_default();
            format!(
                "({}){suffix}",
                inputs.iter().map(type_text).collect::<Vec<_>>().join(", ")
            )
        }
        GenericArgs::ReturnTypeNotation => "(..)".into(),
    }
}

fn generic_bound_text(bound: &GenericBound) -> String {
    match bound {
        GenericBound::TraitBound { trait_, .. } => format!(
            "{}{}",
            trait_.path,
            trait_
                .args
                .as_deref()
                .map(generic_args_text)
                .unwrap_or_default()
        ),
        GenericBound::Outlives(value) => value.clone(),
        GenericBound::Use(values) => format!("use<{values:?}>"),
    }
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

fn read_guides(repository_root: &Path, crate_name: &str) -> Result<Vec<Guide>, Error> {
    let repository_root = repository_root.canonicalize()?;
    let mut package_roots = Vec::new();
    let normalized = crate_name.replace('_', "-");
    let mut manifests = Vec::new();
    collect_manifests(&repository_root.join("rust/crates"), &mut manifests)?;
    for manifest in manifests {
        let text = fs::read_to_string(&manifest)?;
        let package_name = text.lines().find_map(|line| {
            let (key, value) = line.split_once('=')?;
            if key.trim() != "name" {
                return None;
            }
            Some(value.trim().trim_matches('"').to_owned())
        });
        if package_name.as_deref().map(|name| name.replace('_', "-")) == Some(normalized.clone()) {
            if let Some(root) = manifest.parent() {
                package_roots.push(root.to_owned());
            }
        }
    }
    if package_roots.is_empty() {
        package_roots.push(repository_root.join("rust/crates").join(crate_name));
    }
    let candidates: Vec<PathBuf> = package_roots
        .into_iter()
        .flat_map(|root| [root.join("README.md"), root.join("docs")])
        .collect();
    let mut files = Vec::new();
    for candidate in candidates {
        if !candidate.exists() {
            continue;
        }
        let canonical = candidate.canonicalize()?;
        if !canonical.starts_with(&repository_root) {
            return Err(Error::Invalid(format!(
                "guide path escapes repository root: {}",
                candidate.display()
            )));
        }
        if canonical.is_file() {
            files.push(canonical);
        } else if canonical.is_dir() {
            collect_markdown(&canonical, &mut files)?;
        }
    }
    files.sort();
    files.dedup();
    files
        .into_iter()
        .map(|path| {
            let markdown = fs::read_to_string(&path)?;
            let title = markdown
                .lines()
                .find_map(|line| line.strip_prefix("# "))
                .unwrap_or("Guide")
                .trim()
                .to_owned();
            let relative = path.strip_prefix(&repository_root).unwrap_or(&path);
            Ok(Guide {
                path: normalize_path(relative),
                title,
                markdown,
            })
        })
        .collect()
}

fn collect_manifests(root: &Path, manifests: &mut Vec<PathBuf>) -> Result<(), Error> {
    if !root.is_dir() {
        return Ok(());
    }
    for entry in fs::read_dir(root)? {
        let path = entry?.path();
        if path.is_dir() {
            collect_manifests(&path, manifests)?;
        } else if path.file_name().is_some_and(|name| name == "Cargo.toml") {
            manifests.push(path);
        }
    }
    manifests.sort();
    Ok(())
}
fn collect_markdown(root: &Path, files: &mut Vec<PathBuf>) -> Result<(), Error> {
    for entry in fs::read_dir(root)? {
        let path = entry?.path();
        if path.is_dir() {
            collect_markdown(&path, files)?;
        } else if path.extension().is_some_and(|extension| extension == "md") {
            files.push(path);
        }
    }
    Ok(())
}
fn sha256_hex(bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(bytes);
    format!("{:x}", digest.finalize())
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
                input_sha256: "input".into(),
                rustdoc_format_versions: vec![31],
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
                input_sha256: "input".into(),
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
        assert_eq!(index.releases.len(), 2);
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
            "includes_private": true,
            "index": {
                "0": {"id": 0, "crate_id": 0, "name": "demo", "span": null, "visibility": "public", "docs": null, "links": {}, "attrs": [], "deprecation": null, "stability": null, "const_stability": null, "inner": {"module": {"is_crate": true, "items": [1, 2], "is_stripped": false}}},
                "1": {"id": 1, "crate_id": 0, "name": "hidden", "span": null, "visibility": "default", "docs": null, "links": {}, "attrs": [], "deprecation": null, "stability": null, "const_stability": null, "inner": {"module": {"is_crate": false, "items": [3], "is_stripped": false}}},
                "2": {"id": 2, "crate_id": 0, "name": null, "span": null, "visibility": "public", "docs": "alias", "links": {}, "attrs": [], "deprecation": null, "stability": null, "const_stability": null, "inner": {"use": {"source": "hidden::Visible", "name": "Visible", "id": 3, "is_glob": false}}},
                "3": {"id": 3, "crate_id": 0, "name": "private_function", "span": null, "visibility": "public", "docs": "hidden", "links": {}, "attrs": [], "deprecation": null, "stability": null, "const_stability": null, "inner": {"function": {"sig": {"inputs": [], "output": null, "is_c_variadic": false}, "generics": {"params": [], "where_predicates": []}, "header": {"is_const": false, "is_unsafe": false, "is_async": false, "abi": "Rust"}, "has_body": true, "default_unstable": null}}}
            },
            "paths": {"3": {"crate_id": 0, "path": ["demo", "hidden", "private_function"], "kind": "function"}},
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
            repository_root: root.clone(),
            rustdoc_files: vec![rustdoc_path],
            mark_latest: false,
        };
        let first = build_data(&input).expect("typed fixture should build");
        let second = build_data(&input).expect("typed fixture should build deterministically");
        assert_eq!(first, second);
        let family = &first.families[0];
        assert!(family
            .items
            .iter()
            .any(|item| item.reexport.as_deref() == Some("hidden::Visible")
                && item.reexport_target.as_deref() == Some("demo::hidden::private_function")));
        assert!(!family
            .items
            .iter()
            .any(|item| item.name == "private_function"));
        fs::remove_dir_all(root).expect("fixture directory should be removable");
    }
}
