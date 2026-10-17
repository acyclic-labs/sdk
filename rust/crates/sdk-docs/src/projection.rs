use super::*;

pub(super) fn build_family(
    repository_root: &Path,
    rustdoc_source_root: &Path,
    generated_sources: &HashMap<PathBuf, GeneratedSource>,
    json_path: &Path,
    raw_json: &std::rc::Rc<[u8]>,
    krate: &Crate,
) -> Result<Family, Error> {
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
    let mut public_items = public_api::extract(json_path, raw_json, krate)?;
    deduplicate_public_items(&mut public_items);
    let public_occurrence_paths = public_occurrence_paths(&public_items, &crate_name);
    let use_occurrences = public_use_occurrences(krate, &crate_name)?;
    let mut items = Vec::new();
    let mut source_paths = HashMap::new();
    for public_item in &public_items {
        let id = public_item.id;
        let path = public_item.path.clone();
        // `public-api` reports impl occurrences as public signatures too. Their
        // target may be a tuple, reference, or dyn type without a crate path;
        // retain the typed occurrence for identity/link analysis, but do not
        // subject it to the ordinary exported-item path assertion.
        if krate
            .index
            .get(&id)
            .is_some_and(|item| matches!(item.inner, ItemEnum::Impl(_)))
        {
            continue;
        }
        // public-api renders an implementation method through the receiver's
        // path. An impl owned by this crate can therefore legitimately have a
        // foreign-looking path, for example `String::from` for a local
        // `impl From<&ActorId> for String`. Keep that local implementation
        // occurrence instead of treating the receiver path as an external
        // definition.
        if path.first() != Some(&crate_name) && !is_local_impl_member(krate, public_item) {
            return Err(Error::Invalid(format!(
                "{} public-api item {} ({}) does not resolve to the crate root: {}",
                json_path.display(),
                id.0,
                public_item.display,
                path.join("::")
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
        // A public `use` is represented by its own item, but Rustdoc attaches
        // inherited documentation and links to the referenced definition. Keep
        // those projections on the same effective item so aliases do not lose
        // the definition's links or accidentally expose links authored only on
        // the re-export node.
        let effective_item = target.unwrap_or(item);
        let docs = item.docs.clone().or_else(|| effective_item.docs.clone());
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
            links: rustdoc_links(effective_item, &public_occurrence_paths),
            source: item
                .span
                .as_ref()
                .map(|span| {
                    cached_source_span(
                        repository_root,
                        rustdoc_source_root,
                        generated_sources,
                        &mut source_paths,
                        span,
                    )
                })
                .transpose()?,
            reexport,
            reexport_target,
        });
    }
    items.sort_by(|a, b| a.path.cmp(&b.path).then(a.id.cmp(&b.id)));
    let guides = guides_from_rustdoc(
        krate,
        &crate_name,
        root_item,
        &public_items,
        &public_occurrence_paths,
        &use_occurrences,
    )?;
    validate_source_paths(
        repository_root,
        rustdoc_source_root,
        generated_sources,
        &source_paths,
    )?;
    Ok(Family {
        slug,
        title,
        crate_name,
        items,
        guides,
    })
}

pub(super) fn navigation_entries(families: &[Family]) -> Vec<NavigationEntry> {
    let mut entries = families
        .iter()
        .map(|family| NavigationEntry {
            slug: family.slug.clone(),
            title: family.title.clone(),
            crate_name: family.crate_name.clone(),
        })
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| left.slug.cmp(&right.slug));
    entries
}

pub(super) fn package_catalog(families: &[(Family, PackageMetadata)]) -> PackageCatalog {
    let mut entries = families
        .iter()
        .map(|(family, metadata)| PackageEntry {
            package_name: metadata.package_name.clone(),
            crate_name: metadata.crate_name.clone(),
            family_slug: family.slug.clone(),
            version: metadata.version.clone(),
        })
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| {
        left.package_name
            .cmp(&right.package_name)
            .then(left.crate_name.cmp(&right.crate_name))
    });
    PackageCatalog { entries }
}

pub(super) fn search_index(families: &[Family]) -> SearchIndex {
    let mut entries = Vec::new();
    for family in families {
        entries.push(SearchEntry {
            id: format!("family:{}", family.slug),
            family_slug: family.slug.clone(),
            title: family.title.clone(),
            path: family.slug.clone(),
            kind: "family".into(),
            text: format!("{} {}", family.title, family.crate_name),
        });
        for guide in &family.guides {
            entries.push(SearchEntry {
                id: format!("guide:{}:{}", family.slug, guide.path),
                family_slug: family.slug.clone(),
                title: guide.title.clone(),
                path: guide.path.clone(),
                kind: "guide".into(),
                text: guide.markdown.clone(),
            });
        }
        for item in &family.items {
            let docs = item.docs.as_deref().unwrap_or_default();
            entries.push(SearchEntry {
                id: format!("item:{}:{}", family.slug, item.id),
                family_slug: family.slug.clone(),
                title: item.name.clone(),
                path: item.path.clone(),
                kind: item.kind.clone(),
                text: format!("{} {} {}", item.name, item.signature, docs),
            });
        }
    }
    entries.sort_by(|left, right| left.id.cmp(&right.id));
    SearchIndex { entries }
}

pub(super) fn validate_generated_catalog(data: &DocsData) -> Result<(), Error> {
    if data.packages.entries.len() != data.families.len()
        || data.packages.entries.iter().any(|entry| {
            entry.package_name.trim().is_empty()
                || entry.crate_name.trim().is_empty()
                || entry.family_slug.trim().is_empty()
                || entry.version.trim().is_empty()
                || Version::parse(&entry.version).is_err()
        })
    {
        return Err(Error::Invalid(
            "package catalog must contain one complete Cargo-backed entry per family".into(),
        ));
    }
    let mut package_names = HashSet::new();
    let mut family_slugs = HashSet::new();
    for entry in &data.packages.entries {
        if !package_names.insert(entry.package_name.as_str())
            || !family_slugs.insert(entry.family_slug.as_str())
            || !data.families.iter().any(|family| {
                family.slug == entry.family_slug && family.crate_name == entry.crate_name
            })
        {
            return Err(Error::Invalid(
                "package catalog contains duplicate or unbound Cargo identity".into(),
            ));
        }
    }
    let expected_search = search_index(&data.families);
    if data.search != expected_search {
        return Err(Error::Invalid(
            "search index must exactly match the generated Rust-owned data".into(),
        ));
    }
    Ok(())
}

pub(super) fn package_metadata_for_path<'a>(
    metadata: &'a [PackageMetadata],
    path: &Path,
) -> Result<&'a PackageMetadata, Error> {
    let matches = metadata
        .iter()
        .filter(|entry| normalize_path(&entry.rustdoc_file) == normalize_path(path))
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [entry] => Ok(entry),
        [] => Err(Error::Invalid(format!(
            "rustdoc input {} has no Cargo package metadata",
            path.display()
        ))),
        _ => Err(Error::Invalid(format!(
            "rustdoc input {} has duplicate Cargo package metadata",
            path.display()
        ))),
    }
}

pub(super) fn validate_package_metadata(metadata: &PackageMetadata) -> Result<(), Error> {
    if metadata.package_name.trim().is_empty()
        || metadata.crate_name.trim().is_empty()
        || metadata.version.trim().is_empty()
    {
        return Err(Error::Invalid(
            "Cargo package metadata requires package name, crate name, and version".into(),
        ));
    }
    Version::parse(&metadata.version).map_err(|error| {
        Error::Invalid(format!(
            "Cargo package {} has invalid version {}: {error}",
            metadata.package_name, metadata.version
        ))
    })?;
    Ok(())
}

pub(super) fn validate_navigation(data: &DocsData) -> Result<(), Error> {
    let expected = navigation_entries(&data.families);
    if data.navigation.entries != expected {
        return Err(Error::Invalid(
            "navigation entries must exactly match the sorted family entries".into(),
        ));
    }
    validate_generated_catalog(data)?;
    Ok(())
}

pub(super) fn public_occurrence_paths(
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

pub(super) fn is_local_impl_member(
    krate: &Crate,
    public_item: &public_api::PublicItemSignature,
) -> bool {
    // public-api queues both explicit impl items and inherited trait members
    // with the impl as their logical parent. Keep either only when Rustdoc
    // proves the child belongs to this local impl and, for inherited members,
    // to the local trait that supplied the default.
    let Some(item) = krate.index.get(&public_item.id) else {
        return false;
    };
    if item.crate_id != 0 {
        return false;
    }
    let Some(parent) = public_item
        .parent_id
        .and_then(|parent_id| krate.index.get(&parent_id))
    else {
        return false;
    };
    let ItemEnum::Impl(implementation) = &parent.inner else {
        return false;
    };
    if parent.crate_id != 0 {
        return false;
    }
    if implementation.items.contains(&public_item.id) {
        return true;
    }

    let Some(trait_path) = &implementation.trait_ else {
        return false;
    };
    let Some(trait_item) = krate.index.get(&trait_path.id) else {
        return false;
    };
    let Some(name) = item.name.as_deref() else {
        return false;
    };
    trait_item.crate_id == 0
        && !implementation
            .items
            .iter()
            .filter_map(|id| krate.index.get(id))
            .any(|explicit| explicit.name.as_deref() == Some(name))
        && matches!(
            &trait_item.inner,
            ItemEnum::Trait(trait_definition)
                if trait_definition.items.contains(&public_item.id)
        )
}

pub(super) fn rustdoc_links(
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

pub(super) fn deduplicate_public_items(items: &mut Vec<public_api::PublicItemSignature>) {
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

pub(super) fn public_use_occurrences(
    krate: &Crate,
    crate_name: &str,
) -> Result<HashMap<(Id, String), Id>, Error> {
    let mut occurrences = HashMap::new();
    let mut ancestors = Vec::new();
    collect_public_use_occurrences(
        krate,
        krate.root,
        vec![crate_name.to_owned()],
        &mut occurrences,
        &mut ancestors,
        false,
    )?;
    Ok(occurrences)
}

pub(super) fn collect_public_use_occurrences(
    krate: &Crate,
    id: Id,
    path: Vec<String>,
    occurrences: &mut HashMap<(Id, String), Id>,
    ancestors: &mut Vec<Id>,
    via_glob: bool,
) -> Result<(), Error> {
    // A module can be reachable through multiple public aliases. Keep the
    // current path in the cycle guard so each alias gets its own descendants,
    // while a re-export back to an ancestor terminates immediately.
    if ancestors.contains(&id) {
        return Ok(());
    }
    ancestors.push(id);
    let item = krate.index.get(&id).ok_or_else(|| {
        Error::Invalid(format!("rustdoc module item {} is absent from index", id.0))
    })?;
    let ItemEnum::Module(module) = &item.inner else {
        ancestors.pop();
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
                    let exported_path = exported.join("::");
                    let occurrence = (target, exported_path);
                    let occurrence_is_glob = via_glob || use_.is_glob;
                    if occurrence_is_glob {
                        // Explicit re-exports win over a glob reaching the
                        // same target under the same public path.
                        occurrences.entry(occurrence).or_insert(*child_id);
                    } else {
                        occurrences.insert(occurrence, *child_id);
                    }
                    if matches!(
                        krate.index.get(&target).map(|item| &item.inner),
                        Some(ItemEnum::Module(_))
                    ) {
                        let nested_path = if use_.is_glob { path.clone() } else { exported };
                        collect_public_use_occurrences(
                            krate,
                            target,
                            nested_path,
                            occurrences,
                            ancestors,
                            occurrence_is_glob,
                        )?;
                    }
                }
            }
            ItemEnum::Module(_) => {
                if let Some(name) = item_name(child) {
                    let mut nested = path.clone();
                    nested.push(name);
                    collect_public_use_occurrences(
                        krate,
                        *child_id,
                        nested,
                        occurrences,
                        ancestors,
                        via_glob,
                    )?;
                }
            }
            _ => {}
        }
    }
    ancestors.pop();
    Ok(())
}
