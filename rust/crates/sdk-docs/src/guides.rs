use super::*;

pub(super) fn kind_name(kind: ItemKind) -> &'static str {
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

pub(super) fn guides_from_rustdoc(
    krate: &Crate,
    crate_name: &str,
    root_item: &Item,
    public_items: &[public_api::PublicItemSignature],
    public_occurrence_paths: &HashMap<Id, Vec<String>>,
    use_occurrences: &HashMap<(Id, String), Id>,
) -> Result<Vec<Guide>, Error> {
    let mut guides = Vec::new();
    if let Some(markdown) = root_item
        .docs
        .clone()
        .filter(|docs| !docs.trim().is_empty())
    {
        guides.push(Guide {
            path: crate_name.to_owned(),
            title: guide_title(&markdown, crate_name),
            markdown,
            links: rustdoc_links(root_item, public_occurrence_paths),
        });
    }
    for public_item in public_items {
        if public_item.path.first().map(String::as_str) != Some(crate_name) {
            continue;
        }
        let Some(target) = krate.index.get(&public_item.id) else {
            continue;
        };
        let path = public_item.path.join("::");
        let (item, effective) = if let Some(use_id) = use_occurrences
            .get(&(public_item.id, path.clone()))
            .copied()
        {
            let use_item = krate.index.get(&use_id).ok_or_else(|| {
                Error::Invalid(format!(
                    "public use item {} is absent from rustdoc index",
                    use_id.0
                ))
            })?;
            (use_item, target)
        } else {
            (target, target)
        };
        if !matches!(&effective.inner, ItemEnum::Module(_)) {
            continue;
        }
        let Some(markdown) = item
            .docs
            .clone()
            .or_else(|| effective.docs.clone())
            .filter(|docs| !docs.trim().is_empty())
        else {
            continue;
        };
        let fallback = public_item
            .path
            .last()
            .map(String::as_str)
            .unwrap_or(crate_name);
        // Preserve links authored on a public re-export.  The definition's
        // links are only a fallback when that occurrence has no link metadata.
        let link_source = if item.links.is_empty() {
            effective
        } else {
            item
        };
        guides.push(Guide {
            path,
            title: guide_title(&markdown, fallback),
            markdown,
            links: rustdoc_links(link_source, public_occurrence_paths),
        });
    }
    guides.sort_by(|a, b| a.path.cmp(&b.path));
    guides.dedup_by(|a, b| a.path == b.path);
    Ok(guides)
}

pub(super) fn guide_title(markdown: &str, fallback: &str) -> String {
    markdown
        .lines()
        .find_map(|line| line.strip_prefix("# "))
        .map(str::trim)
        .filter(|title| !title.is_empty())
        .unwrap_or(fallback)
        .to_owned()
}
pub(super) fn sha256_hex(bytes: &[u8]) -> String {
    hex_digest(ring::digest::digest(&ring::digest::SHA256, bytes).as_ref())
}

pub(super) fn hex_digest(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        result.push(DIGITS[(byte >> 4) as usize] as char);
        result.push(DIGITS[(byte & 0xf) as usize] as char);
    }
    result
}
