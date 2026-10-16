use super::*;

#[test]
fn typed_rustdoc_projection_respects_public_reachability_and_use_aliases() {
    let root = std::env::temp_dir().join(format!("sdk-docs-rustdoc-{}", std::process::id()));
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).expect("fixture directory should be creatable");
    let rustdoc_path = root.join("demo.json");
    let fixture = serde_json::json!({
        "root": 0,
        "crate_version": "1.0.0",
        "includes_private": false,
        "index": {
            "0": {"id": 0, "crate_id": 0, "name": "demo", "span": null, "visibility": "public", "docs": "# Demo\n\nRoot guide", "links": {}, "attrs": [], "deprecation": null, "stability": null, "const_stability": null, "inner": {"module": {"is_crate": true, "items": [2, 4, 5, 6], "is_stripped": false}}},
            "2": {"id": 2, "crate_id": 0, "name": null, "span": null, "visibility": "public", "docs": null, "links": {}, "attrs": [], "deprecation": null, "stability": null, "const_stability": null, "inner": {"use": {"source": "hidden::Visible", "name": "Visible", "id": 3, "is_glob": false}}},
            "3": {"id": 3, "crate_id": 0, "name": "private_function", "span": null, "visibility": "public", "docs": "hidden", "links": {"target link": 5}, "attrs": [], "deprecation": null, "stability": null, "const_stability": null, "inner": {"function": {"sig": {"inputs": [], "output": null, "is_c_variadic": false}, "generics": {"params": [], "where_predicates": []}, "header": {"is_const": false, "is_unsafe": false, "is_async": false, "abi": "Rust"}, "has_body": true, "default_unstable": null}}},
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
        rustdoc_files: vec![rustdoc_path.clone()],
        package_metadata: vec![PackageMetadata {
            rustdoc_source_root: None,
            rustdoc_file: rustdoc_path.clone(),
            package_name: "demo".into(),
            crate_name: "demo".into(),
            version: "1.0.0".into(),
        }],
        generated_sources: Vec::new(),
        mark_latest: false,
    };
    let first = build_data(&input, &[]).expect("typed fixture should build");
    let second = build_data(&input, &[]).expect("typed fixture should build deterministically");
    assert_eq!(first, second);
    assert_eq!(
        first.packages.entries,
        vec![PackageEntry {
            package_name: "demo".into(),
            crate_name: "demo".into(),
            family_slug: "demo".into(),
            version: "1.0.0".into(),
        }]
    );
    let mut preview_input = input.clone();
    preview_input.version = "feature-preview".into();
    preview_input.channel = Channel::Preview;
    preview_input.source_state = "working-tree".into();
    preview_input.source_sha256 = None;
    let preview = build_data(&preview_input, &[]).expect("preview fixture should build");
    assert_eq!(preview.version, "feature-preview");
    assert_eq!(preview.packages.entries[0].version, "1.0.0");
    let mut escaped_source_root = input.clone();
    escaped_source_root.package_metadata[0].rustdoc_source_root =
        Some(root.parent().unwrap().to_path_buf());
    let error = build_data(&escaped_source_root, &[])
        .expect_err("an explicit owner source root must stay inside captured source");
    assert!(error.to_string().contains("source root escapes"));
    let mut mismatched_metadata = input.clone();
    mismatched_metadata.package_metadata[0].crate_name = "different-crate".into();
    let error =
        build_data(&mismatched_metadata, &[]).expect_err("Cargo crate identity mismatch must fail");
    assert!(error
        .to_string()
        .contains("Cargo metadata names different-crate"));
    let mut missing_metadata = input.clone();
    missing_metadata.package_metadata[0].version.clear();
    let error =
        build_data(&missing_metadata, &[]).expect_err("missing Cargo package version must fail");
    assert!(error
        .to_string()
        .contains("requires package name, crate name, and version"));
    assert!(first
        .search
        .entries
        .iter()
        .any(|entry| entry.id == "family:demo" && entry.kind == "family"));
    assert!(first
        .search
        .entries
        .iter()
        .any(|entry| entry.id == "item:demo:4" && entry.text.contains("links")));
    let mut changed_source = input.clone();
    changed_source.source_sha256 = Some(format!("sha256:{}", "c".repeat(64)));
    let changed = build_data(&changed_source, &[]).expect("source identity should be retained");
    assert_ne!(first.source.source_sha256, changed.source.source_sha256);

    let mut alias_graph = fixture.clone();
    alias_graph["index"]["0"]["inner"]["module"]["items"] =
        serde_json::json!([2, 4, 5, 6, 8, 10, 11, 12]);
    alias_graph["index"]["8"] = serde_json::json!({
        "id": 8,
        "crate_id": 0,
        "name": "source",
        "span": null,
        "visibility": "public",
        "docs": "# Source\n\nDefinition guide",
        "links": {"target-only": 5},
        "attrs": [],
        "deprecation": null,
        "stability": null,
        "const_stability": null,
        "inner": {"module": {"is_crate": false, "items": [9, 14], "is_stripped": false}}
    });
    alias_graph["index"]["9"] = serde_json::json!({
        "id": 9,
        "crate_id": 0,
        "name": null,
        "span": null,
        "visibility": "public",
        "docs": null,
        "links": {},
        "attrs": [],
        "deprecation": null,
        "stability": null,
        "const_stability": null,
        "inner": {"use": {"source": "associated_target", "name": "associated_target", "id": 5, "is_glob": false}}
    });
    alias_graph["index"]["10"] = serde_json::json!({
        "id": 10,
        "crate_id": 0,
        "name": null,
        "span": null,
        "visibility": "public",
        "docs": "# First\n\nAlias guide",
        "links": {"alias-only": 5},
        "attrs": [],
        "deprecation": null,
        "stability": null,
        "const_stability": null,
        "inner": {"use": {"source": "source", "name": "first", "id": 8, "is_glob": false}}
    });
    alias_graph["index"]["11"] = serde_json::json!({
        "id": 11,
        "crate_id": 0,
        "name": null,
        "span": null,
        "visibility": "public",
        "docs": null,
        "links": {},
        "attrs": [],
        "deprecation": null,
        "stability": null,
        "const_stability": null,
        "inner": {"use": {"source": "source", "name": "second", "id": 8, "is_glob": false}}
    });
    alias_graph["index"]["12"] = serde_json::json!({
        "id": 12,
        "crate_id": 0,
        "name": null,
        "span": null,
        "visibility": "public",
        "docs": null,
        "links": {},
        "attrs": [],
        "deprecation": null,
        "stability": null,
        "const_stability": null,
        "inner": {"use": {"source": "source", "name": "source", "id": 8, "is_glob": true}}
    });
    alias_graph["index"]["14"] = serde_json::json!({
        "id": 14,
        "crate_id": 0,
        "name": null,
        "span": null,
        "visibility": "public",
        "docs": null,
        "links": {},
        "attrs": [],
        "deprecation": null,
        "stability": null,
        "const_stability": null,
        "inner": {"use": {"source": "source", "name": "first", "id": 8, "is_glob": false}}
    });
    let alias_graph: Crate =
        serde_json::from_value(alias_graph).expect("module alias fixture should deserialize");
    let occurrences = public_use_occurrences(&alias_graph, "demo")
        .expect("module alias occurrences should resolve");
    assert_eq!(
        occurrences.get(&(Id(5), "demo::source::associated_target".into())),
        Some(&Id(9))
    );
    assert_eq!(
        occurrences.get(&(Id(5), "demo::associated_target".into())),
        Some(&Id(9))
    );
    assert_eq!(
        occurrences.get(&(Id(5), "demo::first::associated_target".into())),
        Some(&Id(9))
    );
    assert_eq!(
        occurrences.get(&(Id(5), "demo::second::associated_target".into())),
        Some(&Id(9))
    );
    assert_eq!(
        occurrences.get(&(Id(8), "demo::first".into())),
        Some(&Id(10)),
        "an explicit re-export must win over the same path reached through a glob"
    );
    assert_eq!(
        occurrences.get(&(Id(0), "demo::nested::crate".into())),
        Some(&Id(7))
    );
    let alias_public_items = vec![
        public_api::PublicItemSignature {
            id: Id(8),
            parent_id: Some(Id(0)),
            display: "pub use source as first".into(),
            path: vec!["demo".into(), "first".into()],
        },
        public_api::PublicItemSignature {
            id: Id(8),
            parent_id: Some(Id(0)),
            display: "pub use source as second".into(),
            path: vec!["demo".into(), "second".into()],
        },
        public_api::PublicItemSignature {
            id: Id(5),
            parent_id: Some(Id(0)),
            display: "pub fn associated_target".into(),
            path: vec!["demo".into(), "associated_target".into()],
        },
    ];
    let alias_public_paths = public_occurrence_paths(&alias_public_items, "demo");
    let alias_root = alias_graph
        .index
        .get(&alias_graph.root)
        .expect("module alias fixture root should exist");
    let alias_guides = guides_from_rustdoc(
        &alias_graph,
        "demo",
        alias_root,
        &alias_public_items,
        &alias_public_paths,
        &occurrences,
    )
    .expect("module alias guide should resolve");
    let alias_guide = alias_guides
        .iter()
        .find(|guide| guide.path == "demo::first")
        .expect("the alias guide should be projected");
    assert_eq!(alias_guide.title, "First");
    assert_eq!(alias_guide.markdown, "# First\n\nAlias guide");
    assert!(alias_guide.links.contains_key("alias-only"));
    assert!(!alias_guide.links.contains_key("target-only"));
    let fallback_guide = alias_guides
        .iter()
        .find(|guide| guide.path == "demo::second")
        .expect("the target guide should be projected for an undocumented alias");
    assert!(fallback_guide.links.contains_key("target-only"));
    let missing_version_path = root.join("missing-version.json");
    let mut missing_version = fixture.clone();
    missing_version["crate_version"] = serde_json::Value::Null;
    fs::write(
        &missing_version_path,
        serde_json::to_vec(&missing_version).expect("missing-version fixture should serialize"),
    )
    .expect("missing-version fixture should write");
    let mut missing_version_input = input.clone();
    missing_version_input.rustdoc_files = vec![missing_version_path.clone()];
    missing_version_input.package_metadata[0].rustdoc_file = missing_version_path;
    let error = build_data(&missing_version_input, &[])
        .expect_err("release rustdoc without crate version must fail");
    assert!(error.to_string().contains("does not declare crate version"));
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
    assert_eq!(alias.docs.as_deref(), Some("hidden"));
    assert!(alias
        .links
        .get("target link")
        .is_some_and(|paths| paths.contains(&"demo::associated_target".to_owned())));
    assert!(!alias.links.contains_key("alias-only"));
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
    let root_guide = family
        .guides
        .iter()
        .find(|guide| guide.path == "demo")
        .expect("the crate-root guide should be projected");
    assert_eq!(root_guide.title, "Demo");
    assert_eq!(root_guide.markdown, "# Demo\n\nRoot guide");
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
    let alias_json = serde_json::to_value(alias).expect("inherited links should serialize");
    let alias_links = alias_json
        .get("links")
        .and_then(serde_json::Value::as_object)
        .expect("inherited links should be present");
    assert!(alias_links.contains_key("target link"));
    assert!(!alias_links.contains_key("alias-only"));
    let alias_roundtrip: ApiItem =
        serde_json::from_value(alias_json).expect("inherited links should deserialize");
    assert_eq!(&alias_roundtrip, alias);
    let mismatched_path = root.join("mismatched.json");
    let mut mismatched = fixture.clone();
    mismatched["crate_version"] = serde_json::json!("9.9.9");
    fs::write(
        &mismatched_path,
        serde_json::to_vec(&mismatched).expect("mismatched fixture should serialize"),
    )
    .expect("mismatched fixture should write");
    let mut mismatched_input = input;
    mismatched_input.rustdoc_files = vec![mismatched_path.clone()];
    mismatched_input.package_metadata[0].rustdoc_file = mismatched_path;
    let error = build_data(&mismatched_input, &[]).expect_err("crate version mismatch must fail");
    assert!(error.to_string().contains("reports crate version 9.9.9"));
    fs::remove_dir_all(root).expect("fixture directory should be removable");
}
