use super::*;
use std::sync::{Arc, Barrier};
use std::thread;

#[test]
fn rustdoc_digest_omits_only_external_crate_load_paths() {
    let original = serde_json::json!({
        "external_crates": {"1": {
            "name": "dependency", "html_root_url": "https://example.invalid/docs",
            "path": "checkout-a/target/dependency.rlib"
        }},
        "index": {"0": {"path": "demo::Witness", "filename": "src/lib.rs"}}
    });
    let digest = |value: &serde_json::Value| {
        normalized_rustdoc_digest_bytes(&serde_json::to_vec(value).unwrap().into(), Path::new("."))
            .unwrap()
    };
    let mut relocated = original.clone();
    relocated["external_crates"]["1"]["path"] =
        serde_json::json!("checkout-b/target/dependency.rlib");
    assert_eq!(digest(&original), digest(&relocated));
    for (pointer, value) in [
        ("/external_crates/1/name", "different_dependency"),
        (
            "/external_crates/1/html_root_url",
            "https://example.invalid/other",
        ),
        ("/index/0/path", "demo::Other"),
        ("/index/0/filename", "src/other.rs"),
    ] {
        let mut changed = original.clone();
        *changed.pointer_mut(pointer).unwrap() = serde_json::json!(value);
        assert_ne!(digest(&original), digest(&changed), "{pointer}");
    }
    assert_eq!(
        original["external_crates"]["1"]["path"],
        "checkout-a/target/dependency.rlib"
    );
}

#[test]
fn profile_catalog_union_retains_feature_only_items() {
    let source = SourceInfo {
        captured_source: None,
        publication_status: PublicationStatus::Candidate,
        released_packages: Vec::new(),
        revision: "a".repeat(40),
        source_state: "working-tree".into(),
        source_sha256: Some(format!("sha256:{}", "b".repeat(64))),
        input_sha256: "c".repeat(64),
        rustdoc_format_versions: vec![FORMAT_VERSION],
        generator: "sdk-docs/test".into(),
    };
    let item = |id: &str, name: &str| ApiItem {
        id: id.into(),
        parent_id: None,
        name: name.into(),
        kind: "struct".into(),
        path: format!("demo::{name}"),
        signature: format!("pub struct {name}"),
        docs: Some(format!("{name} docs")),
        links: BTreeMap::new(),
        source: None,
        reexport: None,
        reexport_target: None,
    };
    let family = |items| Family {
        slug: "demo".into(),
        title: "Demo".into(),
        crate_name: "demo".into(),
        items,
        guides: Vec::new(),
    };
    let base = DocsData {
        schema: DATA_SCHEMA_VERSION.into(),
        schema_version: DATA_SCHEMA_VERSION.into(),
        version: "0.2.0-preview".into(),
        channel: Channel::Preview,
        source: source.clone(),
        navigation: Navigation {
            entries: Vec::new(),
        },
        packages: PackageCatalog::default(),
        search: SearchIndex::default(),
        families: vec![family(vec![item("1", "Base")])],
    };
    let variant = DocsData {
        families: vec![family(vec![item("1", "Base"), item("2", "FeatureOnly")])],
        ..base.clone()
    };
    let mut merged = base;
    merge_profile_catalog(&mut merged, variant).expect("profile family should merge");
    let items = &merged.families[0].items;
    assert_eq!(items.len(), 2);
    assert!(items
        .iter()
        .any(|item| item.name == "FeatureOnly" && item.id.starts_with("profile-")));
    assert!(merged
        .search
        .entries
        .iter()
        .any(|entry| entry.path == "demo::FeatureOnly"));
}

#[test]
fn schema_is_stable_and_identifies_the_public_contract() {
    let first = schema_json().expect("schema should serialize");
    let second = schema_json().expect("schema should serialize");
    assert_eq!(first, second);
    assert_eq!(first["title"], "DocsData");
    let required = first["required"]
        .as_array()
        .expect("DocsData JSON schema should declare required fields");
    assert!(required.iter().any(|field| field == "packages"));
    assert!(required.iter().any(|field| field == "search"));
    assert_eq!(first["properties"]["schema"]["const"], DATA_SCHEMA_VERSION);
    assert_eq!(
        first["properties"]["schemaVersion"]["const"],
        DATA_SCHEMA_VERSION
    );
    let mut wrong = serde_json::json!({
        "schema": "unsupported-sdk-docs-data",
        "schemaVersion": DATA_SCHEMA_VERSION,
        "packages": {},
        "search": {}
    });
    let error = validate_document_shape(
        &serde_json::to_vec(&wrong).expect("wrong-schema fixture should serialize"),
        "1.0.0",
    )
    .expect_err("schema validator must reject the wrong schema constant");
    assert!(error.to_string().contains("must set schema"));
    wrong["schema"] = DATA_SCHEMA_VERSION.into();
    wrong["schemaVersion"] = "unsupported-sdk-docs-data".into();
    let error = validate_document_shape(
        &serde_json::to_vec(&wrong).expect("wrong-version fixture should serialize"),
        "1.0.0",
    )
    .expect_err("schema validator must reject the wrong schemaVersion constant");
    assert!(error.to_string().contains("must set schemaVersion"));
}
#[test]
fn slug_and_title_are_deterministic() {
    assert_eq!(slugify("sdk_stream"), "sdk-stream");
    assert_eq!(titleize("sdk-stream"), "Sdk Stream");
}

#[test]
fn preview_version_paths_are_injective_and_portable() {
    assert_eq!(safe_version("preview-1").unwrap(), "preview-1");
    assert_eq!(safe_version("preview:1").unwrap(), "~707265766965773a31");
    assert_eq!(safe_version("preview_1").unwrap(), "preview_1");
    assert_eq!(safe_version("branch_ABC").unwrap(), "~6272616e63685f414243");
    assert_ne!(
        safe_version("preview:1").unwrap(),
        safe_version("preview_1").unwrap()
    );
    assert_eq!(safe_version("a%b").unwrap(), "~612562");
    assert_eq!(safe_version("CON").unwrap(), "~434f4e");
    assert_eq!(safe_version("con.txt").unwrap(), "~636f6e2e747874");
    assert_eq!(safe_version("a/b").unwrap(), "~612f62");
    assert_eq!(safe_version(&"a".repeat(255)).unwrap().len(), 255);
    assert!(safe_version(&"a".repeat(256))
        .unwrap_err()
        .to_string()
        .contains("exceeds 255 bytes"));
    assert_eq!(safe_version(&":".repeat(127)).unwrap().len(), 255);
    assert!(safe_version(&":".repeat(128))
        .unwrap_err()
        .to_string()
        .contains("exceeds 255 bytes"));
}

#[test]
fn generated_source_spans_require_attestation_and_preserve_lines() {
    let suffix = std::process::id();
    let repository_root =
        std::env::temp_dir().join(format!("sdk-docs-generated-source-root-{suffix}"));
    let external_root =
        std::env::temp_dir().join(format!("sdk-docs-generated-source-external-{suffix}"));
    let _ = fs::remove_dir_all(&repository_root);
    let _ = fs::remove_dir_all(&external_root);
    fs::create_dir_all(repository_root.join("src")).expect("repository root should be writable");
    fs::create_dir_all(repository_root.join("target/out"))
        .expect("generated checkout directory should be writable");
    fs::create_dir_all(&external_root).expect("external source root should be writable");

    let inside = repository_root.join("src/inside.rs");
    fs::write(&inside, b"pub struct Inside;\n").expect("checkout source should be writable");
    let external = external_root.join("wire.rs");
    let bytes = b"pub struct Wire;\n";
    fs::write(&external, bytes).expect("generated source should be writable");
    let in_tree = repository_root.join("target/out/wire.rs");
    let in_tree_bytes = b"pub struct InTreeWire;\n";
    fs::write(&in_tree, in_tree_bytes).expect("in-tree generated source should be writable");
    let digest = sha256_hex(bytes);
    let in_tree_generated = GeneratedSource {
        physical_path: in_tree.clone(),
        logical_path: PathBuf::from("generated/actors/in-tree-wire.rs"),
        sha256: sha256_hex(in_tree_bytes),
    };
    let generated = GeneratedSource {
        physical_path: external.clone(),
        logical_path: PathBuf::from("generated/actors/wire.rs"),
        sha256: digest.clone(),
    };
    let attested = attest_generated_sources(
        &repository_root,
        &[generated.clone(), in_tree_generated.clone()],
    )
    .expect("matching generated source should be attested");
    let repository_root = canonical_repository_root(&repository_root)
        .expect("repository fixture should have a canonical path");

    let external_span = rustdoc_types::Span {
        filename: external.clone(),
        begin: (7, 3),
        end: (8, 9),
    };
    let projected = source_span_at_root(&repository_root, &attested, &external_span)
        .expect("attested generated source should project");
    assert_eq!(projected.path, "generated/actors/wire.rs");
    assert_eq!(projected.begin_line, 7);
    assert_eq!(projected.begin_column, 3);
    assert_eq!(projected.end_line, 8);
    assert_eq!(projected.end_column, 9);

    let staged = repository_root.join("target/out/staged/wire.rs");
    fs::create_dir_all(staged.parent().expect("staged source has a parent"))
        .expect("staged source directory should be writable");
    fs::write(&staged, bytes).expect("staged source should be writable");
    let staged_attested = attest_generated_sources(
        &repository_root,
        &[GeneratedSource {
            physical_path: staged,
            logical_path: PathBuf::from("generated/actors/staged-wire.rs"),
            sha256: digest,
        }],
    )
    .expect("staged generated source should be attested");
    let missing_span = rustdoc_types::Span {
        filename: external_root.join("missing/wire.rs"),
        begin: (2, 1),
        end: (3, 5),
    };
    let staged_projection = source_span_at_root(&repository_root, &staged_attested, &missing_span)
        .expect("missing external span should resolve to its attested staged basename");
    assert_eq!(staged_projection.path, "generated/actors/staged-wire.rs");

    let in_tree_span = rustdoc_types::Span {
        filename: in_tree.clone(),
        begin: (1, 1),
        end: (1, 24),
    };
    let in_tree_projection = source_span_at_root(&repository_root, &attested, &in_tree_span)
        .expect("attested in-tree generated source should use its logical path");
    assert_eq!(in_tree_projection.path, "generated/actors/in-tree-wire.rs");

    let checkout_span = rustdoc_types::Span {
        filename: PathBuf::from("src/inside.rs"),
        begin: (1, 1),
        end: (1, 18),
    };
    let checkout_projection = source_span_at_root(&repository_root, &attested, &checkout_span)
        .expect("checkout source should take precedence over generated mappings");
    assert_eq!(checkout_projection.path, "src/inside.rs");

    let error = source_span_at_root(&repository_root, &HashMap::new(), &external_span)
        .expect_err("unattested external source must be rejected");
    assert!(error
        .to_string()
        .contains("without an attested generated source"));

    let mut bad_digest = generated.clone();
    bad_digest.sha256 = "0".repeat(64);
    let error = attest_generated_sources(&repository_root, &[bad_digest])
        .expect_err("incorrect generated digest must be rejected");
    assert!(error.to_string().contains("digest does not match"));

    let mut escaping = generated.clone();
    escaping.logical_path = PathBuf::from("../wire.rs");
    let error = attest_generated_sources(&repository_root, &[escaping])
        .expect_err("escaping generated logical path must be rejected");
    assert!(error.to_string().contains("escapes its bundle root"));

    for logical_path in [
        "generated/actors/CON.txt",
        "generated/actors/unicode-ÃƒÆ’Ã…Â½Ãƒâ€šÃ‚Â».rs",
        "generated/actors/bad<name.rs",
        "generated/actors/bad>name.rs",
        "generated/actors/bad\"name.rs",
        "generated/actors/bad|name.rs",
        "generated/actors/bad?name.rs",
        "generated/actors/bad*name.rs",
    ] {
        let mut non_portable = generated.clone();
        non_portable.logical_path = PathBuf::from(logical_path);
        let error = attest_generated_sources(&repository_root, &[non_portable])
            .expect_err("non-portable generated logical path must be rejected");
        assert!(error
            .to_string()
            .contains("contains a non-portable segment"));
    }

    let collision_path = repository_root.join("generated/actors/existing.rs");
    fs::create_dir_all(
        collision_path
            .parent()
            .expect("collision parent should exist"),
    )
    .expect("collision parent should be writable");
    fs::write(&collision_path, b"pub struct Existing;\n")
        .expect("collision source should be writable");
    let mut collision = generated.clone();
    collision.logical_path = PathBuf::from("generated/actors/existing.rs");
    let error = attest_generated_sources(&repository_root, &[collision])
        .expect_err("generated logical path colliding with repository source must be rejected");
    assert!(error
        .to_string()
        .contains("collides with a repository path"));

    let mut duplicate = generated.clone();
    duplicate.logical_path = PathBuf::from("generated/actors/./wire.rs");
    let error = attest_generated_sources(&repository_root, &[generated.clone(), duplicate])
        .expect_err("duplicate physical paths must be rejected");
    assert!(error.to_string().contains("physical path is duplicated"));

    let alias_path = external_root.join("alias/wire.rs");
    fs::create_dir_all(alias_path.parent().unwrap()).unwrap();
    fs::write(&alias_path, bytes).unwrap();
    let mut alias = generated.clone();
    alias.physical_path = alias_path.clone();
    let aliases = attest_generated_sources(&repository_root, &[generated.clone(), alias.clone()])
        .expect("identical independently attested physical aliases share one logical identity");
    assert_eq!(aliases.len(), 2);
    let alias_span = rustdoc_types::Span {
        filename: alias_path.clone(),
        begin: (7, 3),
        end: (8, 9),
    };
    assert_eq!(
        source_span_at_root(&repository_root, &aliases, &alias_span).unwrap(),
        projected
    );
    fs::write(&alias_path, b"pub struct Conflicting;\n").unwrap();
    alias.sha256 = sha256_hex(b"pub struct Conflicting;\n");
    let error = attest_generated_sources(&repository_root, &[generated.clone(), alias])
        .expect_err("one logical identity must never refer to different bytes");
    assert!(error.to_string().contains("conflicting digests"));

    fs::write(&external, b"pub struct Changed;\n").expect("generated source should be mutable");
    let error = attest_generated_sources(&repository_root, &[generated])
        .expect_err("generated source mutation must invalidate attestation");
    assert!(error.to_string().contains("digest does not match"));

    fs::remove_dir_all(&repository_root).expect("repository fixture should be removed");
    fs::remove_dir_all(&external_root).expect("external fixture should be removed");
}

#[cfg(windows)]
#[test]
fn generated_sources_reject_junction_ancestors() {
    let suffix = std::process::id();
    let repository_root = std::env::temp_dir().join(format!("sdk-docs-junction-root-{suffix}"));
    let external_root = std::env::temp_dir().join(format!("sdk-docs-junction-external-{suffix}"));
    let _ = fs::remove_dir_all(&repository_root);
    let _ = fs::remove_dir_all(&external_root);
    fs::create_dir_all(&repository_root).expect("repository fixture should be writable");
    fs::create_dir_all(&external_root).expect("external fixture should be writable");
    let external_file = external_root.join("wire.rs");
    let bytes = b"pub struct Wire;\n";
    fs::write(&external_file, bytes).expect("generated source should be writable");
    let junction = repository_root.join("linked");
    let status = std::process::Command::new("powershell.exe")
        .env("SDK_DOCS_JUNCTION", &junction)
        .env("SDK_DOCS_JUNCTION_TARGET", &external_root)
        .args([
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            "New-Item -ItemType Junction -Path $env:SDK_DOCS_JUNCTION -Target $env:SDK_DOCS_JUNCTION_TARGET -Force | Out-Null",
        ])
        .status()
        .expect("PowerShell should launch");
    if !status.success() {
        let _ = fs::remove_dir_all(&repository_root);
        let _ = fs::remove_dir_all(&external_root);
        panic!("PowerShell should create the junction");
    }
    let error = canonical_repository_root(&junction)
        .expect_err("repository root junctions must be rejected");
    assert!(error.to_string().contains("reparse point or symlink"));
    let source = GeneratedSource {
        physical_path: junction.join("wire.rs"),
        logical_path: PathBuf::from("generated/actors/junction-wire.rs"),
        sha256: sha256_hex(bytes),
    };
    let error = attest_generated_sources(&repository_root, &[source])
        .expect_err("junction ancestors must be rejected");
    assert!(error.to_string().contains("reparse point or symlink"));
    fs::remove_dir(&junction).expect("junction itself should be removable");
    fs::remove_dir_all(&repository_root).expect("repository fixture should be removed");
    fs::remove_dir_all(&external_root).expect("external fixture should be removed");
}

#[test]
fn publication_rejects_navigation_that_does_not_match_sorted_families() {
    let output = std::env::temp_dir().join(format!("sdk-docs-navigation-{}", std::process::id()));
    let _ = fs::remove_dir_all(&output);
    let data = DocsData {
        schema: DATA_SCHEMA_VERSION.into(),
        schema_version: DATA_SCHEMA_VERSION.into(),
        version: "1.0.0".into(),
        channel: Channel::Release,
        source: SourceInfo {
            captured_source: None,
            publication_status: PublicationStatus::Candidate,
            released_packages: Vec::new(),
            revision: "a".repeat(40),
            source_state: "captured-snapshot".into(),
            source_sha256: Some(format!("sha256:{}", "a".repeat(64))),
            input_sha256: "a".repeat(64),
            rustdoc_format_versions: vec![FORMAT_VERSION],
            generator: "sdk-docs/test".into(),
        },
        navigation: Navigation {
            entries: vec![NavigationEntry {
                slug: "zeta".into(),
                title: "Zeta".into(),
                crate_name: "zeta".into(),
            }],
        },
        packages: PackageCatalog {
            entries: Vec::new(),
        },
        search: SearchIndex {
            entries: Vec::new(),
        },
        families: vec![
            Family {
                slug: "zeta".into(),
                title: "Zeta".into(),
                crate_name: "zeta".into(),
                items: Vec::new(),
                guides: Vec::new(),
            },
            Family {
                slug: "alpha".into(),
                title: "Alpha".into(),
                crate_name: "alpha".into(),
                items: Vec::new(),
                guides: Vec::new(),
            },
        ],
    };
    let error = write_bundle(&data, &output, false)
        .expect_err("publication must reject stale or incomplete navigation");
    assert!(error
        .to_string()
        .contains("navigation entries must exactly match"));
    assert!(!output.exists());
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
fn local_impl_member_keeps_a_foreign_receiver_path() {
    let krate: Crate = serde_json::from_value(serde_json::json!({
        "root": 0,
        "crate_version": "1.0.0",
        "includes_private": false,
        "index": {
            "0": {
                "id": 0, "crate_id": 0, "name": "demo", "span": null,
                "visibility": "public", "docs": null, "links": {}, "attrs": [],
                "deprecation": null, "stability": null, "const_stability": null,
                "inner": {"module": {"is_crate": true, "items": [7], "is_stripped": false}}
            },
            "7": {
                "id": 7, "crate_id": 0, "name": null, "span": null,
                "visibility": "default", "docs": null, "links": {}, "attrs": [],
                "deprecation": null, "stability": null, "const_stability": null,
                "inner": {"impl": {
                    "is_unsafe": false,
                    "generics": {"params": [], "where_predicates": []},
                    "provided_trait_methods": [],
                    "trait": {"path": "From", "id": 8, "args": null},
                    "for": {"resolved_path": {"path": "String", "id": 8, "args": null}},
                    "items": [129], "is_negative": false, "is_synthetic": false,
                    "blanket_impl": null
                }}
            },
            "129": {
                "id": 129, "crate_id": 0, "name": "from", "span": null,
                "visibility": "default", "docs": null, "links": {}, "attrs": [],
                "deprecation": null, "stability": null, "const_stability": null,
                "inner": {"function": {
                    "sig": {"inputs": [], "output": null, "is_c_variadic": false},
                    "generics": {"params": [], "where_predicates": []},
                    "header": {"is_const": false, "is_unsafe": false, "is_async": false, "abi": "Rust"},
                    "has_body": true, "default_unstable": null
                }}
            }
        },
        "paths": {},
        "external_crates": {},
        "target": {"triple": "x86_64-pc-windows-msvc", "target_features": []},
        "format_version": 60
    }))
    .expect("local implementation fixture should deserialize");
    let occurrence = public_api::PublicItemSignature {
        id: Id(129),
        parent_id: Some(Id(7)),
        display: "fn from(&ActorId) -> String".into(),
        path: ["alloc", "string", "String", "from"]
            .map(String::from)
            .to_vec(),
    };
    assert!(is_local_impl_member(&krate, &occurrence));
    let mut unparented = occurrence;
    unparented.parent_id = None;
    assert!(!is_local_impl_member(&krate, &unparented));
}

#[test]
fn inherited_local_trait_associated_members_use_typed_membership() {
    let krate: Crate = serde_json::from_value(serde_json::json!({
        "root": 0,
        "crate_version": "1.0.0",
        "includes_private": false,
        "index": {
            "0": {
                "id": 0, "crate_id": 0, "name": "demo", "span": null,
                "visibility": "public", "docs": null, "links": {}, "attrs": [],
                "deprecation": null, "stability": null, "const_stability": null,
                "inner": {"module": {"is_crate": true, "items": [8], "is_stripped": false}}
            },
            "8": {
                "id": 8, "crate_id": 0, "name": "LocalTrait", "span": null,
                "visibility": "public", "docs": null, "links": {}, "attrs": [],
                "deprecation": null, "stability": null, "const_stability": null,
                "inner": {"trait": {
                    "is_auto": false, "is_unsafe": false, "is_dyn_compatible": true,
                    "items": [130, 131, 133],
                    "generics": {"params": [], "where_predicates": []},
                    "bounds": [], "implementations": [7]
                }}
            },
            "7": {
                "id": 7, "crate_id": 0, "name": null, "span": null,
                "visibility": "default", "docs": null, "links": {}, "attrs": [],
                "deprecation": null, "stability": null, "const_stability": null,
                "inner": {"impl": {
                    "is_unsafe": false,
                    "generics": {"params": [], "where_predicates": []},
                    "provided_trait_methods": [],
                    "trait": {"path": "LocalTrait", "id": 8, "args": null},
                    "for": {"resolved_path": {"path": "String", "id": 99, "args": null}},
                    "items": [132], "is_negative": false, "is_synthetic": false,
                    "blanket_impl": null
                }}
            },
            "130": {
                "id": 130, "crate_id": 0, "name": "VALUE", "span": null,
                "visibility": "default", "docs": null, "links": {}, "attrs": [],
                "deprecation": null, "stability": null, "const_stability": null,
                "inner": {"assoc_const": {
                    "type": {"primitive": "usize"}, "value": "0", "default_unstable": null
                }}
            },
            "131": {
                "id": 131, "crate_id": 0, "name": "Output", "span": null,
                "visibility": "default", "docs": null, "links": {}, "attrs": [],
                "deprecation": null, "stability": null, "const_stability": null,
                "inner": {"assoc_type": {
                    "generics": {"params": [], "where_predicates": []},
                    "bounds": [], "type": {"primitive": "usize"}, "default_unstable": null
                }}
            },
            "132": {
                "id": 132, "crate_id": 0, "name": "VALUE", "span": null,
                "visibility": "default", "docs": null, "links": {}, "attrs": [],
                "deprecation": null, "stability": null, "const_stability": null,
                "inner": {"assoc_const": {
                    "type": {"primitive": "usize"}, "value": "1", "default_unstable": null
                }}
            },
            "133": {
                "id": 133, "crate_id": 0, "name": "Other", "span": null,
                "visibility": "default", "docs": null, "links": {}, "attrs": [],
                "deprecation": null, "stability": null, "const_stability": null,
                "inner": {"assoc_const": {
                    "type": {"primitive": "usize"}, "value": "2", "default_unstable": null
                }}
            }
        },
        "paths": {
            "99": {"crate_id": 1, "path": ["alloc", "string", "String"], "kind": "struct"}
        },
        "external_crates": {},
        "target": {"triple": "x86_64-pc-windows-msvc", "target_features": []},
        "format_version": 60
    }))
    .expect("associated-member fixture should deserialize");

    for (id, name) in [(131, "Output"), (133, "Other")] {
        let occurrence = public_api::PublicItemSignature {
            id: Id(id),
            parent_id: Some(Id(7)),
            display: name.to_string(),
            path: vec![
                "alloc".into(),
                "string".into(),
                "String".into(),
                name.into(),
            ],
        };
        assert!(is_local_impl_member(&krate, &occurrence));
    }
    let overridden = public_api::PublicItemSignature {
        id: Id(130),
        parent_id: Some(Id(7)),
        display: "VALUE".into(),
        path: vec![
            "alloc".into(),
            "string".into(),
            "String".into(),
            "VALUE".into(),
        ],
    };
    assert!(!is_local_impl_member(&krate, &overridden));
    let explicit = public_api::PublicItemSignature {
        id: Id(132),
        parent_id: Some(Id(7)),
        display: "VALUE".into(),
        path: vec![
            "alloc".into(),
            "string".into(),
            "String".into(),
            "VALUE".into(),
        ],
    };
    assert!(is_local_impl_member(&krate, &explicit));
}

#[test]
fn build_family_keeps_inherited_local_trait_member_and_source_span() {
    let root = std::env::temp_dir().join(format!("sdk-docs-inherited-impl-{}", std::process::id()));
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(root.join("src")).expect("fixture source directory should be creatable");
    fs::write(
        root.join("src/lib.rs"),
        "pub trait LocalTrait { fn default(&self) {} }\n",
    )
    .expect("fixture source should be writable");
    let rustdoc_path = root.join("demo.json");
    let fixture = serde_json::json!({
        "root": 0,
        "crate_version": "1.0.0",
        "includes_private": false,
        "index": {
            "0": {
                "id": 0, "crate_id": 0, "name": "demo", "span": null,
                "visibility": "public", "docs": null, "links": {}, "attrs": [],
                "deprecation": null, "stability": null, "const_stability": null,
                "inner": {"module": {"is_crate": true, "items": [20], "is_stripped": false}}
            },
            "20": {
                "id": 20, "crate_id": 0, "name": "LocalTrait",
                "span": {"filename": "src/lib.rs", "begin": [1, 1], "end": [1, 44]},
                "visibility": "public", "docs": null, "links": {}, "attrs": [],
                "deprecation": null, "stability": null, "const_stability": null,
                "inner": {"trait": {
                    "is_auto": false, "is_unsafe": false, "is_dyn_compatible": true,
                    "items": [21],
                    "generics": {"params": [], "where_predicates": []},
                    "bounds": [], "implementations": [22]
                }}
            },
            "21": {
                "id": 21, "crate_id": 0, "name": "default",
                "span": {"filename": "src/lib.rs", "begin": [1, 24], "end": [1, 43]},
                "visibility": "default", "docs": "Default behavior.", "links": {}, "attrs": [],
                "deprecation": null, "stability": null, "const_stability": null,
                "inner": {"function": {
                    "sig": {"inputs": [], "output": null, "is_c_variadic": false},
                    "generics": {"params": [], "where_predicates": []},
                    "header": {"is_const": false, "is_unsafe": false, "is_async": false, "abi": "Rust"},
                    "has_body": true, "default_unstable": null
                }}
            },
            "22": {
                "id": 22, "crate_id": 0, "name": null,
                "span": {"filename": "src/lib.rs", "begin": [2, 1], "end": [2, 25]},
                "visibility": "default", "docs": null, "links": {}, "attrs": [],
                "deprecation": null, "stability": null, "const_stability": null,
                "inner": {"impl": {
                    "is_unsafe": false,
                    "generics": {"params": [], "where_predicates": []},
                    "provided_trait_methods": ["default"],
                    "trait": {"path": "LocalTrait", "id": 20, "args": null},
                    "for": {"resolved_path": {"path": "String", "id": 99, "args": null}},
                    "items": [], "is_negative": false, "is_synthetic": false,
                    "blanket_impl": null
                }}
            }
        },
        "paths": {
            "99": {"crate_id": 1, "path": ["alloc", "string", "String"], "kind": "struct"}
        },
        "external_crates": {},
        "target": {"triple": "x86_64-pc-windows-msvc", "target_features": []},
        "format_version": 60
    });
    fs::write(
        &rustdoc_path,
        serde_json::to_vec(&fixture).expect("fixture should serialize"),
    )
    .expect("fixture should be writable");
    let input = BuildInput {
        version: "1.0.0".into(),
        channel: Channel::Release,
        revision: "a".repeat(40),
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
    source::SOURCE_PATH_CHECKS.set(0);
    let data = build_data(&input, &[]).expect("inherited local trait fixture should build");
    assert_eq!(
        source::SOURCE_PATH_CHECKS.get(),
        2,
        "one shared source file is validated at both ends, regardless of aliases"
    );
    let family = &data.families[0];
    let inherited = family
        .items
        .iter()
        .find(|item| item.id == "21" && item.path == "alloc::string::String::default")
        .unwrap_or_else(|| {
            panic!(
                "inherited default method should retain its foreign receiver path: {:?}",
                family
                    .items
                    .iter()
                    .map(|item| (&item.id, &item.path, &item.signature))
                    .collect::<Vec<_>>()
            )
        });
    assert_eq!(
        inherited.source,
        Some(SourceSpan {
            path: "src/lib.rs".into(),
            begin_line: 1,
            begin_column: 24,
            end_line: 1,
            end_column: 43,
        })
    );
    assert!(family
        .items
        .iter()
        .any(|item| item.id == "21" && item.path == "demo::LocalTrait::default"));

    let mut external_trait = fixture;
    external_trait["index"]["20"]["crate_id"] = serde_json::json!(1);
    fs::write(
        &rustdoc_path,
        serde_json::to_vec(&external_trait).expect("negative fixture should serialize"),
    )
    .expect("negative fixture should be writable");
    let error = build_data(&input, &[]).expect_err("foreign inherited member must be rejected");
    let message = error.to_string();
    assert!(message.contains("does not resolve to the crate root"));
    assert!(message.contains("String::default"));
    fs::remove_dir_all(root).expect("fixture directory should be removable");
}

mod projection;
mod publication;
mod scratch;

mod identity;
mod rustdoc_adapter;
